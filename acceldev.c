#include <linux/init.h>
#include <linux/module.h>
#include <linux/pci.h>
#include <linux/fs.h>
#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/uaccess.h>
#include <linux/slab.h>
#include <linux/interrupt.h>
#include <linux/mm.h>
#include <linux/anon_inodes.h>
#include <linux/file.h>
#include <linux/fdtable.h>
#include <linux/dma-mapping.h>
#include <linux/wait.h>
#include <linux/mutex.h>
#include <linux/spinlock.h>
#include <linux/list.h>
#include <linux/vmalloc.h>
#include <linux/io.h>
#include "acceldev.h"

#define ACCELDEV_MAX_DEVICES 16

MODULE_LICENSE("GPL");

static int acceldev_major;
static struct class *acceldev_class;
static int acceldev_device_count = 0;
static DEFINE_MUTEX(acceldev_device_count_mutex);

struct acceldev_buffer {
    void *vaddr;
    dma_addr_t dma_addr;
    size_t size;
    int num_pages;
    dma_addr_t page_table_dma;
    u32 *page_table;
    enum acceldev_buffer_type type;
    struct acceldev_context *ctx;
    u32 slot;
    struct mutex lock;
    struct file *ctx_file;
    struct acceldev_device *dev;
};

struct acceldev_context {
    struct acceldev_device *dev;
    u32 context_id;
    struct acceldev_buffer *data_buffers[ACCELDEV_NUM_BUFFERS];
    struct mutex lock;
    wait_queue_head_t user_fence_wait_queue;
    u32 user_fence_counter;
    bool error_state;
};

struct acceldev_pending_cmd {
    struct list_head list;
    u32 cmd[ACCELDEV_DEVICE_CMD_WORDS];
};

struct acceldev_device {
    struct pci_dev *pdev;
    void __iomem *mmio;
    int device_number;
    struct cdev cdev;
    struct device *device;
    
    struct acceldev_context *contexts[ACCELDEV_MAX_CONTEXTS];
    struct mutex contexts_lock;
    
    struct acceldev_context_on_device_config *device_configs;
    dma_addr_t device_configs_dma;
    
    u32 cmd_cnt;
    struct mutex cmd_lock;
    struct list_head pending_commands;
    u32 pending_cmd_cnt;
    struct work_struct cmd_work;
    struct work_struct error_work;
    struct work_struct user_fence_work;
    struct workqueue_struct *cmd_workqueue;
    
    wait_queue_head_t fence_wait_queue;
    u32 fence_cnt;
    u32 fence_last;
};

static const struct pci_device_id acceldev_pci_ids[] = {
    { PCI_DEVICE(ACCELDEV_VENDOR_ID, ACCELDEV_DEVICE_ID) },
    { 0, }
};

static irqreturn_t acceldev_irq_handler(int irq, void *dev_id);

static int acceldev_buffer_release(struct inode *inode, struct file *filp);
static int acceldev_buffer_mmap(struct file *filp, struct vm_area_struct *vma);

static const struct file_operations acceldev_buffer_fops = {
    .owner = THIS_MODULE,
    .release = acceldev_buffer_release,
    .mmap = acceldev_buffer_mmap,
};

static vm_fault_t acceldev_vm_fault(struct vm_fault *vmf);

static const struct vm_operations_struct acceldev_vm_ops = {
    .fault = acceldev_vm_fault,
};

static int acceldev_open(struct inode *inode, struct file *filp);
static int acceldev_release(struct inode *inode, struct file *filp);
static long acceldev_ioctl(struct file *filp, unsigned int cmd, unsigned long arg);

static const struct file_operations acceldev_fops = {
    .owner = THIS_MODULE,
    .open = acceldev_open,
    .release = acceldev_release,
    .unlocked_ioctl = acceldev_ioctl,
};

static int acceldev_pci_probe(struct pci_dev *pdev, const struct pci_device_id *id);
static void acceldev_pci_remove(struct pci_dev *pdev);

static struct pci_driver acceldev_pci_driver = {
    .name = ACCELDEV_NAME,
    .id_table = acceldev_pci_ids,
    .probe = acceldev_pci_probe,
    .remove = acceldev_pci_remove,
    .suspend = NULL,
    .resume = NULL,
};

static void acceldev_write_reg32(struct acceldev_device *dev, u32 offset, u32 value)
{
    iowrite32(value, dev->mmio + offset);
}

static u32 acceldev_read_reg32(struct acceldev_device *dev, u32 offset)
{
    return ioread32(dev->mmio + offset);
}

static void acceldev_send_device_command(struct acceldev_device *dev, u32* cmd)
{
    for (int i = 0; i < ACCELDEV_DEVICE_CMD_WORDS; i++) {
        acceldev_write_reg32(dev, CMD_MANUAL_FEED + i * 4, cmd[i]);
    }
}

static void acceldev_queue_or_send_device_command(struct acceldev_device *dev, u32* cmd)
{
    u32 cmds_left = acceldev_read_reg32(dev, CMD_MANUAL_FREE);

    if (cmds_left > 0 && dev->pending_cmd_cnt == 0) {
        acceldev_send_device_command(dev, cmd);
        return;
    }

    struct acceldev_pending_cmd *pending_cmd;
    pending_cmd = kmalloc(sizeof(*pending_cmd), GFP_ATOMIC);

    if (!pending_cmd) {
        return;
    }

    dev->pending_cmd_cnt++;
    memcpy(pending_cmd->cmd, cmd, 4 * ACCELDEV_DEVICE_CMD_WORDS);

    list_add_tail(&pending_cmd->list, &dev->pending_commands);
}

static void acceldev_submit_device_command(struct acceldev_device *dev, u32* cmd)
{
    u32 fence_cmd[ACCELDEV_DEVICE_CMD_WORDS];
    if ((dev->cmd_cnt + 1) % CMDS_BUFFER_SIZE == 0) {
        fence_cmd[0] = ACCELDEV_DEVICE_CMD_TYPE_FENCE;
        fence_cmd[1] = 0xFFFFFFFF;
        fence_cmd[2] = 0;
        fence_cmd[3] = 0;
        fence_cmd[4] = 0;

        acceldev_queue_or_send_device_command(dev, fence_cmd);
        dev->cmd_cnt++;
        dev->fence_last++;
    }

    acceldev_queue_or_send_device_command(dev, cmd);
    dev->cmd_cnt++;
}

static void acceldev_cmd_work_handler(struct work_struct *work)
{
    struct acceldev_device *dev = container_of(work, struct acceldev_device, cmd_work);
    struct acceldev_pending_cmd *cmd, *tmp;
    u32 cmds_left;

    mutex_lock(&dev->cmd_lock);

    dev->fence_cnt++;
    cmds_left = acceldev_read_reg32(dev, CMD_MANUAL_FREE);
    list_for_each_entry_safe(cmd, tmp, &dev->pending_commands, list) {
        if (cmds_left == 0)
            break;

        cmds_left--;
        dev->pending_cmd_cnt--;
        acceldev_send_device_command(dev, cmd->cmd);

        list_del(&cmd->list);
        kfree(cmd);
    }

    mutex_unlock(&dev->cmd_lock);
    wake_up_all(&dev->fence_wait_queue);
}

static void acceldev_error_work_handler(struct work_struct *work)
{
    struct acceldev_device *dev = container_of(work, struct acceldev_device, error_work);

    mutex_lock(&dev->contexts_lock);
    for (int i = 0; i < ACCELDEV_MAX_CONTEXTS; i++) {
        struct acceldev_context *ctx = dev->contexts[i];
        if (ctx && acceldev_context_on_device_config_is_error(dev->device_configs[i].status)) {
            ctx->error_state = 1;
            wake_up_all(&ctx->user_fence_wait_queue);
        }
    }
    mutex_unlock(&dev->contexts_lock);
}

static void acceldev_user_fence_work_handler(struct work_struct *work)
{
    struct acceldev_device *dev = container_of(work, struct acceldev_device, user_fence_work);

    mutex_lock(&dev->contexts_lock);
    for (int i = 0; i < ACCELDEV_MAX_CONTEXTS; i++) {
        struct acceldev_context *ctx = dev->contexts[i];
        if (ctx) {
            wake_up_all(&ctx->user_fence_wait_queue);
        }
    }
    mutex_unlock(&dev->contexts_lock);
}

static irqreturn_t acceldev_irq_handler(int irq, void *dev_id)
{
    struct acceldev_device *dev = dev_id;
    u32 intr_status;
    
    intr_status = acceldev_read_reg32(dev, ACCELDEV_INTR);
    if (!intr_status)
        return IRQ_NONE;
    
    acceldev_write_reg32(dev, ACCELDEV_INTR, intr_status);
    
    if (intr_status & ACCELDEV_INTR_FENCE_WAIT) {
        queue_work(dev->cmd_workqueue, &dev->cmd_work);
    }
    
    if (intr_status & ACCELDEV_INTR_USER_FENCE_WAIT) {
        queue_work(dev->cmd_workqueue, &dev->user_fence_work);
    }
    
    if (intr_status & (ACCELDEV_INTR_CMD_ERROR | ACCELDEV_INTR_MEM_ERROR | ACCELDEV_INTR_SLOT_ERROR)) {
        queue_work(dev->cmd_workqueue, &dev->error_work);
    }
    
    return IRQ_HANDLED;
}

static void acceldev_buffer_free(struct acceldev_buffer *buf)
{
    struct acceldev_device *dev = buf->dev;
    
    if (!buf)
        return;
    
    if (buf->page_table) {
        dma_free_coherent(&dev->pdev->dev, ACCELDEV_PAGE_SIZE,
                         buf->page_table, buf->page_table_dma);
    }
    
    if (buf->vaddr) {
        dma_free_coherent(&dev->pdev->dev, buf->size, buf->vaddr, buf->dma_addr);
    }

    kfree(buf);
}

static int acceldev_buffer_release(struct inode *inode, struct file *filp)
{
    struct acceldev_buffer *buf = filp->private_data;
    struct acceldev_context *ctx = buf->ctx;
    struct acceldev_device *dev = buf->dev;
    int fence_nr;
    u32 cmd[ACCELDEV_DEVICE_CMD_WORDS];
    
    if (ctx) {
        mutex_lock(&dev->cmd_lock);
        fence_nr = dev->fence_last++;

        cmd[0] = ACCELDEV_DEVICE_CMD_BIND_SLOT_HEADER(ctx->context_id);
        cmd[1] = buf->slot;
        cmd[2] = 0;
        cmd[3] = 0;
        cmd[4] = 0;
        acceldev_submit_device_command(dev, cmd);

        cmd[0] = ACCELDEV_DEVICE_CMD_TYPE_FENCE;
        cmd[1] = 0xFFFFFFFF;
        cmd[2] = 0;
        cmd[3] = 0;
        cmd[4] = 0;
        acceldev_submit_device_command(dev, cmd);

        mutex_unlock(&dev->cmd_lock);
        
        wait_event(dev->fence_wait_queue, 
                  dev->fence_cnt >= fence_nr);

        mutex_lock(&ctx->lock);
        ctx->data_buffers[buf->slot] = NULL;
        mutex_unlock(&ctx->lock);

        if (buf->ctx_file) {
            fput(buf->ctx_file);
            buf->ctx_file = NULL;
        }
    }
    
    acceldev_buffer_free(buf);
    return 0;
}

static int acceldev_buffer_mmap(struct file *filp, struct vm_area_struct *vma)
{
    struct acceldev_buffer *buf = filp->private_data;
    
    vma->vm_ops = &acceldev_vm_ops;
    vma->vm_private_data = buf;
    vm_flags_set(vma, VM_DONTEXPAND | VM_DONTDUMP | VM_IO);
    
    return 0;
}

static vm_fault_t acceldev_vm_fault(struct vm_fault *vmf)
{
    struct acceldev_buffer *buf = vmf->vma->vm_private_data;
    unsigned long offset = vmf->pgoff << PAGE_SHIFT;
    struct page *page;
    
    if (offset >= buf->size)
        return VM_FAULT_SIGBUS;
    
    page = virt_to_page(buf->vaddr + offset);

    get_page(page);
    vmf->page = page;
    
    return 0;
}

static struct acceldev_context *acceldev_context_alloc(struct acceldev_device *dev)
{
    struct acceldev_context *ctx;
    int context_id;
    
    mutex_lock(&dev->contexts_lock);
    
    for (context_id = 0; context_id < ACCELDEV_MAX_CONTEXTS; context_id++) {
        if (!dev->contexts[context_id])
            break;
    }
    if (context_id >= ACCELDEV_MAX_CONTEXTS) {
        mutex_unlock(&dev->contexts_lock);
        return ERR_PTR(-EBUSY);
    }
    
    ctx = kzalloc(sizeof(*ctx), GFP_KERNEL);
    if (!ctx) {
        mutex_unlock(&dev->contexts_lock);
        return ERR_PTR(-ENOMEM);
    }
    
    ctx->dev = dev;
    ctx->context_id = context_id;
    mutex_init(&ctx->lock);
    init_waitqueue_head(&ctx->user_fence_wait_queue);
    ctx->user_fence_counter = 0;
    ctx->error_state = 0;
    dev->contexts[context_id] = ctx;
    
    memset(&dev->device_configs[context_id], 0, sizeof(dev->device_configs[context_id]));
    mutex_unlock(&dev->contexts_lock);
    
    return ctx;
}

static int acceldev_open(struct inode *inode, struct file *filp)
{
    struct acceldev_device *dev;
    struct acceldev_context *ctx;
    
    dev = container_of(inode->i_cdev, struct acceldev_device, cdev);
    
    ctx = acceldev_context_alloc(dev);
    if (IS_ERR(ctx))
        return PTR_ERR(ctx);
    
    filp->private_data = ctx;
    return 0;
}

static void acceldev_context_free(struct acceldev_context *ctx)
{
    struct acceldev_device *dev = ctx->dev;
    
    if (!ctx)
        return;
    
    mutex_lock(&dev->contexts_lock);
    
    memset(&dev->device_configs[ctx->context_id], 0, sizeof(dev->device_configs[ctx->context_id]));
    dev->contexts[ctx->context_id] = NULL;
    
    mutex_unlock(&dev->contexts_lock);
    
    kfree(ctx);
}

static int acceldev_release(struct inode *inode, struct file *filp)
{
    struct acceldev_context *ctx = filp->private_data;
    
    acceldev_context_free(ctx);
    return 0;
}

static struct acceldev_buffer *acceldev_buffer_alloc(struct acceldev_device *dev,
                                                    size_t size,
                                                    enum acceldev_buffer_type type)
{
    struct acceldev_buffer *buf;
    int num_pages, i;
    
    if (size == 0 || size > ACCELDEV_BUFFER_MAX_SIZE)
        return ERR_PTR(-EINVAL);
    
    buf = kzalloc(sizeof(*buf), GFP_KERNEL);
    if (!buf)
        return ERR_PTR(-ENOMEM);
    
    buf->size = size;
    buf->type = type;
    mutex_init(&buf->lock);
    
    num_pages = (size + PAGE_SIZE - 1) >> PAGE_SHIFT;
    buf->num_pages = num_pages;
    
    buf->vaddr = dma_alloc_coherent(&dev->pdev->dev, size, &buf->dma_addr, GFP_KERNEL);
    if (!buf->vaddr) {
        kfree(buf);
        return ERR_PTR(-ENOMEM);
    }
    
    buf->page_table = dma_alloc_coherent(&dev->pdev->dev, ACCELDEV_PAGE_SIZE,
                                        &buf->page_table_dma, GFP_KERNEL);
    if (!buf->page_table) {
        dma_free_coherent(&dev->pdev->dev, size, buf->vaddr, buf->dma_addr);
        kfree(buf);
        return ERR_PTR(-ENOMEM);
    }
    
    for (i = 0; i < num_pages; i++) {
        dma_addr_t page_addr = buf->dma_addr + i * ACCELDEV_PAGE_SIZE;
        buf->page_table[i] = 1 | ((page_addr >> 12) << 4);
    }
    
    for (i = num_pages; i < 1024; i++) {
        buf->page_table[i] = 0;
    }
    
    buf->dev = dev;

    return buf;
}

static long acceldev_ioctl_create_buffer(struct acceldev_context *ctx, unsigned long arg, struct file *filp)
{
    struct acceldev_ioctl_create_buffer req;
    struct acceldev_ioctl_create_buffer_result result;
    struct acceldev_buffer *buf;
    int fd, slot;
    u32 cmd[ACCELDEV_DEVICE_CMD_WORDS] = {0};
    
    if (copy_from_user(&req, (void __user *)arg, sizeof(req)))
        return -EFAULT;
    
    if (req.size <= 0 || req.size > ACCELDEV_BUFFER_MAX_SIZE)
        return -EINVAL;
    
    if (req.type != BUFFER_TYPE_CODE && req.type != BUFFER_TYPE_DATA)
        return -EINVAL;
    
    buf = acceldev_buffer_alloc(ctx->dev, req.size, req.type);
    if (IS_ERR(buf))
        return PTR_ERR(buf);

    fd = anon_inode_getfd("acceldev_buffer", &acceldev_buffer_fops, buf, O_RDWR);
    if (fd < 0) {
        acceldev_buffer_free(buf);
        return fd;
    }
    
    if (req.type == BUFFER_TYPE_DATA) {
        buf->ctx = ctx;
        mutex_lock(&ctx->lock);
        
        for (slot = 0; slot < ACCELDEV_NUM_BUFFERS; slot++) {
            if (!ctx->data_buffers[slot])
                break;
        }
        
        if (slot >= ACCELDEV_NUM_BUFFERS) {
            mutex_unlock(&ctx->lock);
            put_unused_fd(fd);
            acceldev_buffer_free(buf);
            return -EBUSY;
        }

        buf->slot = slot;
        ctx->data_buffers[slot] = buf;
        
        ctx->dev->device_configs[ctx->context_id].buffers_slots_config_ptr[slot] = buf->page_table_dma;

        cmd[0] = ACCELDEV_DEVICE_CMD_BIND_SLOT_HEADER(ctx->context_id);
        cmd[1] = slot;
        cmd[2] = (u32)(buf->page_table_dma & 0xFFFFFFFF);
        cmd[3] = (u32)(buf->page_table_dma >> 32);
        cmd[4] = 0;
        
        result.buffer_slot = slot;
        buf->ctx_file = get_file(filp);

        mutex_unlock(&ctx->lock);
    }

    if (req.result && copy_to_user(req.result, &result, sizeof(result))) {
        if (req.type == BUFFER_TYPE_DATA) {
            mutex_lock(&ctx->lock);
            ctx->data_buffers[slot] = NULL;
            mutex_unlock(&ctx->lock);
            
            if (buf->ctx_file) {
                fput(buf->ctx_file);
                buf->ctx_file = NULL;
            }
        }
        
        put_unused_fd(fd);
        acceldev_buffer_free(buf);
        return -EFAULT;
    }

    if (req.type == BUFFER_TYPE_DATA) {
        mutex_lock(&ctx->dev->cmd_lock);
        acceldev_submit_device_command(ctx->dev, cmd);
        mutex_unlock(&ctx->dev->cmd_lock);
    }
    
    return fd;
}

static long acceldev_ioctl_run(struct acceldev_context *ctx, unsigned long arg)
{
    struct acceldev_ioctl_run req;
    struct file *file;
    struct acceldev_buffer *code_buf;
    u32 cmd[ACCELDEV_DEVICE_CMD_WORDS] = {0};
    
    if (copy_from_user(&req, (void __user *)arg, sizeof(req)))
        return -EFAULT;
    
    if (ctx->error_state)
        return -EIO;
    
    if (req.addr & 3 || req.size & 3)
        return -EINVAL;
    
    file = fget(req.cfd);
    if (!file)
        return -EBADF;
    
    if (file->f_op != &acceldev_buffer_fops) {
        fput(file);
        return -EINVAL;
    }
    
    code_buf = file->private_data;
    if (code_buf->type != BUFFER_TYPE_CODE) {
        fput(file);
        return -EINVAL;
    }
    
    cmd[0] = ACCELDEV_DEVICE_CMD_RUN_HEADER(ctx->context_id);
    cmd[1] = (u32)(code_buf->page_table_dma & 0xFFFFFFFF);
    cmd[2] = (u32)(code_buf->page_table_dma >> 32);
    cmd[3] = req.addr;
    cmd[4] = req.size;
    
    mutex_lock(&ctx->dev->cmd_lock);
    acceldev_submit_device_command(ctx->dev, cmd);
    mutex_unlock(&ctx->dev->cmd_lock);
    
    fput(file);
    return 0;
}

static long acceldev_ioctl_wait(struct acceldev_context *ctx, unsigned long arg)
{
    struct acceldev_ioctl_wait req;
    int ret;
    
    if (copy_from_user(&req, (void __user *)arg, sizeof(req)))
        return -EFAULT;
    
    ret = wait_event_interruptible(ctx->user_fence_wait_queue, ctx->error_state ||
                                  ctx->dev->device_configs[ctx->context_id].fence_counter >= req.fence_wait);
    
    if (ret) {
        return -EINTR;
    }
    
    if (ctx->error_state) {
        return -EIO;
    }
    
    return 0;
}

static long acceldev_ioctl(struct file *filp, unsigned int cmd, unsigned long arg)
{
    struct acceldev_context *ctx = filp->private_data;
    
    switch (cmd) {
    case ACCELDEV_IOCTL_CREATE_BUFFER:
        return acceldev_ioctl_create_buffer(ctx, arg, filp);
    case ACCELDEV_IOCTL_RUN:
        return acceldev_ioctl_run(ctx, arg);
    case ACCELDEV_IOCTL_WAIT:
        return acceldev_ioctl_wait(ctx, arg);
    default:
        return -ENOTTY;
    }
}

static int acceldev_device_start(struct acceldev_device *dev)
{
    acceldev_write_reg32(dev, ACCELDEV_INTR, 0xFFFFFFFF);
    acceldev_write_reg32(dev, ACCELDEV_INTR_ENABLE, 
                        ACCELDEV_INTR_FENCE_WAIT |
                        ACCELDEV_INTR_FEED_ERROR |
                        ACCELDEV_INTR_CMD_ERROR |
                        ACCELDEV_INTR_MEM_ERROR |
                        ACCELDEV_INTR_SLOT_ERROR |
                        ACCELDEV_INTR_USER_FENCE_WAIT);
    
    acceldev_write_reg32(dev, ACCELDEV_CONTEXTS_CONFIGS, (u32)(dev->device_configs_dma & 0xFFFFFFFF));
    acceldev_write_reg32(dev, ACCELDEV_CONTEXTS_CONFIGS + 4, (u32)(dev->device_configs_dma >> 32));
    
    acceldev_write_reg32(dev, ACCELDEV_ENABLE, 1);
    acceldev_write_reg32(dev, ACCELDEV_CMD_FENCE_WAIT, 0xFFFFFFFF);

    return 0;
}

static void acceldev_device_stop(struct acceldev_device *dev)
{
    acceldev_write_reg32(dev, ACCELDEV_ENABLE, 0);
    acceldev_write_reg32(dev, ACCELDEV_INTR_ENABLE, 0);
}

static int acceldev_pci_probe(struct pci_dev *pdev, const struct pci_device_id *id)
{
    struct acceldev_device *dev;
    int ret, device_number;
    dev_t devt;
    
    dev = kzalloc(sizeof(*dev), GFP_KERNEL);
    if (!dev)
        return -ENOMEM;
    
    dev->pdev = pdev;
    pci_set_drvdata(pdev, dev);

    mutex_lock(&acceldev_device_count_mutex);
    device_number = acceldev_device_count++;
    dev->device_number = device_number;
    
    mutex_init(&dev->contexts_lock);
    mutex_init(&dev->cmd_lock);
    init_waitqueue_head(&dev->fence_wait_queue);
    dev->fence_last = 1;
    dev->fence_cnt = 0;
    INIT_LIST_HEAD(&dev->pending_commands);
    dev->pending_cmd_cnt = 0;
    dev->cmd_cnt = 0;
    INIT_WORK(&dev->cmd_work, acceldev_cmd_work_handler);
    INIT_WORK(&dev->error_work, acceldev_error_work_handler);
    INIT_WORK(&dev->user_fence_work, acceldev_user_fence_work_handler);
    
    ret = pci_enable_device(pdev);
    if (ret) {
        goto err_free_dev;
    }
    
    ret = dma_set_mask_and_coherent(&pdev->dev, DMA_BIT_MASK(64));
    if (ret) {
        goto err_disable_device;
    }
    
    ret = pci_request_regions(pdev, ACCELDEV_NAME);
    if (ret) {
        goto err_disable_device;
    }
    
    dev->mmio = pci_iomap(pdev, 0, ACCELDEV_BAR_SIZE);
    if (!dev->mmio) {
        ret = -ENOMEM;
        goto err_release_regions;
    }
    
    dev->device_configs = dma_alloc_coherent(&pdev->dev,
                                           ACCELDEV_MAX_CONTEXTS * sizeof(struct acceldev_context_on_device_config),
                                           &dev->device_configs_dma, GFP_KERNEL);
    if (!dev->device_configs) {
        ret = -ENOMEM;
        goto err_unmap_mmio;
    }
    
    memset(dev->device_configs, 0, ACCELDEV_MAX_CONTEXTS * sizeof(struct acceldev_context_on_device_config));
    
    pci_set_master(pdev);
    ret = request_irq(pdev->irq, acceldev_irq_handler, IRQF_SHARED, ACCELDEV_NAME, dev);
    if (ret) {
        goto err_free_configs;
    }
    
    devt = MKDEV(acceldev_major, device_number);
    cdev_init(&dev->cdev, &acceldev_fops);
    dev->cdev.owner = THIS_MODULE;
    
    ret = cdev_add(&dev->cdev, devt, 1);
    if (ret) {
        goto err_free_irq;
    }
    
    dev->device = device_create(acceldev_class, &pdev->dev, devt, NULL, "acceldev%d", device_number);
    if (IS_ERR(dev->device)) {
        ret = PTR_ERR(dev->device);
        goto err_del_cdev;
    }
    
    ret = acceldev_device_start(dev);
    if (ret) {
        goto err_destroy_device;
    }

    dev->cmd_workqueue = create_singlethread_workqueue("acceldev_cmd");
    if (!dev->cmd_workqueue) {
        ret = -ENOMEM;
        goto err_stop_dev;
    }
    
    mutex_unlock(&acceldev_device_count_mutex);

    return 0;
    
err_stop_dev:
    acceldev_device_stop(dev);
err_destroy_device:
    device_destroy(acceldev_class, devt);
err_del_cdev:
    cdev_del(&dev->cdev);
err_free_irq:
    free_irq(pdev->irq, dev);
err_free_configs:
    dma_free_coherent(&pdev->dev, ACCELDEV_MAX_CONTEXTS * sizeof(struct acceldev_context_on_device_config),
                     dev->device_configs, dev->device_configs_dma);
err_unmap_mmio:
    pci_iounmap(pdev, dev->mmio);
err_release_regions:
    pci_release_regions(pdev);
err_disable_device:
    pci_disable_device(pdev);
err_free_dev:
    kfree(dev);

    if (ret)
        acceldev_device_count--;
    mutex_unlock(&acceldev_device_count_mutex);

    return ret;
}

static void acceldev_pci_remove(struct pci_dev *pdev)
{
    struct acceldev_device *dev = pci_get_drvdata(pdev);
    dev_t devt;
    int i;
    
    if (!dev)
        return;
    
    acceldev_device_stop(dev);
    free_irq(pdev->irq, dev);

    flush_workqueue(dev->cmd_workqueue);
    destroy_workqueue(dev->cmd_workqueue);
    
    mutex_lock(&dev->contexts_lock);
    for (i = 0; i < ACCELDEV_MAX_CONTEXTS; i++) {
        if (dev->contexts[i]) {
            acceldev_context_free(dev->contexts[i]);
        }
    }
    mutex_unlock(&dev->contexts_lock);
    
    devt = MKDEV(acceldev_major, dev->device_number);
    device_destroy(acceldev_class, devt);
    cdev_del(&dev->cdev);
        
    dma_free_coherent(&pdev->dev, ACCELDEV_MAX_CONTEXTS * sizeof(struct acceldev_context_on_device_config),
                     dev->device_configs, dev->device_configs_dma);
    
    pci_iounmap(pdev, dev->mmio);
    
    pci_release_regions(pdev);
    pci_disable_device(pdev);
    
    kfree(dev);

    mutex_lock(&acceldev_device_count_mutex);
    acceldev_device_count--;
    mutex_unlock(&acceldev_device_count_mutex);
}

static int __init acceldev_init(void)
{
    int ret;
    dev_t devt;
    
    ret = alloc_chrdev_region(&devt, 0, ACCELDEV_MAX_DEVICES, ACCELDEV_NAME);
    if (ret) {
        return ret;
    }
    acceldev_major = MAJOR(devt);
    
    acceldev_class = class_create(ACCELDEV_NAME);
    if (IS_ERR(acceldev_class)) {
        ret = PTR_ERR(acceldev_class);
        goto err_unregister_chrdev;
    }
    
    ret = pci_register_driver(&acceldev_pci_driver);
    if (ret) {
        goto err_destroy_class;
    }
    
    return 0;
    
err_destroy_class:
    class_destroy(acceldev_class);
err_unregister_chrdev:
    unregister_chrdev_region(devt, ACCELDEV_MAX_DEVICES);
    return ret;
}

static void __exit acceldev_exit(void)
{
    dev_t devt = MKDEV(acceldev_major, 0);
    
    pci_unregister_driver(&acceldev_pci_driver);
    class_destroy(acceldev_class);
    unregister_chrdev_region(devt, ACCELDEV_MAX_DEVICES);    
}

module_init(acceldev_init);
module_exit(acceldev_exit);
