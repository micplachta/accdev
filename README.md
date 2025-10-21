A linux pci driver for a simple hypothetical acceleration device created during
operating systems course. It creates a new character device for every device
detected. User can perform operations using syscalls:

 - open/close - creation and destruction of contexts
 - ioctl - depending on the arguments (ACCELDEV_IOCTL_CREATE_BUFFER,
   ACCELDEV_IOCTL_RUN, ACCELDEV_IOCTL_WAIT) it creates buffers, schedules runs
   on the device or synchronizes with the execution
