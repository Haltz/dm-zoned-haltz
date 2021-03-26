#include "dmz.h"

unsigned long dmz_dev_capacity(struct dmz_target *dmz) {
	blkdev_ioctl(dmz->dev->bdev, 0, BLKGETSIZE, 0);
}

int dmz_flush(struct dmz_target *dmz) {
	return 0;
}