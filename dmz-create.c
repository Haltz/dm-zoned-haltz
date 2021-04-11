#include "dmz.h"

#define DEVICE_NAME "dm"
#define DEVICE_PATH "/dev/nullb0"

static unsigned int major = 255;

struct dmz_target *dmz_tgt;

static blk_qc_t dmz_bops_submit_bio(struct bio *bio) {
	return dmz_map(dmz_tgt, bio);
}

static int dmz_open(struct block_device *bdev, fmode_t mode) {
	refcount_inc(&dmz_tgt->ref);
	return 0;
}

static void dmz_close(struct gendisk *disk, fmode_t mode) {
	refcount_dec(&dmz_tgt->ref);
	return;
}

static const struct block_device_operations dmz_blk_dops = { .submit_bio = dmz_bops_submit_bio, .open = dmz_open, .release = dmz_close, .owner = THIS_MODULE };

static int queue_rw_rq(struct dmz_target *dmz, struct request *req) {
	struct bio *next = req->bio;

	int ret = 0;

	while (next) {
		if (dmz_map(dmz, next))
			ret = -1;
		next = next->bi_next;
	}

	return ret;
}

static blk_status_t dmz_queue_rq(struct blk_mq_hw_ctx *hctx, const struct blk_mq_queue_data *bd) {
	struct dmz_target *dmz = hctx->queue->queuedata;
	struct request *req = bd->rq;
	int ret = BLK_STS_OK;

	blk_mq_start_request(req);

	switch (req_op(req)) {
	/* operations with no lentgth/offset arguments */
	case REQ_OP_FLUSH:
		pr_err("NOT NOW FLUSH.\n");
		break;
	case REQ_OP_READ:
	case REQ_OP_WRITE:
		ret = queue_rw_rq(dmz, req);
		break;
	case REQ_OP_DISCARD:
	case REQ_OP_WRITE_ZEROES:
		ret = queue_rw_rq(dmz, req);
		break;
	default:
		ret = BLK_STS_NOTSUPP;
	}

	blk_mq_end_request(req, ret);

	return ret;
}

static const struct blk_mq_ops dmz_mq_ops = { .queue_rq = dmz_queue_rq };

int next_free_minor(void) {
	return 0;
}

struct dmz_dev *dev_create(struct dmz_target *dmz) {
	struct dmz_dev *dev = kzalloc(sizeof(struct dmz_dev), GFP_KERNEL);
	if (!dev) {
		goto dev;
	}

	int r = 0, minor = 0;

	if (minor < 0) {
		goto dev;
	}

	struct blk_mq_tag_set *set = &dev->set;
	set->ops = &dmz_mq_ops;
	set->nr_hw_queues = 1;
	set->cmd_size = 64;
	set->flags = BLK_MQ_F_SHOULD_MERGE;
	set->queue_depth = 64;
	set->nr_maps = 1;
	set->numa_node = NUMA_NO_NODE;

	r = blk_mq_alloc_tag_set(set);
	if (r) {
		goto dev;
	}

	dev->queue = blk_mq_init_queue_data(set, dmz);

	if (IS_ERR(dev->queue)) {
		goto set;
	}

	blk_queue_max_hw_sectors(dev->queue, BLK_DEF_MAX_SECTORS);

	dev->disk = alloc_disk(1);
	if (!dev->disk)
		goto allocdisk;

	dev->disk->major = major;
	dev->disk->first_minor = minor;
	dev->disk->fops = &dmz_blk_dops;
	dev->disk->queue = dev->queue;
	dev->disk->private_data = dev;
	sprintf(dev->disk->disk_name, "dm-%d", minor);
	set_capacity(dev->disk, i_size_read(dmz->target_bdev->bd_inode) >> SECTOR_SHIFT);

	add_disk(dev->disk);
	format_dev_t(dev->major_minor_id, MKDEV(major, minor));

	dev->bdev = bdget_disk(dev->disk, 0);
	bdput(dev->bdev);

	dev->capacity = i_size_read(dmz->target_bdev->bd_inode) >> SECTOR_SHIFT;
	sprintf(dev->name, "dm-%d", minor);
	// bdevname(dev->bdev, dev->name);

	dev->nr_zones = blkdev_nr_zones(dmz->target_bdev->bd_disk);
	dev->nr_zone_sectors = blk_queue_zone_sectors(dmz->target_bdev->bd_disk->queue);

	return dev;

allocdisk:
	if (dev->queue) {
		blk_cleanup_queue(dev->queue);
		dev->queue = NULL;
	}

	if (dev->disk)
		put_disk(dev->disk);

set:
	blk_mq_free_tag_set(set);
dev:
	if (dev)
		kfree(dev);
	return NULL;
}

void dev_destroy(struct dmz_target *dmz) {
	struct dmz_dev *dev = dmz->dev;
	if (!dev)
		return;

	if (dev->queue)
		blk_cleanup_queue(dev->queue);

	if (dev->disk)
		put_disk(dev->disk);

	blk_mq_free_tag_set(&dev->set);

	kfree(dev);
}

/* Initilize device mapper */
int dmz_ctr(struct dmz_target *dmz) {
	int ret;

	dmz->target_bdev = blkdev_get_by_path(DEVICE_PATH, FMODE_READ | FMODE_WRITE, "dm-zoned-haltz");
	if (IS_ERR(dmz->target_bdev)) {
		goto target_bdev;
	}

	dmz->dev = dev_create(dmz);
	if (!dmz->dev) {
		goto dev_create;
	}

	ret = bioset_init(&dmz->bio_set, DMZ_MIN_BIOS, 0, 0);
	if (ret) {
		goto bioset;
	}

	ret = dmz_ctr_metadata(dmz);
	if (ret) {
		goto ctr_meta;
	}

	return 0;

ctr_meta:
	bioset_exit(&dmz->bio_set);
bioset:
	dev_destroy(dmz);
dev_create:
	blkdev_put(dmz->target_bdev, FMODE_READ | FMODE_WRITE);
target_bdev:
	return -1;
}

void dmz_dtr(struct dmz_target *dmz) {
	if (!dmz) {
		return;
	}

	dmz_dtr_metadata(dmz->zmd);

	bioset_exit(&dmz->bio_set);

	dev_destroy(dmz);

	blkdev_put(dmz->target_bdev, FMODE_READ | FMODE_WRITE);
}

static int __init dmz_init(void) {
	int r;

	dmz_tgt = kzalloc(sizeof(struct dmz_target), GFP_KERNEL);
	if (!dmz_tgt) {
		goto dmz_alloc;
	}

	refcount_set(&dmz_tgt->ref, 1);

	r = dmz_ctr(dmz_tgt);
	if (r) {
		goto dmz_ctr;
	}

	r = register_blkdev(major, DEVICE_NAME);
	if (r < 0)
		goto dmz_register;

	return 0;

dmz_register:
	dmz_dtr(dmz_tgt);
dmz_ctr:
	kfree(dmz_tgt);
dmz_alloc:
	return r;
}

static void __exit dmz_exit(void) {
	if (dmz_tgt)
		return;

	dmz_dtr(dmz_tgt);

	kfree(dmz_tgt);

	unregister_blkdev(major, DEVICE_NAME);
}

module_init(dmz_init);
module_exit(dmz_exit);

MODULE_DESCRIPTION("DM-ZONED Device Driver.");
MODULE_LICENSE("GPL");