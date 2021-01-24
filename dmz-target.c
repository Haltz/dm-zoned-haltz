#include "dmz.h"

struct dmz_bioctx {
	struct dmz_dev *dev;
	struct bio *bio;
	refcount_t ref;
};

/* Get Zoned Device Infomation */
static int dmz_get_zoned_device(struct dm_target *ti, char *path, struct dmz_dev *dev) {
	struct dm_dev *ddev;
	struct dmz_target *dmz = ti->private;

	int ret = dm_get_device(ti, path, dm_table_get_mode(ti->table), &ddev);
	if (ret) {
		ti->error = "Get target device failed";
		return ret;
	}

	dmz->ddev = ddev;
	dev->bdev = ddev->bdev;
	dev->capacity = i_size_read(dev->bdev->bd_inode) >> SECTOR_SHIFT;
	bdevname(dev->bdev, dev->name);

	return 0;
}

/*
 * Cleanup zoned device information.
 */
static void dmz_put_zoned_device(struct dm_target *ti) {
	struct dmz_target *dmz = ti->private;

	if (dmz->ddev) {
		dm_put_device(ti, dmz->ddev);
		dmz->ddev = NULL;
	}
}

// init zone info
static int dmz_fixup_device(struct dm_target *ti) {
	struct dmz_target *dmz = ti->private;
	struct dmz_dev *zoned_dev;
	struct request_queue *q;

	zoned_dev = dmz->dev;
	q = bdev_get_queue(zoned_dev->bdev);
	zoned_dev->nr_zones = blkdev_nr_zones(zoned_dev->bdev->bd_disk);
	zoned_dev->nr_zone_sectors = blk_queue_zone_sectors(q);

	return 0;
}

/* Initilize device mapper */
static int dmz_ctr(struct dm_target *ti, unsigned int argc, char **argv) {
	pr_info("[dmz]: ctr Called.\n");
	if (argc > 1 || argc == 0) {
		ti->error = "Invalid argument count";
		pr_err("ZBD number is invalid. Only 1 supported.");
		return -EINVAL;
	}

	int ret;

	char *path = argv[0];

	struct dmz_target *dmz = kzalloc(sizeof(struct dmz_target), GFP_KERNEL);
	if (!dmz) {
		ti->error = "Unable to allocate the zoned device descriptors";
		return -ENOMEM;
	}

	ret = bioset_init(&dmz->bio_set, DMZ_MIN_BIOS, 0, 0);
	if (ret) {
		ti->error = "Create BIO set failed";
		return -ENOMEM;
	}

	dmz->dev = kzalloc(sizeof(struct dmz_dev), GFP_KERNEL);
	if (!dmz->dev) {
		ti->error = "Unable to allocate the zoned device descriptors";
		return -ENOMEM;
	}

	dmz->ddev = kzalloc(sizeof(struct dm_dev *), GFP_KERNEL);
	if (!dmz->ddev) {
		ti->error = "Unable to allocate the zoned device descriptors";
		return -ENOMEM;
	}

	ti->private = dmz;

	strcpy(dmz->dev->name, path);
	ret = dmz_get_zoned_device(ti, argv[0], dmz->dev);
	ret = dmz_fixup_device(ti);
	ret = dmz_ctr_metadata(dmz);

	// dmz_ctr_reclaim();

	/* Set target (no write same support) */
	ti->max_io_len = dmz->zmd->zone_nr_sectors;
	ti->num_flush_bios = 1;
	ti->num_discard_bios = 1;
	ti->num_write_zeroes_bios = 1;
	ti->per_io_data_size = sizeof(struct dmz_bioctx);
	ti->flush_supported = true;
	ti->discards_supported = true;

	return 0;
}

static void dmz_dtr(struct dm_target *ti) {
	pr_info("[dmz]: dtr Called.");
	struct dmz_target *dmz = ti->private;
	dmz_put_zoned_device(ti);

	if (!dmz) {
		return;
	}

	dmz_dtr_metadata(dmz->zmd);
	if (dmz->ddev) {
		kfree(dmz->ddev);
	}
	if (dmz->dev) {
		kfree(dmz->dev);
	}

	kfree(dmz);
}

sector_t dmz_first_free_block(struct dmz_metadata *zmd) {
	return 0;
}

int dmz_invalidate_blocks(struct dmz_metadata *zmd, sector_t start, sector_t len) {
	return 0;
}

static inline void dmz_bio_endio(struct bio *bio, blk_status_t status) {
	if (status != BLK_STS_OK && bio->bi_status == BLK_STS_OK)
		bio->bi_status = status;

	bio_endio(bio);
}

static void dmz_clone_endio(struct bio *clone) {
	blk_status_t status = clone->bi_status;
	struct bio *bio = clone->bi_private;

	bio_put(clone);
	dmz_bio_endio(bio, status);
}

static int dmz_handle_read(struct dmz_target *dmz, struct bio *bio) {
	pr_info("[dmz-map]: R\n");

	struct dmz_metadata *zmd = dmz->zmd;

	// find first free block.
	sector_t nr_sectors = bio_sectors(bio), logic_addr_start_sector = bio->bi_iter.bi_sector;
	sector_t nr_blocks = dmz_sect2blk(nr_sectors), logic_addr_start_block = dmz_sect2blk(logic_addr_start_sector);

	struct bio *cloned_bio = bio_clone_fast(bio, GFP_ATOMIC, &dmz->bio_set);
	if (!cloned_bio) {
		return -ENOMEM;
	}

	cloned_bio->bi_private = bio;
	bio_set_dev(cloned_bio, zmd->dev->bdev);
	cloned_bio->bi_iter.bi_sector = dmz_start_sector(dmz) + logic_addr_start_sector;
	cloned_bio->bi_iter.bi_size = dmz_blk2sect(nr_blocks) << SECTOR_SHIFT;
	cloned_bio->bi_end_io = dmz_clone_endio;

	pr_info("[dmz-read]: nr_blocks: 0x%llx, useable_start: 0x%llx, read_from_block: 0x%llx, read_size: 0x%llx\n", zmd->nr_blocks, zmd->useable_start, dmz_sect2blk(dmz_start_sector(dmz) + logic_addr_start_sector), nr_blocks);

	// submit bio.
	submit_bio_noacct(cloned_bio);

	return 0;
}

static int dmz_handle_write(struct dmz_target *dmz, struct bio *bio) {
	pr_info("[dmz-map]: W\n");

	// submit bio.

	return 0;
}

/* Map bio */
static int dmz_map(struct dm_target *ti, struct bio *bio) {
	pr_info("[dmz]: Map Called.");

	struct dmz_target *dmz = ti->private;
	struct dmz_metadata *zmd = dmz->zmd;
	int ret = 0;

	pr_info("[dmz-info]: bi_sector: %llx\t bi_size: %x\n", bio->bi_iter.bi_sector, bio->bi_iter.bi_size);

	switch (bio_op(bio)) {
	case REQ_OP_READ:
		ret = dmz_handle_read(dmz, bio);
		break;
	case REQ_OP_WRITE:
		ret = dmz_handle_write(dmz, bio);
		break;
	case REQ_OP_DISCARD:
	case REQ_OP_WRITE_ZEROES:
		pr_info("[dmz-map]: discard & write zeros\n");
		// ret = dmz_handle_discard(dmz, bio);
		break;
	default:
		pr_info("[dmz-map]: default\n");
		// DMERR("(%s): Unsupported BIO operation 0x%x", dmz_metadata_label(dmz->metadata), bio_op(bio));
		ret = -EIO;
	}

	return DM_MAPIO_SUBMITTED;
}

static void dmz_suspend(struct dm_target *ti) {
	pr_info("[dmz]: Suspended\n");
}

static void dmz_resume(struct dm_target *ti) {
	pr_info("[dmz]: Resumed\n");
}

static int dmz_iterate_devices(struct dm_target *ti, iterate_devices_callout_fn fn, void *data) {
	pr_info("[dmz]: Iterate Called.\n");
	struct dmz_target *dmz = ti->private;
	unsigned int zone_nr_sectors = dmz->zmd->zone_nr_sectors;
	sector_t capacity;
	int i, r;

	capacity = dmz->dev->capacity & ~(zone_nr_sectors - 1);
	r = fn(ti, dmz->ddev, 0, capacity, data);

	return r;
}

static void dmz_status(struct dm_target *ti, status_type_t type, unsigned int status_flags, char *result, unsigned int maxlen) {
	pr_info("Status Called.\n");
}

/*
 * Setup target request queue limits.
 */
static void dmz_io_hints(struct dm_target *ti, struct queue_limits *limits) {
	pr_info("[dmz]: IO_hints Called.\n");
	struct dmz_target *dmz = ti->private;
	unsigned int chunk_sectors = dmz->zmd->zone_nr_sectors;

	pr_info("[dmz-info]: zone_nr_sectors: %llx, block_size %llx\n", dmz->zmd->zone_nr_sectors, DMZ_BLOCK_SIZE);

	limits->logical_block_size = DMZ_BLOCK_SIZE;
	limits->physical_block_size = DMZ_BLOCK_SIZE;

	blk_limits_io_min(limits, DMZ_BLOCK_SIZE);
	blk_limits_io_opt(limits, DMZ_BLOCK_SIZE);

	limits->discard_alignment = DMZ_BLOCK_SIZE;
	limits->discard_granularity = DMZ_BLOCK_SIZE;
	limits->max_discard_sectors = chunk_sectors;
	limits->max_hw_discard_sectors = chunk_sectors;
	limits->max_write_zeroes_sectors = chunk_sectors;

	/* FS hint to try to align to the device zone size */
	limits->chunk_sectors = chunk_sectors;
	limits->max_sectors = chunk_sectors;

	/* We are exposing a drive-managed zoned block device */
	limits->zoned = BLK_ZONED_NONE;
}

static int dmz_prepare_ioctl(struct dm_target *ti, struct block_device **bdev) {
	pr_info("[dmz]: Ioctl Called\n");
	struct dmz_target *dmz = ti->private;
	struct dmz_dev *dev = dmz->dev;

	*bdev = dev->bdev;

	return 0;
}

static int dmz_message(struct dm_target *ti, unsigned int argc, char **argv, char *result, unsigned int maxlen) {
	pr_info("[dmz]: Message Called.\n");
	return 0;
}

static struct target_type dmz_type = {
	.name = "zoned",
	.version = { 2, 0, 0 },
	.features = DM_TARGET_SINGLETON | DM_TARGET_ZONED_HM,
	.module = THIS_MODULE,
	.ctr = dmz_ctr,
	.dtr = dmz_dtr,
	.map = dmz_map,
	.io_hints = dmz_io_hints,
	.prepare_ioctl = dmz_prepare_ioctl,
	.postsuspend = dmz_suspend,
	.resume = dmz_resume,
	.iterate_devices = dmz_iterate_devices,
	.status = dmz_status,
	.message = dmz_message,
};

static int __init dmz_init(void) {
	return dm_register_target(&dmz_type);
}

static void __exit dmz_exit(void) {
	dm_unregister_target(&dmz_type);
}

module_init(dmz_init);
module_exit(dmz_exit);

MODULE_DESCRIPTION(DM_NAME " target for zoned block devices");
MODULE_AUTHOR("Haltz <huangdong.mu@outlook.com>");
MODULE_LICENSE("GPL");
