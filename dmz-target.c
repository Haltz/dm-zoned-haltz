#include "dmz.h"

enum { DMZ_BLK_FREE, DMZ_BLK_VALID, DMZ_BLK_INVALID };
enum { DMZ_UNMAPPED, DMZ_MAPPED };

struct dmz_bioctx {
	struct dmz_dev *dev;
	struct bio *bio;
	refcount_t ref;
};

struct dmz_clone_bioctx {
	struct dmz_bioctx *bioctx;
	struct dmz_target *dmz;
	unsigned long logic_block;
	unsigned long evermapped, willmapped;
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
		// kfree(dmz->ddev);
	}
	if (dmz->dev) {
		// kfree(dmz->dev);
	}

	// kfree(dmz);
}

// return zone_id which free block is available
int dmz_first_free_block(struct dmz_target *dmz) {
	struct dmz_metadata *zmd = dmz->zmd;

	for (int i = 0; i < zmd->nr_zones; i++) {
		if ((zmd->zone_start + i)->wp < zmd->zone_nr_blocks) {
			return i;
		}
	}

	return ~0;
}

static void dmz_invalidate_block(struct dmz_target *dmz, sector_t block) {
	struct dmz_metadata *zmd = dmz->zmd;
	unsigned long *bmp = zmd->bitmap_start + (block >> 6);

	pr_info("[dmz_invalidate_block]: offset %llx, before %lx, after %lx\n", block & 0x3f, (unsigned long)*bmp, (unsigned long)(*bmp) & (unsigned long)(~(((u64)1) << (0x3f - (block & 0x3f)))));

	*bmp = (unsigned long)(*bmp) & (unsigned long)(~(((u64)1) << (0x3f - (block & 0x3f))));
}

static void dmz_validate_block(struct dmz_target *dmz, sector_t block) {
	struct dmz_metadata *zmd = dmz->zmd;
	unsigned long *bmp = zmd->bitmap_start + (block >> 6);

	pr_info("[dmz_validate_block]: offset %llx, before %lx, after %lx\n", block & 0x3f, (unsigned long)*bmp, (unsigned long)(*bmp) | (unsigned long)(((u64)1) << (0x3f - (block & 0x3f))));

	*bmp = (unsigned long)(*bmp) | (unsigned long)(((u64)1) << (0x3f - (block & 0x3f)));
}

// map logic to physical. if unmapped, return 0xffff ffff ffff ffff(default reserved blk_id representing invalid)
static u64 dmz_l2p(struct dmz_target *dmz, sector_t logic) {
	struct dmz_map *map_start = dmz->zmd->map_start;

	u64 phy_blkid = (u64)(map_start + logic)->block_id;

	if (phy_blkid >= (dmz->zmd->nr_zones * dmz->zmd->zone_nr_blocks)) {
		pr_info("[dmz-phyical]: invalid physical: %llx\n", phy_blkid);
		phy_blkid = ~0;
	}

	pr_info("[dmz-phyical]: logic: %llx, physical: %llx\n", logic, phy_blkid);

	return phy_blkid;
}

// get physical block status.
// static int dmz_blk_status(struct dmz_target *dmz, unsigned long block) {
// 	struct dmz_metadata *zmd = dmz->zmd;

// 	if (!((~(zmd->map_start + block)->block_id) | 0)) {
// 		return DMZ_BLK_FREE;
// 	}

// 	unsigned long bitmap = bitmap_get_value8(zmd->bitmap_start, block >> 6); // block / 64
// 	int offset = block & (0x3f); // block%64
// 	if (bitmap & (1 << (63 - offset))) {
// 		return DMZ_BLK_VALID;
// 	}

// 	return DMZ_BLK_INVALID;
// }

static void dmz_bio_endio(struct bio *bio, blk_status_t status) {
	if (status != BLK_STS_OK && bio->bi_status == BLK_STS_OK)
		bio->bi_status = status;

	bio_endio(bio);
}

static void dmz_update_map(struct dmz_target *dmz, unsigned long logic, unsigned long physic) {
	struct dmz_map *map_ptr = dmz->zmd->map_start + logic;

	pr_info("[dmz_update_map]: logic: %lx, before update_map %llx\n ", logic, map_ptr->block_id);

	map_ptr->block_id = physic;

	pr_info("[dmz_update_map]: logic: %lx, after update_map %llx\n", logic, map_ptr->block_id);
}

static void dmz_clone_endio(struct bio *clone) {
	blk_status_t status = clone->bi_status;
	struct dmz_clone_bioctx *clone_bioctx = clone->bi_private;
	struct dmz_target *dmz = clone_bioctx->dmz;
	struct dmz_bioctx *bioctx = clone_bioctx->bioctx;

	if (status != BLK_STS_OK) {
		pr_info("[dmz_clone_endio]: bio end unexpectedly.\n");
		return;
	}

	bio_put(clone);
	refcount_dec(&bioctx->ref);

	// if write op succeeds, update mapping. (validate wp and invalidate evermapped if evermapped exists.)
	if (status == BLK_STS_OK && bio_op(bioctx->bio) == REQ_OP_WRITE) {
		// if ever mapped, we need to invalidate it.
		dmz_validate_block(dmz, clone_bioctx->willmapped);

		if (dmz_is_valid_blkid(clone_bioctx->evermapped)) {
			dmz_invalidate_block(dmz, clone_bioctx->evermapped);
		}

		dmz_update_map(dmz, clone_bioctx->logic_block, clone_bioctx->willmapped);

		// pr_info("[dmz_clone_endio]: %d\n", ilog2(dmz->zmd->zone_nr_blocks));
		unsigned int zone_id = clone_bioctx->willmapped >> ilog2(dmz->zmd->zone_nr_blocks);

		// (dmz->zmd->zone_start + zone_id)->wp++;
	}

	pr_info("[dmz_clone_endio]: refcount: %d.\n", refcount_read(&bioctx->ref));

	if (refcount_dec_if_one(&bioctx->ref)) {
		pr_info("[dmz_clone_endio]: refcount_dec_and_test: %d.\n", refcount_read(&bioctx->ref));
		dmz_bio_endio(bioctx->bio, status);
	}
}

static int dmz_submit_bio(struct dmz_target *dmz, struct bio *bio) {
	struct dmz_bioctx *bioctx = dm_per_bio_data(bio, sizeof(struct dmz_bioctx));
	struct dmz_metadata *zmd = dmz->zmd;
	int op = bio_op(bio);

	// find first free block.
	sector_t nr_sectors = bio_sectors(bio), logic_sector = bio->bi_iter.bi_sector;
	sector_t nr_blocks = dmz_sect2blk(nr_sectors), logic_block = dmz_sect2blk(logic_sector);

	// test if align to block size.
	if (nr_sectors & 0x7) {
		pr_info("[dmz-warning]: nr_sectors is not aligned.\n");
	}
	if (logic_sector & 0x7) {
		pr_info("[dmz-warning]: nr_sectors is not aligned.\n");
	}

	bioctx->dev = zmd->dev;

	for (int i = 0; i < nr_blocks; i++) {
		struct bio *cloned_bio = bio_clone_fast(bio, GFP_ATOMIC, &dmz->bio_set);
		if (!cloned_bio) {
			return -ENOMEM;
		}

		bio_set_dev(cloned_bio, zmd->dev->bdev);

		// default 0xffff ffff ffff ffff
		sector_t phy_blkid = dmz_l2p(dmz, logic_block + i);

		struct dmz_clone_bioctx *clone_bioctx = kzalloc(sizeof(struct dmz_clone_bioctx), GFP_ATOMIC);
		clone_bioctx->bioctx = bioctx;
		clone_bioctx->dmz = dmz;
		clone_bioctx->logic_block = logic_block + i;
		clone_bioctx->evermapped = phy_blkid;

		// unmapped
		if (!(~phy_blkid)) {
			if (op == REQ_OP_WRITE) {
				// alloc a free block to write.
				int zone_id = dmz_first_free_block(dmz);
				phy_blkid = (zmd->zone_start + zone_id)->wp++;
				clone_bioctx->willmapped = phy_blkid;
			} else { // read unmapped is invalid.
				// return -EINVAL;

				// if unmapped, return physical block number == logic number
				phy_blkid = logic_block;
			}
		}

		cloned_bio->bi_iter.bi_sector = dmz_start_sector(dmz) + dmz_blk2sect(phy_blkid);
		cloned_bio->bi_iter.bi_size = dmz_blk2sect(1) << SECTOR_SHIFT;
		cloned_bio->bi_end_io = dmz_clone_endio;

		pr_info("[dmz_submit_bio]: logic: %llx, physical: %llx\n", logic_block + i, phy_blkid);

		bio_advance(bio, cloned_bio->bi_iter.bi_size);

		refcount_inc(&bioctx->ref);

		cloned_bio->bi_private = clone_bioctx;

		submit_bio_noacct(cloned_bio);
	}

	return 0;
}

static int dmz_handle_read(struct dmz_target *dmz, struct bio *bio) {
	pr_info("[dmz-map]: R\n");

	dmz_submit_bio(dmz, bio);

	return 0;
}

static int dmz_handle_write(struct dmz_target *dmz, struct bio *bio) {
	pr_info("[dmz-map]: W\n");

	// submit bio.
	dmz_submit_bio(dmz, bio);

	return 0;
}

/* Map bio */
static int dmz_map(struct dm_target *ti, struct bio *bio) {
	pr_info("[dmz]: Map Called.");

	struct dmz_bioctx *bioctx = dm_per_bio_data(bio, sizeof(struct dmz_bioctx));
	struct dmz_target *dmz = ti->private;
	struct dmz_metadata *zmd = dmz->zmd;
	int ret = 0;

	bioctx->dev = zmd->dev;
	bioctx->bio = bio;

	// TODO why refcount_set(&bioctx->ref, 0); has a bug
	refcount_set(&bioctx->ref, 1);

	pr_info("[dmz-info]: bi_sector: %llx\t bi_size: %x\n", bio->bi_iter.bi_sector, bio->bi_iter.bi_size);

	switch (bio_op(bio)) {
	case REQ_OP_READ:
		pr_info("[dmz-map]: read\n");
		ret = dmz_handle_read(dmz, bio);
		break;
	case REQ_OP_WRITE:
		pr_info("[dmz-map]: write\n");
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
		break;
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
	int r;

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

	pr_info("[dmz-info]: zone_nr_sectors: %llx, block_size %x\n", dmz->zmd->zone_nr_sectors, DMZ_BLOCK_SIZE);

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
