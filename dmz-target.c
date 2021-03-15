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
	unsigned long lba;
	unsigned long old_pba, new_pba;
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

/* 
* check sanity & correctness of metadata
*/
static void dmz_check_meta(struct dmz_target *dmz) {
	struct dmz_metadata *zmd = dmz->zmd;
}

/* Initilize device mapper */
static int dmz_ctr(struct dm_target *ti, unsigned int argc, char **argv) {
	// pr_info("[dmz]: ctr Called.\n");
	if (argc > 1 || argc == 0) {
		ti->error = "Invalid argument count";
		pr_err("ZBD number is invalid. Only 1 supported.");
		return -EINVAL;
	}

	int ret;

	char *path = argv[0];

	struct dmz_target *dmz = kzalloc(sizeof(struct dmz_target), GFP_KERNEL);
	if (!dmz) {
		ti->error = "Unable to allocate the zoned device descriptors.";
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

	if (ret) {
		return ret;
	}

	struct dmz_metadata *zmd = dmz->zmd;

	spin_lock_init(&zmd->meta_lock);
	spin_lock_init(&zmd->maptable_lock);
	spin_lock_init(&zmd->bitmap_lock);
	spin_lock_init(&dmz->single_thread_lock);

	// dmz_ctr_reclaim();

	/* Set target (no write same support) */
	ti->max_io_len = dmz->zmd->zone_nr_sectors;
	ti->num_flush_bios = 1;
	ti->num_discard_bios = 1;
	ti->num_write_zeroes_bios = 1;
	ti->per_io_data_size = sizeof(struct dmz_bioctx);
	ti->flush_supported = false;
	ti->discards_supported = true;

	dmz_check_meta(dmz);

	return 0;
}

static void dmz_dtr(struct dm_target *ti) {
	// pr_info("[dmz]: dtr Called.");
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
	unsigned long flags;

	for (int i = 1; i < zmd->nr_zones; i++) {
		// spin_lock_irqsave(&zmd->meta_lock, flags);
		if ((zmd->zone_start + i)->wp < zmd->zone_nr_blocks) {
			// spin_unlock_irqrestore(&zmd->meta_lock, flags);
			return i;
		}
		// spin_unlock_irqrestore(&zmd->meta_lock, flags);
	}

	return ~0;
}

static void dmz_invalidate_block(struct dmz_target *dmz, sector_t block) {
	struct dmz_metadata *zmd = dmz->zmd;
	unsigned long flags;

	// spin_lock_irqsave(&zmd->bitmap_lock, flags);
	unsigned long *bmp = zmd->bitmap_start + (block >> 6);

	// pr_info("[dmz_invalidate_block]: offset %llx, before %lx, after %lx\n", block & 0x3f, (unsigned long)*bmp, (unsigned long)(*bmp) & (unsigned long)(~(((u64)1) << (0x3f - (block & 0x3f)))));

	*bmp = (unsigned long)(*bmp) & (unsigned long)(~(((u64)1) << (0x3f - (block & 0x3f))));
	// spin_unlock_irqrestore(&zmd->bitmap_lock, flags);
}

static void dmz_validate_block(struct dmz_target *dmz, sector_t block) {
	struct dmz_metadata *zmd = dmz->zmd;
	unsigned long flags;

	// spin_lock_irqsave(&zmd->bitmap_lock, flags);
	unsigned long *bmp = zmd->bitmap_start + (block >> 6);

	// pr_info("[dmz_validate_block]: offset %llx, before %lx, after %lx\n", block & 0x3f, (unsigned long)*bmp, (unsigned long)(*bmp) | (unsigned long)(((u64)1) << (0x3f - (block & 0x3f))));

	*bmp = (unsigned long)(*bmp) | (unsigned long)(((u64)1) << (0x3f - (block & 0x3f)));
	// spin_unlock_irqrestore(&zmd->bitmap_lock, flags);
}

u64 dmz_get_map(struct dmz_metadata *zmd, u64 lba) {
	unsigned long flags;
	u64 index = lba / zmd->zone_nr_blocks;
	u64 offset = lba % zmd->zone_nr_blocks;

	struct dm_zone *cur_zone = zmd->zone_start + index;
	u64 ret = (cur_zone->mt + offset)->block_id;

	// pr_info("index: %llx, offset: %llx, pba: %llx\n", index, offset, ret);

	return ret;
}

// map logic to physical. if unmapped, return 0xffff ffff ffff ffff(default reserved blk_id representing invalid)
static u64 dmz_l2p(struct dmz_target *dmz, sector_t lba) {
	struct dmz_metadata *zmd = dmz->zmd;
	unsigned long flags;

	// spin_lock_irqsave(&zmd->maptable_lock, flags);

	u64 pba = dmz_get_map(zmd, lba);

	// pr_info("l2p: %llx, %llx\n", lba, pba);

	if (pba >= (zmd->nr_blocks)) {
		pba = ~0;
	}

	// spin_unlock_irqrestore(&zmd->maptable_lock, flags);

	return pba;
}

static void dmz_bio_endio(struct bio *bio, blk_status_t status) {
	if (status != BLK_STS_OK && bio->bi_status == BLK_STS_OK)
		bio->bi_status = status;

	bio_endio(bio);
}

static void dmz_update_map(struct dmz_target *dmz, unsigned long lba, unsigned long pba) {
	struct dmz_metadata *zmd = dmz->zmd;
	int index = lba / zmd->zone_nr_blocks;
	int offset = lba % zmd->zone_nr_blocks;
	unsigned long flags;

	struct dm_zone *cur_zone = zmd->zone_start + index;
	// spin_lock_irqsave(&zmd->maptable_lock, flags);
	(cur_zone->mt + offset)->block_id = pba;
	// spin_unlock_irqrestore(&zmd->maptable_lock, flags);
}

static void dmz_clone_endio(struct bio *clone) {
	blk_status_t status = clone->bi_status;
	struct dmz_clone_bioctx *clone_bioctx = clone->bi_private;
	struct dmz_target *dmz = clone_bioctx->dmz;
	struct dmz_metadata *zmd = dmz->zmd;
	struct dmz_bioctx *bioctx = clone_bioctx->bioctx;
	unsigned long flags;

	bio_put(clone);
	refcount_dec(&bioctx->ref);

	// if write op succeeds, update mapping. (validate wp and invalidate old_pba if old_pba exists.)
	if (status == BLK_STS_OK && bio_op(bioctx->bio) == REQ_OP_WRITE) {
		dmz_validate_block(dmz, clone_bioctx->new_pba);

		// if lba is mapped yet, we need to invalidate it's old pba.
		if (!dmz_is_default_pba(clone_bioctx->old_pba)) {
			dmz_invalidate_block(dmz, clone_bioctx->old_pba);
		}

		dmz_update_map(dmz, clone_bioctx->lba, clone_bioctx->new_pba);
	} else {
	}

	if (refcount_dec_if_one(&bioctx->ref)) {
		dmz_bio_endio(bioctx->bio, status);
	}
}

static int dmz_submit_bio(struct dmz_target *dmz, struct bio *bio) {
	struct dmz_bioctx *bioctx = dm_per_bio_data(bio, sizeof(struct dmz_bioctx));
	struct dmz_metadata *zmd = dmz->zmd;
	int op = bio_op(bio);

	unsigned long lock_flags, mt_flags;

	sector_t nr_sectors = bio_sectors(bio), logic_sector = bio->bi_iter.bi_sector;
	sector_t nr_blocks = dmz_sect2blk(nr_sectors), lba = dmz_sect2blk(logic_sector);

	bioctx->dev = zmd->dev;

	for (int i = 0; i < nr_blocks; i++) {
		struct bio *cloned_bio = bio_clone_fast(bio, GFP_ATOMIC, &dmz->bio_set);
		if (!cloned_bio) {
			return -ENOMEM;
		}

		bio_set_dev(cloned_bio, zmd->dev->bdev);

		sector_t pba = dmz_l2p(dmz, lba + i);

		struct dmz_clone_bioctx *clone_bioctx = kzalloc(sizeof(struct dmz_clone_bioctx), GFP_ATOMIC);
		clone_bioctx->bioctx = bioctx;
		clone_bioctx->dmz = dmz;
		clone_bioctx->lba = lba + i;
		clone_bioctx->old_pba = pba;

		// unmapped
		// if physical block_id is 0xffff... this block is unmapped.
		if (dmz_is_default_pba(pba)) {
			if (op == REQ_OP_WRITE) {
				// alloc a free block to write.
				int zone_id = dmz_first_free_block(dmz);

				// protect zone->wp;
				// spin_lock_irqsave(&zmd->meta_lock, lock_flags);
				pba = (zone_id << DMZ_ZONE_BLOCKS_SHIFT) + (zmd->zone_start + zone_id)->wp;
				(zmd->zone_start + zone_id)->wp += 1;
				// spin_unlock_irqrestore(&zmd->meta_lock, lock_flags);

				clone_bioctx->new_pba = pba;
				// pr_info("W: lba: %llx, pba: %lx\n", lba + i, clone_bioctx->new_pba);
			} else { // read unmapped is invalid. zero out current block
				// pr_info("zeroing out buffer of current block.\n");

				int size = 1 << DMZ_BLOCK_SHIFT;
				swap(bio->bi_iter.bi_size, size);
				zero_fill_bio(bio);
				swap(bio->bi_iter.bi_size, size);

				bio_advance(bio, size);

				if (i == nr_blocks - 1) {
					bio->bi_status = BLK_STS_OK;
					bio_endio(bio);
				}

				// pr_info("Zero out: lba: %llx, pba: %llx\n", lba + i, pba);

				continue;
			}
		} else {
			if (op == REQ_OP_WRITE) {
				// alloc a free block to write.
				int zone_id = dmz_first_free_block(dmz);

				// protect zone->wp;
				// spin_lock_irqsave(&zmd->meta_lock, lock_flags);
				pba = (zone_id << DMZ_ZONE_BLOCKS_SHIFT) + (zmd->zone_start + zone_id)->wp;
				(zmd->zone_start + zone_id)->wp += 1;
				// spin_unlock_irqrestore(&zmd->meta_lock, lock_flags);

				clone_bioctx->new_pba = pba;
				// pr_info("W: lba: %llx, pba: %lx\n", lba + i, clone_bioctx->new_pba);
			} else {
				// pr_info("R: lba: %llx, pba: %lx\n", lba + i, clone_bioctx->old_pba);
			}
		}

		// mem split
		// sequential w
		cloned_bio->bi_iter.bi_sector = dmz_start_sector(dmz) + dmz_blk2sect(pba);
		cloned_bio->bi_iter.bi_size = DMZ_BLOCK_SIZE;
		cloned_bio->bi_end_io = dmz_clone_endio;

		if ((((u64)lba) << 3) >= zmd->capacity) {
			pr_err("Cross\n");
		}

		bio_advance(bio, cloned_bio->bi_iter.bi_size);

		refcount_inc(&bioctx->ref);

		cloned_bio->bi_private = clone_bioctx;

		submit_bio_noacct(cloned_bio);
	}

	return 0;
}

static int dmz_handle_read(struct dmz_target *dmz, struct bio *bio) {
	// pr_info("Read as follows.\n");

	dmz_submit_bio(dmz, bio);

	return 0;
}

static int dmz_handle_write(struct dmz_target *dmz, struct bio *bio) {
	// pr_info("Write as Follows.\n");

	// flush bio
	if (!bio->bi_iter.bi_size) {
		// pr_err("flush is not supported tempoarily.\n");
		zero_fill_bio(bio);
		bio->bi_status = BLK_STS_OK;
		bio_endio(bio);
		return 0;
	}

	// submit bio.
	dmz_submit_bio(dmz, bio);

	return 0;
}

static int dmz_handle_discard(struct dmz_target *dmz, struct bio *bio) {
	// pr_info("Discard or write zeros\n");

	int ret = 0;

	sector_t nr_sectors = bio_sectors(bio), logic_sector = bio->bi_iter.bi_sector;
	sector_t nr_blocks = dmz_sect2blk(nr_sectors), lba = dmz_sect2blk(logic_sector);

	for (int i = 0; i < nr_blocks; i++) {
		sector_t pba = dmz_l2p(dmz, lba + i);

		if (dmz_is_default_pba(pba)) {
			// discarding unmapped is invalid
			// pr_info("[dmz-err]: try to [discard/write zeros] to unmapped block.(Tempoarily I allow it.\n)");
		} else {
			dmz_invalidate_block(dmz, pba);
		}
	}

	bio->bi_status = BLK_STS_OK;
	bio_endio(bio);

	return ret;
}

/* Map bio */
static int dmz_map(struct dm_target *ti, struct bio *bio) {
	// pr_info("Map\n: bi_sector: %llx\t bi_size: %x\n", bio->bi_iter.bi_sector, bio->bi_iter.bi_size);
	// pr_info("start_sector: %lld, nr_sectors: %d\n", bio->bi_iter.bi_sector, bio_sectors(bio));

	struct dmz_bioctx *bioctx = dm_per_bio_data(bio, sizeof(struct dmz_bioctx));
	struct dmz_target *dmz = ti->private;
	struct dmz_metadata *zmd = dmz->zmd;
	int ret = DM_MAPIO_SUBMITTED;
	unsigned long flags;

	spin_lock_irqsave(&dmz->single_thread_lock, flags);

	bioctx->dev = zmd->dev;
	bioctx->bio = bio;

	refcount_set(&bioctx->ref, 1);

	switch (bio_op(bio)) {
	case REQ_OP_READ:
		ret = dmz_handle_read(dmz, bio);
		break;
	case REQ_OP_WRITE:
		ret = dmz_handle_write(dmz, bio);
		break;
	case REQ_OP_DISCARD:
	case REQ_OP_WRITE_ZEROES:
		ret = dmz_handle_discard(dmz, bio);
		break;
	default:
		ret = -EIO;
		break;
	}

	spin_unlock_irqrestore(&dmz->single_thread_lock, flags);

	return ret;
}

static void dmz_suspend(struct dm_target *ti) {
}

static void dmz_resume(struct dm_target *ti) {
}

static int dmz_iterate_devices(struct dm_target *ti, iterate_devices_callout_fn fn, void *data) {
	struct dmz_target *dmz = ti->private;
	unsigned int zone_nr_sectors = dmz->zmd->zone_nr_sectors;
	sector_t capacity;
	int r;

	capacity = dmz->dev->capacity & ~(zone_nr_sectors - 1);
	r = fn(ti, dmz->ddev, 0, capacity, data);

	return r;
}

static void dmz_status(struct dm_target *ti, status_type_t type, unsigned int status_flags, char *result, unsigned int maxlen) {
}

/*
 * Setup target request queue limits.
 */
static void dmz_io_hints(struct dm_target *ti, struct queue_limits *limits) {
	struct dmz_target *dmz = ti->private;
	unsigned int chunk_sectors = dmz->zmd->zone_nr_sectors;

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
	struct dmz_target *dmz = ti->private;
	struct dmz_dev *dev = dmz->dev;

	*bdev = dev->bdev;

	return 0;
}

static int dmz_message(struct dm_target *ti, unsigned int argc, char **argv, char *result, unsigned int maxlen) {
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
