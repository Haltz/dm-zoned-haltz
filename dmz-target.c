#include "dmz.h"

#define BIO_IS_FLUSH(bio) (bio_op(bio) == REQ_OP_FLUSH)

enum { DMZ_BLK_FREE, DMZ_BLK_VALID, DMZ_BLK_INVALID };
enum { DMZ_UNMAPPED, DMZ_MAPPED };

struct dmz_bioctx {
	struct bio *bio;
	refcount_t ref;
};

struct dmz_clone_bioctx {
	struct dmz_bioctx *bioctx;
	struct dmz_target *dmz;
	unsigned long lba;
	unsigned long old_pba, new_pba;
};

// return zone_id which free block is available
int dmz_pba_alloc(struct dmz_target *dmz) {
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

unsigned long dmz_get_map(struct dmz_metadata *zmd, unsigned long lba) {
	unsigned long flags;
	unsigned long index = lba / zmd->zone_nr_blocks;
	unsigned long offset = lba % zmd->zone_nr_blocks;

	struct dmz_zone *cur_zone = zmd->zone_start + index;

	unsigned long ret = cur_zone->mt[offset].block_id;

	// pr_info("index: %llx, offset: %llx, pba: %llx\n", index, offset, ret);

	return ret;
}

// map logic to physical. if unmapped, return 0xffff ffff ffff ffff(default reserved blk_id representing invalid)
unsigned long dmz_l2p(struct dmz_target *dmz, sector_t lba) {
	struct dmz_metadata *zmd = dmz->zmd;
	unsigned long flags;

	// spin_lock_irqsave(&zmd->maptable_lock, flags);

	unsigned long pba = dmz_get_map(zmd, lba);

	// pr_info("l2p: %llx, %llx\n", lba, pba);

	if (pba >= (zmd->nr_blocks)) {
		pba = ~0;
	}

	// spin_unlock_irqrestore(&zmd->maptable_lock, flags);

	return pba;
}

void dmz_bio_endio(struct bio *bio, blk_status_t status) {
	if (status != BLK_STS_OK && bio->bi_status == BLK_STS_OK)
		bio->bi_status = status;

	pr_info("Endio Status: %d\n", status);

	bio_endio(bio);
}

void dmz_update_map(struct dmz_target *dmz, unsigned long lba, unsigned long pba) {
	// pr_err("%x %x\n", lba, pba);
	struct dmz_metadata *zmd = dmz->zmd;
	int index = lba / zmd->zone_nr_blocks;
	int offset = lba % zmd->zone_nr_blocks;
	unsigned long flags;

	struct dmz_zone *cur_zone = &zmd->zone_start[index];
	// spin_lock_irqsave(&zmd->maptable_lock, flags);
	unsigned long old_pba = cur_zone->mt[offset].block_id;
	cur_zone->mt[offset].block_id = pba;
	// spin_unlock_irqrestore(&zmd->maptable_lock, flags);

	int p_index = pba / zmd->zone_nr_blocks;
	int p_offset = pba % zmd->zone_nr_blocks;
	struct dmz_zone *p_zone = &zmd->zone_start[p_index];
	p_zone->reverse_mt[p_offset].block_id = lba;

	if (!dmz_is_default_pba(old_pba)) {
		int old_p_index = old_pba / zmd->zone_nr_blocks;
		int old_p_offset = old_pba % zmd->zone_nr_blocks;
		struct dmz_zone *old_p_zone = &zmd->zone_start[old_p_index];
		old_p_zone->reverse_mt[old_p_offset].block_id = ~0;
	}

	// update bitmap
	int old_v;

	if (!dmz_is_default_pba(old_pba)) {
		old_v = bitmap_get_value8(zmd->bitmap_start, old_pba);
		bitmap_set_value8(zmd->bitmap_start, old_v & 0x7f, old_pba);
	}

	old_v = bitmap_get_value8(zmd->bitmap_start, pba);
	bitmap_set_value8(zmd->bitmap_start, old_v | 0x80, pba);
}

void dmz_clone_endio(struct bio *clone) {
	blk_status_t status = clone->bi_status;
	struct dmz_clone_bioctx *clone_bioctx = clone->bi_private;
	struct dmz_target *dmz = clone_bioctx->dmz;
	struct dmz_metadata *zmd = dmz->zmd;
	struct dmz_bioctx *bioctx = clone_bioctx->bioctx;
	unsigned long flags;

	pr_info("Clone Endio\n");

	bio_put(clone);
	refcount_dec(&bioctx->ref);

	// if write op succeeds, update mapping. (validate wp and invalidate old_pba if old_pba exists.)
	if (status == BLK_STS_OK && bio_op(bioctx->bio) == REQ_OP_WRITE) {
		dmz_update_map(dmz, clone_bioctx->lba, clone_bioctx->new_pba);
	}

	int index = clone_bioctx->new_pba / zmd->zone_nr_blocks, offset = clone_bioctx->new_pba % zmd->zone_nr_blocks;
	if (offset > zmd->zone_nr_blocks) {
		dmz_reclaim_zone(dmz, index);
	}

	if (refcount_dec_if_one(&bioctx->ref)) {
		dmz_bio_endio(bioctx->bio, status);
		kfree(bioctx);
	}

	kfree(clone_bioctx);
}

int dmz_submit_bio(struct dmz_target *dmz, struct bio *bio, struct dmz_bioctx *bioctx) {
	// struct dmz_bioctx *bioctx = dm_per_bio_data(bio, sizeof(struct dmz_bioctx));
	bioctx->bio = bio;
	struct dmz_metadata *zmd = dmz->zmd;
	int op = bio_op(bio);

	unsigned long lock_flags, mt_flags;

	sector_t nr_sectors = bio_sectors(bio), logic_sector = bio->bi_iter.bi_sector;
	sector_t nr_blocks = dmz_sect2blk(nr_sectors), lba = dmz_sect2blk(logic_sector);

	pr_info("nr_sectors: %d, lsector: %d\n", nr_sectors, logic_sector);
	if ((nr_sectors & 0x7 || logic_sector & 0x7) && bio_op(bio) == REQ_OP_READ) {
		bio_set_dev(bio, zmd->target_bdev);
		submit_bio(bio);
		return DM_MAPIO_SUBMITTED;
	}

	for (int i = 0; i < nr_blocks; i++) {
		struct bio *cloned_bio = bio_clone_fast(bio, GFP_KERNEL, &dmz->bio_set);
		if (!cloned_bio) {
			return -ENOMEM;
		}

		bio_set_dev(cloned_bio, zmd->target_bdev);

		sector_t pba = dmz_l2p(dmz, lba + i);

		struct dmz_clone_bioctx *clone_bioctx = kzalloc(sizeof(struct dmz_clone_bioctx), GFP_KERNEL);
		clone_bioctx->bioctx = bioctx;
		clone_bioctx->dmz = dmz;
		clone_bioctx->lba = lba + i;
		clone_bioctx->old_pba = pba;

		// unmapped
		// if physical block_id is 0xffff... this block is unmapped.
		if (dmz_is_default_pba(pba)) {
			if (op == REQ_OP_WRITE) {
				// alloc a free block to write.
				int zone_id = dmz_pba_alloc(dmz);
				// protect zone->wp;
				// spin_lock_irqsave(&zmd->meta_lock, lock_flags);
				pba = (zone_id << DMZ_ZONE_BLOCKS_SHIFT) + (zmd->zone_start + zone_id)->wp;
				(zmd->zone_start + zone_id)->wp += 1;
				// spin_unlock_irqrestore(&zmd->meta_lock, lock_flags);

				clone_bioctx->new_pba = pba;
			} else { // read unmapped is invalid. zero out current block

				// pr_info("zeroing out buffer of current block.\n");

				int size = 1 << DMZ_BLOCK_SHIFT;
				swap(bio->bi_iter.bi_size, size);
				zero_fill_bio(bio);
				swap(bio->bi_iter.bi_size, size);

				bio_advance(bio, size);

				if (i == nr_blocks - 1) {
					bio->bi_status = BLK_STS_OK;
					dmz_bio_endio(bio, BLK_STS_OK);
					pr_info("Endio In Read Zero.\n");
				}

				// pr_info("Zero out: lba: %llx, pba: %llx\n", lba + i, pba);

				continue;
			}
		} else {
			if (op == REQ_OP_WRITE) {
				// alloc a free block to write.
				int zone_id = dmz_pba_alloc(dmz);

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
		cloned_bio->bi_iter.bi_sector = dmz_blk2sect(pba);
		cloned_bio->bi_iter.bi_size = DMZ_BLOCK_SIZE;
		cloned_bio->bi_end_io = dmz_clone_endio;

		if ((((unsigned long)lba) << 3) >= zmd->capacity) {
			pr_err("Cross\n");
		}

		bio_advance(bio, cloned_bio->bi_iter.bi_size);

		refcount_inc(&bioctx->ref);

		cloned_bio->bi_private = clone_bioctx;

		pr_info("Clone Bio Submitted.\n");

		submit_bio_noacct(cloned_bio);
	}

	return DM_MAPIO_SUBMITTED;
}

int dmz_handle_read(struct dmz_target *dmz, struct bio *bio, struct dmz_bioctx *bioctx) {
	// pr_info("Read as follows.\n");

	dmz_submit_bio(dmz, bio, bioctx);

	return 0;
}

int dmz_handle_write(struct dmz_target *dmz, struct bio *bio, struct dmz_bioctx *bioctx) {
	if (!bio->bi_iter.bi_size) {
		// pr_err("flush is not supported tempoarily.\n");
		zero_fill_bio(bio);
		bio->bi_status = BLK_STS_OK;
		bio_endio(bio);
		return 0;
	}

	// submit bio.
	dmz_submit_bio(dmz, bio, bioctx);

	return 0;
}

int dmz_handle_discard(struct dmz_target *dmz, struct bio *bio) {
	// pr_info("Discard or write zeros\n");
	struct dmz_metadata *zmd = dmz->zmd;
	struct dmz_zone *zone = zmd->zone_start;

	int ret = 0;

	sector_t nr_sectors = bio_sectors(bio), logic_sector = bio->bi_iter.bi_sector;
	sector_t nr_blocks = dmz_sect2blk(nr_sectors), lba = dmz_sect2blk(logic_sector);

	for (int i = 0; i < nr_blocks; i++) {
		sector_t pba = dmz_l2p(dmz, lba + i);

		if (dmz_is_default_pba(pba)) {
			// discarding unmapped is invalid
			// pr_info("[dmz-err]: try to [discard/write zeros] to unmapped block.(Tempoarily I allow it.\n)");
		} else {
			bitmap_set_value8(zmd->bitmap_start, 0x7f & bitmap_get_value8(zmd->bitmap_start, pba), pba);
			// int index = pba / zmd->zone_nr_blocks, offset = pba % zmd->zone_nr_blocks;
			// zone[index].reverse_mt[offset].block_id = ~0;
			// index = lba / zmd->zone_nr_blocks, offset = lba % zmd->zone_nr_blocks;
			// zone[index].mt[offset].block_id = ~0;
		}
	}

	bio->bi_status = BLK_STS_OK;
	bio_endio(bio);

	return ret;
}

/* Map bio */
int dmz_map(struct dmz_target *dmz, struct bio *bio) {
	pr_info("Map: bi_sector: %llx\t bi_size: %x\n", bio->bi_iter.bi_sector, bio->bi_iter.bi_size);
	pr_info("start_sector: %lld, nr_sectors: %d op: %d\n", bio->bi_iter.bi_sector, bio_sectors(bio), bio_op(bio));

	// struct dmz_bioctx *bioctx = dm_per_bio_data(bio, sizeof(struct dmz_bioctx));
	struct dmz_bioctx *bioctx = kmalloc(sizeof(struct dmz_bioctx), GFP_KERNEL);
	struct dmz_metadata *zmd = dmz->zmd;
	int ret = DM_MAPIO_SUBMITTED;
	unsigned long flags;

	spin_lock_irqsave(&dmz->single_thread_lock, flags);

	bioctx->bio = bio;
	refcount_set(&bioctx->ref, 1);

	switch (bio_op(bio)) {
	case REQ_OP_READ:
		ret = dmz_handle_read(dmz, bio, bioctx);
		break;
	case REQ_OP_WRITE:
		ret = dmz_handle_write(dmz, bio, bioctx);
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
