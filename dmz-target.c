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
	unsigned long new_pba;
};

/**
 * @brief allocate one block. returned value is zone_id if allocation succeed else ~0.
 * 
 * @param{struct dmz_target*} dmz 
 * @return{int} zone_id or ~0
 */
int dmz_pba_alloc(struct dmz_target *dmz) {
	struct dmz_metadata *zmd = dmz->zmd;
	struct dmz_zone *zone = zmd->zone_start;

	for (int i = 1; i < zmd->nr_zones; i++) {
		if (zone[i].wp < zmd->zone_nr_blocks) {
			return i;
		}
	}

	return ~0;
}

/**
 * @brief func need hold meta_lock
 * 
 * @param dmz 
 * @param nblocks 
 * @param result 
 * @return{int} zone 
 */
int dmz_pba_alloc_n(struct dmz_target *dmz, int nblocks, int *result) {
	struct dmz_metadata *zmd = dmz->zmd;
	struct dmz_zone *zone = zmd->zone_start;

	int max_zone = 0, max_remain = zmd->zone_nr_blocks - zone[0].wp;

	for (int i = 1; i < zmd->nr_zones; i++) {
		if (zone[i].wp + nblocks < zmd->zone_nr_blocks) {
			*result = 1;
			return i;
		}

		int remain = zmd->zone_nr_blocks - zone[i].wp;
		if (remain > max_remain) {
			max_zone = i;
			max_remain = remain;
		}
	}

	*result = 0;

	if (max_remain == 0) {
		return ~0;
	}

	return max_zone;
}

unsigned long dmz_get_map(struct dmz_metadata *zmd, unsigned long lba) {
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

	// spin_lock_irqsave(&zmd->maptable_lock, flags);

	unsigned long pba = dmz_get_map(zmd, lba);

	// pr_info("l2p: %llx, %llx\n", lba, pba);

	if (pba >= (zmd->nr_blocks)) {
		pba = ~0;
	}

	return pba;
}

void dmz_bio_endio(struct bio *bio, blk_status_t status) {
	if (status != BLK_STS_OK)
		bio->bi_status = status;

	pr_info("Endio Status: %d\n", bio->bi_status);

	bio_endio(bio);
}

void dmz_update_map(struct dmz_target *dmz, unsigned long lba, unsigned long pba) {
	// pr_err("%x %x\n", lba, pba);
	struct dmz_metadata *zmd = dmz->zmd;
	int index = lba / zmd->zone_nr_blocks;
	int offset = lba % zmd->zone_nr_blocks;

	struct dmz_zone *cur_zone = &zmd->zone_start[index];
	// spin_lock_irqsave(&zmd->maptable_lock, flags);
	unsigned long old_pba = cur_zone->mt[offset].block_id;
	cur_zone->mt[offset].block_id = pba;

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

void dmz_read_clone_endio(struct bio *clone) {
	blk_status_t status = clone->bi_status;
	struct dmz_clone_bioctx *clone_bioctx = clone->bi_private;
	struct dmz_bioctx *bioctx = clone_bioctx->bioctx;

	bio_put(clone);
	refcount_dec(&bioctx->ref);

	if (status != BLK_STS_OK) {
		bioctx->bio->bi_status = status;
	}

	if (refcount_dec_if_one(&bioctx->ref)) {
		dmz_bio_endio(bioctx->bio, status);
		kfree(bioctx);
	}

	kfree(clone_bioctx);
	kfree(clone);
}

int dmz_submit_read_bio(struct dmz_target *dmz, struct bio *bio, struct dmz_bioctx *bioctx) {
	int ret = 0;
	int nr_blocks = bio_sectors(bio) >> DMZ_BLOCK_SECTORS_SHIFT;
	struct block_device *tgtdev = dmz->target_bdev;

	unsigned long lba = bio->bi_iter.bi_size >> DMZ_BLOCK_SECTORS_SHIFT;

	while (nr_blocks) {
		int ret = 0;
		unsigned long pba = dmz_l2p(dmz, lba);

		struct bio *clone_bio = bio_clone_fast(bio, GFP_KERNEL, &dmz->bio_set);
		if (!clone_bio) {
			ret = -ENOMEM;
			goto out;
		}

		struct dmz_clone_bioctx *clone_bioctx = kzalloc(sizeof(struct dmz_clone_bioctx), GFP_KERNEL);
		if (!clone_bioctx) {
			kfree(clone_bio);
			ret = -ENOMEM;
			goto out;
		}

		bio_set_dev(bio, tgtdev);

		clone_bioctx->bioctx = bioctx;
		clone_bioctx->dmz = dmz;
		clone_bioctx->lba = lba;
		clone_bioctx->new_pba = ~0; // nothing to do with new_pba when it comes to read.

		clone_bio->bi_iter.bi_sector = pba << DMZ_BLOCK_SECTORS_SHIFT;
		clone_bio->bi_iter.bi_size = DMZ_BLOCK_SIZE;
		clone_bio->bi_end_io = dmz_read_clone_endio;
		clone_bio->bi_private = clone_bioctx;

		refcount_inc(&bioctx->ref);

		if (dmz_is_default_pba(pba)) {
			zero_fill_bio(clone_bio);
			bio_endio(clone_bio);
		} else
			submit_bio(clone_bio);

		bio_advance(bio, clone_bio->bi_iter.bi_size);

		lba++;
		nr_blocks--;
	}

/** Error Handling **/
out:
	return ret;
}

void dmz_write_clone_endio(struct bio *clone) {
	blk_status_t status = clone->bi_status;
	struct dmz_clone_bioctx *clone_bioctx = clone->bi_private;
	struct dmz_target *dmz = clone_bioctx->dmz;
	struct dmz_metadata *zmd = dmz->zmd;
	struct dmz_bioctx *bioctx = clone_bioctx->bioctx;

	bio_put(clone);
	refcount_dec(&bioctx->ref);

	// if write op succeeds, update mapping. (validate wp and invalidate old_pba if old_pba exists.)
	if (status == BLK_STS_OK) {
		dmz_update_map(dmz, clone_bioctx->lba, clone_bioctx->new_pba);
	} else {
		bioctx->bio->bi_status = status;
	}

	int index = clone_bioctx->new_pba >> DMZ_ZONE_NR_BLOCKS_SHIFT;
	int offset = clone_bioctx->new_pba & DMZ_ZONE_NR_BLOCKS_SHIFT;

	// 50% start reclaim
	if (offset > zmd->zone_nr_blocks) {
		dmz_reclaim_zone(dmz, index);
	}

	if (refcount_dec_if_one(&bioctx->ref)) {
		dmz_bio_endio(bioctx->bio, status);
		kfree(bioctx);
	}

	kfree(clone_bioctx);
	kfree(clone);
}

int dmz_submit_write_bio(struct dmz_target *dmz, struct bio *bio, struct dmz_bioctx *bioctx) {
	int ret = 0;
	int nr_sectors = bio_sectors(bio), nr_blocks = bio_sectors(bio) >> DMZ_BLOCK_SECTORS_SHIFT;
	struct block_device *tgtdev = dmz->target_bdev;
	struct dmz_metadata *zmd = dmz->zmd;
	struct dmz_zone *zone = zmd->zone_start;
	unsigned long flags;

	if (!nr_sectors) {
		goto flush;
	}

	if (nr_sectors & 0x7 || bio->bi_iter.bi_sector & 0x7) {
		goto not_aligned;
	}

	unsigned long lba = bio->bi_iter.bi_sector >> DMZ_BLOCK_SECTORS_SHIFT;

	while (nr_blocks) {
		int res = 0, ret = 0;
		unsigned long pba;

		spin_lock_irqsave(&zmd->meta_lock, flags);
		int rzone = dmz_pba_alloc_n(dmz, nr_blocks, &res);
		if (dmz_is_default_pba(rzone)) {
			spin_unlock_irqrestore(&zmd->meta_lock, flags);
			ret = -ENOSPC;
			goto out;
		}

		int blk_num = zmd->zone_nr_blocks - zone[rzone].wp;

		nr_blocks -= blk_num;
		pba = zone[rzone].wp + (rzone << DMZ_ZONE_NR_BLOCKS_SHIFT);
		zone[rzone].wp += blk_num;
		spin_unlock_irqrestore(&zmd->meta_lock, flags);

		struct bio *clone_bio = bio_clone_fast(bio, GFP_KERNEL, &dmz->bio_set);
		if (!clone_bio) {
			ret = -ENOMEM;
			goto out;
		}

		struct dmz_clone_bioctx *clone_bioctx = kzalloc(sizeof(struct dmz_clone_bioctx), GFP_KERNEL);
		if (!clone_bioctx) {
			kfree(clone_bio);
			ret = -ENOMEM;
			goto out;
		}

		bio_set_dev(bio, tgtdev);

		clone_bioctx->bioctx = bioctx;
		clone_bioctx->dmz = dmz;
		clone_bioctx->lba = lba;
		clone_bioctx->new_pba = pba;

		clone_bio->bi_iter.bi_sector = pba << DMZ_BLOCK_SECTORS_SHIFT;
		clone_bio->bi_iter.bi_size = blk_num << DMZ_BLOCK_SHIFT;
		clone_bio->bi_end_io = dmz_write_clone_endio;
		clone_bio->bi_private = clone_bioctx;

		refcount_inc(&bioctx->ref);

		submit_bio(clone_bio);

		bio_advance(bio, clone_bio->bi_iter.bi_size);

		lba += blk_num;
	}

/** Not supported yet. **/
not_aligned:
	pr_err("module require bio aligned to block size.");
	bio->bi_status = BLK_STS_NOTSUPP;
	bio_endio(bio);
	return -EINVAL;

flush:
	zero_fill_bio(bio);
	bio->bi_status = BLK_STS_OK;
	bio_endio(bio);
	return 0;

/** Error Handling **/
out:
	return ret;
}

int dmz_handle_discard(struct dmz_target *dmz, struct bio *bio) {
	// pr_info("Discard or write zeros\n");
	struct dmz_metadata *zmd = dmz->zmd;

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
	// pr_info("Map: bi_sector: %llx\t bi_size: %x\n", bio->bi_iter.bi_sector, bio->bi_iter.bi_size);
	// pr_info("start_sector: %lld, nr_sectors: %d op: %d\n", bio->bi_iter.bi_sector, bio_sectors(bio), bio_op(bio));
	struct dmz_bioctx *bioctx = kmalloc(sizeof(struct dmz_bioctx), GFP_KERNEL);
	int ret = DM_MAPIO_SUBMITTED;

	bioctx->bio = bio;
	refcount_set(&bioctx->ref, 1);

	switch (bio_op(bio)) {
	case REQ_OP_READ:
		ret = dmz_submit_read_bio(dmz, bio, bioctx);
		break;
	case REQ_OP_WRITE:
		ret = dmz_submit_write_bio(dmz, bio, bioctx);
		break;
	case REQ_OP_DISCARD:
	case REQ_OP_WRITE_ZEROES:
		ret = dmz_handle_discard(dmz, bio);
		break;
	default:
		ret = -EIO;
		break;
	}

	return ret;
}
