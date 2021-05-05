#include "dmz.h"

#define BIO_IS_FLUSH(bio) (bio_op(bio) == REQ_OP_FLUSH)

static unsigned tgt_zone = 0;

enum { DMZ_BLK_FREE, DMZ_BLK_VALID, DMZ_BLK_INVALID };
enum { DMZ_UNMAPPED, DMZ_MAPPED };

struct dmz_bioctx {
	struct bio *bio;
	refcount_t ref;
	struct mutex submit_done_lock;
	int submit_done;
};

struct dmz_clone_bioctx {
	struct dmz_bioctx *bioctx;
	struct dmz_target *dmz;
	unsigned long lba;
	unsigned long new_pba;
	unsigned long nr_blocks; // Read/Write Size
};

static inline void dmz_bio_submit_on(struct dmz_bioctx *ctx) {
	mutex_lock(&ctx->submit_done_lock);
}

static inline void dmz_bio_submit_off(struct dmz_bioctx *ctx) {
	mutex_unlock(&ctx->submit_done_lock);
}

static inline int next_tgt_zone(struct dmz_metadata *zmd) {
	unsigned int nr = (unsigned int)zmd->nr_zones;
	tgt_zone++;
	tgt_zone %= nr;
	if (tgt_zone == RESERVED_ZONE_ID)
		tgt_zone++;
	tgt_zone %= nr;
	return tgt_zone;
}

/**
 * @brief func need hold meta_lock
 * make
 * 
 * @param dmz 
 * @param nblocks 
 * @param result 
 * @return{int} zone 
 */
int dmz_pba_alloc_n(struct dmz_target *dmz, int nblocks) {
	struct dmz_metadata *zmd = dmz->zmd;
	struct dmz_zone *zone = zmd->zone_start;
	int cnt = 1;

	dmz_lock_reclaim(zmd);

	int ret = tgt_zone;
	// This process probablly result in reclaim process blocked.
	dmz_start_io(zmd, tgt_zone);

	while (zone[tgt_zone].wp == zmd->zone_nr_blocks || tgt_zone == RESERVED_ZONE_ID) {
		dmz_complete_io(zmd, tgt_zone);

		if (cnt == zmd->nr_zones) {
			if (dmz_is_full(zmd))
				return ~0;

			dmz_unlock_reclaim(zmd);
			for (int i = 0; i < zmd->nr_zones; i++) {
				struct dmz_reclaim_work *rcw = kzalloc(sizeof(struct dmz_reclaim_work), GFP_KERNEL);
				if (rcw) {
					rcw->bdev = zmd->target_bdev;
					rcw->zone = i;
					rcw->dmz = dmz;
					INIT_WORK(&rcw->work, dmz_reclaim_work_process);
					queue_work(zmd->reclaim_wq, &rcw->work);
				} else {
					pr_err("Mem not enough for reclaim.");
				}
			}

			udelay(1000);
			cnt = 0;
			dmz_lock_reclaim(zmd);
		}

		ret = next_tgt_zone(zmd); // function will inc tgt_zone too.
		cnt++;
		dmz_start_io(zmd, tgt_zone);
	}

	next_tgt_zone(zmd);

	dmz_unlock_reclaim(zmd);

	return ret;
}

unsigned long dmz_get_map(struct dmz_metadata *zmd, unsigned long lba) {
	unsigned long index = lba >> DMZ_ZONE_NR_BLOCKS_SHIFT;
	unsigned long offset = lba & DMZ_ZONE_NR_BLOCKS_MASK;

	struct dmz_zone *cur_zone = zmd->zone_start + index;

	return cur_zone->mt[offset].block_id;
}

// map logic to physical. if unmapped, return 0xffff ffff ffff ffff(default reserved blk_id representing invalid)
unsigned long dmz_l2p(struct dmz_target *dmz, sector_t lba) {
	struct dmz_metadata *zmd = dmz->zmd;
	unsigned long pba = dmz_get_map(zmd, lba);

	if (pba >= (zmd->nr_blocks)) {
		pba = ~0;
	}

	return pba;
}

void dmz_bio_try_endio(struct dmz_bioctx *bioctx, struct bio *bio, blk_status_t status) {
	dmz_bio_submit_on(bioctx);
	int able_to_end = bioctx->submit_done;
	dmz_bio_submit_off(bioctx);

	if (!able_to_end)
		return;

	if (!refcount_dec_if_one(&bioctx->ref))
		return;

	if (status != BLK_STS_OK)
		bio->bi_status = status;

	bio_endio(bio);
	kfree(bioctx);
}

void dmz_update_map(struct dmz_target *dmz, unsigned long lba, unsigned long pba) {
	// pr_err("<WRITE-UPDATE-MAP> lba: 0x%lx pba: 0x%lx\n", lba, pba);
	struct dmz_metadata *zmd = dmz->zmd;
	struct dmz_zone *z = zmd->zone_start;
	int index = lba >> DMZ_ZONE_NR_BLOCKS_SHIFT;
	int offset = lba & DMZ_ZONE_NR_BLOCKS_MASK;

	struct dmz_zone *cur_zone = &zmd->zone_start[index];
	unsigned long old_pba = cur_zone->mt[offset].block_id;
	cur_zone->mt[offset].block_id = pba;

	int p_index = pba >> DMZ_ZONE_NR_BLOCKS_SHIFT;
	int p_offset = pba & DMZ_ZONE_NR_BLOCKS_MASK;
	struct dmz_zone *p_zone = &zmd->zone_start[p_index];
	p_zone->reverse_mt[p_offset].block_id = lba;

	if (!dmz_is_default_pba(old_pba)) {
		int old_p_index = old_pba >> DMZ_ZONE_NR_BLOCKS_SHIFT;
		int old_p_offset = old_pba & DMZ_ZONE_NR_BLOCKS_MASK;
		struct dmz_zone *old_p_zone = &zmd->zone_start[old_p_index];
		old_p_zone->reverse_mt[old_p_offset].block_id = ~0;
	}

	// update bitmap

	if (!dmz_is_default_pba(old_pba)) {
		dmz_clear_bit(zmd, old_pba);

		int old_p_index = old_pba >> DMZ_ZONE_NR_BLOCKS_SHIFT;
		z[old_p_index].weight--;
	}

	dmz_set_bit(zmd, pba);
	z[p_index].weight++;
}

void dmz_submit_clone_bio(struct dmz_metadata *zmd, struct bio *clone, int idx, int remain_nr) {
	struct dmz_clone_bioctx *clone_ctx = clone->bi_private;
	struct dmz_bioctx *ctx = clone_ctx->bioctx;

	dmz_bio_submit_on(ctx);
	if (!remain_nr) {
		ctx->submit_done = 1;
	}
	submit_bio(clone);
	dmz_bio_submit_off(ctx);
}

void dmz_put_clone_bio(struct dmz_metadata *zmd, struct bio *clone, int idx) {
	struct dmz_clone_bioctx *clone_ctx = clone->bi_private;
	dmz_complete_io(zmd, idx);
	kfree(clone_ctx);
	bio_put(clone);
}

void dmz_read_clone_endio(struct bio *clone) {
	blk_status_t status = clone->bi_status;
	struct dmz_clone_bioctx *clone_bioctx = clone->bi_private;
	struct dmz_target *dmz = clone_bioctx->dmz;
	struct dmz_metadata *zmd = dmz->zmd;
	struct dmz_bioctx *bioctx = clone_bioctx->bioctx;
	unsigned idx = clone_bioctx->new_pba >> DMZ_ZONE_NR_BLOCKS_SHIFT;
	// pr_info("<dmz_read_clone_endio>");

	refcount_dec(&bioctx->ref);

	if (status != BLK_STS_OK) {
		bioctx->bio->bi_status = status;
	}

	dmz_bio_try_endio(bioctx, bioctx->bio, status);

	dmz_put_clone_bio(zmd, clone, idx);
}

void dmz_handle_read_zero(struct bio *bio, unsigned int nr_blocks) {
	unsigned int size = nr_blocks << DMZ_BLOCK_SHIFT;

	/* Clear nr_blocks */
	swap(bio->bi_iter.bi_size, size);
	zero_fill_bio(bio);
	swap(bio->bi_iter.bi_size, size);

	bio_advance(bio, size);

	pr_err("<READ ZERO>");
}

int dmz_submit_read_bio(struct dmz_target *dmz, struct bio *bio, struct dmz_bioctx *bioctx) {
	int ret = 0;
	int nr_blocks = bio_sectors(bio) >> DMZ_BLOCK_SECTORS_SHIFT;
	struct dmz_metadata *zmd = dmz->zmd;

	unsigned long lba = bio->bi_iter.bi_sector >> DMZ_BLOCK_SECTORS_SHIFT;

	// dmz_lock_reclaim(zmd);

	while (nr_blocks) {
		int ret = 0;
		unsigned long pba = dmz_l2p(dmz, lba);

		if (dmz_is_default_pba(pba)) {
			dmz_handle_read_zero(bio, 1);
			if (!(nr_blocks - 1)) {
				bio_endio(bio);
				// FIXME Under such case, bioctx can't be freed resulting in memory leak.
			}
			goto post_iter;
		}

		struct bio *clone_bio = bio_clone_fast(bio, GFP_KERNEL, NULL);
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

		bio_set_dev(clone_bio, zmd->target_bdev);

		clone_bioctx->bioctx = bioctx;
		clone_bioctx->dmz = dmz;
		clone_bioctx->lba = lba;
		clone_bioctx->new_pba = pba; // unlock process will need it.
		clone_bioctx->nr_blocks = 1;

		clone_bio->bi_iter.bi_sector = pba << DMZ_BLOCK_SECTORS_SHIFT;
		clone_bio->bi_iter.bi_size = DMZ_BLOCK_SIZE;
		clone_bio->bi_end_io = dmz_read_clone_endio;
		clone_bio->bi_private = clone_bioctx;

		refcount_inc(&bioctx->ref);

		dmz_start_io(zmd, pba >> DMZ_ZONE_NR_BLOCKS_SHIFT);
		dmz_submit_clone_bio(zmd, clone_bio, pba >> DMZ_ZONE_NR_BLOCKS_SHIFT, nr_blocks - 1);
		bio_advance(bio, clone_bio->bi_iter.bi_size);
		dmz_complete_io(zmd, pba >> DMZ_ZONE_NR_BLOCKS_SHIFT);

	post_iter:
		lba++;
		nr_blocks--;
	}

	// dmz_unlock_reclaim(zmd);
	return ret;

/** Error Handling **/
out:
	dmz_unlock_reclaim(zmd);
	pr_err("<ERROR READ>");
	return ret;
}

void dmz_write_work_process(struct work_struct *work) {
	struct dmz_write_work *wrwk = container_of(work, struct dmz_write_work, work);
}

void dmz_reclaim_work_process(struct work_struct *work) {
	struct dmz_reclaim_work *rcw = container_of(work, struct dmz_reclaim_work, work);

	dmz_reclaim_zone(rcw->dmz, rcw->zone);
}

void dmz_resubmit_work_process(struct work_struct *work) {
	struct dmz_resubmit_work *rsw = container_of(work, struct dmz_resubmit_work, work);
	// dmz_submit_write_bio
}

void dmz_write_clone_endio(struct bio *clone) {
	blk_status_t status = clone->bi_status;
	struct dmz_clone_bioctx *clone_bioctx = clone->bi_private;
	struct dmz_target *dmz = clone_bioctx->dmz;
	struct dmz_metadata *zmd = dmz->zmd;
	struct dmz_bioctx *bioctx = clone_bioctx->bioctx;
	unsigned nr_blocks = clone_bioctx->nr_blocks;

	int index, offset;

	struct bio *resubmit_bio = NULL;

	refcount_dec(&bioctx->ref);

	// if write op succeeds, update mapping. (validate wp and invalidate old_pba if old_pba exists.)
	if (status == BLK_STS_OK) {
		for (int i = 0; i < nr_blocks; i++)
			dmz_update_map(dmz, clone_bioctx->lba + i, clone_bioctx->new_pba + i);
	} else {
		bioctx->bio->bi_status = status;
		pr_err("Errno %d\n", status);
		goto resubmit;
	}

end_clone:
	index = clone_bioctx->new_pba >> DMZ_ZONE_NR_BLOCKS_SHIFT;
	offset = clone_bioctx->new_pba & DMZ_ZONE_NR_BLOCKS_MASK;

	// When zone is full start reclaim
	if ((zmd->zone_start[index].weight) < zmd->zone_start[index].wp - zmd->zone_start[index].wp / 4) {
		// Start Reclaim.
		struct dmz_reclaim_work *rcw = kzalloc(sizeof(struct dmz_reclaim_work), GFP_KERNEL);
		if (rcw) {
			rcw->bdev = zmd->target_bdev;
			rcw->zone = index;
			rcw->dmz = dmz;
			INIT_WORK(&rcw->work, dmz_reclaim_work_process);
			queue_work(zmd->reclaim_wq, &rcw->work);
		} else {
			pr_err("Mem not enough for reclaim.");
		}
	}

	dmz_bio_try_endio(bioctx, bioctx->bio, status);

	dmz_put_clone_bio(zmd, clone, index);
	return;

resubmit:
	pr_info("RESUBMIT\n");
	resubmit_bio = bio_clone_fast(clone, GFP_KERNEL, NULL);
	if (!resubmit_bio)
		goto resubmit;
	resubmit_bio->bi_iter.bi_sector = clone_bioctx->new_pba << DMZ_BLOCK_SECTORS_SHIFT;
	resubmit_bio->bi_iter.bi_size = nr_blocks << DMZ_BLOCK_SHIFT;

	bio_set_dev(resubmit_bio, zmd->target_bdev);
	submit_bio_wait(resubmit_bio);
	pr_info("Waiting End %s\n", resubmit_bio->bi_status ? "Fail" : "Succ");
	if (resubmit_bio->bi_status)
		goto resubmit;

	goto end_clone;
}

int dmz_submit_write_bio(struct dmz_target *dmz, struct bio *bio, struct dmz_bioctx *bioctx) {
	int ret = 0;
	int nr_sectors = bio_sectors(bio), nr_blocks = bio_sectors(bio) >> DMZ_BLOCK_SECTORS_SHIFT;
	struct dmz_metadata *zmd = dmz->zmd;
	struct dmz_zone *zone = zmd->zone_start;

	if (!nr_sectors) {
		goto flush;
	}

	if (nr_sectors & 0x7 || bio->bi_iter.bi_sector & 0x7) {
		goto not_aligned;
	}

	unsigned long lba = bio->bi_iter.bi_sector >> DMZ_BLOCK_SECTORS_SHIFT;

	while (nr_blocks) {
		int ret = 0;
		unsigned long pba;

		int rzone = dmz_pba_alloc_n(dmz, nr_blocks);
		if (dmz_is_default_pba(rzone)) {
			ret = -ENOSPC;
			goto out;
		}

		int blk_num = min((int)(zmd->zone_nr_blocks - zone[rzone].wp), nr_blocks);

		pba = zone[rzone].wp + (rzone << DMZ_ZONE_NR_BLOCKS_SHIFT);
		zone[rzone].wp += blk_num;

		struct bio *clone_bio = bio_clone_fast(bio, GFP_KERNEL, NULL);
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

		bio_set_dev(clone_bio, zmd->target_bdev);

		clone_bioctx->bioctx = bioctx;
		clone_bioctx->dmz = dmz;
		clone_bioctx->lba = lba;
		clone_bioctx->new_pba = pba;
		clone_bioctx->nr_blocks = blk_num;

		clone_bio->bi_iter.bi_sector = pba << DMZ_BLOCK_SECTORS_SHIFT;
		clone_bio->bi_iter.bi_size = blk_num << DMZ_BLOCK_SHIFT;
		clone_bio->bi_end_io = dmz_write_clone_endio;
		clone_bio->bi_private = clone_bioctx;

		refcount_inc(&bioctx->ref);

		// struct dmz_write_work *wrwk = kmalloc(sizeof(struct dmz_write_work), GFP_KERNEL);
		// if (!wrwk) {
		// 	kfree(clone_bio);
		// 	kfree(clone_bioctx);
		// 	goto out;
		// }

		// INIT_WORK(&wrwk->work, dmz_write_work_process);
		// wrwk->bdev = zmd->target_bdev;
		// wrwk->bio = clone_bio;

		// queue_work(zone[rzone].write_wq, &wrwk->work);

		dmz_submit_clone_bio(zmd, clone_bio, pba >> DMZ_ZONE_NR_BLOCKS_SHIFT, nr_blocks - blk_num);

		bio_advance(bio, clone_bio->bi_iter.bi_size);

		lba += blk_num;
		nr_blocks -= blk_num;
	}

	return 0;

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
	struct dmz_metadata *zmd = dmz->zmd;

	int ret = 0;

	sector_t nr_sectors = bio_sectors(bio), logic_sector = bio->bi_iter.bi_sector;
	sector_t nr_blocks = dmz_sect2blk(nr_sectors), lba = dmz_sect2blk(logic_sector);

	for (int i = 0; i < nr_blocks; i++) {
		sector_t pba = dmz_l2p(dmz, lba + i);

		if (dmz_is_default_pba(pba)) {
			// discarding unmapped is invalid
		} else {
			dmz_clear_bit(zmd, pba);
			// int index = pba >> DMZ_BLOCK_SHIFT, offset = pba % zmd->zone_nr_blocks;
			// zone[index].reverse_mt[offset].block_id = ~0;
			// index = lba >> DMZ_BLOCK_SHIFT, offset = lba % zmd->zone_nr_blocks;
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
	mutex_init(&bioctx->submit_done_lock);
	bioctx->submit_done = 0;

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
