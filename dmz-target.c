#include "dmz.h"

#define BIO_IS_FLUSH(bio) (bio_op(bio) == REQ_OP_FLUSH)

static unsigned tgt_zone = 0;

enum { DMZ_BLK_FREE, DMZ_BLK_VALID, DMZ_BLK_INVALID };
enum { DMZ_UNMAPPED, DMZ_MAPPED };

struct dmz_bioctx {
	struct bio *bio;
	refcount_t ref;
	spinlock_t submit_done_lock;
	int submit_done;
	unsigned long flags;
};

struct dmz_clone_bioctx {
	struct dmz_bioctx *bioctx;
	struct dmz_target *dmz;
	unsigned long lba;
	unsigned long new_pba;
	unsigned long nr_blocks; // Read/Write Size
	struct mutex m;
};

struct workqueue_struct *sad_wq;

static void sad_work_f(struct work_struct *work) {
	struct sad_work *w = container_of(work, struct sad_work, work);
	mutex_unlock(w->m);
}

static void wake_sad_bit(struct mutex *m) {
	struct sad_work *w = kzalloc(sizeof(struct sad_work), GFP_KERNEL);
	INIT_WORK(&w->work, sad_work_f);
	w->m = m;
	queue_work(sad_wq, &w->work);
}

static void dmz_bio_submit_on(struct dmz_bioctx *ctx) {
	spin_lock_irqsave(&ctx->submit_done_lock, ctx->flags);
}

static void dmz_bio_submit_off(struct dmz_bioctx *ctx) {
	spin_unlock_irqrestore(&ctx->submit_done_lock, ctx->flags);
}

static inline int next_tgt_zone(struct dmz_metadata *zmd) {
	unsigned int nr = (unsigned int)zmd->nr_zones;
	tgt_zone++;
	tgt_zone %= nr;
	while (!dmz_zone_ofuse(tgt_zone)) {
		tgt_zone++;
		tgt_zone %= nr;
	}
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
	int cnt = 0;

	dmz_lock_reclaim(zmd);

	int ret = tgt_zone;
	// This process probablly result in reclaim process blocked.
	dmz_start_io(zmd, tgt_zone);

	while (zone[tgt_zone].wp == zmd->zone_nr_blocks || !dmz_zone_ofuse(tgt_zone)) {
		dmz_complete_io(zmd, tgt_zone);

		if (cnt == zmd->nr_zones) {
			if (dmz_is_full(zmd))
				return ~0;

			dmz_unlock_reclaim(zmd);

			struct dmz_reclaim_work *rcw = kzalloc(sizeof(struct dmz_reclaim_work), GFP_KERNEL);
			if (rcw) {
				rcw->bdev = zmd->target_bdev;
				rcw->zone = 0;
				rcw->dmz = dmz;
				INIT_WORK(&rcw->work, dmz_reclaim_work_process);
				queue_work(zmd->reclaim_wq, &rcw->work);
			} else {
				pr_err("Mem not enough for reclaim.");
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
	int index = (lba * sizeof(struct dmz_map)) / DMZ_BLOCK_SIZE;
	int offset = (lba * sizeof(struct dmz_map)) % DMZ_BLOCK_SIZE;

	dmz_lock_metadata(zmd);
	unsigned long pba = dmz_read_cache(zmd, lba);
	if (dmz_is_default_pba(pba)) {
		dmz_unlock_metadata(zmd);
		unsigned long buffer = (unsigned long)wait_read(zmd, (META_ZONE_ID << DMZ_ZONE_NR_BLOCKS_SHIFT) + index);
		struct dmz_map *map = (struct dmz_map *)(buffer + offset);
		unsigned long ret = map->block_id;
		free_page(buffer);
		return ret;
	} else {
		dmz_unlock_metadata(zmd);
		return pba;
	}
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

void dmz_submit_clone_bio(struct dmz_metadata *zmd, struct bio *clone, int idx, int remain_nr, struct dmz_bioctx *ctx) {
	if (!remain_nr) {
		dmz_bio_submit_on(ctx);
		ctx->submit_done = 1;
		dmz_bio_submit_off(ctx);
	}
	submit_bio(clone);
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
		dmz_submit_clone_bio(zmd, clone_bio, pba >> DMZ_ZONE_NR_BLOCKS_SHIFT, nr_blocks - 1, bioctx);
		// clone_bio->bi_private = clone_bioctx;
		// dmz_read_clone_endio(clone_bio);
		// dmz_put_clone_bio(zmd, clone_bio, pba >> DMZ_ZONE_NR_BLOCKS_SHIFT);
		bio_advance(bio, clone_bio->bi_iter.bi_size);

	post_iter:
		lba++;
		nr_blocks--;
	}

	// dmz_unlock_reclaim(zmd);
	return ret;

/** Error Handling **/
out:
	// dmz_unlock_reclaim(zmd);
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
	if (status != BLK_STS_OK) {
		bioctx->bio->bi_status = status;
		pr_err("Errno %d\n", status);
		goto resubmit;
	}

end_clone:
	index = clone_bioctx->new_pba >> DMZ_ZONE_NR_BLOCKS_SHIFT;
	offset = clone_bioctx->new_pba & DMZ_ZONE_NR_BLOCKS_MASK;

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
		zone[rzone].weight += blk_num;

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

		// pr_info("Write Cache Size %x, %lx -> %lx\n", blk_num, lba, pba);
		for (int i = 0; i < blk_num; i++) {
			if (lba + i == 1) {
				pr_info("%lx -> %lx\n", lba + i, pba + i);
			}
			dmz_write_cache(zmd, lba + i, pba + i);
		}

		// pr_info("SUBMIT START IDX %ld, %lx\n", pba >> DMZ_ZONE_NR_BLOCKS_SHIFT, pba & DMZ_ZONE_NR_BLOCKS_MASK);
		dmz_submit_clone_bio(zmd, clone_bio, pba >> DMZ_ZONE_NR_BLOCKS_SHIFT, nr_blocks - blk_num, bioctx);
		// pr_info("SUBMIT END IDX %ld, %lx\n", pba >> DMZ_ZONE_NR_BLOCKS_SHIFT, pba & DMZ_ZONE_NR_BLOCKS_MASK);

		// clone_bio->bi_private = clone_bioctx;
		// dmz_write_clone_endio(clone_bio);
		// dmz_put_clone_bio(zmd, clone_bio, pba >> DMZ_ZONE_NR_BLOCKS_SHIFT);
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
	spin_lock_init(&bioctx->submit_done_lock);
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
