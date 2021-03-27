#include "dmz.h"
#include <linux/mm.h>

unsigned long dmz_dev_capacity(struct dmz_target *dmz) {
	blkdev_ioctl(dmz->dev->bdev, 0, BLKGETSIZE, 0);
}

/**
 * @brief Function allocate a block.(one block is 4KB in default.)
 * 
 * @return int (0 is all ok, !0 indicates corresponding errors.)
 */
int dmz_write_block(struct dmz_metadata *zmd, unsigned long pba, struct page *page) {
	struct bio *bio = bio_alloc(GFP_KERNEL, 1);
	if (!bio)
		goto alloc_bio;

	bio_add_page(bio, page, DMZ_BLOCK_SIZE, 0);
	bio_set_dev(bio, zmd->dev->bdev);
	bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio->bi_iter.bi_sector = pba << 3;
	set_page_writeback(page);
	submit_bio_wait(bio);
	end_page_writeback(page);
	wait_on_page_writeback(page);
	bio_put(bio);

	if (PageError(page)) {
		goto write_err;
	}

	return 0;

alloc_bio:
	return -1;
write_err:
	return -1;
}

struct page *dmz_read_block(struct dmz_metadata *zmd, unsigned long pba) {
	struct page *page = alloc_page(GFP_KERNEL);
	if (!page) {
		goto alloc_page;
	}
	struct bio *bio = bio_alloc(GFP_KERNEL, 1);
	if (!bio)
		goto alloc_bio;

	bio_add_page(bio, page, DMZ_BLOCK_SIZE, 0);
	bio_set_dev(bio, zmd->dev->bdev);
	bio_set_op_attrs(bio, REQ_OP_READ, 0);
	bio->bi_iter.bi_sector = pba << 3;
	submit_bio_wait(bio);
	bio_put(bio);

	if (PageError(page)) {
		goto read_err;
	}

	return 0;

alloc_page:
	return -1;
alloc_bio:
	return -1;
read_err:
	return -1;
}

int dmz_flush(struct dmz_target *dmz) {
	struct dmz_metadata *zmd = dmz->zmd;
	int locked = 0, ret = 0;
	unsigned long flags;
	if (spin_is_locked(&dmz->single_thread_lock)) {
		locked = 1;
	} else {
		spin_lock_irqsave(&dmz->single_thread_lock, flags);
	}

	struct dmz_zone *zone = zmd->zone_start;
	unsigned long prev = ~0, chosen = 0;
	// find minimum weight zone
	for (int i = 0; i < zmd->nr_map_blocks; i++) {
		if ((&zone[i])->wp < prev) {
			prev = (&zone[i])->wp;
			chosen = i;
		}
	}
	dmz_reclaim_zone(dmz, chosen);

	prev = -1;
	for (int i = 0; i < zmd->nr_map_blocks; i++) {
		struct dmz_map *map = zone[i].mt, *rmap = zone[i].reverse_mt;
		// here IO must complete before func return.
		dmz_write_block(zmd, (&zone[chosen])->wp++, virt_to_page((unsigned long)map));
		dmz_write_block(zmd, (&zone[chosen])->wp++, virt_to_page((unsigned long)rmap));
	}

	if (!locked) {
		spin_unlock_irqrestore(&dmz->single_thread_lock, flags);
	}
	return 0;

ptr_null:
	pr_err("dmz-flush ptr_null.\n");
	return -1;
}