#include "dmz.h"
#include <linux/mm.h>

unsigned long dmz_dev_capacity(struct dmz_target *dmz) {
	blkdev_ioctl(dmz->dev->bdev, 0, BLKGETSIZE, 0);
}

// TODO inc_wp should trigger recliam process under proper circumustance.
int dmz_inc_wp(struct dmz_metadata *zmd, struct dmz_zone *zone) {
	return 0;
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

int dmz_determine_target_zone(struct dmz_metadata *zmd, int need, int *wp_after_flush, int *target_zones, int index) {
	struct dmz_zone *zone = zmd->zone_start;
	unsigned long wp;
	int target_zone = ~0;
	for (int i = 0; i < zmd->nr_zones; i++) {
		if (zmd->zone_nr_blocks - wp_after_flush[i] >= need) {
			target_zone = i;
			wp = wp_after_flush[i];
			wp_after_flush[i] += need;
			break;
		}
	}

	if (!(~target_zone)) {
		return -1;
	}

	target_zones[index] = target_zone * zmd->zone_nr_blocks + wp;
	return 0;
}

int dmz_flush_do(struct dmz_target *dmz) {
	struct dmz_metadata *zmd = dmz->zmd;
	struct dmz_zone *zone = zmd->zone_start;
	struct bitmap *bitmap = zmd->bitmap_start;
	unsigned long flags;
	int ret = 0, reclaim_zone = 0;

	dmz_reclaim_zone(dmz, reclaim_zone++);
	zone[0].wp = 1;

	// compute how many blocks mappings, reverser mappings, bitmap, metadata needs.

	// how many blocks mappings of each zone needs. For example, 256MB zone need 128 Blocks to store mappings.
	int nr_zone_mt_need_blocks = ((zmd->zone_nr_blocks * sizeof(struct dmz_map)) / DMZ_BLOCK_SIZE) + 1;

	// how many blocks bitmap need. For example, 256MB zone need 2 Blocks to store bitmaps.
	int nr_zone_bitmap_need_blocks = ((zmd->zone_nr_blocks >> 3) / DMZ_BLOCK_SIZE) + 1;

	// how many blocks zone structs need.
	int nr_zone_struct_need_blocks = ((zmd->nr_zones * sizeof(struct dmz_zone)) / DMZ_BLOCK_SIZE) + 1;

	/** All things flush do is changing zones->wp in order in advance and write info into blocks in the same order **/

	// init wp_after_flush
	unsigned long *wp_after_flush, *mapping_target_zones, *rmapping_target_zones, *bitmap_target_zones;

repeat_try:
	wp_after_flush = kcalloc(zmd->nr_zones, sizeof(unsigned long), GFP_KERNEL);
	mapping_target_zones = kcalloc(zmd->nr_zones, sizeof(unsigned long), GFP_KERNEL);
	rmapping_target_zones = kcalloc(zmd->nr_zones, sizeof(unsigned long), GFP_KERNEL);
	bitmap_target_zones = kcalloc(zmd->nr_zones, sizeof(unsigned long), GFP_KERNEL);

	if (!wp_after_flush || !mapping_target_zones || !rmapping_target_zones || !bitmap_target_zones) {
		goto fail;
	}

	for (int i = 0; i < zmd->nr_zones; i++)
		wp_after_flush[i] = zone[i].wp;

	for (int i = 0; i < zmd->nr_zones; i++) {
		if (dmz_determine_target_zone(zmd, nr_zone_mt_need_blocks, wp_after_flush, mapping_target_zones, i) \ 
		|| dmz_determine_target_zone(zmd, nr_zone_mt_need_blocks, wp_after_flush, rmapping_target_zones, i) \ 
		|| dmz_determine_target_zone(zmd, nr_zone_bitmap_need_blocks, wp_after_flush, bitmap_target_zones, i)) {
			goto again;
		}
	}

	int zone_struct_target_zonewp;
	if (dmz_determine_target_zone(zmd, nr_zone_struct_need_blocks, wp_after_flush, &zone_struct_target_zonewp, 0)) {
		goto again;
	}

	for (int i = 0; i < zmd->nr_zones; i++) {
		int target = mapping_target_zones[i] / zmd->zone_nr_blocks;
		zone[i].mt_blk_n = mapping_target_zones[i];
		zone[target].wp += nr_zone_mt_need_blocks;

		target = rmapping_target_zones[i] / zmd->zone_nr_blocks;
		zone[i].rmt_blk_n = rmapping_target_zones[i];
		zone[target].wp += nr_zone_mt_need_blocks;

		target = bitmap_target_zones[i] / zmd->zone_nr_blocks;
		zone[i].bitmap_blk_n = bitmap_target_zones[i];
		zone[target].wp += nr_zone_bitmap_need_blocks;
	}

	struct page *sblk_page = alloc_page(GFP_KERNEL);
	if (!sblk_page)
		goto fail;
	struct dmz_super *super = (struct dmz_super *)page_address(sblk_page);
	super->magic = ~0;
	super->zones_info = zone_struct_target_zonewp;
	zone[zone_struct_target_zonewp / zmd->zone_nr_blocks].wp += nr_zone_struct_need_blocks;

	dmz_write_block(zmd, 0, virt_to_page(super));

	for (int i = 0; i < zmd->nr_zones; i++) {
		for (int j = 0; j < nr_zone_mt_need_blocks; j++) {
			dmz_write_block(zmd, zone[i].mt_blk_n + j, virt_to_page(zone[i].mt));
		}
		for (int j = 0; j < nr_zone_mt_need_blocks; j++) {
			dmz_write_block(zmd, zone[i].rmt_blk_n + j, virt_to_page(zone[i].reverse_mt));
		}
		for (int j = 0; j < nr_zone_bitmap_need_blocks; j++) {
			dmz_write_block(zmd, zone[i].bitmap_blk_n + j, virt_to_page(zone[i].bitmap));
		}
	}

	for (int i = 0; i < nr_zone_struct_need_blocks; i++) {
		dmz_write_block(zmd, zone_struct_target_zonewp + i, virt_to_page(zmd->zone_start));
	}

no_enough_space:
fail:
	kfree(wp_after_flush);
	kfree(mapping_target_zones);
	kfree(rmapping_target_zones);
	kfree(bitmap_target_zones);
	return -1;

again:
	kfree(wp_after_flush);
	kfree(mapping_target_zones);
	kfree(rmapping_target_zones);
	kfree(bitmap_target_zones);

	if (reclaim_zone >= zmd->nr_zones)
		goto fail;

	dmz_reclaim_zone(zmd, reclaim_zone++);
	goto repeat_try;
}

/**
 * @brief Reclaim one zone and write metadata and mappings and reverse mappings and bitmaps into device.
 * Note IO is not allowed when flush is under process.
 * 
 * @param dmz 
 * @return int 
 */
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

	// predicate mappings, bitmap, zones, metadata after flush and then do the flush.
	ret = dmz_flush_do(zmd);
	if (ret) {
		pr_err("flush failed.\n");
	}

	if (!locked) {
		spin_unlock_irqrestore(&dmz->single_thread_lock, flags);
	}
	return 0;

ptr_null:
	pr_err("dmz-flush ptr_null.\n");
	return -1;
}