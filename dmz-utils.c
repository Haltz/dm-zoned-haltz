#include "dmz-utils.h"
#include <linux/mm.h>

unsigned long meta_flags;
unsigned long *zone_lock_flags;

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
	bio_set_dev(bio, zmd->target_bdev);
	bio_set_op_attrs(bio, REQ_OP_WRITE, 0);
	bio->bi_iter.bi_sector = pba << 3;
	submit_bio_wait(bio);

	bio_put(bio);

	if (bio->bi_status != BLK_STS_OK) {
		goto write_err;
	}

	return 0;

alloc_bio:
	return -1;
write_err:
	return -1;
}

int dmz_determine_target_zone(struct dmz_metadata *zmd, int need, unsigned long *wp_after_flush, unsigned long *target_zones, int index) {
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
	int ret = 0, reclaim_zone = 0;

	ret = dmz_reclaim_zone(dmz, reclaim_zone++);
	if (ret) {
		pr_err("Recalim failed.\n");
	}
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
	wp_after_flush = kzalloc(zmd->nr_zones * sizeof(unsigned long), GFP_KERNEL);
	mapping_target_zones = kzalloc(zmd->nr_zones * sizeof(unsigned long), GFP_KERNEL);
	rmapping_target_zones = kzalloc(zmd->nr_zones * sizeof(unsigned long), GFP_KERNEL);
	bitmap_target_zones = kzalloc(zmd->nr_zones * sizeof(unsigned long), GFP_KERNEL);

	if (!wp_after_flush || !mapping_target_zones || !rmapping_target_zones || !bitmap_target_zones) {
		pr_info("alloc wp arrays failed.\n");
		goto fail;
	}

	for (int i = 0; i < zmd->nr_zones; i++)
		wp_after_flush[i] = zone[i].wp;

	for (int i = 0; i < zmd->nr_zones; i++) {
		if (dmz_determine_target_zone(zmd, nr_zone_mt_need_blocks, wp_after_flush, mapping_target_zones, i) || dmz_determine_target_zone(zmd, nr_zone_mt_need_blocks, wp_after_flush, rmapping_target_zones, i) ||
		    dmz_determine_target_zone(zmd, nr_zone_bitmap_need_blocks, wp_after_flush, bitmap_target_zones, i)) {
			goto again;
		}
	}

	unsigned long zone_struct_target_zonewp;
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
	if (!sblk_page) {
		pr_info("alloc page failed.\n");
		goto fail;
	}
	struct dmz_super *super = (struct dmz_super *)page_address(sblk_page);
	memcpy(super, zmd->sblk, 512);
	super->magic = ~0;
	super->zones_info = zone_struct_target_zonewp;
	zone[zone_struct_target_zonewp >> DMZ_BLOCK_SHIFT].wp += nr_zone_struct_need_blocks;

	ret = dmz_write_block(zmd, 0, virt_to_page(super));
	if (ret) {
		pr_err("write failed.\n");
	}

	for (int i = 0; i < zmd->nr_zones; i++) {
		for (int j = 0; j < nr_zone_mt_need_blocks; j++) {
			unsigned long lint = (unsigned long)zone[i].mt;
			ret = dmz_write_block(zmd, zone[i].mt_blk_n + j, virt_to_page(lint + (j << DMZ_BLOCK_SHIFT)));
			if (ret) {
				pr_err("write failed.\n");
			}
		}
		for (int j = 0; j < nr_zone_mt_need_blocks; j++) {
			unsigned long lint = (unsigned long)zone[i].reverse_mt;
			ret = dmz_write_block(zmd, zone[i].rmt_blk_n + j, virt_to_page(lint + (j << DMZ_BLOCK_SHIFT)));
			if (ret) {
				pr_err("write failed.\n");
			}
		}
		for (int j = 0; j < nr_zone_bitmap_need_blocks; j++) {
			unsigned long bitmap_longint = (unsigned long)zone[i].bitmap;
			ret = dmz_write_block(zmd, zone[i].bitmap_blk_n + j, virt_to_page(bitmap_longint + (j << DMZ_BLOCK_SHIFT)));
			if (ret) {
				pr_err("write failed.\n");
			}
		}
	}

	for (int i = 0; i < nr_zone_struct_need_blocks; i++) {
		unsigned long lint = (unsigned long)zmd->zone_start;
		dmz_write_block(zmd, zone_struct_target_zonewp + i, virt_to_page(lint + (i << DMZ_BLOCK_SHIFT)));
	}

	kfree(wp_after_flush);
	kfree(mapping_target_zones);
	kfree(rmapping_target_zones);
	kfree(bitmap_target_zones);

	return 0;
fail:
	kfree(wp_after_flush);
	kfree(mapping_target_zones);
	kfree(rmapping_target_zones);
	kfree(bitmap_target_zones);
	return -1;

again:

	if (reclaim_zone >= zmd->nr_zones)
		goto fail;

	pr_info("Repeat try %d.\n", reclaim_zone);
	dmz_reclaim_zone(dmz, reclaim_zone++);
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
	// ret = dmz_flush_do(dmz);
	// if (ret) {
	// 	pr_err("flush failed.\n");
	// }

	return 0;
}

int dmz_locks_init(struct dmz_metadata *zmd) {
	spin_lock_init(&zmd->meta_lock);
	mutex_init(&zmd->reclaim_lock);
	mutex_init(&zmd->freezone_lock);

	struct dmz_zone *zone = zmd->zone_start;
	for (int i = 0; i < zmd->nr_zones; i++) {
		spin_lock_init(&zone[i].lock);
		mutex_init(&zone[i].io_lock);
		mutex_init(&zone[i].map_lock);
	}

	zone_lock_flags = kcalloc(zmd->nr_zones, sizeof(unsigned long), GFP_KERNEL);
	if (!zone_lock_flags)
		return -1;

	return 0;
}

void dmz_locks_cleanup(struct dmz_metadata *zmd) {
	if (zone_lock_flags)
		kfree(zone_lock_flags);
}

int dmz_lock_metadata(struct dmz_metadata *zmd) {
	if (spin_is_locked(&zmd->meta_lock))
		return -1;

	spin_lock_irqsave(&zmd->meta_lock, meta_flags);

	return 0;
}

void dmz_unlock_metadata(struct dmz_metadata *zmd) {
	if (!spin_is_locked(&zmd->meta_lock))
		return;

	spin_unlock_irqrestore(&zmd->meta_lock, meta_flags);
}

int dmz_lock_zone(struct dmz_metadata *zmd, int idx) {
	struct dmz_zone *zone = zmd->zone_start;
	if (spin_is_locked(&zone[idx].lock))
		return -1;

	spin_lock_irqsave(&zone[idx].lock, zone_lock_flags[idx]);

	return 0;
}

void dmz_unlock_zone(struct dmz_metadata *zmd, int idx) {
	struct dmz_zone *zone = zmd->zone_start;
	if (!spin_is_locked(&zone[idx].lock))
		return;

	spin_unlock_irqrestore(&zone[idx].lock, zone_lock_flags[idx]);
}

void dmz_start_io(struct dmz_metadata *zmd, int idx) {
	struct dmz_zone *zone = zmd->zone_start;
	mutex_lock(&zone[idx].io_lock);
	pr_info("Zone %d IO LOCKED.", idx);
}

void dmz_complete_io(struct dmz_metadata *zmd, int idx) {
	struct dmz_zone *zone = zmd->zone_start;
	mutex_unlock(&zone[idx].io_lock);
	pr_info("Zone %d IO UNLOCKED.", idx);
}

int dmz_is_on_io(struct dmz_metadata *zmd, int idx) {
	struct dmz_zone *zone = zmd->zone_start;

	if (mutex_is_locked(&zone[idx].io_lock))
		return 1;

	return 0;
}

int dmz_lock_reclaim(struct dmz_metadata *zmd) {
	if (mutex_is_locked(&zmd->reclaim_lock))
		return -1;

	mutex_lock(&zmd->reclaim_lock);

	return 0;
}

void dmz_unlock_reclaim(struct dmz_metadata *zmd) {
	if (!mutex_is_locked(&zmd->reclaim_lock))
		return;

	mutex_unlock(&zmd->reclaim_lock);
}

void dmz_lock_map(struct dmz_metadata *zmd, int idx) {
	struct dmz_zone *zone = zmd->zone_start;
	mutex_lock(&zone[idx].map_lock);
}

void dmz_unlock_map(struct dmz_metadata *zmd, int idx) {
	struct dmz_zone *zone = zmd->zone_start;
	mutex_unlock(&zone[idx].map_lock);
}
