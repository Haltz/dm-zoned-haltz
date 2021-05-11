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
	spin_lock_irqsave(&zmd->meta_lock, meta_flags);

	return 0;
}

void dmz_unlock_metadata(struct dmz_metadata *zmd) {
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
	// pr_info("<Zone %d IO Locked>.", idx);
}

void dmz_complete_io(struct dmz_metadata *zmd, int idx) {
	struct dmz_zone *zone = zmd->zone_start;
	mutex_unlock(&zone[idx].io_lock);
	// pr_info("<Zone %d IO Unlocked>.", idx);
}

int dmz_is_on_io(struct dmz_metadata *zmd, int idx) {
	struct dmz_zone *zone = zmd->zone_start;

	if (mutex_is_locked(&zone[idx].io_lock))
		return 1;

	return 0;
}

int dmz_lock_reclaim(struct dmz_metadata *zmd) {
	mutex_lock(&zmd->reclaim_lock);
	return 0;
}

void dmz_unlock_reclaim(struct dmz_metadata *zmd) {
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

int dmz_open_zone(struct dmz_metadata *zmd, int idx) {
	struct dmz_zone *zone = zmd->zone_start;
	int ret = 0;
	if (DMZ_IS_SEQ(&zone[idx]))
		ret = blkdev_zone_mgmt(zmd->target_bdev, REQ_OP_ZONE_OPEN, idx << (DMZ_ZONE_NR_BLOCKS_SHIFT + DMZ_BLOCK_SECTORS_SHIFT), zmd->zone_nr_sectors, GFP_KERNEL);
	if (ret)
		pr_err("Open Zone %d Failed.", idx);
	return ret;
}

int dmz_close_zone(struct dmz_metadata *zmd, int idx) {
	struct dmz_zone *zone = zmd->zone_start;
	int ret = 0;
	if (DMZ_IS_SEQ(&zone[idx]))
		ret = blkdev_zone_mgmt(zmd->target_bdev, REQ_OP_ZONE_CLOSE, idx << (DMZ_ZONE_NR_BLOCKS_SHIFT + DMZ_BLOCK_SECTORS_SHIFT), zmd->zone_nr_sectors, GFP_KERNEL);
	if (ret)
		pr_err("Close Zone %d Failed.", idx);
	return ret;
}

int dmz_finish_zone(struct dmz_metadata *zmd, int idx) {
	struct dmz_zone *zone = zmd->zone_start;
	int ret = 0;
	if (DMZ_IS_SEQ(&zone[idx]))
		ret = blkdev_zone_mgmt(zmd->target_bdev, REQ_OP_ZONE_FINISH, idx << (DMZ_ZONE_NR_BLOCKS_SHIFT + DMZ_BLOCK_SECTORS_SHIFT), zmd->zone_nr_sectors, GFP_KERNEL);
	if (ret)
		pr_err("Finish Zone %d Failed.", idx);
	return ret;
}

int dmz_reset_zone(struct dmz_metadata *zmd, int idx) {
	struct dmz_zone *zone = zmd->zone_start;
	int ret = 0;

	if (DMZ_IS_SEQ(&zone[idx]))
		ret = blkdev_zone_mgmt(zmd->target_bdev, REQ_OP_ZONE_RESET, idx << (DMZ_ZONE_NR_BLOCKS_SHIFT + DMZ_BLOCK_SECTORS_SHIFT), 1 << (DMZ_ZONE_NR_BLOCKS_SHIFT + DMZ_BLOCK_SECTORS_SHIFT), GFP_KERNEL);

	if (ret)
		pr_err("Reset Zone %d Failed. Err: %d\n", idx, ret);
	else
		pr_info("Reset Zone %d Succ. Wp: %d We: %d\n", idx, zone[idx].wp, zone[idx].weight);

	zone[idx].wp = 0;
	zone[idx].weight = 0;
	return ret;
}

bool dmz_is_full(struct dmz_metadata *zmd) {
	struct dmz_zone *zone = zmd->zone_start;
	bool full = true;
	for (int i = 0; i < zmd->nr_zones; i++) {
		if (zmd->zone_nr_blocks != zone[i].weight) {
			full = false;
		}
	}

	return full;
}

unsigned long *dmz_bitmap_alloc(unsigned long size) {
	unsigned long *bitmap;

	bitmap = kzalloc(size, GFP_KERNEL);
	if (!bitmap)
		return NULL;
	return bitmap;
}

void dmz_bitmap_free(unsigned long *bitmap) {
	kfree(bitmap);
}

void dmz_set_bit(struct dmz_metadata *zmd, unsigned long pos) {
	unsigned long bitmap = (unsigned long)zmd->bitmap_start;
	unsigned long index = pos >> 3, offset = pos & ((0x1 << 3) - 1); // a byte can contain 8 bit (8 blocks's validity)
	bitmap += index;
	char v = *((char *)bitmap);
	v = v | (0x1 << offset);
	*((char *)bitmap) = v;
}

void dmz_clear_bit(struct dmz_metadata *zmd, unsigned long pos) {
	unsigned long bitmap = (unsigned long)zmd->bitmap_start;
	unsigned long index = pos >> 3, offset = pos & ((0x1 << 3) - 1); // a byte can contain 8 bit (8 blocks's validity)
	bitmap += index;
	char v = *((char *)bitmap);
	v = v & (~(0x1 << offset));
	*((char *)bitmap) = v;
}

bool dmz_test_bit(struct dmz_metadata *zmd, unsigned long pos) {
	unsigned long bitmap = (unsigned long)zmd->bitmap_start;
	unsigned long index = pos >> 3, offset = pos & ((0x1 << 3) - 1); // a byte can contain 8 bit (8 blocks's validity)
	bitmap += index;
	char v = *((char *)bitmap);
	v = v & (0x1 << offset);
	return !!v;
}

void dmz_print_zones(struct dmz_metadata *zmd, char *tag) {
	struct dmz_zone *z = zmd->zone_start;
	for (int i = 0; i < zmd->nr_zones; i++) {
		pr_info("%s zone %d: %x, we: %x", tag, i, z[i].wp, z[i].weight);
	}
}

/** lock in ascending order **/
void dmz_lock_two_zone(struct dmz_metadata *zmd, int zone1, int zone2) {
	if (zone1 < zone2) {
		dmz_start_io(zmd, zone1);
		dmz_start_io(zmd, zone2);
	} else if (zone1 > zone2) {
		dmz_start_io(zmd, zone2);
		dmz_start_io(zmd, zone1);
	} else {
		dmz_start_io(zmd, zone1);
	}
}

/** unlock in descending order **/
void dmz_unlock_two_zone(struct dmz_metadata *zmd, int zone1, int zone2) {
	if (zone1 > zone2) {
		dmz_complete_io(zmd, zone1);
		dmz_complete_io(zmd, zone2);
	} else if (zone1 < zone2) {
		dmz_complete_io(zmd, zone2);
		dmz_complete_io(zmd, zone1);
	} else {
		dmz_complete_io(zmd, zone1);
	}
}

unsigned long dmz_read_cache(struct dmz_metadata *zmd, unsigned long lba) {
	struct dmz_cache_node *ret = radix_tree_lookup(&zmd->cache, lba);
	unsigned long pba = ret ? ret->pba : ~0;
	return pba;
}

void dmz_write_cache(struct dmz_metadata *zmd, unsigned long lba, unsigned long pba) {
	struct radix_tree_root *cache = &zmd->cache;
	dmz_lock_metadata(zmd);
	dmz_set_bit(zmd, pba);
	if (lba < 16)
		pr_info("SETMAP %ld -> %ld\n", lba, pba);

	struct dmz_cache_node *node = radix_tree_lookup(cache, lba);
	if (node) {
		if (!dmz_is_default_pba(node->pba)) {
			dmz_clear_bit(zmd, node->pba);
			zmd->zone_start[node->pba >> DMZ_ZONE_NR_BLOCKS_SHIFT].weight--;
		}

		// put lba node on head of cache list
		struct dmz_cache_node *next = node->next;
		struct dmz_cache_node *prev = node->prev;
		if (next)
			next->prev = prev;
		if (prev)
			prev->next = next;
		node->prev = NULL;
		node->next = zmd->cache_head;
		if (zmd->cache_head)
			zmd->cache_head->prev = node;
		zmd->cache_head = node;
		// update lba->pba
		node->pba = pba;
		dmz_unlock_metadata(zmd);
		return;
	} else {
		struct dmz_cache_node *new = kzalloc(sizeof(struct dmz_cache_node), GFP_KERNEL);
		if (!new) {
			pr_err("Cache fault.\n");
			dmz_unlock_metadata(zmd);
			return;
		}
		new->next = zmd->cache_head;
		if (zmd->cache_head)
			zmd->cache_head->prev = new;
		new->prev = NULL;
		new->lba = lba;
		new->pba = pba;
		zmd->cache_head = new;
		if (zmd->cache_tail == NULL)
			zmd->cache_tail = new;
		radix_tree_insert(cache, lba, new);

		// get tail node
		struct dmz_cache_node *tail = zmd->cache_tail;
		if (zmd->cache_size < DMZ_MAP_CACHE_SIZE)
			zmd->cache_size++;
		// write to meta_zone
		int index = (tail->lba * sizeof(struct dmz_map)) / DMZ_BLOCK_SIZE;
		int offset = (tail->lba * sizeof(struct dmz_map)) % DMZ_BLOCK_SIZE;
		unsigned long pba = tail->pba, lba = tail->lba;

		dmz_unlock_metadata(zmd);

		unsigned long buffer = wait_read(zmd, (META_ZONE_ID << DMZ_ZONE_NR_BLOCKS_SHIFT) + index);
		if (!buffer) {
			pr_err("Cache fault.\n");
			return;
		}
		struct dmz_map *map = (struct dmz_map *)(buffer + offset);

		if (!dmz_is_default_pba(map->block_id)) {
			dmz_lock_metadata(zmd);
			dmz_clear_bit(zmd, map->block_id);
			zmd->zone_start[map->block_id >> DMZ_ZONE_NR_BLOCKS_SHIFT].weight--;
			dmz_unlock_metadata(zmd);
		}

		if (tail && zmd->cache_size >= DMZ_MAP_CACHE_SIZE) {
			map->block_id = pba;
			wait_write(zmd, (META_ZONE_ID << DMZ_ZONE_NR_BLOCKS_SHIFT) + index, buffer);

			dmz_lock_metadata(zmd);

			zmd->cache_tail = tail->prev;
			if (tail->prev)
				tail->prev->next = NULL;

			// evict tail
			radix_tree_delete(cache, lba);

			dmz_unlock_metadata(zmd);
		} else {
			free_page(buffer);
		}
		// put cache node on head of cache list
	}

	// change reverse map
	int index = (pba * sizeof(struct dmz_map)) / DMZ_BLOCK_SIZE;
	int offset = (pba * sizeof(struct dmz_map)) % DMZ_BLOCK_SIZE;
	unsigned long buffer = wait_read(zmd, (META_ZONE_ID << DMZ_ZONE_NR_BLOCKS_SHIFT) + index + (1 << (DMZ_ZONE_NR_BLOCKS_SHIFT - 1)));
	if (!buffer) {
		pr_err("Cache fault.\n");
		return;
	}
	struct dmz_map *rmap = (struct dmz_map *)(buffer + offset);
	rmap->block_id = lba;
	wait_write(zmd, (META_ZONE_ID << DMZ_ZONE_NR_BLOCKS_SHIFT) + index + (1 << (DMZ_ZONE_NR_BLOCKS_SHIFT - 1)), buffer);
}