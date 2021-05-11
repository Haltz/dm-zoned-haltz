#include "dmz-utils.h"
#include <linux/mm.h>

unsigned long meta_flags;
unsigned long *zone_lock_flags;

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
	// mutex_lock(&zone[idx].io_lock);
	// pr_info("<Zone %d IO Locked>.", idx);
	wait_on_bit_io(&zone[idx].status, 1, TASK_UNINTERRUPTIBLE);
	set_bit(1, &zone[idx].status);
}

void dmz_complete_io(struct dmz_metadata *zmd, int idx) {
	struct dmz_zone *zone = zmd->zone_start;
	// mutex_unlock(&zone[idx].io_lock);
	// pr_info("<Zone %d IO Unlocked>.", idx);
	clear_bit_unlock(1, &zone[idx].status);
	smp_mb__after_atomic();
	wake_up_bit(&zone[idx].status, 1);
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