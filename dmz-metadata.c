#include "dmz.h"

/*
 * Metadata block state flags.
 */
enum {
	DMZ_META_DIRTY,
	DMZ_META_READING,
	DMZ_META_WRITING,
	DMZ_META_ERROR,
};

struct dmz_super {
	/* Magic number */
	__le32 magic; /*   4 */

	/* Metadata version number */
	__le32 version; /*   8 */

	/* Generation number */
	__le64 gen; /*  16 */

	/* This block number */
	__le64 sb_block; /*  24 */

	/* The number of metadata blocks, including this super block */
	__le32 nr_meta_blocks; /*  28 */

	/* The number of sequential zones reserved for reclaim */
	__le32 nr_reserved_seq; /*  32 */

	/* The number of entries in the mapping table */
	__le32 nr_chunks; /*  36 */

	/* The number of blocks used for the chunk mapping table */
	__le32 nr_map_blocks; /*  40 */

	/* The number of blocks used for the block bitmaps */
	__le32 nr_bitmap_blocks; /*  44 */

	/* Checksum */
	__le32 crc; /*  48 */

	/* DM-Zoned label */
	u8 dmz_label[32]; /*  80 */

	/* DM-Zoned UUID */
	u8 dmz_uuid[16]; /*  96 */

	/* Device UUID */
	u8 dev_uuid[16]; /* 112 */

	/* Padding to full 512B sector */
	u8 reserved[400]; /* 512 */
};

struct dmz_mblk {
	struct page *page;
	void *data;
	unsigned long state;
	unsigned int ref;
};

struct dmz_mblk *dmz_alloc_mblk(void) {
	struct dmz_mblk *mblk = kzalloc(sizeof(struct dmz_mblk), GFP_ATOMIC);
	if (!mblk) {
		return NULL;
	}

	mblk->page = alloc_page(GFP_ATOMIC);
	if (!mblk->page) {
		kfree(mblk);
		return NULL;
	}

	mblk->data = page_address(mblk->page);
	mblk->ref = 0;
	mblk->state = 0;

	return mblk;
}

void dmz_free_mblk(struct dmz_mblk *mblk) {
	if (!mblk) {
		return;
	}
	if (mblk->page) {
		__free_pages(mblk->page, 0);
		kfree(mblk);
	}
}

void dmz_mblk_bio_end_io(struct bio *bio) {
	struct dmz_mblk *mblk = bio->bi_private;
	unsigned int flag;

	if (bio->bi_status)
		set_bit(DMZ_META_ERROR, &mblk->state);

	if (bio_op(bio) == REQ_OP_WRITE)
		flag = DMZ_META_WRITING;
	else
		flag = DMZ_META_READING;

	clear_bit_unlock(flag, &mblk->state);
	smp_mb__after_atomic();
	wake_up_bit(&mblk->state, flag);

	bio_put(bio);
}

struct dmz_mblk *dmz_get_mblk(struct dmz_metadata *zmd, sector_t start_no) {
	struct dmz_mblk *mblk = dmz_alloc_mblk();
	if (!mblk) {
		goto mblk_err;
	}

	struct bio *bio = bio_alloc(GFP_ATOMIC, 1);
	if (!bio) {
		goto bio_err;
	}

	set_bit(DMZ_META_READING, &mblk->state);

	bio_set_dev(bio, zmd->dev->bdev);
	bio->bi_private = mblk;
	bio->bi_iter.bi_sector = dmz_blk2sect(start_no);
	bio->bi_end_io = dmz_mblk_bio_end_io;
	bio_set_op_attrs(bio, REQ_OP_READ, REQ_META | REQ_PRIO);
	bio_add_page(bio, mblk->page, DMZ_BLOCK_SIZE, 0);
	submit_bio(bio);

	return mblk;

bio_err:
	dmz_free_mblk(mblk);
mblk_err:
	return NULL;
}

struct dmz_map *dmz_load_map(struct dmz_metadata *zmd) {
	struct dmz_map *map_ptr = kcalloc(zmd->nr_blocks, sizeof(struct dmz_map), GFP_ATOMIC);
	if (!map_ptr) {
		pr_err("Allocate memory for mapping table ptr failed. Memory is not enough.\n");
		goto end;
	}

	for (int i = 0; i < zmd->nr_map_blocks; i++) {
		struct dmz_mblk *mblk = dmz_get_mblk(zmd, zmd->map_block + i);
		if (!mblk) {
			goto read_err;
		}
		wait_on_bit_io(&mblk->state, DMZ_META_READING, TASK_UNINTERRUPTIBLE);
		if (test_bit(DMZ_META_ERROR, &mblk->state)) {
			dmz_free_mblk(mblk);
			goto read_err;
		}
		memcpy(map_ptr + i * (DMZ_BLOCK_SIZE / sizeof(struct dmz_map)), mblk->data, DMZ_BLOCK_SIZE);
		dmz_free_mblk(mblk);
	}
	return map_ptr;

read_err:
	pr_err("[dmz-err]: read map blocks error.\n");
	kfree(map_ptr);
end:
	return NULL;
}

struct dmz_zone *dmz_load_zones(struct dmz_metadata *zmd, unsigned long *bitmap) {
	struct dmz_zone *zone_start = kcalloc(zmd->nr_zones, sizeof(struct dmz_zone), GFP_ATOMIC);
	if (!zone_start) {
		return NULL;
	}

	for (int i = 0; i < zmd->nr_zones; i++) {
		struct dmz_zone *cur_zone = zone_start + i;
		cur_zone->weight = 0;
		cur_zone->wp = 0;
		cur_zone->bitmap = bitmap + (zmd->zone_nr_blocks >> 6) * i;
		cur_zone->mt = kcalloc(zmd->zone_nr_blocks, sizeof(struct dmz_map), GFP_KERNEL);
		cur_zone->reverse_mt = kcalloc(zmd->zone_nr_blocks, sizeof(struct dmz_map), GFP_KERNEL);
		for (int j = 0; j < zmd->zone_nr_blocks; j++) {
			cur_zone->mt[j].block_id = ~0;
			cur_zone->reverse_mt[j].block_id = ~0;
		}
	}

	for (int i = 0; i < zmd->useable_start; i++) {
		unsigned long index = i / zmd->zone_nr_blocks;
		unsigned long offset = i % zmd->zone_nr_blocks;
		zone_start[index].mt[offset].block_id = i;
		zone_start[index].reverse_mt[offset].block_id = i;
	}

	return zone_start;
}

void dmz_unload_zones(struct dmz_metadata *zmd) {
	struct dmz_zone *zone = zmd->zone_start;
	if (!zone)
		return;
	for (int i = 0; i < zmd->nr_zones; i++) {
		struct dmz_zone *cur = &zone[i];
		if (!cur) {
			pr_err("dmz_unload_zones err");
			continue;
		}
		if (cur->mt) {
			kfree(cur->mt);
		}
		if (cur->reverse_mt) {
			kfree(cur->reverse_mt);
		}
		kfree(cur);
	}
}

/* Load bitmap and zones */
unsigned long *dmz_load_bitmap(struct dmz_metadata *zmd) {
	// First >>3 is sector to block, second >>3 is bit to byte
	zmd->nr_bitmap_blocks = zmd->capacity >> 3 >> 3 >> DMZ_BLOCK_SHIFT;
	pr_info("[dmz-load]: bitmap loading, %lld bitmap blocks in total.\n", zmd->nr_bitmap_blocks);
	unsigned long *bitmap = bitmap_zalloc(zmd->capacity >> 3, GFP_ATOMIC);
	if (!bitmap) {
		return NULL;
	}

	// for (int i = 0; i < zmd->nr_bitmap_blocks; i++) {
	// 	struct dmz_mblk *mblk = dmz_get_mblk(zmd, zmd->bitmap_block + i);
	// 	if (!mblk) {
	// 		goto read_err;
	// 	}
	// 	wait_on_bit_io(&mblk->state, DMZ_META_READING, TASK_UNINTERRUPTIBLE);
	// 	if (test_bit(DMZ_META_ERROR, &mblk->state)) {
	// 		dmz_free_mblk(mblk);
	// 		goto read_err;
	// 	}
	// 	memcpy(bitmap + (DMZ_BLOCK_SIZE / sizeof(unsigned long)) * i, mblk->data, DMZ_BLOCK_SIZE);
	// 	dmz_free_mblk(mblk);
	// }

	for (int i = 0; i < zmd->useable_start; i++) {
		bitmap_set(bitmap, i, 1);
	}

	return bitmap;

read_err:
	pr_err("[dmz-err]: read bitmap err.\n");
	return NULL;
}

void dmz_unload_bitmap(struct dmz_metadata *zmd) {
	if (!zmd->bitmap_start) {
		pr_err("dmz_unload_bitmap");
		return;
	}

	bitmap_free(zmd->bitmap_start);
}

int dmz_get_metadata(struct dmz_metadata *zmd) {
	int ret = 0;

	struct dmz_mblk *mblk = dmz_get_mblk(zmd, 0);
	if (!mblk) {
		ret = -ENOMEM;
		goto mblk;
	}

	wait_on_bit_io(&mblk->state, DMZ_META_READING, TASK_UNINTERRUPTIBLE);
	if (test_bit(DMZ_META_ERROR, &mblk->state)) {
		pr_err("[dm-err]: read mblock error.");
		return -EINVAL;
	}
	struct dmz_super *super = kzalloc(DMZ_BLOCK_SIZE, GFP_ATOMIC);
	if (!super) {
		ret = -EINVAL;
		goto super;
	}
	memcpy(super, mblk->data, DMZ_BLOCK_SIZE);
	dmz_free_mblk(mblk);
	zmd->sb = super;

	zmd->nr_blocks = zmd->capacity >> 3; // the unit of capacity is sectors

	zmd->nr_map_blocks = zmd->nr_blocks >> 9;
	zmd->nr_bitmap_blocks = zmd->nr_blocks >> 15;

	// zmd->sb_block = (sector_t)super->sb_block;
	zmd->sb_block = 0;
	zmd->map_block = (sector_t)super->sb_block + 1;
	zmd->bitmap_block = (sector_t)super->sb_block + (sector_t)super->nr_map_blocks + 1;

	zmd->useable_start = 1 + zmd->nr_map_blocks + zmd->nr_bitmap_blocks;

	// struct dmz_map *map_ptr = dmz_load_map(zmd);
	pr_info("mapping table size: %lld\n", zmd->nr_blocks * sizeof(struct dmz_map));

	unsigned long *bitmap_ptr = dmz_load_bitmap(zmd);
	// unsigned long *bitmap_ptr = bitmap_alloc(zmd->capacity >> 3, GFP_ATOMIC);
	if (!bitmap_ptr) {
		ret = -ENOMEM;
		goto bitmap;
	}
	zmd->bitmap_start = bitmap_ptr;

	pr_info("Bitmap succeed.\n");

	struct dmz_zone *zone_start = dmz_load_zones(zmd, zmd->bitmap_start);
	if (!zone_start) {
		ret = -ENOMEM;
		goto zones;
	}
	zmd->zone_start = zone_start;
	pr_info("Zones succeed.\n");

	// Allocating large continuous memory for mapping table tends to fail.
	// In such case, I allocate small memory for each zone to split mapping table, which reduce pressure for memory and still easy to update mapping table.
	return 0;

zones:
bitmap:
map:
super:
mblk:
	pr_err("Load metadata failed.\n");
	return ret;
}

int dmz_ctr_metadata(struct dmz_target *dmz) {
	if (!dmz) {
		return -EINVAL;
	}

	struct dmz_metadata *zmd;
	struct dmz_dev *dev = dmz->dev;
	int ret;

	zmd = kzalloc(sizeof(struct dmz_metadata), GFP_ATOMIC);
	if (!zmd) {
		return -ENOMEM;
	}

	zmd->capacity = dev->capacity;
	zmd->dev = dev;
	strcpy(zmd->name, dev->name);

	zmd->zone_nr_sectors = dev->nr_zone_sectors;
	zmd->zone_nr_blocks = 1 << DMZ_ZONE_BLOCKS_SHIFT;

	zmd->nr_blocks = dev->nr_zones << DMZ_ZONE_BLOCKS_SHIFT;
	zmd->nr_zones = dev->nr_zones;

	ret = dmz_get_metadata(zmd);
	if (ret) {
		return ret;
	}

	dmz->zmd = zmd;

	return 0;
}

void dmz_dtr_metadata(struct dmz_metadata *zmd) {
	if (!zmd)
		return;

	if (zmd->sb) {
		kfree(zmd->sb);
	}

	// dmz_unload_bitmap(zmd);
	// dmz_unload_zones(zmd);

	// if (zmd)
	// 	kfree(zmd);
}