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

unsigned long *dmz_read_mblk(struct dmz_metadata *zmd, unsigned long pba, int num) {
	struct page *page = alloc_pages(GFP_KERNEL, get_count_order(num));
	if (!page) {
		return NULL;
	}

	struct bio *bio = bio_alloc(GFP_KERNEL, 1);
	bio_add_page(bio, page, num * DMZ_BLOCK_SIZE, 0);
	bio_set_dev(bio, zmd->dev->bdev);
	bio_set_op_attrs(bio, REQ_OP_READ, 0);
	bio->bi_iter.bi_sector = pba << 3;
	submit_bio_wait(bio);
	if (PageError(page)) {
		goto io;
	}

	bio_put(bio);
	return page_address(page);

io:
	bio_put(bio);
bio:
	free_page(page_address(page));
page:
	return NULL;
}

struct dmz_map *dmz_load_map(struct dmz_metadata *zmd) {
	return NULL;
}

static int dmz_init_zones_type(struct blk_zone *blkz, unsigned int num, void *data) {
	struct dmz_zone *zone = (struct dmz_zone *)data;
	struct dmz_zone *cur_zone = &zone[num];

	switch (blkz->type) {
	case BLK_ZONE_TYPE_CONVENTIONAL:
		cur_zone->type = DMZ_ZONE_RND;
		break;
	case BLK_ZONE_TYPE_SEQWRITE_REQ:
	case BLK_ZONE_TYPE_SEQWRITE_PREF:
		cur_zone->type = DMZ_ZONE_SEQ;
		break;
	default:
		cur_zone->type = DMZ_ZONE_NONE;
	}

	return 0;
}

struct dmz_zone *dmz_load_zones(struct dmz_metadata *zmd, unsigned long *bitmap) {
	struct dmz_zone *zone_start = kcalloc(zmd->nr_zones, sizeof(struct dmz_zone), GFP_ATOMIC);
	if (!zone_start) {
		return NULL;
	}

	if (!(~zmd->sblk->magic)) {
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

		int ret = blkdev_report_zones(zmd->dev->bdev, 0, BLK_ALL_ZONES, dmz_init_zones_type, zmd->zone_start);
		if (ret) {
			// FIXME
			pr_err("Errno is 80, but callback is excuted correctly, how to fix it?\n");
		}
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

	if (!(~zmd->sblk->magic)) {
		for (int i = 0; i < zmd->useable_start; i++) {
			int v = bitmap_get_value8(bitmap, i);
			bitmap_set_value8(bitmap, i, v | 0x80);
		}
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

int dmz_reload_metadata(struct dmz_metadata *zmd) {
	struct dmz_zone *zone = zmd->zone_start;
	struct dmz_super *super = zmd->sblk;

	unsigned long *zones_info = dmz_read_mblk(zmd, super->zones_info, zmd->nr_zone_struct_need_blocks);
	memcpy(zmd->zone_start, zones_info, zmd->nr_zones * sizeof(struct dmz_zone));
	free_pages(zones_info, get_count_order(zmd->nr_zone_struct_need_blocks));

	for (int i = 0; i < zmd->nr_zones; i++) {
		// reload mappings
		unsigned long mt = dmz_read_mblk(zmd, zone[i].mt_blk_n, zmd->nr_zone_mt_need_blocks);
		if (!mt) {
			goto err;
		}
		memcpy(zone[i].mt, mt, zmd->zone_nr_blocks * sizeof(struct dmz_map));
		free_pages(mt, get_count_order(zmd->nr_zone_mt_need_blocks));
		// reload reverse_mappings
		unsigned long rmt = dmz_read_mblk(zmd, zone[i].rmt_blk_n, zmd->nr_zone_mt_need_blocks);
		if (!mt) {
			goto err;
		}
		memcpy(zone[i].reverse_mt, rmt, zmd->zone_nr_blocks * sizeof(struct dmz_map));
		free_pages(rmt, get_count_order(zmd->nr_zone_mt_need_blocks));
	}

	for (int i = 0; i < zmd->nr_zones; i++) {
		unsigned long *bmp = dmz_read_mblk(zmd, zone[i].bitmap_blk_n, zmd->nr_zone_bitmap_need_blocks);
		if (!bmp) {
			goto err;
		}
		memcpy(&zmd->bitmap_start[i * (zmd->zone_nr_blocks >> 3)], bmp, zmd->zone_nr_blocks >> 3);
	}

	return 0;

err:
	return -1;
}

int dmz_load_metadata(struct dmz_metadata *zmd) {
	int ret = 0;

	unsigned long *sblk = dmz_read_mblk(zmd, 0, 1);
	zmd->sblk = (struct dmz_super *)dmz_read_mblk(zmd, 0, 1);

	zmd->nr_blocks = zmd->capacity >> 3; // the unit of capacity is sectors

	// one mapping occpuy 8 bytes, 4KB block can contain 4K/8=512 mappings. bits to right shift is ilog2(512)=9
	zmd->nr_map_blocks = zmd->nr_blocks >> 9;
	zmd->nr_bitmap_blocks = zmd->nr_blocks >> 15;

	// Temporary set 256.
	zmd->useable_start = 1;

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
	zone_start->wp = zmd->useable_start;
	pr_info("Zones succeed.\n");

	if (!(~zmd->sblk->magic)) {
		ret = dmz_reload_metadata(zmd);
		if (ret) {
			pr_err("Reload Failed.\n");
		}
	}

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

	// Set dev and bdev first.
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

	// how many blocks mappings of each zone needs. For example, 256MB zone need 128 Blocks to store mappings.
	zmd->nr_zone_mt_need_blocks = ((zmd->zone_nr_blocks * sizeof(struct dmz_map)) / DMZ_BLOCK_SIZE) + 1;

	// how many blocks bitmap need. For example, 256MB zone need 2 Blocks to store bitmaps.
	zmd->nr_zone_bitmap_need_blocks = ((zmd->zone_nr_blocks >> 3) / DMZ_BLOCK_SIZE) + 1;

	// how many blocks zone structs need.
	zmd->nr_zone_struct_need_blocks = ((zmd->nr_zones * sizeof(struct dmz_zone)) / DMZ_BLOCK_SIZE) + 1;

	ret = dmz_load_metadata(zmd);
	if (ret) {
		return ret;
	}

	dmz->zmd = zmd;

	return 0;
}

void dmz_dtr_metadata(struct dmz_metadata *zmd) {
	if (!zmd)
		return;

	if (zmd->sblk) {
		kfree(zmd->sblk);
	}

	// dmz_unload_bitmap(zmd);
	// dmz_unload_zones(zmd);

	// if (zmd)
	// 	kfree(zmd);
}