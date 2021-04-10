#include "dmz-metadata.h"

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
	unsigned long *buffer = kzalloc(num << DMZ_BLOCK_SHIFT, GFP_KERNEL);
	if (!buffer) {
		goto page;
	}

	struct bio *bio = bio_alloc(GFP_KERNEL, num);
	for (int i = 0; i < num; i++) {
		bio_add_page(bio, virt_to_page(buffer + (i << 9)), DMZ_BLOCK_SIZE, 0);
	}
	bio_set_dev(bio, zmd->target_bdev);
	bio_set_op_attrs(bio, REQ_OP_READ, 0);
	bio->bi_iter.bi_sector = pba << 3;
	submit_bio_wait(bio);
	if (bio->bi_status != BLK_STS_OK) {
		goto io;
	}

	bio_put(bio);
	return buffer;

io:
	bio_put(bio);
	kfree((void *)buffer);
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
	struct dmz_zone *zone_start = kzalloc(zmd->nr_zones * sizeof(struct dmz_zone), GFP_KERNEL);
	if (!zone_start) {
		goto out;
	}

	for (int i = 0; i < zmd->nr_zones; i++) {
		struct dmz_zone *cur_zone = zone_start + i;
		cur_zone->weight = 0;
		cur_zone->wp = 0;
		cur_zone->bitmap = bitmap + (zmd->zone_nr_blocks >> 6) * i;
		cur_zone->mt = kzalloc(zmd->zone_nr_blocks * sizeof(struct dmz_map), GFP_KERNEL);
		if (!cur_zone->mt) {
			pr_err("mt err.\n");
			goto mt;
		}
		cur_zone->reverse_mt = kzalloc(zmd->zone_nr_blocks * sizeof(struct dmz_map), GFP_KERNEL);
		if (!cur_zone->reverse_mt) {
			pr_err("reverse_mt err.\n");
			goto mt;
		}
		for (int j = 0; j < zmd->zone_nr_blocks; j++) {
			cur_zone->mt[j].block_id = ~0;
			cur_zone->reverse_mt[j].block_id = ~0;
		}
	}

	int ret = blkdev_report_zones(zmd->target_bdev, 0, BLK_ALL_ZONES, dmz_init_zones_type, zone_start);
	if (ret) {
		// FIXME
		pr_err("Errno is 80, but callback is excuted correctly, how to fix it?\n");
	}

	return zone_start;

mt:
	dmz_unload_zones(zmd);
out:
	return NULL;
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
	}

	kfree(zone);
}

/* Load bitmap and zones */
unsigned long *dmz_load_bitmap(struct dmz_metadata *zmd) {
	// First >>3 is sector to block, second >>3 is bit to byte
	zmd->nr_bitmap_blocks = zmd->capacity >> 3 >> 3 >> DMZ_BLOCK_SHIFT;
	pr_info("[dmz-load]: bitmap loading, %ld bitmap blocks in total.\n", zmd->nr_bitmap_blocks);
	unsigned long *bitmap = bitmap_zalloc(zmd->capacity >> 3, GFP_KERNEL);
	if (!bitmap) {
		return NULL;
	}

	return bitmap;
}

void dmz_unload_bitmap(struct dmz_metadata *zmd) {
	if (!zmd->bitmap_start) {
		return;
	}

	bitmap_free(zmd->bitmap_start);
}

int dmz_reload_metadata(struct dmz_metadata *zmd) {
	struct dmz_zone *zone = zmd->zone_start;
	struct dmz_super *super = zmd->sblk;

	pr_info("Reload Read.\n");
	unsigned long *zones_info = dmz_read_mblk(zmd, super->zones_info, zmd->nr_zone_struct_need_blocks);
	if (!zones_info) {
		pr_err("zones_info read failed.\n");
		goto err;
	}
	memcpy(zmd->zone_start, zones_info, zmd->nr_zones * sizeof(struct dmz_zone));
	kfree(zones_info);

	int stepsize = min(MAX_NR_BLOCKS_ONCE_READ, zmd->nr_zone_mt_need_blocks);
	for (int i = 0; i < zmd->nr_zones; i++) {
		pr_info("id: %d, m: %lx, rm: %lx, bm: %lx\n", i, zone[i].mt_blk_n, zone[i].rmt_blk_n, zone[i].bitmap_blk_n);
		// reload mappings
		unsigned long readsize = 0;
		for (int loc = 0; loc < zmd->nr_zone_mt_need_blocks; loc += stepsize) {
			unsigned long mt = (unsigned long)dmz_read_mblk(zmd, zone[i].mt_blk_n, min(stepsize, zmd->nr_zone_mt_need_blocks - loc));
			if (!mt) {
				goto err;
			}
			unsigned long next_readsize = readsize + (min(stepsize, zmd->nr_zone_mt_need_blocks - loc) << DMZ_BLOCK_SHIFT) / sizeof(struct dmz_map);
			next_readsize = min(next_readsize, zmd->nr_blocks);

			memcpy(&zone[i].mt[readsize], (void *)mt, next_readsize - readsize);
			readsize = next_readsize;

			kfree((void *)mt);
		}

		readsize = 0;
		// reload reverse_mappings
		for (int loc = 0; loc < zmd->nr_zone_mt_need_blocks; loc += stepsize) {
			unsigned long rmt = (unsigned long)dmz_read_mblk(zmd, zone[i].mt_blk_n, min(stepsize, zmd->nr_zone_mt_need_blocks - loc));
			if (!rmt) {
				goto err;
			}

			unsigned long next_readsize = readsize + (min(stepsize, zmd->nr_zone_mt_need_blocks - loc) << DMZ_BLOCK_SHIFT) / sizeof(struct dmz_map);
			next_readsize = min(next_readsize, zmd->nr_blocks);

			memcpy(&zone[i].reverse_mt[readsize], (void *)rmt, next_readsize - readsize);
			readsize = next_readsize;

			kfree((void *)rmt);
		}
		pr_info("Zone %d Good.\n", i);
	}

	for (int i = 0; i < zmd->nr_zones; i++) {
		unsigned long *bmp = dmz_read_mblk(zmd, zone[i].bitmap_blk_n, zmd->nr_zone_bitmap_need_blocks);
		if (!bmp) {
			goto err;
		}
		memcpy(&zmd->bitmap_start[i * (zmd->zone_nr_blocks >> 3)], bmp, zmd->zone_nr_blocks >> 3);
		memcpy(zone[i].bitmap, bmp, zmd->zone_nr_blocks >> 3);
		kfree(bmp);
	}

	pr_info("Bitmap Good.\n");

	return 0;

err:
	return -1;
}

int dmz_load_metadata(struct dmz_metadata *zmd) {
	int ret = 0;

	unsigned long *sblk = dmz_read_mblk(zmd, 0, 1);
	zmd->sblk = (struct dmz_super *)sblk;

	zmd->nr_blocks = zmd->capacity >> 3; // the unit of capacity is sectors

	// one mapping occpuy 8 bytes, 4KB block can contain 4K/8=512 mappings. bits to right shift is ilog2(512)=9
	zmd->nr_map_blocks = zmd->nr_blocks >> 9;
	zmd->nr_bitmap_blocks = zmd->nr_blocks >> 15;

	zmd->useable_start = 0;

	unsigned long *bitmap_ptr = dmz_load_bitmap(zmd);
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

	if (dmz_locks_init(zmd))
		goto locks;

	/** There is no need to support flush right now. **/
	// if (!(~zmd->sblk->magic)) {
	// 	pr_info("Start Initlization.\n");
	// 	ret = dmz_reload_metadata(zmd);
	// 	if (ret) {
	// 		pr_err("Reload Failed.\n");
	// 		ret = -1;
	// 		goto reload;
	// 	}
	// }

	// Allocating large continuous memory for mapping table tends to fail.
	// In such case, I allocate small memory for each zone to split mapping table, which reduce pressure for memory and still easy to update mapping table.
	return ret;

locks:
	dmz_unload_zones(zmd);
zones:
	dmz_unload_bitmap(zmd);
bitmap:
	pr_err("Load metadata failed.\n");
	return ret;
}

void dmz_unload_metadata(struct dmz_metadata *zmd) {
	dmz_unload_bitmap(zmd);

	dmz_unload_zones(zmd);

	dmz_locks_cleanup(zmd);
}

int dmz_ctr_metadata(struct dmz_target *dmz) {
	if (!dmz) {
		return -EINVAL;
	}

	// Set dev and bdev first.
	struct dmz_metadata *zmd;
	struct dmz_dev *dev = dmz->dev;
	int ret;

	zmd = kzalloc(sizeof(struct dmz_metadata), GFP_KERNEL);
	if (!zmd) {
		goto alloc;
	}

	zmd->capacity = dev->capacity;
	zmd->dev = dev;
	zmd->target_bdev = dmz->target_bdev;
	strcpy(zmd->name, dev->name);

	zmd->zone_nr_sectors = dev->nr_zone_sectors;
	zmd->zone_nr_blocks = 1 << DMZ_ZONE_NR_BLOCKS_SHIFT;

	zmd->nr_blocks = dev->nr_zones << DMZ_ZONE_NR_BLOCKS_SHIFT;
	zmd->nr_zones = dev->nr_zones;

	// how many blocks mappings of each zone needs. For example, 256MB zone need 128 Blocks to store mappings.
	zmd->nr_zone_mt_need_blocks = ((zmd->zone_nr_blocks * sizeof(struct dmz_map)) / DMZ_BLOCK_SIZE) + 1;

	// how many blocks bitmap need. For example, 256MB zone need 2 Blocks to store bitmaps.
	zmd->nr_zone_bitmap_need_blocks = ((zmd->zone_nr_blocks >> 3) / DMZ_BLOCK_SIZE) + 1;

	// how many blocks zone structs need.
	zmd->nr_zone_struct_need_blocks = ((zmd->nr_zones * sizeof(struct dmz_zone)) / DMZ_BLOCK_SIZE) + 1;

	ret = dmz_load_metadata(zmd);
	if (ret) {
		goto load_meta;
	}

	dmz->zmd = zmd;
	
	return 0;

load_meta:
	kfree(zmd);
alloc:
	return -1;
}

void dmz_dtr_metadata(struct dmz_metadata *zmd) {
	if (!zmd)
		return;

	kfree(zmd->sblk);

	dmz_unload_metadata(zmd);

	kfree(zmd);
}