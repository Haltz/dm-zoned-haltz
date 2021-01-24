/**
 * @file sdmz.h
 * @author Haltz (mudong.huang@gmail.com;huangdong.mu@outlook.com)
 * @brief Simple Device Mapper Zoned.
 * @version 0.1
 * @date 2020-12-23
 * 
 * @copyright Copyright (c) 2020
 * 
 */
#ifndef DMZ_H
#define DMZ_H

#include <linux/types.h>
#include <linux/blkdev.h>
#include <linux/device-mapper.h>
#include <linux/dm-kcopyd.h>
#include <linux/list.h>
#include <linux/spinlock.h>
#include <linux/mutex.h>
#include <linux/workqueue.h>
#include <linux/rwsem.h>
#include <linux/rbtree.h>
#include <linux/radix-tree.h>
#include <linux/shrinker.h>
#include <linux/module.h>
#include <linux/bio.h>
#include <linux/log2.h>

#define KB (1 << 10)
#define MB (1 << 20)
#define GB (1 << 30)

#define BDEVNAME_SIZE 32
#define DEV_CAPACITY 2 << 30;
#define ZONE_SIZE (256 * MB)
#define DEVICE_NAME_SIZE 512

/*
 * dm-zoned creates block devices with 4KB blocks, always.
 */
#define DMZ_BLOCK_SHIFT 12
#define DMZ_BLOCK_SIZE (1 << DMZ_BLOCK_SHIFT)
#define DMZ_BLOCK_MASK (DMZ_BLOCK_SIZE - 1)

#define DMZ_BLOCK_SHIFT_BITS (DMZ_BLOCK_SHIFT + 3)
#define DMZ_BLOCK_SIZE_BITS (1 << DMZ_BLOCK_SHIFT_BITS)
#define DMZ_BLOCK_MASK_BITS (DMZ_BLOCK_SIZE_BITS - 1)

#define DMZ_BLOCK_SECTORS_SHIFT (DMZ_BLOCK_SHIFT - SECTOR_SHIFT)
#define DMZ_BLOCK_SECTORS (DMZ_BLOCK_SIZE >> SECTOR_SHIFT)
#define DMZ_BLOCK_SECTORS_MASK (DMZ_BLOCK_SECTORS - 1)

#define DMZ_ZONE_BLOCKS_SHIFT 16

/*
 * 4KB block <-> 512B sector conversion.
 */
#define dmz_blk2sect(b) ((sector_t)(b) << DMZ_BLOCK_SECTORS_SHIFT)
#define dmz_sect2blk(s) ((sector_t)(s) >> DMZ_BLOCK_SECTORS_SHIFT)

#define dmz_bio_block(bio) dmz_sect2blk((bio)->bi_iter.bi_sector)
#define dmz_bio_blocks(bio) dmz_sect2blk(bio_sectors(bio))

#define dmz_start_sector(dmz) dmz_blk2sect((dmz)->zmd->useable_start)
#define dmz_start_block(dmz) ((dmz)->zmd->useable_start)

#define DMZ_MIN_BIOS 8192

/*
 * Super block information (one per metadata set).
 */
struct dmz_sb {
	sector_t block;
	struct dmz_dev *dev;
	struct dmz_mblk *mblk;
	struct dmz_super *sb;
	struct dm_zone *zone;
};

struct dmz_metadata {
	struct dmz_dev *dev;

	u64 capacity;
	char name[BDEVNAME_SIZE];

	sector_t mapping_size;
	sector_t bitmap_size;

	sector_t nr_zones;
	sector_t nr_blocks;

	sector_t zone_nr_sectors;
	sector_t zone_nr_blocks;

	sector_t nr_maps;

	sector_t nr_map_blocks;
	sector_t nr_bitmap_blocks;

	sector_t sb_block;
	sector_t map_block;
	sector_t bitmap_block;

	struct dmz_super *sb;
	struct dmz_sb super_block_info;

	struct dm_zone *zone_start;
	struct dmz_map *map_start;
	unsigned long *bitmap_start;

	// first useable block number.
	unsigned long useable_start;
};

struct dmz_map {
	__le64 block_id;
};

struct dmz_dev {
	struct block_device *bdev;

	char name[BDEVNAME_SIZE];

	sector_t capacity;

	unsigned int nr_zones;
	sector_t nr_zone_sectors;
};

/*
 * Target descriptor.
 */
struct dmz_target {
	struct dm_dev *ddev;
	struct dmz_dev *dev;

	struct dmz_metadata *zmd;

	unsigned int flags;

	// if we want to clone bios, bio_set is neccessary.
	struct bio_set bio_set;
};

struct dm_zone {
	// struct dmz_dev *dev;
	// unsigned long flags;
	// atomic_t refcount;
	// unsigned int id;
	unsigned int wp;
	unsigned int weight;
	// unsigned int physical_zone;
	unsigned long *bitmap;
};

int dmz_ctr_metadata(struct dmz_target *);
void dmz_dtr_metadata(struct dmz_metadata *);

int dmz_ctr_reclaim(void);

#endif
