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
#define MAX_NR_BLOCKS_ONCE_READ 64

/*
 * dm-zoned creates block devices with 4KB blocks, always.
 */
#define DMZ_BLOCK_SHIFT (12)
#define DMZ_BLOCK_SIZE (1 << DMZ_BLOCK_SHIFT)
#define DMZ_BLOCK_MASK (DMZ_BLOCK_SIZE - 1)

#define DMZ_BLOCK_SHIFT_BITS (DMZ_BLOCK_SHIFT + 3)
#define DMZ_BLOCK_SIZE_BITS (1 << DMZ_BLOCK_SHIFT_BITS)
#define DMZ_BLOCK_MASK_BITS (DMZ_BLOCK_SIZE_BITS - 1)

#define DMZ_BLOCK_SECTORS_SHIFT (DMZ_BLOCK_SHIFT - SECTOR_SHIFT)
#define DMZ_BLOCK_SECTORS (DMZ_BLOCK_SIZE >> SECTOR_SHIFT)
#define DMZ_BLOCK_SECTORS_MASK (DMZ_BLOCK_SECTORS - 1)

#define DMZ_ZONE_BLOCKS_SHIFT (16)

/*
 * 4KB block <-> 512B sector conversion.
 */
#define dmz_blk2sect(b) ((sector_t)(b) << DMZ_BLOCK_SECTORS_SHIFT)
#define dmz_sect2blk(s) ((sector_t)(s) >> DMZ_BLOCK_SECTORS_SHIFT)

#define dmz_bio_block(bio) dmz_sect2blk((bio)->bi_iter.bi_sector)
#define dmz_bio_blocks(bio) dmz_sect2blk(bio_sectors(bio))

#define dmz_start_sector(dmz) dmz_blk2sect((dmz)->zmd->useable_start)
#define dmz_start_block(dmz) ((dmz)->zmd->useable_start)

#define dmz_is_valid_blkid(blk_id) (~blk_id)

// default pba which value is 0x ffff ffff ffff ffff indicates that lba is not wrote yet
#define dmz_is_default_pba(pba) (!(~pba))

#define DMZ_IS_SEQ(zone) (zone->type == DMZ_ZONE_SEQ)
#define DMZ_IS_RND(zone) (zone->type == DMZ_ZONE_RND)

#define DMZ_MIN_BIOS 8192

enum DMZ_STATUS { DMZ_BLOCK_FREE, DMZ_BLOCK_INVALID, DMZ_BLOCK_VALID };
enum DMZ_ZONE_TYPE { DMZ_ZONE_NONE, DMZ_ZONE_SEQ, DMZ_ZONE_RND };

struct dmz_super {
	__u64 magic; // 8

	__u64 zones_info; // 8;

	__u8 dmz_uuid[16];

	__u8 dev_uuid[16];

	__u8 dmz_label[32];

	__u8 reserved[432];
};

struct dmz_metadata {
	struct dmz_dev *dev;

	unsigned long capacity;
	char name[BDEVNAME_SIZE];

	unsigned long mapping_size;
	unsigned long bitmap_size;

	unsigned long nr_zones;
	unsigned long nr_blocks;

	unsigned long zone_nr_sectors;
	unsigned long zone_nr_blocks;

	unsigned long nr_map_blocks;
	unsigned long nr_bitmap_blocks;

	int nr_zone_mt_need_blocks;
	int nr_zone_bitmap_need_blocks;
	int nr_zone_struct_need_blocks;

	struct dmz_super *sblk;

	struct dmz_zone *zone_start;
	unsigned long *bitmap_start;

	int useable_start;

	// locks
	spinlock_t meta_lock;
	spinlock_t maptable_lock;
	spinlock_t bitmap_lock;
};

/** Note: sizeof(struct dmz_map) must be power of 2 to make sure block_size is aligned to sizeof(struct dmz_map) **/
struct dmz_map {
	unsigned long block_id;
};

struct dmz_dev {
	struct block_device *bdev;

	char name[BDEVNAME_SIZE];

	unsigned long capacity;

	unsigned int nr_zones;
	unsigned long nr_zone_sectors;
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

	// This lock is to simplify development of demo by making program single-thread.
	// Disable it when pipeline is good.
	spinlock_t single_thread_lock;
};

struct dmz_zone {
	unsigned int wp; // 4
	unsigned int weight; // 4
	unsigned long *bitmap; // 8

	int type; // 4

	// Mapping Table
	struct dmz_map *mt; // 8
	// Reverse Mapping Table，when block store mappings(which has no lba), store corresponding zone.
	struct dmz_map *reverse_mt; // 8

	// mt block pbn
	unsigned long mt_blk_n; // 8
	// reverse mt block pbn
	unsigned long rmt_blk_n; // 8
	// bitmap block pbn
	unsigned long bitmap_blk_n; // 8

	// make sure size is power of 2
	u8 reserved[4];
};

int dmz_ctr_metadata(struct dmz_target *);
void dmz_dtr_metadata(struct dmz_metadata *);
int dmz_ctr_reclaim(void);
int dmz_reclaim_zone(struct dmz_target *dmz, int zone);

unsigned long dmz_get_map(struct dmz_metadata *zmd, unsigned long lba);
void dmz_update_map(struct dmz_target *dmz, unsigned long lba, unsigned long pba);

int dmz_pba_alloc(struct dmz_target *dmz);
unsigned long dmz_reclaim_pba_alloc(struct dmz_target *dmz, int reclaim_zone);

int dmz_flush(struct dmz_target *dmz);

#endif
