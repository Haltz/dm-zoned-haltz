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
#ifndef _DMZ_H_
#define _DMZ_H_

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
#include <linux/blk-mq.h>
#include <linux/workqueue.h>
#include <linux/delay.h>

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

#define DMZ_ZONE_NR_BLOCKS_SHIFT (16)
#define DMZ_ZONE_NR_BLOCKS_MASK ((1 << DMZ_ZONE_NR_BLOCKS_SHIFT) - 1)

#define DMZ_MAP_CACHE_SIZE (8)

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

#define DMZ_IS_SEQ(zone) ((zone)->type == DMZ_ZONE_SEQ)
#define DMZ_IS_RND(zone) ((zone)->type == DMZ_ZONE_RND)

#define DMZ_MIN_BIOS 8192

enum DMZ_STATUS { DMZ_BLOCK_FREE, DMZ_BLOCK_INVALID, DMZ_BLOCK_VALID };
enum DMZ_ZONE_TYPE { DMZ_ZONE_NONE, DMZ_ZONE_SEQ, DMZ_ZONE_RND };
enum DMZ_ZONE_STATUS { DMZ_ZONE_FREE, DMZ_ZONE_RECLAIM };

extern int RESERVED_ZONE_ID;
extern int META_ZONE_ID;

extern spinlock_t reclaim_spin;

// Why name it sad? Because I'm sad when i'm writing it.
extern struct workqueue_struct *sad_wq;

struct sad_work {
	struct work_struct work;
	struct mutex *m;
};

struct dmz_cache_node {
	struct dmz_cache_node *prev, *next;
	unsigned long lba, pba;
};

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
	struct block_device *target_bdev;

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
	struct mutex reclaim_lock;
	struct mutex freezone_lock;

	struct radix_tree_root cache;
	struct dmz_cache_node *cache_head;
	struct dmz_cache_node *cache_tail;
	unsigned long cache_size;

	struct workqueue_struct *reclaim_wq;
};

/**
 * @brief Each work do a single bio job.
 * 
 */
struct dmz_write_work {
	struct work_struct work;
	struct block_device *bdev;
	struct bio *bio;
};

struct dmz_reclaim_work {
	struct work_struct work;
	struct block_device *bdev;
	struct dmz_target *dmz;
	int zone;
};

struct dmz_resubmit_work {
	struct work_struct work;
	struct bio *bio;
	struct dmz_target *dmz;
};

/** Note: sizeof(struct dmz_map) must be power of 2 to make sure block_size is aligned to sizeof(struct dmz_map) **/
struct dmz_map {
	unsigned long block_id;
};

struct dmz_dev {
	struct block_device *bdev;

	char name[BDEVNAME_SIZE];
	char major_minor_id[16];

	unsigned long capacity;

	unsigned int nr_zones;
	unsigned long nr_zone_sectors;

	struct request_queue *queue;
	struct gendisk *disk;

	struct blk_mq_tag_set set;
};

/*
 * Target descriptor.
 */
struct dmz_target {
	struct dmz_dev *dev;

	struct dmz_metadata *zmd;

	unsigned int flags;

	struct block_device *target_bdev;

	// if we want to clone bios, bio_set is neccessary.
	struct bio_set bio_set;

	refcount_t ref;
};

/** make sure size is power of 2 in order to fit one block size. **/
struct dmz_zone {
	unsigned int wp; // 4
	unsigned int weight; // 4
	unsigned long *bitmap; // 8

	int type; // 4
	unsigned long status;

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

	// lock for wp
	spinlock_t lock; // 4

	// lock for io
	struct mutex io_lock; // 32
	// lock for mapping and bitmap
	struct mutex map_lock; // 32

	struct workqueue_struct *write_wq; // 8
};

void dmz_load_reclaim(struct dmz_metadata *zmd);
int dmz_reclaim_zone(struct dmz_target *dmz, int zone);

unsigned long dmz_get_map(struct dmz_metadata *zmd, unsigned long lba);

int dmz_pba_alloc(struct dmz_target *dmz);
unsigned long dmz_reclaim_pba_alloc(struct dmz_target *dmz, int reclaim_zone);

int dmz_map(struct dmz_target *dmz, struct bio *bio);
unsigned long dmz_l2p(struct dmz_target *dmz, sector_t lba);

void dmz_reclaim_work_process(struct work_struct *work);
void *dmz_reclaim_read_block(struct dmz_metadata *zmd, unsigned long pba);
int dmz_reclaim_write_block(struct dmz_metadata *zmd, unsigned long pba, unsigned long buffer);
bool zone_if_in_reclaim_queue(int zone);
void zone_clear_in_reclaim_queue(int zone);
void zone_set_in_reclaim_queue(int zone);
bool dmz_zone_ofuse(int zone);

unsigned long wait_read(struct dmz_metadata *zmd, unsigned long pba);
void wait_write(struct dmz_metadata *zmd, unsigned long pba, unsigned long buffer);

/** functions defined in dmz-metadata.h depends on structs defined above. **/
#include "dmz-metadata.h"
#include "dmz-utils.h"

#endif
