#include "dmz.h"

int RESERVED_ZONE_ID = 0;

bool *zone_in_reclaim_queue;
spinlock_t zone_in_reclaim_queue_spin;
unsigned long zone_in_reclaim_queue_spin_flags;

spinlock_t reclaim_spin;

struct workqueue_struct *util_wq;

struct dmz_util_work {
	struct work_struct work;
	unsigned long buffer;
	unsigned long pba;
	struct dmz_metadata *zmd;
	unsigned long m;
};

bool zone_if_in_reclaim_queue(int zone) {
	spin_lock_irqsave(&zone_in_reclaim_queue_spin, zone_in_reclaim_queue_spin_flags);
	bool ret = zone_in_reclaim_queue[zone];
	spin_unlock_irqrestore(&zone_in_reclaim_queue_spin, zone_in_reclaim_queue_spin_flags);
	return ret;
}

void zone_clear_in_reclaim_queue(int zone) {
	spin_lock_irqsave(&zone_in_reclaim_queue_spin, zone_in_reclaim_queue_spin_flags);
	zone_in_reclaim_queue[zone] = false;
	spin_unlock_irqrestore(&zone_in_reclaim_queue_spin, zone_in_reclaim_queue_spin_flags);
}

void zone_set_in_reclaim_queue(int zone) {
	spin_lock_irqsave(&zone_in_reclaim_queue_spin, zone_in_reclaim_queue_spin_flags);
	zone_in_reclaim_queue[zone] = true;
	spin_unlock_irqrestore(&zone_in_reclaim_queue_spin, zone_in_reclaim_queue_spin_flags);
}

bool dmz_zone_ofuse(int zone) {
	if (zone == META_ZONE_ID)
		return 0;

	unsigned long f;
	spin_lock_irqsave(&reclaim_spin, f);
	int ret = 1;
	if (zone == RESERVED_ZONE_ID)
		ret = 0;
	spin_unlock_irqrestore(&reclaim_spin, f);

	return ret;
}

static void set_reserved_zone(int idx, int toset) {
	unsigned long f;
	spin_lock_irqsave(&reclaim_spin, f);
	pr_info("%d, idx: %d, toset: %d\n", RESERVED_ZONE_ID, idx, toset);
	RESERVED_ZONE_ID = toset;
	spin_unlock_irqrestore(&reclaim_spin, f);
}

static unsigned long dmz_reserved_zone_pba_alloc(struct dmz_metadata *zmd, int reserve) {
	struct dmz_zone *zone = zmd->zone_start;
	return ((reserve << DMZ_ZONE_NR_BLOCKS_SHIFT) + zone[reserve].wp);
}

/**
 * @brief When GC has been started, valid blocks should be move to another zone. I need to update mapping, therefore ph
 * 
 * @param{unsigned long} pba 
 * @return{unsigned long} unsigned long 
 */
unsigned long dmz_p2l(struct dmz_metadata *zmd, unsigned long pba) {
	int index = (pba * sizeof(struct dmz_map)) / DMZ_BLOCK_SIZE;
	int offset = (pba * sizeof(struct dmz_map)) % DMZ_BLOCK_SIZE;

	unsigned long buffer = wait_read(zmd, (META_ZONE_ID << DMZ_ZONE_NR_BLOCKS_SHIFT) + index);
	if (!buffer) {
		pr_err("Cache fault.\n");
		return ~0;
	}
	struct dmz_map *map = (struct dmz_map *)(buffer + offset);
	unsigned long ret = map->block_id;
	free_page(buffer);
	return ret;
}

void *dmz_reclaim_read_block(struct dmz_metadata *zmd, unsigned long pba) {
	struct page *page = alloc_page(GFP_KERNEL);
	if (!page)
		goto buffer_alloc;
	unsigned long buffer = (unsigned long)page_address(page);

	struct bio *rbio = bio_alloc(GFP_KERNEL, 1);
	if (!rbio)
		goto bio_alloc;
	bio_set_dev(rbio, zmd->target_bdev);
	rbio->bi_iter.bi_sector = pba << 3;
	bio_add_page(rbio, page, PAGE_SIZE, 0);
	bio_set_op_attrs(rbio, REQ_OP_READ, 0);

	int status = submit_bio_wait(rbio);
	bio_put(rbio);

	if (status) {
		pr_err("REC R ERR %d.", status);
		goto read_err;
	}
	return (void *)buffer;

read_err:
	// unlock_page(page);
bio_alloc:
	free_page(buffer);
buffer_alloc:
	return NULL;
}

int dmz_reclaim_write_block(struct dmz_metadata *zmd, unsigned long pba, unsigned long buffer) {
	struct page *page = virt_to_page(buffer);
	if (!page) {
		goto invalid_kaddr;
	}

	struct bio *wbio = bio_alloc(GFP_KERNEL, 1);
	if (!wbio)
		goto bio_alloc;
	bio_set_dev(wbio, zmd->target_bdev);
	wbio->bi_iter.bi_sector = pba << 3;
	bio_add_page(wbio, page, PAGE_SIZE, 0);
	bio_set_op_attrs(wbio, REQ_OP_WRITE, 0);

	int status = submit_bio_wait(wbio);
	bio_put(wbio);

	if (status) {
		pr_err("REC W ERR %d.", status);
		goto write_err;
	}

	free_page(buffer);

	return status;

write_err:
bio_alloc:
invalid_kaddr:
	return -1;
}

/**
 * @brief Reclaim a zone need to put all valid blocks in other one or several zones which has enough free space.
 * I need to make a bio to do this job.
 * 1. Read block in device into memory.
 * 2. Write memory to device.
 * 
 * @param{struct dm_taregt*} dmz
 * @param{unsigned long} lba 
 * @param{unsigned long} pba 
 * @param{void *func(struct bio*)} endio 
 * @return{struct bio*} a bio waiting to be submitted
 */
int dmz_make_reclaim_bio(struct dmz_target *dmz, unsigned long lba, unsigned long pba, int reserve) {
	struct dmz_metadata *zmd = dmz->zmd;
	int ret = 0;

	unsigned long buffer = (unsigned long)dmz_reclaim_read_block(zmd, pba);
	if (!buffer) {
		goto read_err;
	}

	unsigned long new_pba = dmz_reserved_zone_pba_alloc(zmd, reserve);
	if (dmz_is_default_pba(new_pba)) {
		pr_err("alloc new_pba err.\n");
		goto alloc_err;
	}

	ret = dmz_reclaim_write_block(zmd, new_pba, buffer);
	zmd->zone_start[reserve].wp += 1;

	dmz_write_cache(zmd, lba, new_pba);

	return ret;

alloc_err:
read_err:
	pr_err("reclam bio failed.\n");
	return -1;
}

/*
* Reclaim specified zone.
*/
int dmz_reclaim_zone(struct dmz_target *dmz, int zone) {
	struct dmz_metadata *zmd = dmz->zmd;
	struct dmz_zone *cur_zone = &zmd->zone_start[zone];
	struct dmz_zone *z = zmd->zone_start;
	int ret = 0, errno = 0, idx = 0;
	int cnt = 0, origin_zone = RESERVED_ZONE_ID;

	if (z[zone].weight == z[zone].wp || !z[zone].wp) {
		goto end;
	}

	for (int i = 0; i < zmd->nr_zones; i++)
		dmz_start_io(zmd, i);

rep:
	cnt = 0;
	origin_zone = RESERVED_ZONE_ID;

	if (zone == META_ZONE_ID || zone == RESERVED_ZONE_ID) {
		zone++;
		if (zone >= zmd->nr_zones)
			goto reclaim_bio_err;
		goto rep;
	}

	if (z[origin_zone].wp) {
		pr_err("Not expected %d.\n", origin_zone);
		errno = dmz_reset_zone(zmd, origin_zone);
	}
	if (errno) {
		ret = errno;
		goto reclaim_bio_err;
	}

	for (unsigned long offset = 0; offset < (unsigned long)cur_zone->wp; offset++) {
		if (dmz_test_bit(zmd, (zone << DMZ_ZONE_NR_BLOCKS_SHIFT) + offset)) {
			unsigned long lba = dmz_p2l(zmd, (zone << DMZ_ZONE_NR_BLOCKS_SHIFT) + offset);
			if (dmz_is_default_pba(lba)) {
				continue;
			}

			cnt++;
			int ret = dmz_make_reclaim_bio(dmz, lba, (zone << DMZ_ZONE_NR_BLOCKS_SHIFT) + offset, origin_zone);
			if (ret) {
				pr_err("Mem not enough for reclaim.");
			}
		}
	}

	if ((errno = dmz_reset_zone(zmd, zone))) {
		pr_err("Reset Current Zone %d Failed. Errno: %d", zone, errno);
	}

	set_reserved_zone(idx, zone);

	zone++;
	if (zone < zmd->nr_zones)
		goto rep;

reclaim_bio_err:
	for (int i = zmd->nr_zones - 1; i >= 0; i--)
		dmz_complete_io(zmd, i);
end:
	return ret;
}

void dmz_utilwq_work_process_r(struct work_struct *work) {
	struct dmz_util_work *w = container_of(work, struct dmz_util_work, work);
	w->buffer = (unsigned long)dmz_reclaim_read_block(w->zmd, w->pba);
	clear_bit_unlock(1, &w->m);
	smp_mb__after_atomic();
	wake_up_bit(&w->m, 1);
}

void dmz_utilwq_work_process_w(struct work_struct *work) {
	struct dmz_util_work *w = container_of(work, struct dmz_util_work, work);
	dmz_reclaim_write_block(w->zmd, w->pba, w->buffer);
	clear_bit_unlock(1, &w->m);
	smp_mb__after_atomic();
	wake_up_bit(&w->m, 1);
}

unsigned long wait_read(struct dmz_metadata *zmd, unsigned long pba) {
	struct dmz_util_work *w = kzalloc(sizeof(struct dmz_util_work), GFP_KERNEL);
	w->zmd = zmd;
	w->pba = pba;
	INIT_WORK(&w->work, dmz_utilwq_work_process_r);
	set_bit(1, &w->m);
	queue_work(util_wq, &w->work);
	wait_on_bit_io(&w->m, 1, TASK_UNINTERRUPTIBLE);
	unsigned long buf = w->buffer;
	kfree(w);
	return buf;
}

void wait_write(struct dmz_metadata *zmd, unsigned long pba, unsigned long buffer) {
	struct dmz_util_work *w = kzalloc(sizeof(struct dmz_util_work), GFP_KERNEL);
	w->zmd = zmd;
	w->buffer = buffer;
	w->pba = pba;
	INIT_WORK(&w->work, dmz_utilwq_work_process_w);
	set_bit(1, &w->m);
	queue_work(util_wq, &w->work);
	wait_on_bit_io(&w->m, 1, TASK_UNINTERRUPTIBLE);
	kfree(w);
	return;
}

void dmz_load_reclaim(struct dmz_metadata *zmd) {
	spin_lock_init(&reclaim_spin);
	spin_lock_init(&zone_in_reclaim_queue_spin);
	zone_in_reclaim_queue = kzalloc(zmd->nr_zones * sizeof(bool), GFP_KERNEL);

	util_wq = alloc_workqueue("dmz-util-wq", WQ_MEM_RECLAIM | WQ_UNBOUND, 0);

	for (int i = 0; i < zmd->zone_nr_blocks; i++) {
		struct page *p = alloc_page(GFP_KERNEL);
		memset(page_address(p), ~0, DMZ_BLOCK_SIZE);
		dmz_reclaim_write_block(zmd, (META_ZONE_ID << DMZ_ZONE_NR_BLOCKS_SHIFT) + i, (unsigned long)page_address(p));
	}
}