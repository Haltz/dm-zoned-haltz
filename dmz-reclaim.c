#include "dmz.h"

int RESERVED_ZONE_ID = 0, RESERVED_ZONE_ID_BACK = 1, RESERVED_ZONE_ID_MORE2 = 2, RESERVED_ZONE_ID_MORE3 = 3, IDX_RESERVED_ZONE = 0;

bool *zone_in_reclaim_queue;
spinlock_t zone_in_reclaim_queue_spin;
unsigned long zone_in_reclaim_queue_spin_flags;

spinlock_t reclaim_spin;
unsigned long reclaim_spin_flags;

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

static int wait_free_reserved_zone(struct dmz_metadata *zmd) {
	spin_lock_irqsave(&reclaim_spin, reclaim_spin_flags);
	int ret;
	switch (IDX_RESERVED_ZONE) {
	case 0:
		ret = RESERVED_ZONE_ID;
		break;
	case 1:
		ret = RESERVED_ZONE_ID_BACK;
		break;
	case 2:
		ret = RESERVED_ZONE_ID_MORE2;
		break;
	case 3:
		ret = RESERVED_ZONE_ID_MORE3;
		break;
	}
	IDX_RESERVED_ZONE++;
	IDX_RESERVED_ZONE %= 4;
	spin_unlock_irqrestore(&reclaim_spin, reclaim_spin_flags);

	pr_info("Choose %d as rec zone.\n", ret);
	dmz_start_io(zmd, ret);
	return ret;
}

static void set_reserved_zone(int origin, int toset) {
	spin_lock_irqsave(&reclaim_spin, reclaim_spin_flags);
	if (origin == RESERVED_ZONE_ID)
		RESERVED_ZONE_ID = toset;
	if (origin == RESERVED_ZONE_ID_BACK)
		RESERVED_ZONE_ID_BACK = toset;
	if (origin == RESERVED_ZONE_ID_MORE2)
		RESERVED_ZONE_ID_MORE2 = toset;
	if (origin == RESERVED_ZONE_ID_MORE3)
		RESERVED_ZONE_ID_MORE2 = toset;
	pr_info("%d %d %d %d, origin: %d, toset: %d\n", RESERVED_ZONE_ID, RESERVED_ZONE_ID_BACK, RESERVED_ZONE_ID_MORE2, RESERVED_ZONE_ID_MORE3, origin, toset);
	spin_unlock_irqrestore(&reclaim_spin, reclaim_spin_flags);
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
	int index = pba >> DMZ_ZONE_NR_BLOCKS_SHIFT;
	int offset = pba & DMZ_ZONE_NR_BLOCKS_MASK;

	struct dmz_zone *zone = &zmd->zone_start[index];
	return (unsigned long)zone->reverse_mt[offset].block_id;
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
int dmz_make_reclaim_bio(struct dmz_target *dmz, unsigned long lba, int reserve) {
	struct dmz_metadata *zmd = dmz->zmd;
	int ret = 0;

	int index = lba >> DMZ_ZONE_NR_BLOCKS_SHIFT, offset = lba & DMZ_ZONE_NR_BLOCKS_MASK;
	unsigned long pba = zmd->zone_start[index].mt[offset].block_id;

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

	if (!ret) {
		dmz_update_map(dmz, lba, new_pba);
	} else {
		pr_err("WRITE ERR P MEM.");
	}

	free_page(buffer);

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
	int ret = 0, errno = 0;

	if (!dmz_zone_ofuse(zone)) {
		goto end;
	}

	if (z[zone].weight == z[zone].wp) {
		goto end;
	}

	int cnt = 0, origin_zone = wait_free_reserved_zone(zmd);

	// for (int i = 0; i < zmd->nr_zones; i++)
	// 	dmz_start_io(zmd, i);
	dmz_start_io(zmd, zone);

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
			int ret = dmz_make_reclaim_bio(dmz, lba, origin_zone);
			if (ret) {
				struct dmz_reclaim_work *rcw = kzalloc(sizeof(struct dmz_reclaim_work), GFP_KERNEL);
				if (rcw) {
					rcw->bdev = zmd->target_bdev;
					rcw->zone = zone;
					rcw->dmz = dmz;
					INIT_WORK(&rcw->work, dmz_reclaim_work_process);
					queue_work(zmd->reclaim_wq, &rcw->work);
				} else {
					pr_err("Mem not enough for reclaim.");
				}
				goto reclaim_bio_err;
			}
		}
	}

	if ((errno = dmz_reset_zone(zmd, zone))) {
		pr_err("Reset Current Zone %d Failed. Errno: %d", zone, errno);
	}

	set_reserved_zone(origin_zone, zone);

reclaim_bio_err:
	// for (int i = zmd->nr_zones - 1; i >= 0; i--)
	// 	dmz_complete_io(zmd, i);
	dmz_complete_io(zmd, zone);
	dmz_complete_io(zmd, origin_zone);

	zone_clear_in_reclaim_queue(zone);

end:
	return ret;
}

void dmz_load_reclaim(struct dmz_metadata *zmd) {
	spin_lock_init(&reclaim_spin);
	spin_lock_init(&zone_in_reclaim_queue_spin);
	zone_in_reclaim_queue = kzalloc(zmd->nr_zones * sizeof(bool), GFP_KERNEL);
}