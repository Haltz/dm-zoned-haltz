#include "dmz.h"

int dmz_ctr_reclaim(void) {
	return 0;
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

void dmz_reclaim_read_bio_endio(struct bio *bio) {
	struct bio_vec *bv = bio->bi_io_vec;
	struct page *page = bv->bv_page;
	struct page *private = bio->bi_private;

	// read failed.
	if (bio->bi_status) {
		ClearPageUptodate(private);
		SetPageError(private);
	} else {
		SetPageUptodate(private);
	}
	if (PageLocked(page)) {
		unlock_page(page);
	}

	bio_put(bio);
}

void *dmz_reclaim_read_block(struct dmz_target *dmz, unsigned long pba) {
	struct dmz_metadata *zmd = dmz->zmd;
	struct page *page = alloc_page(GFP_KERNEL);
	if (!page)
		goto buffer_alloc;
	unsigned long buffer = (unsigned long)page_address(page);

	struct bio *rbio = bio_alloc(GFP_KERNEL, 1);
	if (!rbio)
		goto bio_alloc;
	bio_set_dev(rbio, zmd->target_bdev);
	rbio->bi_iter.bi_sector = pba << 3;
	rbio->bi_private = page;
	bio_add_page(rbio, page, PAGE_SIZE, 0);
	bio_set_op_attrs(rbio, REQ_OP_READ, 0);
	lock_page(page);

	ClearPageUptodate(page);
	rbio->bi_end_io = dmz_reclaim_read_bio_endio;

	submit_bio(rbio);

	lock_page(page);

	if (!PageUptodate(page)) {
		pr_err("Read page err.\n");
		goto read_err;
	}
	unlock_page(page);

	return (void *)buffer;

read_err:
	unlock_page(page);
bio_alloc:
	free_page(buffer);
buffer_alloc:
	return NULL;
}

int dmz_reclaim_write_block(struct dmz_target *dmz, unsigned long pba, unsigned long buffer) {
	struct dmz_metadata *zmd = dmz->zmd;
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
 * @brief return zone_id which free block is available
 * Note: Here zone under reclaim should not be chosen as target zone.
 * 
 * @param dmz 
 * @return int 
 */
unsigned long dmz_reclaim_pba_alloc(struct dmz_target *dmz, int reclaim_zone) {
	struct dmz_metadata *zmd = dmz->zmd;
	struct dmz_zone *zone = zmd->zone_start;

	for (int i = 0; i < zmd->nr_zones; i++) {
		if (i == reclaim_zone)
			continue;

		if (zone[i].wp < zmd->zone_nr_blocks) {
			return ((i << DMZ_ZONE_NR_BLOCKS_SHIFT) + zmd->zone_start[i].wp);
		}
	}

	return ~0;
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
int dmz_make_reclaim_bio(struct dmz_target *dmz, unsigned long lba) {
	struct dmz_metadata *zmd = dmz->zmd;
	int ret = 0;

	int index = lba >> DMZ_ZONE_NR_BLOCKS_SHIFT, offset = lba & DMZ_ZONE_NR_BLOCKS_MASK;
	unsigned long pba = zmd->zone_start[index].mt[offset].block_id;

	unsigned long buffer = (unsigned long)dmz_reclaim_read_block(dmz, pba);
	if (!buffer) {
		goto read_err;
	}

	unsigned long new_pba = dmz_reclaim_pba_alloc(dmz, pba >> DMZ_ZONE_NR_BLOCKS_SHIFT);
	if (dmz_is_default_pba(new_pba)) {
		pr_err("alloc new_pba err.\n");
		goto alloc_err;
	}

	// pr_info("READ %ld WRITE %ld", pba, new_pba);

	ret = dmz_reclaim_write_block(dmz, new_pba, buffer);
	zmd->zone_start[new_pba >> DMZ_ZONE_NR_BLOCKS_SHIFT].wp += 1;

	if (!ret) {
		dmz_update_map(dmz, lba, new_pba);
	} else {
		pr_err("WRITE ERR P MEM.");
	}

	free_page(buffer);

	return ret;

alloc_err:
read_err:
	pr_err("DMZ_MAKE_RECLAIM_BIO\n");
	return -1;
}

/*
* Reclaim specified zone.
*/
// TODO support flush (seems no need, because all metadata is in memory)
int dmz_reclaim_zone(struct dmz_target *dmz, int zone) {
	pr_info("Reclaim Zone %d.\n", zone);
	struct dmz_metadata *zmd = dmz->zmd;
	struct dmz_zone *cur_zone = &zmd->zone_start[zone];
	int ret = 0;
	// dmz_check_zones(zmd);

	// make sure only one zone is under reclaim process.
	dmz_lock_reclaim(zmd);

	// suspend all other zones.
	for (int i = 0; i < zmd->nr_zones; i++) {
		dmz_start_io(zmd, i);
	}

	unsigned long *bitmap = cur_zone->bitmap;

	for (unsigned long offset = 0; offset < (unsigned long)cur_zone->wp; offset++) {
		unsigned long valid_bitmap = bitmap_get_value8(bitmap, offset);

		// First bit indicates blocks at this offset is valid or not.
		// Because bitmap_get_value8 is based on sizeof(unsigned long), so when get n (n>=56) bit value,
		// value got in form of bits is n.n+1.n+2...63th bit, meaning can't always get all 8bit as return value.
		unsigned int size = sizeof(unsigned long) << 3;
		int shift = ((offset % size) + 8 > size) ? size - 1 - (offset % size) : 7;
		if (valid_bitmap & (0x1 << shift)) {
			unsigned long lba = dmz_p2l(zmd, (zone << DMZ_ZONE_NR_BLOCKS_SHIFT) + offset);
			if (dmz_is_default_pba(lba)) {
				// Here means that it is blk stores mt or rmt or bitmap. I have not update bitmap for them.
				// Just ignore them because we have to flush them again before unload device.
				continue;
			}

			int ret = dmz_make_reclaim_bio(dmz, lba);
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

	cur_zone->wp = 0;

	if (DMZ_IS_SEQ(cur_zone)) {
		if (blkdev_zone_mgmt(zmd->target_bdev, REQ_OP_ZONE_RESET, zone << DMZ_ZONE_NR_BLOCKS_SHIFT << 3, 1 << DMZ_ZONE_NR_BLOCKS_SHIFT << 3, GFP_KERNEL))
			pr_err("blkdev_zone_mgmt errcode %d\n", ret);
	}

reclaim_bio_err:
	for (int i = 0; i < zmd->nr_zones; i++) {
		dmz_complete_io(zmd, i);
	}

	dmz_unlock_reclaim(zmd);
	pr_info("End Reclaim Zone %d.\n", zone);
	return ret;
}