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
	int index = pba / zmd->zone_nr_blocks;
	int offset = pba % zmd->zone_nr_blocks;

	struct dmz_zone *zone = &zmd->zone_start[index];
	return (unsigned long)zone->reverse_mt[offset].block_id;
}

void dmz_reclaim_write_bio_endio(struct bio *bio) {
	struct bvec_iter_all iter_all;
	struct bio_vec *bv = bio->bi_io_vec;
	struct page *page = bv->bv_page;
	unsigned long buffer = page_address(page);
	struct page *private = bio->bi_private;

	// read failed.
	if (bio->bi_status) {
		set_page_dirty(private);
		SetPageError(private);
	}
	end_page_writeback(private);
	if (PageLocked(private)) {
		unlock_page(private);
	}

	bio_put(bio);
}

void dmz_reclaim_read_bio_endio(struct bio *bio) {
	struct bvec_iter_all iter_all;
	struct bio_vec *bv = bio->bi_io_vec;
	struct page *page = bv->bv_page;
	unsigned long buffer = page_address(page);
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

unsigned long dmz_reclaim_read_block(struct dmz_target *dmz, unsigned long pba) {
	struct dmz_metadata *zmd = dmz->zmd;
	struct page *page = alloc_page(GFP_ATOMIC);
	if (!page)
		goto buffer_alloc;
	unsigned long buffer = page_address(page);

	struct bio *rbio = bio_alloc(GFP_ATOMIC, 1);
	if (!rbio)
		goto bio_alloc;
	bio_set_dev(rbio, zmd->dev->bdev);
	rbio->bi_iter.bi_sector = pba << 3;
	rbio->bi_private = page;
	// Here PAGE_SIZE = DMZ_BLOCK_SIZE = 4KB
	bio_add_page(rbio, page, PAGE_SIZE, 0);
	bio_set_op_attrs(rbio, REQ_OP_READ, 0);
	int err = lock_page_killable(page);
	if (unlikely(err)) {
		pr_err("Lock failed before read.\n");
	}
	ClearPageUptodate(page);
	rbio->bi_end_io = dmz_reclaim_read_bio_endio;

	submit_bio(rbio);
	err = lock_page_killable(page);
	if (unlikely(err)) {
		pr_err("Lock failed after read.\n");
	}
	if (!PageUptodate(page)) {
		pr_err("Read page err.\n");
		goto read_err;
	}
	unlock_page(page);

	return buffer;

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

	struct bio *wbio = bio_alloc(GFP_ATOMIC, 1);
	if (!wbio)
		goto bio_alloc;
	bio_set_dev(wbio, zmd->dev->bdev);
	wbio->bi_iter.bi_sector = pba << 3;
	wbio->bi_private = page;
	// Here PAGE_SIZE = DMZ_BLOCK_SIZE = 4KB
	bio_add_page(wbio, page, PAGE_SIZE, 0);
	bio_set_op_attrs(wbio, REQ_OP_WRITE, 0);
	set_page_writeback(page);
	wbio->bi_end_io = dmz_reclaim_write_bio_endio;
	submit_bio(wbio);
	wait_on_page_writeback(page);

	if (PageDirty(page)) {
		goto write_err;
	}

	return 0;

write_err:
	return 1;
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
	unsigned long flags;

	for (int i = 0; i < zmd->nr_zones; i++) {
		if (i == reclaim_zone)
			continue;
		// spin_lock_irqsave(&zmd->meta_lock, flags);
		if (zmd->zone_start[i].wp < zmd->zone_nr_blocks) {
			// spin_unlock_irqrestore(&zmd->meta_lock, flags);
			return i * zmd->zone_nr_blocks + zmd->zone_start[i].wp;
		}
		// spin_unlock_irqrestore(&zmd->meta_lock, flags);
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

	int index = lba / zmd->zone_nr_blocks, offset = lba % zmd->zone_nr_blocks;
	// pr_info("%x, %x, %x\n", lba, index, offset);
	unsigned long pba = zmd->zone_start[index].mt[offset].block_id;

	unsigned long buffer = dmz_reclaim_read_block(dmz, pba);
	if (!buffer) {
		goto read_err;
	}

	unsigned long new_pba = dmz_reclaim_pba_alloc(dmz, pba / zmd->zone_nr_blocks);
	if (dmz_is_default_pba(new_pba)) {
		pr_err("alloc new_pba err.\n");
		goto alloc_err;
	}
	ret = dmz_reclaim_write_block(dmz, new_pba, buffer);
	if (!ret) {
		zmd->zone_start[new_pba / zmd->zone_nr_blocks].wp += 1;
	}

	if (!ret) {
		dmz_update_map(dmz, lba, new_pba);
	}

	free_page(buffer);

	return ret;

alloc_err:
read_err:
	return -1;
}

/*
* Reclaim specified zone.
*/
int dmz_reclaim_zone(struct dmz_target *dmz, int zone) {
	struct dmz_metadata *zmd = dmz->zmd;
	unsigned long flags, sgth_flags;
	unsigned long remain = 0;
	struct dmz_zone *cur_zone = &zmd->zone_start[zone];
	int ret = 0, locked = 0;

	if (!spin_is_locked(&dmz->single_thread_lock)) {
		locked = 0;
		spin_lock_irqsave(&dmz->single_thread_lock, sgth_flags);
	} else {
		locked = 1;
	}

	unsigned long *bitmap = cur_zone->bitmap;
	unsigned int wp = cur_zone->wp;
	struct dmz_map *map = cur_zone->mt;

	// for (unsigned long offset = 0; offset < zmd->zone_nr_blocks; offset++) {
	for (unsigned long offset = 0; offset < zmd->zone_nr_blocks; offset++) {
		unsigned long valid_bitmap = bitmap_get_value8(bitmap, offset);

		// First bit indicates blocks at this offset is valid or not.
		// Because bitmap_get_value8 is based on sizeof(unsigned long), so when get n (n>=56) bit value,
		// value got in form of bits is n.n+1.n+2...63th bit, meaning can't always get all 8bit as return value.
		unsigned int size = sizeof(unsigned long) << 3;
		int shift = ((offset % size) + 8 > size) ? size - 1 - (offset % size) : 7;
		if (valid_bitmap & (0x1 << shift)) {
			unsigned long pba = dmz_p2l(zmd, zone * zmd->zone_nr_blocks + offset);
			if (dmz_is_default_pba(pba)) {
				pr_err("pba is error(default value means no such pba).\n");
				goto pba_err;
			}
			int ret = dmz_make_reclaim_bio(dmz, pba);
			if (ret) {
				goto reclaim_bio_err;
			}
		}
	}

	cur_zone->wp = 0;

	if (DMZ_IS_SEQ(cur_zone)) {
		ret = blkdev_zone_mgmt(zmd->dev->bdev, REQ_OP_ZONE_RESET, zmd->zone_nr_sectors * zone, zmd->zone_nr_sectors, GFP_NOIO);
	} else {
	}

	if (ret)
		pr_err("blkdev_zone_mgmt errcode %d\n", ret);

	if (locked)
		spin_unlock_irqrestore(&dmz->single_thread_lock, sgth_flags);

	return 0;

reclaim_bio_err:
	spin_unlock_irqrestore(&dmz->single_thread_lock, sgth_flags);
	return -3;
pba_err:
	spin_unlock_irqrestore(&dmz->single_thread_lock, sgth_flags);
	return -2;
make_bio:
	spin_unlock_irqrestore(&dmz->single_thread_lock, sgth_flags);
	return -1;
}