#include "dmz.h"

int dmz_ctr_reclaim(void) {
	return 0;
}

// TODO
/**
 * @brief When GC has been started, valid blocks should be move to another zone. I need to update mapping, therefore ph
 * 
 * @param{unsigned long} pba 
 * @return{unsigned long} unsigned long 
 */
unsigned long dmz_p2l(unsigned long pba) {
	return 0;
}

void dmz_reclaim_write_bio_endio(struct bio *bio) {
	struct bvec_iter_all iter_all;
	struct bio_vec *bv = bio->bi_io_vec;
	struct page *page = bv->bv_page;
	unsigned long buffer = page_address(page);

	// read failed.
	if (bio->bi_status) {
		set_page_dirty(page);
		SetPageError(page);
	}
	end_page_writeback(page);
	if (PageLocked(page)) {
		unlock_page(page);
	}

	if (buffer) {
		free_page(buffer);
	}

	bio_put(bio);
}

void dmz_reclaim_read_bio_endio(struct bio *bio) {
	struct bvec_iter_all iter_all;
	struct bio_vec *bv = bio->bi_io_vec;
	struct page *page = bv->bv_page;
	unsigned long buffer = page_address(page);

	// read failed.
	if (bio->bi_status) {
		ClearPageUptodate(page);
		SetPageError(page);
	} else {
		SetPageUptodate(page);
	}
	if (PageLocked(page)) {
		unlock_page(page);
	}

	bio_put(bio);
}

unsigned long dmz_reclaim_read_block(struct dmz_target *dmz, unsigned long pba) {
	unsigned long buffer = __get_free_page(GFP_ATOMIC);
	if (!buffer) {
		goto buffer_alloc;
	}
	struct page *page = virt_to_page(buffer);

	struct bio *rbio = bio_alloc(GFP_ATOMIC, 1);
	if (!rbio)
		goto bio_alloc;
	bio_set_dev(rbio, dmz->dev->bdev);
	rbio->bi_iter.bi_sector = pba << 3;
	rbio->bi_iter.bi_size = DMZ_BLOCK_SIZE;
	rbio->bi_private = dmz;
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
bio_alloc:
	free_page(buffer);
buffer_alloc:
	return NULL;
}

int dmz_reclaim_write_block(struct dmz_target *dmz, unsigned long pba, unsigned long buffer) {
	struct page *page = virt_to_page(buffer);
	if (!page) {
		goto invalid_kaddr;
	}

	struct bio *wbio = bio_alloc(GFP_ATOMIC, 1);
	if (!wbio)
		goto bio_alloc;
	bio_set_dev(wbio, dmz->dev->bdev);
	wbio->bi_iter.bi_sector = pba << 3;
	wbio->bi_iter.bi_size = DMZ_BLOCK_SIZE;
	wbio->bi_private = dmz;
	// Here PAGE_SIZE = DMZ_BLOCK_SIZE = 4KB
	bio_add_page(wbio, page, PAGE_SIZE, 0);
	bio_set_op_attrs(wbio, REQ_OP_WRITE, 0);
	set_page_writeback(page);
	wbio->bi_end_io = dmz_reclaim_write_bio_endio;
	wait_on_page_writeback(page);

	if (PageDirty(page)) {
		goto write_err;
	}

	return 0;

bio_alloc:
invalid_kaddr:
	return -1;
write_err:
	return 1;
}

// TODO
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
int dmz_make_reclaim_bio(struct dmz_target *dmz, unsigned long lba, unsigned long pba) {
	int ret = 0;

	unsigned long buffer = dmz_reclaim_read_block(dmz, pba);
	if (!buffer) {
		goto read_err;
	}

	unsigned long new_pba = dmz_pba_alloc(dmz);
	ret = dmz_reclaim_write_block(dmz, new_pba, buffer);

	if (!ret) {
		dmz_update_map(dmz, lba, new_pba);
	}

	return ret;

read_err:
	return -1;
}

/*
* free first zone
*/
int dmz_reclaim_zone(struct dmz_target *dmz, int zone) {
	struct dmz_metadata *zmd = dmz->zmd;
	unsigned long flags;
	unsigned long remain = 0;
	struct dm_zone *cur_zone = zmd->zone_start;

	for (int zone_i = 0; zone_i < zmd->nr_zones; zone_i++) {
		if (zone_i == zone)
			continue;

		cur_zone = zmd->zone_start + zone_i;
		unsigned long *bitmap = cur_zone->bitmap;
		unsigned int wp = cur_zone->wp;
		struct dmz_map *map = cur_zone->mt;

		for (unsigned long offset = 0; offset < zmd->zone_nr_blocks; offset++) {
			unsigned long valid_bitmap = bitmap_get_value8(bitmap, offset);

			// first bit indicates blocks at this offset is whether valid
			if (valid_bitmap & (0x1 << 7)) {
				unsigned long pba = dmz_pba_alloc(dmz);
				struct bio *reclaim_bio = dmz_make_reclaim_bio(dmz, dmz_p2l(zone_i * zmd->zone_nr_blocks + offset), pba);

				if (!reclaim_bio)
					goto make_bio;

				// TODO Write lock should be applied.
				submit_bio_noacct(reclaim_bio);
			}
		}
	}

	return 0;

make_bio:
	return -1;
}