#ifndef _DMZ_UTILS_H_
#define _DMZ_UTILS_H_

#include "dmz.h"

int dmz_flush(struct dmz_target *dmz);

int dmz_locks_init(struct dmz_metadata *zmd);
void dmz_locks_cleanup(struct dmz_metadata *zmd);

int dmz_lock_metadata(struct dmz_metadata *zmd);
void dmz_unlock_metadata(struct dmz_metadata *zmd);

int dmz_lock_zone(struct dmz_metadata *zmd, int zone);
void dmz_unlock_zone(struct dmz_metadata *zmd, int zone);

void dmz_start_io(struct dmz_metadata *zmd, int zone);
void dmz_complete_io(struct dmz_metadata *zmd, int zone);
int dmz_is_on_io(struct dmz_metadata *zmd, int zone);

void dmz_lock_map(struct dmz_metadata *zmd, int zone);
void dmz_unlock_map(struct dmz_metadata *zmd, int zone);

int dmz_lock_reclaim(struct dmz_metadata *zmd);
void dmz_unlock_reclaim(struct dmz_metadata *zmd);

int dmz_open_zone(struct dmz_metadata *zmd, int zone);
int dmz_close_zone(struct dmz_metadata *zmd, int zone);
int dmz_finish_zone(struct dmz_metadata *zmd, int zone);
int dmz_reset_all_zones(struct dmz_metadata *zmd);

#endif