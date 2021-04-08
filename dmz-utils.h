#ifndef _DMZ_UTILS_H_
#define _DMZ_UTILS_H_

#include "dmz.h"

int dmz_flush(struct dmz_target *dmz);

int dmz_locks_init(struct dmz_metadata* zmd);
void dmz_locks_cleanup(struct dmz_metadata *zmd);

int dmz_lock_metadata(struct dmz_metadata* zmd);
void dmz_unlock_metadata(struct dmz_metadata* zmd);

int dmz_lock_zone(struct dmz_metadata* zmd, int zone);
void dmz_unlock_zone(struct dmz_metadata* zmd, int zone);

#endif