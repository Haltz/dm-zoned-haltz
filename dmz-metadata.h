#ifndef _DMZ_METADATA_H_
#define _DMZ_METADATA_H_

#include "dmz.h"

int dmz_ctr_metadata(struct dmz_target *);
void dmz_dtr_metadata(struct dmz_metadata *);

struct dmz_zone *dmz_load_zones(struct dmz_metadata *zmd, unsigned long *bitmap);
void dmz_unload_zones(struct dmz_metadata *zmd);

unsigned long *dmz_load_bitmap(struct dmz_metadata *zmd);
void dmz_unload_bitmap(struct dmz_metadata *zmd);

#endif