#include "dmz.h"

/*
* Allocate discrete memory for mapping table.
* Find mapping based on index.
*/
struct dmz_map *dmz_allocate_mapping_table(size_t size) {
	struct dmz_map *map = kcalloc(size, sizeof(struct dmz_map), GFP_ATOMIC);
	return map;
}