# dm-zoned-haltz

## Synchronization

Data needs to be protected:
- mapping table
- bitmap
- write pointer of zones

### write pointer
1-start-->allocate free pba1-->
2-start-->allocate free pba2-->
possibly get same pba, which obviously is wrong.

### mapping table
