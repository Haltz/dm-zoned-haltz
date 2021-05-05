# dm-zoned-haltz


## TODO
- [ ] 多线程锁的同步
- [ ] 热数据缓存
- [ ] Block-Mapping是否比Page-Mapping更优？

## Problem Log
- [ ] Reclaim（也可能是写导致的）时多次出现 blk_update_request: I/O error, dev sdb, sector 524288 op 0x1:(WRITE) flags 0x8800 phys_seg 0 prio class 0（这是1号Zone的开始）需要检查这个位置。
    - 看情况可能是前面的没有空间但是还要写导致的。如果是这样的话那么好像并非问题，还是再看看。
    - 预留出一个ZONE用作垃圾回收
    - 可能是读没有上锁导致的。但是这并不影响现有的功能。
- [ ] 读取太慢，主要是读写会访问一把锁导致的，读一个zone应当可以多个线程同时干活，但是读写不行（报错）。要加个状态保存一下是读是写，如果是写那么应该等待上锁，如果不是，直接读就可以。