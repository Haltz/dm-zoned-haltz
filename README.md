# dm-zoned-haltz

## Description
这是使用FTL将普通块设备节点映射到Zoned Block Device的内核模块，可以使用户忽略ZBD的顺序写约束。  
Linux kernel已经有dm-zoned内核模块，但是dm-zoned有如下缺点：
- 不能尽可能有效的利用磁盘空间。dm-zoned需要至少三个conventional zone作为保留空间（分别存储metadata，mapping+bitmap以及至少一个作为buffer zone）。
- 随机读写速度过慢。随机读写会导致频繁的擦除和zone内所有有效块的复制。

FTL可以帮助解决这两个问题。目前采用的是Page-Mapping的FTL。

## TODO
- [ ] 多线程锁的同步
- [ ] 热数据缓存
- [ ] Block-Mapping是否比Page-Mapping更优？

## Problem Log
- [ ] Reclaim（也可能是写导致的）时多次出现 blk_update_request: I/O error, dev sdb, sector 524288 op 0x1:(WRITE) flags 0x8800 phys_seg 0 prio class 0（这是1号Zone的开始）需要检查这个位置。
    - 看情况可能是前面的没有空间但是还要写导致的。如果是这样的话那么好像并非问题，还是再看看。
    - 预留出一个ZONE用作垃圾回收
    - 可能是读没有上锁导致的。但是这并不影响现有的功能。