# dm-zoned-haltz

## Description
这是使用FTL将普通块设备节点映射到Zoned Block Device的内核模块，可以使用户忽略ZBD的顺序写约束。  
Linux kernel已经有dm-zoned内核模块，但是dm-zoned有如下缺点：
- 不能尽可能有效的利用磁盘空间。dm-zoned需要至少三个conventional zone作为保留空间（分别存储metadata，mapping+bitmap以及至少一个作为buffer zone）。
- 随机读写速度过慢。随机读写会导致频繁的擦除和zone内所有有效块的复制。