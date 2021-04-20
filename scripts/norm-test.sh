# !/bin/bash

sudo bash scripts/nullblk.sh 512 256 0 10

bdev=/dev/nullb0
ddev=/dev/dm-0
ko=dmzoned.ko

make
sudo insmod $ko
