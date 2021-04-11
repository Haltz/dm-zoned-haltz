# !/bin/bash

bdev=$1
ddev=$2
ko=dmzoned.ko

make
sudo insmod $ko
