sudo dmesg --clear

cd $(pwd)

make

sudo insmod dm-zoned.ko

sudo dmzadm --format /dev/sdb

sudo dmzadm --start /dev/sdb

# mkfs -t ext4 /dev/dm-0