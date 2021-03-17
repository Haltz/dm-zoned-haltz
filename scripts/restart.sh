sudo dmzadm --stop /dev/sdb

sudo rmmod dm-zoned

sudo dmesg --clear

cd $(pwd)

sudo make

sudo insmod dm-zoned.ko

sudo dmzadm --format /dev/sdb

sudo dmzadm --start /dev/sdb

# mkfs -t ext4 /dev/dm-0