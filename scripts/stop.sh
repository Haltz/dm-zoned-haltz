cd $(pwd)

sudo dmzadm --stop /dev/sdb

sudo rmmod dm-zoned

sudo dmesg --clear
