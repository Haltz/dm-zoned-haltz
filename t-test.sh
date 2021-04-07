# !/bin/bash

set -x


################## clear
sudo dmesg --clear

sudo dmzadm --stop /dev/sdb
sudo rmmod dm-zoned
################## clear

# cd /home/mu/rwGenerator/fioscripts/verify
# sudo ./verifydev.sh ./dmz-offical.txt

cd /home/mu/dm-zone
sudo make
sudo insmod dm-zoned.ko

cd /home/mu/test/dm-zoned-tools
sudo make install 
sudo dmzadm --format /dev/sdb
sudo dmzadm --start /dev/sdb

cd /home/mu/rwGenerator/fioscripts/verify
sudo ./verifydev.sh ./dmz-offical.txt

################## clear
sudo dmzadm --stop /dev/sdb
sudo rmmod dm-zoned
################## clear