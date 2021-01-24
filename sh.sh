sudo dmesg --clear

cat /dev/null > .log

echo "***Make Stage.***\n"

make

echo "***Module Reinsert and Device Mapper Restart.***\n"

sudo rmmod dm-zoned >> .log
sudo insmod dm-zoned.ko >> .log
sudo dmzadm --start /dev/sdb >> .log

echo "***Module Reinserted and Device Mapper Restarted.***\n"

echo "***Cat Queried Infomation***\n"
dmesg | grep -i test