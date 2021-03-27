sudo mkdir /home/mu/mount

sudo mkfs -t ext4 /dev/dm-0

sudo mount -t ext4 /dev/dm-0 /home/mu/mount
