#! /bin/bash
cd $(pwd)

sudo dmesg --clear

sudo bash scripts/restart.sh

sudo dmsetup status dmz-sdb
