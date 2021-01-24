#
# A shared Makefile from "https://www.kernel.org/doc/Documentation/kbuild/modules.txt"
# and optimized by myself
#

modname ?= dm-zoned
sourcelist ?= dmz-target.o dmz-metadata.o dmz-reclaim.o

ccflags-y := -std=gnu99 -Wno-declaration-after-statement

#==========================================================
ifneq ($(KERNELRELEASE),)
# kbuild part of makefile
obj-m  := $(modname).o
$(modname)-y := $(sourcelist)

else
# normal makefile
KDIR ?= /lib/modules/`uname -r`/build

default:
	$(MAKE) -C $(KDIR) M=$$PWD
	rm -rf modules.order .tmp_versions *.mod* *.o *.o.cmd .*.cmd
clean:
	rm -rf modules.order Module.symvers built-in.a .tmp_versions *.ko* *.mod* *.o *.o.cmd .*.cmd 

#Module specific targets
hello:
	echo "hello"
endif