LINUX_SRC_PATH = /lib/modules/$(shell uname -r)/build

#
## If building against OFED, include the OFED headers first.
#
ifneq ($(OFA_DIR),)
EXTRA_CFLAGS += -DOFED_BUILD
FOO := -I$(OFA_DIR)/include $(LINUXINCLUDE)
override LINUXINCLUDE=$(FOO)
endif

default: modules

install: modules
	@${MAKE} -C $(LINUX_SRC_PATH) M=`pwd` modules_install 
	
modules: 
	@${MAKE} -C $(LINUX_SRC_PATH) M=`pwd` modules 

all: 
	@${MAKE} -C $(LINUX_SRC_PATH) M=`pwd` modules 

clean:
	-@${MAKE} -C $(LINUX_SRC_PATH) M=`pwd` clean

.PHONY: clean modules install

