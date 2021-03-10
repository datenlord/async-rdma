#! /bin/sh

# Remove related kernel modules if any
sudo rmmod rdma_rxe
sudo rmmod siw
sudo modprobe -r --remove-dependencies rdma_ucm
sudo modprobe -r --remove-dependencies ib_core

set -o errexit
set -o nounset
set -o xtrace

HOST_IP=`ifconfig eth0 | grep 'inet ' | awk '{print $2}'`
SRV_PORT=9527
MSG_CNT=10
RXE_DEV=rxe_eth0

# Load required IB and RDMA kernel modules
sudo modprobe ib_core
sudo modprobe rdma_ucm

# Build and install soft-roce kernel module
# The rxe code is from Kernel 5.5.0
cd ./src/rxe
make
sudo insmod ./rdma_rxe.ko
# Build and install softiwarp kernel module
# The siw code is from Kernel 5.4.0
cd ../siw
make
sudo insmod ./siw.ko

# Setup soft-roce device
sudo rdma link add $RXE_DEV type rxe netdev eth0
rdma link | grep $RXE_DEV

# Test soft-roce
ibv_rc_pingpong -d $RXE_DEV -g 0 &
sleep 1
ibv_rc_pingpong -d $RXE_DEV -g 0 $HOST_IP

udaddy &
sleep 1
udaddy -s $HOST_IP

rping -s -C $MSG_CNT -v &
sleep 1
rping -c -a $HOST_IP -C $MSG_CNT -v

rdma_server -s 0.0.0.0 -p $SRV_PORT &
sleep 1
rdma_client -s $HOST_IP -p $SRV_PORT

ucmatose &
sleep 1
ucmatose -s $HOST_IP

# Cargo run async-rdma
cd ../../
cargo build
timeout 3 cargo run $SRV_PORT &
sleep 1
cargo run $HOST_IP $SRV_PORT
sleep 3

# Remove soft-roce device
sudo rdma link delete $RXE_DEV


# Setup softiwarp device
sudo rdma link add siw_eth0 type siw netdev eth0
rdma link | grep siw_eth0

# Test softiwarp
rping -s -C $MSG_CNT -v &
sleep 1
rping -c -a $HOST_IP -C $MSG_CNT -v

rdma_server -s 0.0.0.0 -p $SRV_PORT &
sleep 1
rdma_client -s $HOST_IP -p $SRV_PORT

ucmatose &
sleep 1
ucmatose -s $HOST_IP

rdma_xserver -p $SRV_PORT -c r &
sleep 1
rdma_xclient -s $HOST_IP -p $SRV_PORT -c r

# Cargo run async-rdma
timeout 3 cargo run $SRV_PORT &
sleep 1
cargo run $HOST_IP $SRV_PORT
sleep 3

# Remove softiwarp device
sudo rdma link delete siw_eth0

