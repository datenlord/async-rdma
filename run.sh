#! /bin/sh

set -o errexit
set -o nounset
set -o xtrace

# Build and install softiwarp kernel module
cd ./src/siw
make
sudo modprobe ib_core
sudo modprobe rdma_ucm
sudo insmod ./siw.ko

# Setup softiwarp device
sudo rdma link add siw_eth0 type siw netdev eth0
rdma link | grep siw_eth0

HOST_IP=`ifconfig eth0 | grep 'inet ' | awk '{print $2}'`
SRV_PORT=9527
MSG_CNT=10

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
