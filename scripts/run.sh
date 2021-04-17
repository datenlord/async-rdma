#! /bin/sh

RXE_DEV=rxe_eth0
SIW_DEV=siw_eth0

# Remove existing devices if any
sudo rdma link delete $RXE_DEV
sudo rdma link delete $SIW_DEV

set -o errexit
set -o nounset
set -o xtrace

ETH_DEV=eth0
if [ `ifconfig | grep -c $ETH_DEV` -eq 0 ]; then
    echo "no eth0, try enp4s0"
    ETH_DEV=enp4s0
    echo "verify eth dev: $ETH_DEV"
    ifconfig | grep $ETH_DEV
fi

HOST_IP=`ifconfig $ETH_DEV | grep 'inet ' | awk '{print $2}'`
SRV_PORT=9527
MSG_CNT=10

# Setup soft-roce device
sudo rdma link add $RXE_DEV type rxe netdev $ETH_DEV
rdma link | grep $RXE_DEV

# Cargo run async-rdma
cargo build
timeout 3 target/debug/async-rdma -p $SRV_PORT &
sleep 1
target/debug/async-rdma -s $HOST_IP -p $SRV_PORT
sleep 1

# Test soft-roce
ibv_rc_pingpong -d $RXE_DEV -g 0 &
sleep 1
ibv_rc_pingpong -d $RXE_DEV -g 0 $HOST_IP

ibv_uc_pingpong -d $RXE_DEV -g 0 &
sleep 1
ibv_uc_pingpong -d $RXE_DEV -g 0 $HOST_IP

ibv_ud_pingpong -d $RXE_DEV -g 0 &
sleep 1
ibv_ud_pingpong -d $RXE_DEV -g 0 $HOST_IP

ibv_srq_pingpong -d $RXE_DEV -g 0 &
sleep 1
ibv_srq_pingpong -d $RXE_DEV -g 0 $HOST_IP

udaddy &
sleep 1
udaddy -s $HOST_IP

rping -s -C $MSG_CNT -v &
sleep 1
rping -c -a $HOST_IP -C $MSG_CNT -v

# rdma_server -s 0.0.0.0 -p $SRV_PORT &
# sleep 1
# rdma_client -s $HOST_IP -p $SRV_PORT

ucmatose &
sleep 1
ucmatose -s $HOST_IP

# ib_send_bw -d $RXE_DEV &
# sleep 1
# ib_send_bw -d $RXE_DEV $HOST_IP

# Remove soft-roce device
sudo rdma link delete $RXE_DEV


# Setup softiwarp device
sudo rdma link add $SIW_DEV type siw netdev $ETH_DEV
rdma link | grep $SIW_DEV

# Test softiwarp
rping -s -C $MSG_CNT -v &
sleep 1
rping -c -a $HOST_IP -C $MSG_CNT -v

# rdma_server -s 0.0.0.0 -p $SRV_PORT &
# sleep 1
# rdma_client -s $HOST_IP -p $SRV_PORT

ucmatose &
sleep 1
ucmatose -s $HOST_IP

# rdma_xserver -p $SRV_PORT -c r &
# sleep 1
# rdma_xclient -s $HOST_IP -p $SRV_PORT -c r

# Remove softiwarp device
sudo rdma link delete $SIW_DEV
