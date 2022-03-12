#! /bin/sh

RXE_DEV=rxe_eth0
SIW_DEV=siw_eth0

# Remove existing devices if any
sudo rdma link delete $RXE_DEV
sudo rdma link delete $SIW_DEV

set -o errexit
set -o nounset
set -o xtrace

if [ `ifconfig -s | grep -c '^e'` -eq 0 ]; then
    echo "no eth device"
    exit 1
elif [ `ifconfig -s | grep -c '^e'` -gt 1 ]; then
    echo "multiple eth devices, select the first one"
    ifconfig -s | grep '^e'
fi

ETH_DEV=`ifconfig -s | grep '^e' | cut -d ' ' -f 1 | head -n 1`

HOST_IP=`ifconfig $ETH_DEV | grep 'inet ' | awk '{print $2}'`
SRV_PORT=9527
MSG_CNT=10


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

# Setup soft-roce device
sudo rdma link add $RXE_DEV type rxe netdev $ETH_DEV
rdma link | grep $RXE_DEV

# Cargo run async-rdma
cargo build
RUST_BACKTRACE=full cargo test
timeout 3 target/debug/examples/server &
sleep 1
target/debug/examples/client
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
#sudo rdma link delete $RXE_DEV
