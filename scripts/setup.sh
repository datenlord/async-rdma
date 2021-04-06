#! /bin/sh

# Remove related kernel modules if any
sudo rmmod rdma_rxe
sudo rmmod siw
sudo modprobe -r --remove-dependencies rdma_ucm
sudo modprobe -r --remove-dependencies ib_core

set -o errexit
set -o nounset
set -o xtrace

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
