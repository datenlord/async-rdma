# async-rdma

A framework for writing RDMA applications with high-level abstraction and asynchronous APIs.

[![MIT licensed][gpl-badge]][gpl-url]
[![Build Status][actions-badge]][actions-url]

[gpl-badge]: https://img.shields.io/badge/license-GPLv3.0-blue.svg
[gpl-url]: https://github.com/datenlord/async-rdma/blob/master/LICENSE
[actions-badge]: https://github.com/datenlord/async-rdma/actions/workflows/ci.yml/badge.svg
[actions-url]: https://github.com/datenlord/async-rdma/actions

It provides a few major components:

* Tools for establishing connections with rdma endpoints such as `RdmaBuilder`.

*  High-level APIs for data transmission between endpoints including `read`,
`write`, `send`, `receive`.

*  High-level APIs for rdma memory region management including `alloc_local_mr`,
`request_remote_mr`, `send_mr`, `receive_local_mr`, `receive_remote_mr`.

*  A framework including `agent` and `event_listener` working behind APIs for memory
region management and executing rdma requests such as `post_send` and `poll`.

## Environment Setup
This section is for RDMA novices who want to try this library.   

You can skip if your Machines have been configured with RDMA.   

Next we will configure the RDMA environment in an Ubuntu20.04 VM.
If you are using another operating system distribution, please search and replace the relevant commands.
### 1. Check whether the current kernel supports RXE
Run the following command and if the CONFIG_RDMA_RXE = `y` or `m`, the current operating system supports RXE.
If not you need to search how to recompile the kernel to support RDMA.
```shell
cat /boot/config-$(uname -r) | grep RXE
```
### 2. Install Dependencies
```shell
sudo apt install -y libibverbs1 ibverbs-utils librdmacm1 libibumad3 ibverbs-providers rdma-core libibverbs-dev iproute2 perftest build-essential net-tools git librdmacm-dev rdmacm-utils cmake libprotobuf-dev protobuf-compiler clang curl
```

### 3. Configure RDMA netdev
(1) Load kernel driver
```shell
modprobe rdma_rxe
```

(2) User mode RDMA netdev configuration.
```shell
sudo rdma link add rxe_0 type rxe netdev ens33
```
`rxe_0` is the RDMA device name, and you can name it whatever you want. `ens33` is the name of the network device. The name of the network device may be different in each VM, and we can see it by running command "ifconfig".   

(3) Check the RDMA device state   

Run the following command and check if the state is `ACTIVE`.
```shell
rdma link
```

(4) Test it   

Ib_send_bw is a program used to test the bandwidth of `RDMA SEND` operations.   

Run the following command in a terminal.
```shell
ib_send_bw -d rxe_0
```
And run the following command in another terminal.
```shell
ib_send_bw -d rxe_0 localhost
```
### 4. Install Rust
```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env
```

### 5. Try an example
```shell
git clone https://github.com/datenlord/async-rdma.git
cd async-rdma
cargo run --example rpc
```
## Example
A simple example: client request a remote memory region and put data into this remote
memory region by rdma `write`.
And finally client `send_mr` to make server aware of this memory region.
Server `receive_local_mr`, and then get data from this mr.

```rust
use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, Rdma, RdmaListener};
use portpicker::pick_unused_port;
use std::{
    alloc::Layout,
    io,
    net::{Ipv4Addr, SocketAddrV4},
    time::Duration,
};

struct Data(String);

async fn client(addr: SocketAddrV4) -> io::Result<()> {
    let rdma = Rdma::connect(addr, 1, 1, 512).await?;
    let mut lmr = rdma.alloc_local_mr(Layout::new::<Data>())?;
    let mut rmr = rdma.request_remote_mr(Layout::new::<Data>()).await?;
    // then send this mr to server to make server aware of this mr.
    unsafe { *(lmr.as_mut_ptr() as *mut Data) = Data("hello world".to_string()) };
    rdma.write(&lmr, &mut rmr).await?;
    // send the content of lmr to server
    rdma.send_remote_mr(rmr).await?;
    Ok(())
}

#[tokio::main]
async fn server(addr: SocketAddrV4) -> io::Result<()> {
    let rdma_listener = RdmaListener::bind(addr).await?;
    let rdma = rdma_listener.accept(1, 1, 512).await?;
    let lmr = rdma.receive_local_mr().await?;
    // print the content of lmr, which was `write` by client
    unsafe { println!("{}", &*(*(lmr.as_ptr() as *const Data)).0) };
    Ok(())
}

#[tokio::main]
async fn main() {
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
    std::thread::spawn(move || server(addr));
    tokio::time::sleep(Duration::new(1, 0)).await;
    client(addr)
        .await
        .map_err(|err| println!("{}", err))
        .unwrap();
}

```
## Getting Help
First, see if the answer to your question can be found in the found [API doc] or [Design doc]. If the answer is not here, please open an issue and describe your problem in detail.   

[Design doc]: https://github.com/datenlord/async-rdma/tree/master/doc
## Related Projects
* [`rdma-sys`]: Rust bindings for RDMA fundamental libraries: libibverbs-dev and librdmacm-dev.

[`rdma-sys`]: https://github.com/datenlord/rdma-sys
