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


## Example
A simple example: client request a remote memory region and put data into this remote
memory region by rdma `write`.
And finally client `send_mr` to make server aware of this memory region.
Server `receive_local_mr`, and then get data from this mr.

```rust
use async_rdma::{Rdma, RdmaListener};
use std::{alloc::Layout, sync::Arc, io, time::Duration, net::{Ipv4Addr, SocketAddrV4}};
use portpicker::pick_unused_port;

struct Data(String);

async fn client(addr: SocketAddrV4) -> io::Result<()> {
    let rdma = Rdma::connect(addr, 1, 1, 512).await?;
    let mut lmr = rdma.alloc_local_mr(Layout::new::<Data>())?;
    let rmr = Arc::new(rdma.request_remote_mr(Layout::new::<Data>()).await?);
    // then send this mr to server to make server aware of this mr.
    unsafe { *(lmr.as_mut_ptr() as *mut Data) = Data("hello world".to_string()) };
    rdma.write(&lmr, &rmr).await?;
    // send the content of lmr to server
    rdma.send_mr(rmr.clone()).await?;
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
    std::thread::spawn(move || server(addr.clone()));
    tokio::time::sleep(Duration::new(1, 0)).await;
    client(addr).await.map_err(|err| println!("{}", err)).unwrap();
}
```

## Getting Help
First, see if the answer to your question can be found in the found [API doc] or [Design doc]. If the answer is not here, please open an issue and describe your problem in detail.   

[Design doc]: https://github.com/datenlord/async-rdma/tree/master/doc
## Related Projects
* [`rdma-sys`]: Rust bindings for RDMA fundamental libraries: libibverbs-dev and librdmacm-dev.

[`rdma-sys`]: https://github.com/datenlord/rdma-sys
