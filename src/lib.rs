//! RDMA high-level abstraction, providing several useful APIs.
//!
//! Async-rdma is a framework for writing asynchronous rdma applications with the Rust
//! programing language. At a high level, it provides a few major components:
//!
//! * Tools for establishing connections with rdma endpoints such as `RdmaBuilder`.
//!
//! *  High-level APIs for data transmission between endpoints including `read`,
//! `write`, `send`, `receive`.
//!
//! *  High-level APIs for rdma memory region management including `alloc_local_mr`,
//! `request_remote_mr`, `send_mr`, `receive_local_mr`, `receive_remote_mr`.
//!
//! *  A framework including `agent` and `event_listener` working behind APIs for memory
//! region management and executing rdma requests such as `post_send` and `poll`.
//!
//! #### Example
//! A simple example: client request a remote memory region and put data into this remote
//! memory region by rdma `write`.
//! And finally client `send_mr` to make server aware of this memory region.
//! Server `receive_local_mr`, and then get data from this mr.
//!
//! ```
//! use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, Rdma, RdmaListener};
//! use portpicker::pick_unused_port;
//! use std::{
//!     alloc::Layout,
//!     io,
//!     net::{Ipv4Addr, SocketAddrV4},
//!     time::Duration,
//! };
//!
//! struct Data(String);
//!
//! async fn client(addr: SocketAddrV4) -> io::Result<()> {
//!     let rdma = Rdma::connect(addr, 1, 1, 512).await?;
//!     let mut lmr = rdma.alloc_local_mr(Layout::new::<Data>())?;
//!     let mut rmr = rdma.request_remote_mr(Layout::new::<Data>()).await?;
//!     // load data into lmr
//!     unsafe { *(lmr.as_mut_ptr() as *mut Data) = Data("hello world".to_string()) };
//!     // write the content of local mr into remote mr
//!     rdma.write(&lmr, &mut rmr).await?;
//!     // then send rmr's metadata to server to make server aware of it
//!     rdma.send_remote_mr(rmr).await?;
//!     Ok(())
//! }
//!
//! #[tokio::main]
//! async fn server(addr: SocketAddrV4) -> io::Result<()> {
//!     let rdma_listener = RdmaListener::bind(addr).await?;
//!     let rdma = rdma_listener.accept(1, 1, 512).await?;
//!     // receive the metadata of the mr sent by client
//!     let lmr = rdma.receive_local_mr().await?;
//!     // print the content of lmr, which was `write` by client
//!     unsafe { println!("{}", &*(*(lmr.as_ptr() as *const Data)).0) };
//!     // wait for the agent thread to send all reponses to the remote.
//!     tokio::time::sleep(Duration::from_secs(1)).await;
//!     Ok(())
//! }
//! #[tokio::main]
//! async fn main() {
//!     let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
//!     std::thread::spawn(move || server(addr));
//!     tokio::time::sleep(Duration::from_secs(3)).await;
//!     client(addr)
//!         .await
//!         .map_err(|err| println!("{}", err))
//!         .unwrap();
//! }
//! ```
//!
//!
#![deny(
    // The following are allowed by default lints according to
    // https://doc.rust-lang.org/rustc/lints/listing/allowed-by-default.html
    anonymous_parameters,
    bare_trait_objects,
    // box_pointers, // use box pointer to allocate on heap
    // elided_lifetimes_in_paths, // allow anonymous lifetime
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs, // TODO: add documents
    single_use_lifetimes, // TODO: fix lifetime names only used once
    trivial_casts, // TODO: remove trivial casts in code
    trivial_numeric_casts,
    // unreachable_pub, allow clippy::redundant_pub_crate lint instead
    // unsafe_code,
    unstable_features,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications,
    unused_results,
    variant_size_differences,

    warnings, // treat all wanings as errors

    clippy::all,
    clippy::restriction,
    clippy::pedantic,
    // clippy::nursery, // It's still under development
    clippy::cargo,
    unreachable_pub,
)]
#![allow(
    // Some explicitly allowed Clippy lints, must have clear reason to allow
    clippy::blanket_clippy_restriction_lints, // allow clippy::restriction
    clippy::implicit_return, // actually omitting the return keyword is idiomatic Rust code
    clippy::module_name_repetitions, // repeation of module name in a struct name is not big deal
    clippy::multiple_crate_versions, // multi-version dependency crates is not able to fix
    clippy::missing_errors_doc, // TODO: add error docs
    clippy::missing_panics_doc, // TODO: add panic docs
    clippy::panic_in_result_fn,
    clippy::shadow_same, // Not too much bad
    clippy::shadow_reuse, // Not too much bad
    clippy::exhaustive_enums,
    clippy::exhaustive_structs,
    clippy::indexing_slicing,
)]

/// The agent that handles async events in the background
mod agent;
/// The completion queue that handles the completion event
mod completion_queue;
/// The rmda device context
mod context;
/// The event channel that notifies the completion or error of a request
mod event_channel;
/// The driver to poll the completion queue
mod event_listener;
/// Gid for device
mod gid;
/// id utils
mod id;
/// Memory region abstraction
mod memory_region;
/// Memory window abstraction
mod memory_window;
/// Memory Region allocator
mod mr_allocator;
/// Protection Domain
mod protection_domain;
/// Queue Pair
mod queue_pair;
/// Remote memory region manager
mod rmr_manager;
/// Work Request wrapper
mod work_request;

use agent::Agent;
use clippy_utilities::Cast;
use context::Context;
use enumflags2::{bitflags, BitFlags};
use event_listener::EventListener;
pub use memory_region::{
    local::{LocalMr, LocalMrReadAccess, LocalMrWriteAccess},
    remote::{RemoteMr, RemoteMrReadAccess, RemoteMrWriteAccess},
    MrAccess,
};
use mr_allocator::MrAllocator;
use protection_domain::ProtectionDomain;
use queue_pair::{QueuePair, QueuePairEndpoint};
use rdma_sys::ibv_access_flags;
use rmr_manager::DEFAULT_RMR_TIMEOUT;
use std::{alloc::Layout, fmt::Debug, io, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, ToSocketAddrs},
};
use tracing::debug;

#[macro_use]
extern crate lazy_static;

/// A wrapper for ibv_access_flag, hide the ibv binding types
#[bitflags]
#[repr(u64)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum AccessFlag {
    /// local write permission
    LocalWrite,
    /// remote write permission
    RemoteWrite,
    /// remote read permission
    RemoteRead,
    /// remote atomic operation permission
    RemoteAtomic,
    /// enable memory window binding
    MwBind,
    /// use byte offset from beginning of MR to access this MR, instead of a pointer address
    ZeroBased,
    /// create an on-demand paging MR
    OnDemand,
    /// huge pages are guaranteed to be used for this MR, only used with `OnDemand`
    HugeTlb,
    /// allow system to reorder accesses to the MR to improve performance
    RelaxOrder,
}

/// The builder for the `Rdma`, it follows the builder pattern.
pub struct RdmaBuilder {
    /// Rdma device name
    dev_name: Option<String>,
    /// Access flag
    access: ibv_access_flags,
    /// Complete queue size
    cq_size: u32,
    /// Gid index
    gid_index: usize,
    /// Device port number
    port_num: u8,
}

impl RdmaBuilder {
    /// Create a default builder
    /// The default settings are:
    ///     dev name: None
    ///     access right: `LocalWrite` | `RemoteRead` | `RemoteWrite` | `RemoteAtomic`
    ///     complete queue size: 16
    ///     port number: 1
    ///     gid index: 0
    ///
    /// Note: We highly recommend setting the port number and the gid index.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a `Rdma` from this builder
    #[inline]
    pub fn build(&self) -> io::Result<Rdma> {
        Rdma::new(
            self.dev_name.as_deref(),
            self.access,
            self.cq_size,
            self.port_num,
            self.gid_index,
        )
    }

    /// Set device name
    #[inline]
    #[must_use]
    pub fn set_dev(mut self, dev: &str) -> Self {
        self.dev_name = Some(dev.to_owned());
        self
    }

    /// Set the complete queue size
    #[inline]
    #[must_use]
    pub fn set_cq_size(mut self, cq_size: u32) -> Self {
        self.cq_size = cq_size;
        self
    }

    /// Set the gid index
    #[inline]
    #[must_use]
    pub fn set_gid_index(mut self, gid_index: usize) -> Self {
        self.gid_index = gid_index;
        self
    }

    /// Set the port number
    #[inline]
    #[must_use]
    pub fn set_port_num(mut self, port_num: u8) -> Self {
        self.port_num = port_num;
        self
    }

    /// Set the access right
    #[inline]
    #[must_use]
    pub fn set_access(mut self, flag: BitFlags<AccessFlag>) -> Self {
        self.access = ibv_access_flags(0);
        if flag.contains(AccessFlag::LocalWrite) {
            self.access |= ibv_access_flags::IBV_ACCESS_LOCAL_WRITE;
        }
        if flag.contains(AccessFlag::RemoteWrite) {
            self.access |= ibv_access_flags::IBV_ACCESS_REMOTE_WRITE;
        }
        if flag.contains(AccessFlag::RemoteRead) {
            self.access |= ibv_access_flags::IBV_ACCESS_REMOTE_READ;
        }
        if flag.contains(AccessFlag::RemoteAtomic) {
            self.access |= ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;
        }
        if flag.contains(AccessFlag::MwBind) {
            self.access |= ibv_access_flags::IBV_ACCESS_MW_BIND;
        }
        if flag.contains(AccessFlag::ZeroBased) {
            self.access |= ibv_access_flags::IBV_ACCESS_ZERO_BASED;
        }
        if flag.contains(AccessFlag::OnDemand) {
            self.access |= ibv_access_flags::IBV_ACCESS_ON_DEMAND;
        }
        if flag.contains(AccessFlag::HugeTlb) {
            self.access |= ibv_access_flags::IBV_ACCESS_HUGETLB;
        }
        if flag.contains(AccessFlag::RelaxOrder) {
            self.access |= ibv_access_flags::IBV_ACCESS_RELAXED_ORDERING;
        }
        self
    }
}

impl Debug for RdmaBuilder {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdmaBuilder")
            .field("dev_name", &self.dev_name)
            .field("cq_size", &self.cq_size)
            .finish()
    }
}

impl Default for RdmaBuilder {
    #[inline]
    fn default() -> Self {
        Self {
            dev_name: None,
            access: ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
                | ibv_access_flags::IBV_ACCESS_REMOTE_READ
                | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC,
            cq_size: 16,
            gid_index: 0,
            port_num: 1,
        }
    }
}

/// Rdma handler, the only interface that the users deal with rdma
#[derive(Debug)]
pub struct Rdma {
    /// device context
    #[allow(dead_code)]
    ctx: Arc<Context>,
    /// protection domain
    #[allow(dead_code)]
    pd: Arc<ProtectionDomain>,
    /// Memory region allocator
    allocator: Arc<MrAllocator>,
    /// Queue pair
    qp: Arc<QueuePair>,
    /// Background agent
    agent: Option<Arc<Agent>>,
}

impl Rdma {
    /// create a new `Rdma` instance
    fn new(
        dev_name: Option<&str>,
        access: ibv_access_flags,
        cq_size: u32,
        port_num: u8,
        gid_index: usize,
    ) -> io::Result<Self> {
        let ctx = Arc::new(Context::open(dev_name, port_num, gid_index)?);
        let ec = ctx.create_event_channel()?;
        let cq = Arc::new(ctx.create_completion_queue(cq_size, ec)?);
        let event_listener = EventListener::new(cq);
        let pd = Arc::new(ctx.create_protection_domain()?);
        let allocator = Arc::new(MrAllocator::new(Arc::<ProtectionDomain>::clone(&pd)));
        let qp = Arc::new(
            pd.create_queue_pair_builder()
                .set_event_listener(event_listener)
                .set_port_num(port_num)
                .set_gid_index(gid_index)
                .build()?,
        );
        qp.modify_to_init(access, port_num)?;
        Ok(Self {
            ctx,
            pd,
            qp,
            agent: None,
            allocator,
        })
    }

    /// get the queue pair endpoint information
    fn endpoint(&self) -> QueuePairEndpoint {
        self.qp.endpoint()
    }

    /// to hand shake the qp so that it works
    fn qp_handshake(&mut self, remote: QueuePairEndpoint) -> io::Result<()> {
        self.qp.modify_to_rtr(remote, 0, 1, 0x12)?;
        debug!("rtr");
        self.qp.modify_to_rts(0x12, 6, 6, 0, 1)?;
        debug!("rts");
        Ok(())
    }

    /// Send the content in the `lm`
    ///
    /// Used with `receive`.
    /// Application scenario such as: client put data into a local mr and `send` to server.
    /// Server `receive` the mr sent by client and process data in it.
    ///
    /// Application scenario can be seen in `[/example/rpc.rs]`
    ///
    /// # Examples
    /// ```
    /// use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, Rdma, RdmaListener};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// struct Data(String);
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = Rdma::connect(addr, 1, 1, 512).await?;
    ///     let mut lmr = rdma.alloc_local_mr(Layout::new::<Data>())?;
    ///     // put data into lmr
    ///     unsafe { *(lmr.as_mut_ptr() as *mut Data) = Data("hello world".to_string()) };
    ///     // send the content of lmr to server
    ///     rdma.send(&lmr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma_listener = RdmaListener::bind(addr).await?;
    ///     let rdma = rdma_listener.accept(1, 1, 512).await?;
    ///     // receive the data sent by client and put it into an mr
    ///     let lmr = rdma.receive().await?;
    ///     // read data from mr
    ///     unsafe {
    ///         assert_eq!(
    ///             "hello world".to_string(),
    ///             *(*(lmr.as_ptr() as *const Data)).0
    ///         )
    ///     };
    ///     // wait for the agent thread to send all reponses to the remote.
    ///     tokio::time::sleep(Duration::from_secs(1)).await;
    ///     Ok(())
    /// }
    /// #[tokio::main]
    /// async fn main() {
    ///     let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
    ///     std::thread::spawn(move || server(addr));
    ///     tokio::time::sleep(Duration::from_secs(3)).await;
    ///     client(addr)
    ///         .await
    ///         .map_err(|err| println!("{}", err))
    ///         .unwrap();
    /// }
    /// ```
    #[inline]
    pub async fn send(&self, lm: &LocalMr) -> io::Result<()> {
        self.agent
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Agent is not ready"))?
            .send_data(lm, None)
            .await
    }

    /// Send the content in the `lm` with immediate date.
    ///
    /// Used with `receive_with_imm`.
    ///
    /// # Examples
    /// ```rust
    /// use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, Rdma, RdmaListener};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// struct Data(String);
    /// static IMM_NUM: u32 = 123;
    /// static MSG: &str = "hello world";
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = Rdma::connect(addr, 1, 1, 512).await?;
    ///     let mut lmr = rdma.alloc_local_mr(Layout::new::<Data>())?;
    ///     // put data into lmr
    ///     unsafe { std::ptr::write(lmr.as_mut_ptr() as *mut Data, Data(MSG.to_string())) };
    ///     // send the content of lmr and imm data to server
    ///     rdma.send_with_imm(&lmr, IMM_NUM).await?;
    ///     rdma.send_with_imm(&lmr, IMM_NUM).await?;
    ///     rdma.send(&lmr).await?;
    ///     rdma.send(&lmr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma_listener = RdmaListener::bind(addr).await?;
    ///     let rdma = rdma_listener.accept(1, 1, 512).await?;
    ///     // receive the data and imm sent by the client
    ///     let (lmr, imm) = rdma.receive_with_imm().await?;
    ///     assert_eq!(imm, Some(IMM_NUM));
    ///     unsafe { assert_eq!(MSG.to_string(), *(*(lmr.as_ptr() as *const Data)).0) };
    ///     // receive the data in mr while avoiding the immediate data is ok.
    ///     let lmr = rdma.receive().await?;
    ///     unsafe { assert_eq!(MSG.to_string(), *(*(lmr.as_ptr() as *const Data)).0) };
    ///     // `receive_with_imm` works well even if the client didn't send any immediate data.
    ///     // the imm received will be a `None`.
    ///     let (lmr, imm) = rdma.receive_with_imm().await?;
    ///     assert_eq!(imm, None);
    ///     unsafe { assert_eq!(MSG.to_string(), *(*(lmr.as_ptr() as *const Data)).0) };
    ///     // compared to the above, using `receive` is a better choice.
    ///     let lmr = rdma.receive().await?;
    ///     unsafe { assert_eq!(MSG.to_string(), *(*(lmr.as_ptr() as *const Data)).0) };
    ///     // wait for the agent thread to send all reponses to the remote.
    ///     tokio::time::sleep(Duration::from_secs(1)).await;
    ///     Ok(())
    /// }
    /// #[tokio::main]
    /// async fn main() {
    ///     let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
    ///     let server_handle = std::thread::spawn(move || server(addr));
    ///     tokio::time::sleep(Duration::from_secs(3)).await;
    ///     client(addr)
    ///         .await
    ///         .map_err(|err| println!("{}", err))
    ///         .unwrap();
    ///     server_handle.join().unwrap().unwrap();
    /// }
    /// ```
    #[inline]
    pub async fn send_with_imm(&self, lm: &LocalMr, imm: u32) -> io::Result<()> {
        self.agent
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Agent is not ready"))?
            .send_data(lm, Some(imm))
            .await
    }

    /// Receive the content and stored in the returned memory region
    ///
    /// Used with `send`.
    /// Application scenario such as: client put data into a local mr and `send` to server.
    /// Server `receive` the mr sent by client and process data in it.
    ///
    /// Application scenario can be seen in `[/example/rpc.rs]`
    ///
    /// # Examples
    /// ```
    /// use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, Rdma, RdmaListener};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// struct Data(String);
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = Rdma::connect(addr, 1, 1, 512).await?;
    ///     let mut lmr = rdma.alloc_local_mr(Layout::new::<Data>())?;
    ///     // put data into lmr
    ///     unsafe { *(lmr.as_mut_ptr() as *mut Data) = Data("hello world".to_string()) };
    ///     // send the content of lmr to server
    ///     rdma.send(&lmr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma_listener = RdmaListener::bind(addr).await?;
    ///     let rdma = rdma_listener.accept(1, 1, 512).await?;
    ///     // receive the data sent by client and put it into an mr
    ///     let lmr = rdma.receive().await?;
    ///     // read data from mr
    ///     unsafe {
    ///         assert_eq!(
    ///             "hello world".to_string(),
    ///             *(*(lmr.as_ptr() as *const Data)).0
    ///         )
    ///     };
    ///     // wait for the agent thread to send all reponses to the remote.
    ///     tokio::time::sleep(Duration::from_secs(1)).await;
    ///     Ok(())
    /// }
    /// #[tokio::main]
    /// async fn main() {
    ///     let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
    ///     std::thread::spawn(move || server(addr));
    ///     tokio::time::sleep(Duration::from_secs(3)).await;
    ///     client(addr)
    ///         .await
    ///         .map_err(|err| println!("{}", err))
    ///         .unwrap();
    /// }
    /// ```
    #[inline]
    pub async fn receive(&self) -> io::Result<LocalMr> {
        let (lmr, _) = self.receive_with_imm().await?;
        Ok(lmr)
    }

    /// Receive the content and stored in the returned memory region.
    ///
    /// Used with `send_with_imm`.
    ///
    /// # Examples
    /// ```rust
    /// use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, Rdma, RdmaListener};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// struct Data(String);
    /// static IMM_NUM: u32 = 123;
    /// static MSG: &str = "hello world";
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = Rdma::connect(addr, 1, 1, 512).await?;
    ///     let mut lmr = rdma.alloc_local_mr(Layout::new::<Data>())?;
    ///     // put data into lmr
    ///     unsafe { std::ptr::write(lmr.as_mut_ptr() as *mut Data, Data(MSG.to_string())) };
    ///     // send the content of lmr and imm data to server
    ///     rdma.send_with_imm(&lmr, IMM_NUM).await?;
    ///     rdma.send_with_imm(&lmr, IMM_NUM).await?;
    ///     rdma.send(&lmr).await?;
    ///     rdma.send(&lmr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma_listener = RdmaListener::bind(addr).await?;
    ///     let rdma = rdma_listener.accept(1, 1, 512).await?;
    ///     // receive the data and imm sent by the client
    ///     let (lmr, imm) = rdma.receive_with_imm().await?;
    ///     assert_eq!(imm, Some(IMM_NUM));
    ///     unsafe { assert_eq!(MSG.to_string(), *(*(lmr.as_ptr() as *const Data)).0) };
    ///     // receive the data in mr while avoiding the immediate data is ok.
    ///     let lmr = rdma.receive().await?;
    ///     unsafe { assert_eq!(MSG.to_string(), *(*(lmr.as_ptr() as *const Data)).0) };
    ///     // `receive_with_imm` works well even if the client didn't send any immediate data.
    ///     // the imm received will be a `None`.
    ///     let (lmr, imm) = rdma.receive_with_imm().await?;
    ///     assert_eq!(imm, None);
    ///     unsafe { assert_eq!(MSG.to_string(), *(*(lmr.as_ptr() as *const Data)).0) };
    ///     // compared to the above, using `receive` is a better choice.
    ///     let lmr = rdma.receive().await?;
    ///     unsafe { assert_eq!(MSG.to_string(), *(*(lmr.as_ptr() as *const Data)).0) };
    ///     // wait for the agent thread to send all reponses to the remote.
    ///     tokio::time::sleep(Duration::from_secs(1)).await;
    ///     Ok(())
    /// }
    /// #[tokio::main]
    /// async fn main() {
    ///     let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
    ///     let server_handle = std::thread::spawn(move || server(addr));
    ///     tokio::time::sleep(Duration::from_secs(3)).await;
    ///     client(addr)
    ///         .await
    ///         .map_err(|err| println!("{}", err))
    ///         .unwrap();
    ///     server_handle.join().unwrap().unwrap();
    /// }
    /// ```
    #[inline]
    pub async fn receive_with_imm(&self) -> io::Result<(LocalMr, Option<u32>)> {
        self.agent
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Agent is not ready"))?
            .receive_data()
            .await
    }

    /// Receive the immediate data sent by `write_with_imm`.
    ///
    /// Used with `write_with_imm`.
    ///
    /// # Examples
    /// ```rust
    /// use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, Rdma, RdmaListener};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// static IMM_NUM: u32 = 123;
    /// struct Data(String);
    ///
    /// static MSG: &str = "hello world";
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = Rdma::connect(addr, 1, 1, 512).await?;
    ///     let mut lmr = rdma.alloc_local_mr(Layout::new::<Data>())?;
    ///     let mut rmr = rdma.request_remote_mr(Layout::new::<Data>()).await?;
    ///     let data = Data(MSG.to_string());
    ///     unsafe { *(lmr.as_mut_ptr() as *mut Data) = data };
    ///     // write the content of lmr to remote mr with immediate data.
    ///     rdma.write_with_imm(&lmr, &mut rmr, IMM_NUM).await?;
    ///     // then send the metadata of rmr to server to make server aware of this mr.
    ///     rdma.send_remote_mr(rmr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma_listener = RdmaListener::bind(addr).await?;
    ///     let rdma = rdma_listener.accept(1, 1, 512).await?;
    ///     // receive the immediate data sent by `write_with_imm`
    ///     let imm = rdma.receive_write_imm().await?;
    ///     assert_eq!(imm, IMM_NUM);
    ///     // receive the metadata of the lmr that had been requested by client
    ///     let lmr = rdma.receive_local_mr().await?;
    ///     // assert the content of lmr, which was `write` by client
    ///     unsafe { assert_eq!(MSG.to_string(), *(*(lmr.as_ptr() as *const Data)).0) };
    ///     // wait for the agent thread to send all reponses to the remote.
    ///     tokio::time::sleep(Duration::from_secs(1)).await;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
    ///     let server_handle = std::thread::spawn(move || server(addr));
    ///     tokio::time::sleep(Duration::from_secs(3)).await;
    ///     client(addr)
    ///         .await
    ///         .map_err(|err| println!("{}", err))
    ///         .unwrap();
    ///     server_handle.join().unwrap().unwrap();
    /// }
    /// ```
    #[inline]
    pub async fn receive_write_imm(&self) -> io::Result<u32> {
        self.agent
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Agent is not ready"))?
            .receive_imm()
            .await
    }

    /// Read content in the `rm` and store the content in the `lm`
    ///
    /// Application scenario such as: client put data into a local mr and `send_mr` to server.
    /// Server get a remote mr by `receive_remote_mr`, and then get data from this rmr by rdma `read`.
    ///
    /// Application scenario can be seen in `[/example/rpc.rs]`
    ///
    /// # Examples
    /// ```
    /// use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, Rdma, RdmaListener};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// struct Data(String);
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = Rdma::connect(addr, 1, 1, 512).await?;
    ///     let mut lmr = rdma.alloc_local_mr(Layout::new::<Data>())?;
    ///     // put data into lmr
    ///     unsafe { *(lmr.as_mut_ptr() as *mut Data) = Data("hello world".to_string()) };
    ///     // then send the metadata of this lmr to server to make server aware of this mr.
    ///     rdma.send_local_mr(lmr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma_listener = RdmaListener::bind(addr).await?;
    ///     let rdma = rdma_listener.accept(1, 1, 512).await?;
    ///     let mut lmr = rdma.alloc_local_mr(Layout::new::<Data>())?;
    ///     // receive the metadata of rmr sent by client
    ///     let rmr = rdma.receive_remote_mr().await?;
    ///     // `read` data from rmr to lmr
    ///     rdma.read(&mut lmr, &rmr).await?;
    ///     // assert the content of lmr, which was get from rmr by rdma `read`
    ///     unsafe {
    ///         assert_eq!(
    ///             "hello world".to_string(),
    ///             *(*(lmr.as_ptr() as *const Data)).0
    ///         )
    ///     };
    ///     Ok(())
    /// }
    /// #[tokio::main]
    /// async fn main() {
    ///     let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
    ///     std::thread::spawn(move || server(addr));
    ///     tokio::time::sleep(Duration::from_secs(3)).await;
    ///     client(addr)
    ///         .await
    ///         .map_err(|err| println!("{}", err))
    ///         .unwrap();
    /// }
    /// ```
    #[inline]
    pub async fn read<LW, RR>(&self, lm: &mut LW, rm: &RR) -> io::Result<()>
    where
        LW: LocalMrWriteAccess,
        RR: RemoteMrReadAccess,
    {
        self.qp.read(lm, rm).await
    }

    /// Write content in the `lm` to `rm`
    ///
    /// Application scenario such as: client request a remote mr through `request_remote_mr`,
    /// and then put data into this rmr by rdma `write`. After all client `send_mr` to make
    /// server aware of this mr.
    /// After client `send_mr`, server `receive_local_mr`, and then get data from this mr.
    ///
    /// Application scenario can be seen in `[/example/rpc.rs]`
    ///
    /// # Examples
    /// ```
    /// use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, Rdma, RdmaListener};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// struct Data(String);
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = Rdma::connect(addr, 1, 1, 512).await?;
    ///     let mut lmr = rdma.alloc_local_mr(Layout::new::<Data>())?;
    ///     let mut rmr = rdma.request_remote_mr(Layout::new::<Data>()).await?;
    ///     // put data into lmr
    ///     unsafe { *(lmr.as_mut_ptr() as *mut Data) = Data("hello world".to_string()) };
    ///     // write the content of local mr into remote mr
    ///     rdma.write(&lmr, &mut rmr).await?;
    ///     // then send the metadata of rmr to server to make server aware of this mr.
    ///     rdma.send_remote_mr(rmr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma_listener = RdmaListener::bind(addr).await?;
    ///     let rdma = rdma_listener.accept(1, 1, 512).await?;
    ///     // receive the metadata of the lmr that had been requested by client
    ///     let lmr = rdma.receive_local_mr().await?;
    ///     // assert the content of lmr, which was `write` by client
    ///     unsafe {
    ///         assert_eq!(
    ///             "hello world".to_string(),
    ///             *(*(lmr.as_ptr() as *const Data)).0
    ///         )
    ///     };
    ///     Ok(())
    /// }
    /// #[tokio::main]
    /// async fn main() {
    ///     let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
    ///     std::thread::spawn(move || server(addr));
    ///     tokio::time::sleep(Duration::from_secs(3)).await;
    ///     client(addr)
    ///         .await
    ///         .map_err(|err| println!("{}", err))
    ///         .unwrap();
    /// }
    /// ```
    #[inline]
    pub async fn write<LR, RW>(&self, lm: &LR, rm: &mut RW) -> io::Result<()>
    where
        LR: LocalMrReadAccess,
        RW: RemoteMrWriteAccess,
    {
        self.qp.write(lm, rm, None).await
    }

    /// Write content in the `lm` to `rm` and send a immediate data which
    /// will consume a `rdma receive work request` in the receiver's `receive queue`.
    /// The receiver can receive this immediate data by using `receive_write_imm`.
    ///
    /// Used with `receive_write_imm`.
    ///
    /// # Examples
    /// ```rust
    /// use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, Rdma, RdmaListener};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// static IMM_NUM: u32 = 123;
    /// struct Data(String);
    ///
    /// static MSG: &str = "hello world";
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = Rdma::connect(addr, 1, 1, 512).await?;
    ///     let mut lmr = rdma.alloc_local_mr(Layout::new::<Data>())?;
    ///     let mut rmr = rdma.request_remote_mr(Layout::new::<Data>()).await?;
    ///     let data = Data(MSG.to_string());
    ///     unsafe { *(lmr.as_mut_ptr() as *mut Data) = data };
    ///     // write the content of lmr to server with immediate data.
    ///     rdma.write_with_imm(&lmr, &mut rmr, IMM_NUM).await?;
    ///     // then send the metadata of rmr to server to make server aware of this mr.
    ///     rdma.send_remote_mr(rmr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma_listener = RdmaListener::bind(addr).await?;
    ///     let rdma = rdma_listener.accept(1, 1, 512).await?;
    ///     // receive the immediate data sent by `write_with_imm`
    ///     let imm = rdma.receive_write_imm().await?;
    ///     assert_eq!(imm, IMM_NUM);
    ///     // receive the metadata of the lmr that had been requested by client
    ///     let lmr = rdma.receive_local_mr().await?;
    ///     // assert the content of lmr, which was `write` by client
    ///     unsafe { assert_eq!(MSG.to_string(), *(*(lmr.as_ptr() as *const Data)).0) };
    ///     // wait for the agent thread to send all reponses to the remote.
    ///     tokio::time::sleep(Duration::from_secs(1)).await;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
    ///     let server_handle = std::thread::spawn(move || server(addr));
    ///     tokio::time::sleep(Duration::from_secs(3)).await;
    ///     client(addr)
    ///         .await
    ///         .map_err(|err| println!("{}", err))
    ///         .unwrap();
    ///     server_handle.join().unwrap().unwrap();
    /// }
    /// ```
    #[inline]
    pub async fn write_with_imm<LR, RW>(&self, lm: &LR, rm: &mut RW, imm: u32) -> io::Result<()>
    where
        LR: LocalMrReadAccess,
        RW: RemoteMrWriteAccess,
    {
        self.qp.write(lm, rm, Some(imm)).await
    }

    /// Connect the remote endpoint and build rmda queue pair by TCP connection
    ///
    /// `gid_index`: 0:ipv6, 1:ipv4
    /// `max_message_length`: max length of msg used in `send`&`receive`.
    ///
    /// # Examples
    /// ```
    /// use async_rdma::{Rdma, RdmaListener};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let _rdma = Rdma::connect(addr, 1, 1, 512).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma_listener = RdmaListener::bind(addr).await?;
    ///     let _rdma = rdma_listener.accept(1, 1, 512).await?;
    ///     // run here after client connect
    ///     Ok(())
    /// }
    /// #[tokio::main]
    /// async fn main() {
    ///     let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
    ///     std::thread::spawn(move || server(addr));
    ///     tokio::time::sleep(Duration::from_secs(3)).await;
    ///     client(addr)
    ///         .await
    ///         .map_err(|err| println!("{}", err))
    ///         .unwrap();
    /// }
    /// ```
    #[inline]
    pub async fn connect<A: ToSocketAddrs>(
        addr: A,
        port_num: u8,
        gid_index: usize,
        max_message_length: usize,
    ) -> io::Result<Self> {
        let mut rdma = RdmaBuilder::default()
            .set_port_num(port_num)
            .set_gid_index(gid_index)
            .build()?;
        let mut stream = TcpStream::connect(addr).await?;
        let mut endpoint = bincode::serialize(&rdma.endpoint()).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("failed to serailize the endpoint, {:?}", e),
            )
        })?;
        stream.write_all(&endpoint).await?;
        // the byte number is not important, as read_exact will fill the buffer
        let _ = stream.read_exact(endpoint.as_mut()).await?;
        let remote: QueuePairEndpoint = bincode::deserialize(&endpoint).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("failed to deserailize the endpoint, {:?}", e),
            )
        })?;
        rdma.qp_handshake(remote)?;
        let agent = Arc::new(Agent::new(
            Arc::<QueuePair>::clone(&rdma.qp),
            Arc::<MrAllocator>::clone(&rdma.allocator),
            max_message_length,
        )?);
        rdma.agent = Some(agent);
        // wait for server to initialize
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(rdma)
    }

    /// Allocate a local memory region
    ///
    /// You can use local mr to `send`&`receive` or `read`&`write` with a remote mr.
    /// The parameter `layout` can be obtained by `Layout::new::<Data>()`.
    /// You can learn the way to write or read data in mr in the following example.
    ///
    /// # Examples
    /// ```
    /// use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, Rdma, RdmaListener};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// struct Data(String);
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = Rdma::connect(addr, 1, 1, 512).await?;
    ///     let mut lmr = rdma.alloc_local_mr(Layout::new::<Data>())?;
    ///     // put data into lmr
    ///     unsafe { *(lmr.as_mut_ptr() as *mut Data) = Data("hello world".to_string()) };
    ///     // send the content of lmr to server
    ///     rdma.send(&lmr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma_listener = RdmaListener::bind(addr).await?;
    ///     let rdma = rdma_listener.accept(1, 1, 512).await?;
    ///     // receive the data sent by client and put it into an mr
    ///     let lmr = rdma.receive().await?;
    ///     // assert data in the lmr
    ///     unsafe {
    ///         assert_eq!(
    ///             "hello world".to_string(),
    ///             *(*(lmr.as_ptr() as *const Data)).0
    ///         )
    ///     };
    ///     Ok(())
    /// }
    /// #[tokio::main]
    /// async fn main() {
    ///     let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
    ///     std::thread::spawn(move || server(addr));
    ///     tokio::time::sleep(Duration::from_secs(3)).await;
    ///     client(addr)
    ///         .await
    ///         .map_err(|err| println!("{}", err))
    ///         .unwrap();
    /// }
    /// ```
    #[inline]
    pub fn alloc_local_mr(&self, layout: Layout) -> io::Result<LocalMr> {
        self.allocator.alloc(&layout)
    }

    /// Request a remote memory region with default timeout value.
    ///
    /// **Note**: The operation of this memory region will fail after timeout.
    ///
    /// Used with `send_mr`, `receive_local_mr`, `read` and `write`.
    /// Application scenario such as: client uses `request_remote_mr` to apply for
    /// a remote mr from server, and makes server aware of this mr by `send_mr` to server.
    /// For server, this mr is a local mr, which can be received through `receive_local_mr`.
    ///
    /// Application scenario can be seen in `[/example/rpc.rs]`
    ///
    /// # Examples
    /// ```
    /// use async_rdma::{Rdma, RdmaListener};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// struct Data(String);
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = Rdma::connect(addr, 1, 1, 512).await?;
    ///     // request a mr located in server.
    ///     let rmr = rdma.request_remote_mr(Layout::new::<Data>()).await?;
    ///     // do something with rmr like `write` data into it.
    ///     // then send the metadata of rmr to server to make server aware of this mr.
    ///     rdma.send_remote_mr(rmr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma_listener = RdmaListener::bind(addr).await?;
    ///     let rdma = rdma_listener.accept(1, 1, 512).await?;
    ///     // receive the metadata of the lmr that had been requested by client
    ///     let _lmr = rdma.receive_local_mr().await?;
    ///     // do something with lmr like getting data from it.
    ///     Ok(())
    /// }
    /// #[tokio::main]
    /// async fn main() {
    ///     let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
    ///     std::thread::spawn(move || server(addr));
    ///     tokio::time::sleep(Duration::from_secs(3)).await;
    ///     client(addr)
    ///         .await
    ///         .map_err(|err| println!("{}", err))
    ///         .unwrap();
    /// }
    /// ```
    #[inline]
    pub async fn request_remote_mr(&self, layout: Layout) -> io::Result<RemoteMr> {
        self.request_remote_mr_with_timeout(layout, DEFAULT_RMR_TIMEOUT)
            .await
    }

    /// Request a remote memory region with customized timeout value.
    /// The rest is consistent with `request_remote_mr`.
    ///
    /// **Note**: The operation of this memory region will fail after timeout.
    ///
    /// Used with `send_mr`, `receive_local_mr`, `read` and `write`.
    /// Application scenario such as: client uses `request_remote_mr` to apply for
    /// a remote mr from server, and makes server aware of this mr by `send_mr` to server.
    /// For server, this mr is a local mr, which can be received through `receive_local_mr`.
    ///
    /// # Examples
    /// ```
    /// use async_rdma::{Rdma, RdmaListener, RemoteMrReadAccess};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// struct Data(String);
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = Rdma::connect(addr, 1, 1, 512).await?;
    ///     // request a mr located in server.
    ///     let rmr = rdma
    ///         .request_remote_mr_with_timeout(Layout::new::<Data>(), Duration::from_secs(1))
    ///         .await?;
    ///     assert!(!rmr.timeout_check());
    ///     // do something with rmr like `write` data into it.
    ///     // then send the metadata of rmr to server to make server aware of this mr.
    ///     rdma.send_remote_mr(rmr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma_listener = RdmaListener::bind(addr).await?;
    ///     let rdma = rdma_listener.accept(1, 1, 512).await?;
    ///     // receive the metadata of the lmr that had been requested by client
    ///     let _lmr = rdma.receive_local_mr().await?;
    ///     // do something with lmr like getting data from it.
    ///     Ok(())
    /// }
    /// #[tokio::main]
    /// async fn main() {
    ///     let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
    ///     std::thread::spawn(move || server(addr));
    ///     tokio::time::sleep(Duration::from_secs(3)).await;
    ///     client(addr)
    ///         .await
    ///         .map_err(|err| println!("{}", err))
    ///         .unwrap();
    /// }
    /// ```
    #[inline]
    pub async fn request_remote_mr_with_timeout(
        &self,
        layout: Layout,
        timeout: Duration,
    ) -> io::Result<RemoteMr> {
        if let Some(ref agent) = self.agent {
            agent.request_remote_mr_with_timeout(layout, timeout).await
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Agent is not ready, please wait a while",
            ))
        }
    }

    /// Send a local memory region metadata to remote with default timeout value
    ///
    /// **Note**: The operation of this memory region will fail after timeout.
    ///
    /// Used with `receive_remote_mr`
    ///
    /// Application scenario such as: client uses `alloc_local_mr` to alloc a local mr, and
    /// makes server aware of this mr by `send_local_mr` to server.
    /// For server, this mr is a remote mr, which can be received through `receive_remote_mr`.
    ///
    /// Application scenario can be seen in `[/example/rpc.rs]`
    ///
    /// # Examples
    /// ```
    /// use async_rdma::{Rdma, RdmaListener};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// struct Data(String);
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = Rdma::connect(addr, 1, 1, 512).await?;
    ///     // request a mr located in server.
    ///     let lmr = rdma.alloc_local_mr(Layout::new::<Data>())?;
    ///     // do something with rmr like `write` data into it.
    ///     // then send the metadata of this lmr to server to make server aware of this mr.
    ///     rdma.send_local_mr(lmr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma_listener = RdmaListener::bind(addr).await?;
    ///     let rdma = rdma_listener.accept(1, 1, 512).await?;
    ///     // receive the metadata of rmr sent by client
    ///     let _rmr = rdma.receive_remote_mr().await?;
    ///     // do something with lmr like getting data from it.
    ///     // wait for the agent thread to send all reponses to the remote.
    ///     tokio::time::sleep(Duration::from_secs(1)).await;
    ///     Ok(())
    /// }
    /// #[tokio::main]
    /// async fn main() {
    ///     let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
    ///     std::thread::spawn(move || server(addr));
    ///     tokio::time::sleep(Duration::from_secs(3)).await;
    ///     client(addr)
    ///         .await
    ///         .map_err(|err| println!("{}", err))
    ///         .unwrap();
    /// }
    /// ```
    #[inline]
    pub async fn send_local_mr(&self, mr: LocalMr) -> io::Result<()> {
        self.send_local_mr_with_timeout(mr, DEFAULT_RMR_TIMEOUT)
            .await
    }

    /// Send a local memory region metadata with timeout to remote with customized timeout value.
    ///
    /// **Note**: The operation of this memory region will fail after timeout.
    ///
    /// Used with `receive_remote_mr`
    ///
    /// Application scenario such as: client uses `alloc_local_mr` to alloc a local mr, and
    /// makes server aware of this mr by `send_local_mr` to server.
    /// For server, this mr is a remote mr, which can be received through `receive_remote_mr`.
    ///
    /// # Examples
    /// ```
    /// use async_rdma::{Rdma, RdmaListener, RemoteMrReadAccess};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// struct Data(String);
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = Rdma::connect(addr, 1, 1, 512).await?;
    ///     // request a mr located in server.
    ///     let lmr = rdma.alloc_local_mr(Layout::new::<Data>())?;
    ///     // do something with rmr like `write` data into it.
    ///     // then send the metadata of this lmr to server to make server aware of this mr.
    ///     rdma.send_local_mr_with_timeout(lmr, Duration::from_secs(1))
    ///         .await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma_listener = RdmaListener::bind(addr).await?;
    ///     let rdma = rdma_listener.accept(1, 1, 512).await?;
    ///     // receive the metadata of rmr sent by client
    ///     let rmr = rdma.receive_remote_mr().await?;
    ///     assert!(!rmr.timeout_check());
    ///     // do something with lmr like getting data from it.
    ///     // wait for the agent thread to send all reponses to the remote.
    ///     tokio::time::sleep(Duration::from_secs(1)).await;
    ///     Ok(())
    /// }
    /// #[tokio::main]
    /// async fn main() {
    ///     let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
    ///     std::thread::spawn(move || server(addr));
    ///     tokio::time::sleep(Duration::from_secs(3)).await;
    ///     client(addr)
    ///         .await
    ///         .map_err(|err| println!("{}", err))
    ///         .unwrap();
    /// }
    /// ```
    #[inline]
    pub async fn send_local_mr_with_timeout(
        &self,
        mr: LocalMr,
        timeout: Duration,
    ) -> io::Result<()> {
        if let Some(ref agent) = self.agent {
            agent.send_local_mr_with_timeout(mr, timeout).await
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Agent is not ready, please wait a while",
            ))
        }
    }

    /// Send a remote memory region metadata to remote
    ///
    /// Used with `receive_local_mr`.
    ///
    /// Application scenario such as: client uses `request_remote_mr` to apply for
    /// a remote mr from server, and makes server aware of this mr by `send_remote_mr` to server.
    /// For server, this mr is a local mr, which can be received through `receive_local_mr`.
    ///
    /// Application scenario can be seen in `[/example/rpc.rs]`
    ///
    /// # Examples
    /// ```
    /// use async_rdma::{Rdma, RdmaListener};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// struct Data(String);
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = Rdma::connect(addr, 1, 1, 512).await?;
    ///     // request a mr located in server.
    ///     let rmr = rdma.request_remote_mr(Layout::new::<Data>()).await?;
    ///     // do something with rmr like `write` data into it.
    ///     // then send the metadata of rmr to server to make server aware of this mr.
    ///     rdma.send_remote_mr(rmr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma_listener = RdmaListener::bind(addr).await?;
    ///     let rdma = rdma_listener.accept(1, 1, 512).await?;
    ///     // receive the metadata of the lmr that had been requested by client
    ///     let _lmr = rdma.receive_local_mr().await?;
    ///     // do something with lmr like getting data from it.
    ///     // wait for the agent thread to send all reponses to the remote.
    ///     tokio::time::sleep(Duration::from_secs(1)).await;
    ///     Ok(())
    /// }
    /// #[tokio::main]
    /// async fn main() {
    ///     let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
    ///     std::thread::spawn(move || server(addr));
    ///     tokio::time::sleep(Duration::from_secs(3)).await;
    ///     client(addr)
    ///         .await
    ///         .map_err(|err| println!("{}", err))
    ///         .unwrap();
    /// }
    /// ```
    #[inline]
    pub async fn send_remote_mr(&self, mr: RemoteMr) -> io::Result<()> {
        if let Some(ref agent) = self.agent {
            agent.send_remote_mr(mr).await
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Agent is not ready, please wait a while",
            ))
        }
    }

    /// Receive a local memory region
    ///
    /// Used with `send_mr`.
    /// Application scenario such as: client uses `request_remote_mr` to apply for
    /// a remote mr from server, and makes server aware of this mr by `send_mr` to server.
    /// For server, this mr is a local mr, which can be received through `receive_local_mr`.
    ///
    /// Application scenario can be seen in `[/example/rpc.rs]`
    ///
    /// # Examples
    /// Application scenario such as: client request a remote mr through `request_remote_mr`,
    /// and then put data into this rmr by rdma `write`. After all client `send_mr` to make
    /// server aware of this mr.
    /// After client `send_mr`, server `receive_local_mr`, and then get data from this mr.
    ///
    /// Application scenario can be seen in `[/example/rpc.rs]`
    ///
    /// # Examples
    /// ```
    /// use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, Rdma, RdmaListener};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// struct Data(String);
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = Rdma::connect(addr, 1, 1, 512).await?;
    ///     let mut lmr = rdma.alloc_local_mr(Layout::new::<Data>())?;
    ///     let mut rmr = rdma.request_remote_mr(Layout::new::<Data>()).await?;
    ///     // put data into lmr
    ///     unsafe { *(lmr.as_mut_ptr() as *mut Data) = Data("hello world".to_string()) };
    ///     // write the content of local mr into remote mr
    ///     rdma.write(&lmr, &mut rmr).await?;
    ///     // then send the metadata of rmr to server to make server aware of this mr.
    ///     rdma.send_remote_mr(rmr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma_listener = RdmaListener::bind(addr).await?;
    ///     let rdma = rdma_listener.accept(1, 1, 512).await?;
    ///     // receive the metadata of the lmr that had been requested by client
    ///     let lmr = rdma.receive_local_mr().await?;
    ///     // assert the content of lmr, which was `write` by client
    ///     unsafe {
    ///         assert_eq!(
    ///         "hello world".to_string(),
    ///         *(*(lmr.as_ptr() as *const Data)).0
    ///     )
    ///     };
    ///     // wait for the agent thread to send all reponses to the remote.
    ///     tokio::time::sleep(Duration::from_secs(1)).await;
    ///     Ok(())
    /// }
    /// #[tokio::main]
    /// async fn main() {
    ///     let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
    ///     std::thread::spawn(move || server(addr));
    ///     tokio::time::sleep(Duration::from_secs(3)).await;
    ///     client(addr)
    ///         .await
    ///         .map_err(|err| println!("{}", err))
    ///         .unwrap();
    /// }
    /// ```
    #[inline]
    pub async fn receive_local_mr(&self) -> io::Result<LocalMr> {
        if let Some(ref agent) = self.agent {
            agent.receive_local_mr().await
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Agent is not ready, please wait a while",
            ))
        }
    }

    /// Receive a remote memory region
    ///
    /// Used with `send_mr`.
    /// Application scenario such as: server alloc a local mr and put data into it and let
    /// client know about this mr through `send_mr`. For client, this is a remote mr located
    /// in server.Client receive the metadata of this mr by `receive_remote_mr`.
    ///
    /// Application scenario can be seen in `[/example/rpc.rs]`
    ///
    /// # Examples
    /// ```
    /// use async_rdma::{Rdma, RdmaListener};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// struct Data(String);
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = Rdma::connect(addr, 1, 1, 512).await?;
    ///     // receive the metadata of rmr sent by client
    ///     let _rmr = rdma.receive_remote_mr().await?;
    ///     // do something with rmr like `read` data from it.
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma_listener = RdmaListener::bind(addr).await?;
    ///     let rdma = rdma_listener.accept(1, 1, 512).await?;
    ///     let lmr = rdma.alloc_local_mr(Layout::new::<Data>())?;
    ///     // do something with lmr like put data into it.
    ///     // then send the metadata of this lmr to server to make server aware of this mr.
    ///     rdma.send_local_mr(lmr).await?;
    ///     Ok(())
    /// }
    /// #[tokio::main]
    /// async fn main() {
    ///     let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
    ///     std::thread::spawn(move || server(addr));
    ///     tokio::time::sleep(Duration::from_secs(3)).await;
    ///     client(addr)
    ///         .await
    ///         .map_err(|err| println!("{}", err))
    ///         .unwrap();
    /// }
    /// ```
    #[inline]
    pub async fn receive_remote_mr(&self) -> io::Result<RemoteMr> {
        if let Some(ref agent) = self.agent {
            agent.receive_remote_mr().await
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Agent is not ready, please wait a while",
            ))
        }
    }
}

/// Rdma Listener is the wrapper of a `TcpListener`, which is used to
/// build the rdma queue pair.
#[derive(Debug)]
pub struct RdmaListener {
    /// Tcp listener to establish the queue pair
    tcp_listener: TcpListener,
}

impl RdmaListener {
    /// Bind the address and wait for a connection
    /// # Examples
    /// ```
    /// use async_rdma::{Rdma, RdmaListener};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let _rdma = Rdma::connect(addr, 1, 1, 512).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma_listener = RdmaListener::bind(addr).await?;
    ///     let _rdma = rdma_listener.accept(1, 1, 512).await?;
    ///     // run here after client connect
    ///     Ok(())
    /// }
    /// #[tokio::main]
    /// async fn main() {
    ///     let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
    ///     std::thread::spawn(move || server(addr));
    ///     tokio::time::sleep(Duration::from_secs(3)).await;
    ///     client(addr)
    ///         .await
    ///         .map_err(|err| println!("{}", err))
    ///         .unwrap();
    /// }
    /// ```
    #[inline]
    pub async fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let tcp_listener = TcpListener::bind(addr).await?;
        Ok(Self { tcp_listener })
    }

    /// Wait for a connection from a remote host
    /// # Examples
    /// ```
    /// use async_rdma::{Rdma, RdmaListener};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let _rdma = Rdma::connect(addr, 1, 1, 512).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma_listener = RdmaListener::bind(addr).await?;
    ///     let _rdma = rdma_listener.accept(1, 1, 512).await?;
    ///     // run here after client connect
    ///     Ok(())
    /// }
    /// #[tokio::main]
    /// async fn main() {
    ///     let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
    ///     std::thread::spawn(move || server(addr));
    ///     tokio::time::sleep(Duration::from_secs(3)).await;
    ///     client(addr)
    ///         .await
    ///         .map_err(|err| println!("{}", err))
    ///         .unwrap();
    /// }
    /// ```
    #[inline]
    pub async fn accept(
        &self,
        port_num: u8,
        gid_index: usize,
        max_message_length: usize,
    ) -> io::Result<Rdma> {
        let (mut stream, _) = self.tcp_listener.accept().await?;
        let mut rdma = RdmaBuilder::default()
            .set_port_num(port_num)
            .set_gid_index(gid_index)
            .build()?;
        let endpoint_size = bincode::serialized_size(&rdma.endpoint()).map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Endpoint serialization failed, {:?}", e),
            )
        })?;
        let mut remote = vec![0_u8; endpoint_size.cast()];
        // the byte number is not important, as read_exact will fill the buffer
        let _ = stream.read_exact(remote.as_mut()).await?;
        let remote: QueuePairEndpoint = bincode::deserialize(&remote).map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("failed to deserialize remote endpoint, {:?}", e),
            )
        })?;
        let local = bincode::serialize(&rdma.endpoint()).map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("failed to deserialize remote endpoint, {:?}", e),
            )
        })?;
        stream.write_all(&local).await?;
        rdma.qp_handshake(remote)?;
        debug!("handshake done");
        let agent = Arc::new(Agent::new(
            Arc::<QueuePair>::clone(&rdma.qp),
            Arc::<MrAllocator>::clone(&rdma.allocator),
            max_message_length,
        )?);
        rdma.agent = Some(agent);
        // wait for the remote agent to prepare
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(rdma)
    }
}
