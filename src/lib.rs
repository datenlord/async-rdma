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
//!     unsafe { *(*lmr.as_mut_ptr() as *mut Data) = Data("hello world".to_string()) };
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
//!     unsafe { println!("{}", &*(*(*lmr.as_ptr() as *const Data)).0) };
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
    clippy::separated_literal_suffix, // conflicts with clippy::unseparated_literal_suffix
    clippy::single_char_lifetime_names, // TODO: change lifetime names
)]

/// The agent that handles async events in the background
mod agent;
/// The completion queue that handles the completion event
mod completion_queue;
/// The rmda device context
mod context;
/// The rmda device
pub mod device;

/// Access of `QP` and `MR`
mod access;
/// Error handling utilities
mod error_utilities;
/// The event channel that notifies the completion or error of a request
mod event_channel;
/// The driver to poll the completion queue
mod event_listener;
/// Gid for device
mod gid;
/// `HashMap` extension
mod hashmap_extension;
/// id utils
mod id;
/// Lock utilities
mod lock_utilities;
/// Macro utilities
mod macro_utilities;
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

use access::flags_into_ibv_access;
pub use access::AccessFlag;
use agent::{Agent, MAX_MSG_LEN};
use clippy_utilities::Cast;
use completion_queue::{DEFAULT_CQ_SIZE, DEFAULT_MAX_CQE};
use context::Context;
use derive_builder::Builder;
use enumflags2::BitFlags;
use error_utilities::log_ret_last_os_err;
use event_listener::EventListener;
pub use memory_region::{
    local::{LocalMr, LocalMrReadAccess, LocalMrWriteAccess},
    remote::{RemoteMr, RemoteMrReadAccess, RemoteMrWriteAccess},
    MrAccess,
};
pub use mr_allocator::MRManageStrategy;
use mr_allocator::MrAllocator;
use parking_lot::RwLock;
use protection_domain::ProtectionDomain;
use queue_pair::{
    QueuePair, QueuePairBuilder, QueuePairInitAttrBuilder, RQAttr, RQAttrBuilder, SQAttr,
    SQAttrBuilder, DEFAULT_GID_INDEX, DEFAULT_PORT_NUM,
};
use rdma_sys::ibv_access_flags;
#[cfg(feature = "cm")]
use rdma_sys::{
    rdma_addrinfo, rdma_cm_id, rdma_connect, rdma_create_ep, rdma_disconnect, rdma_freeaddrinfo,
    rdma_getaddrinfo, rdma_port_space,
};
use rmr_manager::DEFAULT_RMR_TIMEOUT;
#[cfg(feature = "cm")]
use std::ptr::null_mut;
use std::{alloc::Layout, fmt::Debug, io, ptr::NonNull, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, ToSocketAddrs},
    sync::Mutex,
};
use tracing::debug;

use crate::queue_pair::builders_into_attrs;
use getset::{CopyGetters, Getters, MutGetters, Setters};
pub use queue_pair::{QueuePairEndpoint, QueuePairEndpointBuilder, QueuePairState, MTU};

#[macro_use]
extern crate lazy_static;

/// initial device attributes
#[derive(Debug, Default)]
pub(crate) struct DeviceInitAttr {
    /// Rdma device name
    dev_name: Option<String>,
}

/// initial CQ attributes
#[derive(Debug, Clone, Copy)]
pub(crate) struct CQInitAttr {
    /// Complete queue size
    cq_size: u32,
    /// Maximum number of completion queue entries (CQE) to poll at a time.
    /// The higher the concurrency, the bigger this value should be and more memory allocated at a time.
    max_cqe: i32,
}

impl Default for CQInitAttr {
    #[inline]
    fn default() -> Self {
        Self {
            cq_size: DEFAULT_CQ_SIZE,
            max_cqe: DEFAULT_MAX_CQE,
        }
    }
}

/// initial QP attributes
#[derive(Debug, Clone, Copy)]
pub(crate) struct QPInitAttr {
    /// Connection type
    conn_type: ConnectionType,
    /// If send/recv raw data
    raw: bool,
    /// Attributes for init QP
    init_attr: QueuePairInitAttrBuilder,
    /// Attributes for QP's send queue to receive messages
    sq_attr: SQAttrBuilder,
    /// Attributes for QP's receive queue to receive messages
    rq_attr: RQAttrBuilder,
}

lazy_static! {
    /// Default `ibv_access_flags`
    static ref DEFAULT_ACCESS:ibv_access_flags = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
    | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
    | ibv_access_flags::IBV_ACCESS_REMOTE_READ
    | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;
}

impl Default for QPInitAttr {
    #[inline]
    fn default() -> Self {
        Self {
            conn_type: ConnectionType::RCSocket,
            raw: false,
            init_attr: QueuePairInitAttrBuilder::default(),
            sq_attr: SQAttrBuilder::default(),
            rq_attr: RQAttrBuilder::default(),
        }
    }
}

/// Initial `MR` attributes
#[derive(Debug, Clone, Copy)]
pub(crate) struct MRInitAttr {
    /// Access flag
    access: ibv_access_flags,
    /// Strategy to manage `MR`s
    strategy: MRManageStrategy,
}

impl Default for MRInitAttr {
    #[inline]
    fn default() -> Self {
        Self {
            access: *DEFAULT_ACCESS,
            strategy: MRManageStrategy::Jemalloc,
        }
    }
}

/// Initial Agent attributes
#[derive(Debug, Clone, Copy, Getters, Setters)]
#[getset(set, get)]
pub(crate) struct AgentInitAttr {
    /// Max length of message send/recv by Agent
    max_message_length: usize,
    /// Max access permission for remote mr requests
    max_rmr_access: ibv_access_flags,
}

impl Default for AgentInitAttr {
    #[inline]
    fn default() -> Self {
        Self {
            max_message_length: MAX_MSG_LEN,
            max_rmr_access: *DEFAULT_ACCESS,
        }
    }
}

/// The builder for the `Rdma`, it follows the builder pattern.
#[derive(Default)]
pub struct RdmaBuilder {
    /// Rdma device name
    dev_attr: DeviceInitAttr,
    /// initial CQ attributes
    cq_attr: CQInitAttr,
    /// initial QP attributes
    qp_attr: QPInitAttr,
    /// initial MR attributes
    mr_attr: MRInitAttr,
    /// initial Agent attributes
    agent_attr: AgentInitAttr,
}

impl RdmaBuilder {
    /// Create a default builder
    /// The default settings are:
    ///     dev name: None
    ///     access right: `LocalWrite` | `RemoteRead` | `RemoteWrite` | `RemoteAtomic`
    ///     complete queue size: 16
    ///     port number: 1
    ///     gid index: 1
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
            &self.dev_attr,
            self.cq_attr,
            self.qp_attr,
            self.mr_attr,
            self.agent_attr,
        )
    }

    /// Establish connection with RDMA server
    ///
    /// Used with `listen`
    ///
    /// # Examples
    ///
    /// ```
    /// use async_rdma::RdmaBuilder;
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let _rdma = RdmaBuilder::default().connect(addr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let _rdma = RdmaBuilder::default().listen(addr).await?;
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
    pub async fn connect<A: ToSocketAddrs>(self, addr: A) -> io::Result<Rdma> {
        match self.qp_attr.conn_type {
            ConnectionType::RCSocket => {
                let mut rdma = self.build()?;
                let remote = tcp_connect_helper(addr, &rdma.endpoint()).await?;
                let (recv_attr, send_attr) =
                    builders_into_attrs(self.qp_attr.rq_attr, self.qp_attr.sq_attr, &remote)?;
                rdma.qp_handshake(recv_attr, send_attr)?;
                rdma.init_agent(
                    self.agent_attr.max_message_length,
                    self.agent_attr.max_rmr_access,
                )
                .await?;
                Ok(rdma)
            }
            ConnectionType::RCCM | ConnectionType::RCIBV => Err(io::Error::new(
                io::ErrorKind::Other,
                "ConnectionType should be XXSocket",
            )),
        }
    }

    /// Connect to remote end by raw ibv information.
    ///
    /// You can get the destination qp information in any way and use this interface to establish connection.
    ///
    /// The example is the same as `Rdma::ibv_connect`.
    #[inline]
    pub async fn ibv_connect(self, remote: QueuePairEndpoint) -> io::Result<Rdma> {
        match self.qp_attr.conn_type {
            ConnectionType::RCIBV => {
                let mut rdma = self.build()?;
                let (recv_attr, send_attr) =
                    builders_into_attrs(self.qp_attr.rq_attr, self.qp_attr.sq_attr, &remote)?;
                rdma.qp_handshake(recv_attr, send_attr)?;
                rdma.init_agent(
                    self.agent_attr.max_message_length,
                    self.agent_attr.max_rmr_access,
                )
                .await?;
                Ok(rdma)
            }
            ConnectionType::RCCM | ConnectionType::RCSocket => Err(io::Error::new(
                io::ErrorKind::Other,
                "ConnectionType should be XXIBV",
            )),
        }
    }

    /// Establish connection with RDMA CM server
    ///
    /// Application scenario can be seen in `[/example/cm_client.rs]`
    ///
    /// # Examples
    ///
    /// ```
    /// use async_rdma::{ConnectionType, RdmaBuilder};
    /// use local_ip_address::local_ip;
    /// use portpicker::pick_unused_port;
    /// use rdma_sys::*;
    /// use std::{io, ptr::null_mut, time::Duration};
    ///
    /// static SERVER_NODE: &str = "0.0.0.0\0";
    ///
    /// async fn client(node: &str, service: &str) -> io::Result<()> {
    ///     let _rdma = RdmaBuilder::default()
    ///         .set_conn_type(ConnectionType::RCCM)
    ///         .set_raw(true)
    ///         .cm_connect(node, service)
    ///         .await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(node: &str, service: &str) -> io::Result<()> {
    ///     let mut hints = unsafe { std::mem::zeroed::<rdma_addrinfo>() };
    ///     let mut res: *mut rdma_addrinfo = null_mut();
    ///     hints.ai_flags = RAI_PASSIVE.try_into().unwrap();
    ///     hints.ai_port_space = rdma_port_space::RDMA_PS_TCP.try_into().unwrap();
    ///     let mut ret = unsafe {
    ///         rdma_getaddrinfo(
    ///             node.as_ptr().cast(),
    ///             service.as_ptr().cast(),
    ///             &hints,
    ///             &mut res,
    ///         )
    ///     };
    ///     if ret != 0 {
    ///         println!("rdma_getaddrinfo");
    ///         return Err(io::Error::last_os_error());
    ///     }
    ///
    ///     let mut listen_id = null_mut();
    ///     let mut id = null_mut();
    ///     let mut init_attr = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
    ///     init_attr.cap.max_send_wr = 1;
    ///     init_attr.cap.max_recv_wr = 1;
    ///     ret = unsafe { rdma_create_ep(&mut listen_id, res, null_mut(), &mut init_attr) };
    ///     if ret != 0 {
    ///         println!("rdma_create_ep");
    ///         return Err(io::Error::last_os_error());
    ///     }
    ///
    ///     ret = unsafe { rdma_listen(listen_id, 0) };
    ///     if ret != 0 {
    ///         println!("rdma_listen");
    ///         return Err(io::Error::last_os_error());
    ///     }
    ///
    ///     ret = unsafe { rdma_get_request(listen_id, &mut id) };
    ///     if ret != 0 {
    ///         println!("rdma_get_request");
    ///         return Err(io::Error::last_os_error());
    ///     }
    ///
    ///     ret = unsafe { rdma_accept(id, null_mut()) };
    ///     if ret != 0 {
    ///         println!("rdma_get_request");
    ///         return Err(io::Error::last_os_error());
    ///     }
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let port = pick_unused_port().unwrap();
    ///     let server_service = port.to_string() + "\0";
    ///     let client_service = server_service.clone();
    ///     std::thread::spawn(move || server(SERVER_NODE, &server_service));
    ///     tokio::time::sleep(Duration::from_secs(1)).await;
    ///     let node = local_ip().unwrap().to_string() + "\0";
    ///     client(&node, &client_service)
    ///         .await
    ///         .map_err(|err| println!("{}", err))
    ///         .unwrap();
    /// }
    /// ```
    #[inline]
    #[cfg(feature = "cm")]
    pub async fn cm_connect(self, node: &str, service: &str) -> io::Result<Rdma> {
        match self.qp_attr.conn_type {
            ConnectionType::RCSocket | ConnectionType::RCIBV => Err(io::Error::new(
                io::ErrorKind::Other,
                "ConnectionType should be XXCM",
            )),
            ConnectionType::RCCM => {
                let max_message_length = self.agent_attr.max_message_length;
                let max_rmr_access = self.agent_attr.max_rmr_access;
                let mut rdma = self.build()?;
                cm_connect_helper(&mut rdma, node, service)?;
                rdma.init_agent(max_message_length, max_rmr_access).await?;
                Ok(rdma)
            }
        }
    }

    /// Listen to the address to wait for a connection to be established
    ///
    /// Used with `connect`
    ///
    /// # Examples
    ///
    /// ```
    /// use async_rdma::RdmaBuilder;
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let _rdma = RdmaBuilder::default().connect(addr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let _rdma = RdmaBuilder::default().listen(addr).await?;
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
    pub async fn listen<A: ToSocketAddrs>(self, addr: A) -> io::Result<Rdma> {
        match self.qp_attr.conn_type {
            ConnectionType::RCSocket => {
                let recv_attr_builder = self.qp_attr.rq_attr;
                let send_attr_builder = self.qp_attr.sq_attr;
                let mut rdma = self.build()?;
                let tcp_listener = TcpListener::bind(addr).await?;
                let remote = tcp_listen(&tcp_listener, &rdma.endpoint()).await?;
                let (recv_attr, send_attr) =
                    builders_into_attrs(recv_attr_builder, send_attr_builder, &remote)?;
                rdma.qp_handshake(recv_attr, send_attr)?;
                debug!("handshake done");
                rdma.init_agent(
                    self.agent_attr.max_message_length,
                    self.agent_attr.max_rmr_access,
                )
                .await?;
                let _ = rdma
                    .clone_attr
                    .set_tcp_listener(Some(Arc::new(Mutex::new(tcp_listener))));
                Ok(rdma)
            }
            ConnectionType::RCCM | ConnectionType::RCIBV => Err(io::Error::new(
                io::ErrorKind::Other,
                "ConnectionType should be XXSocket",
            )),
        }
    }

    /// Set device name
    #[inline]
    #[must_use]
    pub fn set_dev(mut self, dev: &str) -> Self {
        self.dev_attr.dev_name = Some(dev.to_owned());
        self
    }

    /// Set the complete queue size
    #[inline]
    #[must_use]
    pub fn set_cq_size(mut self, cq_size: u32) -> Self {
        self.cq_attr.cq_size = cq_size;
        self
    }

    /// Set the gid index
    #[inline]
    #[must_use]
    pub fn set_gid_index(mut self, gid_index: usize) -> Self {
        // TODO: check gid_index scope(ibv_port_attr.gid_tbl_len)
        let _ = self
            .qp_attr
            .rq_attr
            .address_handler()
            .grh()
            .sgid_index(gid_index.cast());
        self
    }

    /// Set the port number
    #[inline]
    #[must_use]
    pub fn set_port_num(mut self, port_num: u8) -> Self {
        let _ = self.qp_attr.init_attr.port_num(port_num);
        self
    }

    /// Set the connection type
    #[inline]
    #[must_use]
    pub fn set_conn_type(mut self, conn_type: ConnectionType) -> Self {
        self.qp_attr.conn_type = conn_type;
        self
    }

    /// Set if send/recv raw data
    #[inline]
    #[must_use]
    pub fn set_raw(mut self, raw: bool) -> Self {
        self.qp_attr.raw = raw;
        self
    }

    /// Set maximum number of outstanding send requests in the send queue
    #[inline]
    #[must_use]
    pub fn set_qp_max_send_wr(mut self, max_send_wr: u32) -> Self {
        let _ = self.qp_attr.init_attr.qp_cap().max_send_wr(max_send_wr);
        self
    }

    /// Set maximum number of outstanding receive requests in the receive queue
    #[inline]
    #[must_use]
    pub fn set_qp_max_recv_wr(mut self, max_recv_wr: u32) -> Self {
        let _ = self.qp_attr.init_attr.qp_cap().max_recv_wr(max_recv_wr);
        self
    }

    /// Set maximum number of scatter/gather elements (SGE) in a WR on the send queue
    #[inline]
    #[must_use]
    pub fn set_qp_max_send_sge(mut self, max_send_sge: u32) -> Self {
        let _ = self.qp_attr.init_attr.qp_cap().max_send_sge(max_send_sge);
        self
    }

    /// Set maximum number of scatter/gather elements (SGE) in a WR on the receive queue
    #[inline]
    #[must_use]
    pub fn set_qp_max_recv_sge(mut self, max_recv_sge: u32) -> Self {
        let _ = self.qp_attr.init_attr.qp_cap().max_recv_sge(max_recv_sge);
        self
    }

    /// Set default `QP` access
    #[inline]
    #[must_use]
    pub fn set_qp_access(mut self, flags: BitFlags<AccessFlag>) -> Self {
        let _ = self.qp_attr.init_attr.access(flags_into_ibv_access(flags));
        self
    }

    /// Set default `MR` access
    #[inline]
    #[must_use]
    pub fn set_mr_access(mut self, flags: BitFlags<AccessFlag>) -> Self {
        let _ = self.qp_attr.init_attr.access(flags_into_ibv_access(flags));
        self
    }

    /// Set the stragety to manage `MR`s
    #[inline]
    #[must_use]
    pub fn set_mr_strategy(mut self, strategy: MRManageStrategy) -> Self {
        self.mr_attr.strategy = strategy;
        self
    }

    /// Set max length of message send/recv by Agent
    #[inline]
    #[must_use]
    pub fn set_max_message_length(mut self, max_msg_len: usize) -> Self {
        self.agent_attr.max_message_length = max_msg_len;
        self
    }

    /// Set max access permission for remote mr requests
    #[inline]
    #[must_use]
    pub fn set_max_rmr_access(mut self, flags: BitFlags<AccessFlag>) -> Self {
        self.agent_attr.max_rmr_access = flags_into_ibv_access(flags);
        self
    }

    // TODO: check values of rq/sq_attr

    /// Set the number of RDMA Reads & atomic operations outstanding at any time that can be
    /// handled by this QP as a destination. Relevant only for RC QPs.
    #[inline]
    #[must_use]
    pub fn set_max_dest_rd_atomic(mut self, max_dest_rd_atomic: u8) -> Self {
        let _ = self.qp_attr.rq_attr.max_dest_rd_atomic(max_dest_rd_atomic);
        self
    }

    /// Set the minimum RNR NAK Timer Field Value. When an incoming message to this QP should consume a Work
    /// Request from the Receive Queue, but not Work Request is outstanding on that Queue, the QP will
    /// send an RNR NAK packet to the initiator. It does not affect RNR NAKs sent for other reasons.
    ///
    /// The value can be one of the following numeric values since those values aren’t enumerated:
    ///
    /// 0 - 655.36 milliseconds delay
    ///
    /// 1 - 0.01 milliseconds delay
    ///
    /// 2 - 0.02 milliseconds delay
    ///
    /// 3 - 0.03 milliseconds delay
    ///
    /// 4 - 0.04 milliseconds delay
    ///
    /// 5 - 0.06 milliseconds delay
    ///
    /// 6 - 0.08 milliseconds delay
    ///
    /// 7 - 0.12 milliseconds delay
    ///
    /// 8 - 0.16 milliseconds delay
    ///
    /// 9 - 0.24 milliseconds delay
    ///
    /// 10 - 0.32 milliseconds delay
    ///
    /// 11 - 0.48 milliseconds delay
    ///
    /// 12 - 0.64 milliseconds delay
    ///
    /// 13 - 0.96 milliseconds delay
    ///
    /// 14 - 1.28 milliseconds delay
    ///
    /// 15 - 1.92 milliseconds delay
    ///
    /// 16 - 2.56 milliseconds delay
    ///
    /// 17 - 3.84 milliseconds delay
    ///
    /// 18 - 5.12 milliseconds delay
    ///
    /// 19 - 7.68 milliseconds delay
    ///
    /// 20 - 10.24 milliseconds delay
    ///
    /// 21 - 15.36 milliseconds delay
    ///
    /// 22 - 20.48 milliseconds delay
    ///
    /// 23 - 30.72 milliseconds delay
    ///
    /// 24 - 40.96 milliseconds delay
    ///
    /// 25 - 61.44 milliseconds delay
    ///
    /// 26 - 81.92 milliseconds delay
    ///
    /// 27 - 122.88 milliseconds delay
    ///
    /// 28 - 163.84 milliseconds delay
    ///
    /// 29 - 245.76 milliseconds delay
    ///
    /// 30 - 327.68 milliseconds delay
    ///
    /// 31 - 491.52 milliseconds delay
    ///
    /// Relevant only for RC QPs
    #[inline]
    #[must_use]
    pub fn set_min_rnr_timer(mut self, min_rnr_timer: u8) -> Self {
        let _ = self.qp_attr.rq_attr.min_rnr_timer(min_rnr_timer);
        self
    }

    /// Set a 24 bits value of the Packet Sequence Number of the received packets for RC and UC QPs
    #[inline]
    #[must_use]
    pub fn set_rq_psn(mut self, rq_psn: u32) -> Self {
        let _ = self.qp_attr.rq_attr.rq_psn(rq_psn);
        self
    }

    /// Set the number of RDMA Reads & atomic operations outstanding at any time that can be handled by
    /// this QP as an initiator. Relevant only for RC QPs.
    #[inline]
    #[must_use]
    pub fn set_max_rd_atomic(mut self, max_rd_atomic: u8) -> Self {
        let _ = self.qp_attr.sq_attr.max_rd_atomic(max_rd_atomic);
        self
    }

    /// Set a 3 bits value of the total number of times that the QP will try to resend the packets before
    /// reporting an error because the remote side doesn't answer in the primary path
    #[inline]
    #[must_use]
    pub fn set_retry_cnt(mut self, retry_cnt: u8) -> Self {
        let _ = self.qp_attr.sq_attr.retry_cnt(retry_cnt);
        self
    }

    /// Set a 3 bits value of the total number of times that the QP will try to resend the packets when an
    /// RNR NACK was sent by the remote QP before reporting an error. The value 7 is special and specify
    /// to retry infinite times in case of RNR.
    #[inline]
    #[must_use]
    pub fn set_rnr_retry(mut self, rnr_retry: u8) -> Self {
        let _ = self.qp_attr.sq_attr.rnr_retry(rnr_retry);
        self
    }

    /// Set a 24 bits value of the Packet Sequence Number of the sent packets for any QP.
    #[inline]
    #[must_use]
    pub fn set_sq_psn(mut self, sq_psn: u32) -> Self {
        let _ = self.qp_attr.sq_attr.sq_psn(sq_psn);
        self
    }

    /// Set the minimum timeout that a QP waits for ACK/NACK from remote QP before retransmitting the packet.
    /// The value zero is special value which means wait an infinite time for the ACK/NACK (useful for
    /// debugging). For any other value of timeout, the time calculation is: `4.09*2^timeout`.
    /// Relevant only to RC QPs.
    #[inline]
    #[must_use]
    pub fn set_timeout(mut self, timeout: u8) -> Self {
        let _ = self.qp_attr.sq_attr.timeout(timeout);
        self
    }

    /// Set the Service Level to be used. 4 bits.
    #[inline]
    #[must_use]
    pub fn set_service_level(mut self, sl: u8) -> Self {
        let _ = self.qp_attr.rq_attr.address_handler().service_level(sl);
        self
    }

    /// Set the used Source Path Bits. This is useful when LMC is used in the port, i.e. each port
    /// covers a range of LIDs. The packets are being sent with the port's base LID, bitwised `ORed`
    /// with the value of the source path bits. The value 0 indicates the port's base LID is used.
    #[inline]
    #[must_use]
    pub fn set_src_path_bits(mut self, src_path_bits: u8) -> Self {
        let _ = self
            .qp_attr
            .rq_attr
            .address_handler()
            .src_path_bits(src_path_bits);
        self
    }

    /// Set the value which limits the rate of packets that being sent to the subnet. This can be
    /// useful if the rate of the packet origin is higher than the rate of the destination.
    #[inline]
    #[must_use]
    pub fn set_static_rate(mut self, static_rate: u8) -> Self {
        let _ = self
            .qp_attr
            .rq_attr
            .address_handler()
            .static_rate(static_rate);
        self
    }

    /// If this value is set to a non-zero value, it gives a hint for switches and routers
    /// with multiple outbound paths that these sequence of packets must be delivered in order,
    /// those staying on the same path, so that they won't be reordered. 20 bits.
    #[inline]
    #[must_use]
    pub fn set_flow_label(mut self, flow_label: u32) -> Self {
        let _ = self
            .qp_attr
            .rq_attr
            .address_handler()
            .grh()
            .flow_label(flow_label);
        self
    }

    /// Set the number of hops (i.e. the number of routers) that the packet is permitted to take before
    /// being discarded. This ensures that a packet will not loop indefinitely between routers if a
    /// routing loop occur. Each router decrement by one this value at the packet and when this value
    /// reaches 0, this packet is discarded. Setting the value to 0 or 1 will ensure that the packet
    /// won't leave the local subnet.
    #[inline]
    #[must_use]
    pub fn set_hop_limit(mut self, hop_limit: u8) -> Self {
        let _ = self
            .qp_attr
            .rq_attr
            .address_handler()
            .grh()
            .hop_limit(hop_limit);
        self
    }

    /// Using this value, the originator of the packets specifies the required delivery priority for
    /// handling them by the routers.
    #[inline]
    #[must_use]
    pub fn set_traffic_class(mut self, traffic_class: u8) -> Self {
        let _ = self
            .qp_attr
            .rq_attr
            .address_handler()
            .grh()
            .traffic_class(traffic_class);
        self
    }

    /// Set Primary key index. The value of the entry in the pkey table that outgoing
    /// packets from this QP will be sent with and incoming packets to this QP will be
    /// verified within the Primary path.
    #[inline]
    #[must_use]
    pub fn set_pkey_index(mut self, pkey_index: u16) -> Self {
        let _ = self.qp_attr.init_attr.pkey_index(pkey_index);
        self
    }

    /// Set the path MTU (Maximum Transfer Unit) i.e. the maximum payload size of a packet that
    /// can be transferred in the path. For UC and RC QPs, when needed, the RDMA device will
    /// automatically fragment the messages to packet of this size.
    #[inline]
    #[must_use]
    pub fn set_mtu(mut self, mtu: MTU) -> Self {
        let _ = self.qp_attr.rq_attr.mtu(mtu);
        self
    }
}

impl Debug for RdmaBuilder {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RdmaBuilder")
            .field("dev_name", &self.dev_attr.dev_name)
            .field("cq_size", &self.cq_attr.cq_size)
            .finish()
    }
}

/// Exchange metadata through tcp
async fn tcp_connect_helper<A: ToSocketAddrs>(
    addr: A,
    ep: &QueuePairEndpoint,
) -> io::Result<QueuePairEndpoint> {
    let mut stream = TcpStream::connect(addr).await?;
    let mut endpoint = bincode::serialize(ep).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("failed to serailize the endpoint, {:?}", e),
        )
    })?;
    stream.write_all(&endpoint).await?;
    // the byte number is not important, as read_exact will fill the buffer
    let _ = stream.read_exact(endpoint.as_mut()).await?;
    bincode::deserialize(&endpoint).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("failed to deserailize the endpoint, {:?}", e),
        )
    })
}

/// Listen for exchanging metadata through tcp
async fn tcp_listen(
    tcp_listener: &TcpListener,
    ep: &QueuePairEndpoint,
) -> io::Result<QueuePairEndpoint> {
    let (mut stream, _) = tcp_listener.accept().await?;

    let endpoint_size = bincode::serialized_size(ep).map_err(|e| {
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
    let local = bincode::serialize(ep).map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("failed to deserialize remote endpoint, {:?}", e),
        )
    })?;
    stream.write_all(&local).await?;
    Ok(remote)
}

/// Exchange metadata and setup connection through cm
#[inline]
#[cfg(feature = "cm")]
fn cm_connect_helper(rdma: &mut Rdma, node: &str, service: &str) -> io::Result<()> {
    // SAFETY: POD FFI type
    let mut hints = unsafe { std::mem::zeroed::<rdma_addrinfo>() };
    let mut info: *mut rdma_addrinfo = null_mut();
    hints.ai_port_space = rdma_port_space::RDMA_PS_TCP.cast();
    // Safety: ffi
    let mut ret = unsafe {
        rdma_getaddrinfo(
            node.as_ptr().cast(),
            service.as_ptr().cast(),
            &hints,
            &mut info,
        )
    };
    if ret != 0_i32 {
        return Err(log_ret_last_os_err());
    }

    let mut id: *mut rdma_cm_id = null_mut();
    // Safety: ffi
    ret = unsafe { rdma_create_ep(&mut id, info, rdma.pd.as_ptr(), null_mut()) };
    if ret != 0_i32 {
        // Safety: ffi
        unsafe {
            rdma_freeaddrinfo(info);
        }
        return Err(log_ret_last_os_err());
    }

    // Safety: id was initialized by `rdma_create_ep`
    unsafe {
        debug!(
            "cm_id: {:?},{:?},{:?},{:?},{:?},{:?},{:?}",
            (*id).qp,
            (*id).pd,
            (*id).verbs,
            (*id).recv_cq_channel,
            (*id).send_cq_channel,
            (*id).recv_cq,
            (*id).send_cq
        );
        (*id).qp = rdma.qp.as_ptr();
        (*id).pd = rdma.pd.as_ptr();
        (*id).verbs = rdma.ctx.as_ptr();
        (*id).recv_cq_channel = rdma.qp.event_listener().cq.event_channel().as_ptr();
        (*id).recv_cq_channel = rdma.qp.event_listener().cq.event_channel().as_ptr();
        (*id).recv_cq = rdma.qp.event_listener().cq.as_ptr();
        (*id).send_cq = rdma.qp.event_listener().cq.as_ptr();
        debug!(
            "cm_id: {:?},{:?},{:?},{:?},{:?},{:?},{:?}",
            (*id).qp,
            (*id).pd,
            (*id).verbs,
            (*id).recv_cq_channel,
            (*id).send_cq_channel,
            (*id).recv_cq,
            (*id).send_cq
        );
    }
    // Safety: ffi
    ret = unsafe { rdma_connect(id, null_mut()) };
    if ret != 0_i32 {
        // Safety: ffi
        unsafe {
            let _ = rdma_disconnect(id);
        }
        return Err(log_ret_last_os_err());
    }

    Ok(())
}

/// Method of establishing a connection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionType {
    /// Establish reliable connection through `Socket` APIs.
    RCSocket,
    /// Establish reliable connection through `CM` APIs.
    RCCM,
    /// Establish reliable connection through `IBV` APIs.
    RCIBV,
}

/// Attributes for creating new `Rdma`s through `clone`
#[derive(Debug, Clone, Builder, Getters, MutGetters, Setters, CopyGetters)]
#[getset(set, get, get_mut)]
pub(crate) struct CloneAttr {
    /// Tcp listener used for new connections
    #[builder(default = "None")]
    tcp_listener: Option<Arc<Mutex<TcpListener>>>,
    /// Clone `Rdma` with new `ProtectionDomain`
    pd: Arc<ProtectionDomain>,
    /// Clone `Rdma` with new agent attributes
    #[builder(default = "AgentInitAttr::default()")]
    agent_attr: AgentInitAttr,
    /// Attributes for init QP
    qp_init_attr: QueuePairInitAttrBuilder,
    /// Attributes for QP's send queue to receive messages
    sq_attr: SQAttrBuilder,
    /// Attributes for QP's receive queue to receive messages
    rq_attr: RQAttrBuilder,
}

impl_into_io_error!(CloneAttrBuilderError);

/// Rdma handler, the only interface that the users deal with rdma
#[derive(Debug)]
pub struct Rdma {
    /// device context
    ctx: Arc<Context>,
    /// protection domain
    pd: Arc<ProtectionDomain>,
    /// Memory region allocator
    allocator: Arc<MrAllocator>,
    /// Queue pair
    qp: Arc<QueuePair>,
    /// Background agent
    agent: Option<Arc<Agent>>,
    /// Connection type
    conn_type: ConnectionType,
    /// If send/recv raw data
    raw: bool,
    /// Attributes for creating new `Rdma`s through `clone`
    clone_attr: CloneAttr,
}

impl Rdma {
    /// create a new `Rdma` instance
    fn new(
        dev_attr: &DeviceInitAttr,
        cq_attr: CQInitAttr,
        mut qp_attr: QPInitAttr,
        mr_attr: MRInitAttr,
        agent_attr: AgentInitAttr,
    ) -> io::Result<Self> {
        let ctx = Arc::new(Context::open(
            dev_attr.dev_name.as_deref(),
            qp_attr.init_attr.get_port_num().unwrap_or(DEFAULT_PORT_NUM),
            qp_attr
                .rq_attr
                .address_handler()
                .grh()
                .get_sgid_index()
                .unwrap_or_else(|| DEFAULT_GID_INDEX.cast())
                .cast(),
        )?);
        let ec = ctx.create_event_channel()?;
        let cq = Arc::new(ctx.create_completion_queue(cq_attr.cq_size, ec, cq_attr.max_cqe)?);
        let event_listener = EventListener::new(Arc::clone(&cq));
        let pd = Arc::new(ctx.create_protection_domain()?);
        let allocator = Arc::new(MrAllocator::new(
            Arc::<ProtectionDomain>::clone(&pd),
            mr_attr,
        ));

        let _ = qp_attr.init_attr.send_cq(cq.as_ptr()).recv_cq(cq.as_ptr());
        let clone_attr = CloneAttrBuilder::default()
            .pd(Arc::clone(&pd))
            .qp_init_attr(qp_attr.init_attr)
            .sq_attr(qp_attr.sq_attr)
            .rq_attr(qp_attr.rq_attr)
            .agent_attr(agent_attr)
            .build()?;
        // SAFETY: ffi
        let qp_init_attr = qp_attr.init_attr.clone().build()?;
        let inner_qp =
            NonNull::new(unsafe { rdma_sys::ibv_create_qp(pd.as_ptr(), &mut qp_init_attr.into()) })
                .ok_or_else(log_ret_last_os_err)?;

        let mut qp = pd
            .create_queue_pair_builder()
            .event_listener(Arc::new(event_listener))
            .inner_qp(inner_qp)
            .cur_state(Arc::new(RwLock::new(QueuePairState::Unknown)))
            .build()?;

        qp.modify_to_init(
            *qp_init_attr.access(),
            *qp_init_attr.port_num(),
            *qp_init_attr.pkey_index(),
        )?;

        Ok(Self {
            ctx,
            pd,
            qp: Arc::new(qp),
            agent: None,
            allocator,
            conn_type: qp_attr.conn_type,
            raw: qp_attr.raw,
            clone_attr,
        })
    }

    /// Connect to remote end by raw ibv information.
    ///
    /// You can get the destination qp information in any way and use this interface to establish connection.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_rdma::{
    ///     ConnectionType, LocalMrReadAccess, LocalMrWriteAccess, QueuePairEndpoint, Rdma, RdmaBuilder,
    /// };
    /// use std::{
    ///     alloc::Layout,
    ///     io::{self, Write},
    ///     time::Duration,
    /// };
    ///
    /// async fn client(client_rdma: Rdma, server_info: QueuePairEndpoint) -> io::Result<()> {
    ///     let rdma = client_rdma.ibv_connect(server_info).await?;
    ///     // alloc 8 bytes local memory
    ///     let mut lmr = rdma.alloc_local_mr(Layout::new::<[u8; 8]>())?;
    ///     // write data into lmr
    ///     let _num = lmr.as_mut_slice().write(&[1_u8; 8])?;
    ///     // send data in mr to the remote end
    ///     rdma.send(&lmr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(server_rdma: Rdma, client_info: QueuePairEndpoint) -> io::Result<()> {
    ///     let rdma = server_rdma.ibv_connect(client_info).await?;
    ///     // receive data
    ///     let lmr = rdma.receive().await?;
    ///     let data = *lmr.as_slice();
    ///     assert_eq!(data, [1_u8; 8]);
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let server_rdma = RdmaBuilder::default()
    ///         .set_conn_type(ConnectionType::RCIBV)
    ///         .build()
    ///         .unwrap();
    ///     let server_info = server_rdma.get_qp_endpoint();
    ///     let client_rdma = RdmaBuilder::default()
    ///         .set_conn_type(ConnectionType::RCIBV)
    ///         .build()
    ///         .unwrap();
    ///     let client_info = client_rdma.get_qp_endpoint();
    ///     std::thread::spawn(move || server(server_rdma, client_info));
    ///     tokio::time::sleep(Duration::from_secs(3)).await;
    ///     client(client_rdma, server_info)
    ///         .await
    ///         .map_err(|err| println!("{}", err))
    ///         .unwrap();
    /// }
    /// ```
    #[inline]
    pub async fn ibv_connect(mut self, remote: QueuePairEndpoint) -> io::Result<Self> {
        match self.conn_type {
            ConnectionType::RCIBV => {
                let (recv_attr, send_attr) =
                    builders_into_attrs(self.clone_attr.rq_attr, self.clone_attr.sq_attr, &remote)?;
                self.qp_handshake(recv_attr, send_attr)?;
                self.init_agent(
                    self.clone_attr.agent_attr.max_message_length,
                    self.clone_attr.agent_attr.max_rmr_access,
                )
                .await?;
                Ok(self)
            }
            ConnectionType::RCCM | ConnectionType::RCSocket => Err(io::Error::new(
                io::ErrorKind::Other,
                "ConnectionType should be XXIBV",
            )),
        }
    }

    /// Create a new `Rdma` that has the same `mr_allocator` and `event_listener` as parent.
    fn clone(&self) -> io::Result<Self> {
        let qp_init_attr = self.clone_attr.qp_init_attr().build()?;

        // SAFETY: ffi
        let inner_qp = NonNull::new(unsafe {
            rdma_sys::ibv_create_qp(self.pd.as_ptr(), &mut qp_init_attr.into())
        })
        .ok_or_else(log_ret_last_os_err)?;

        let mut qp = QueuePairBuilder::default()
            .pd(Arc::clone(self.clone_attr.pd()))
            .event_listener(Arc::clone(self.qp.event_listener()))
            .inner_qp(inner_qp)
            .cur_state(Arc::new(RwLock::new(QueuePairState::Unknown)))
            .build()?;

        qp.modify_to_init(
            *qp_init_attr.access(),
            *qp_init_attr.port_num(),
            *qp_init_attr.pkey_index(),
        )?;

        Ok(Self {
            ctx: Arc::clone(&self.ctx),
            pd: Arc::clone(self.clone_attr.pd()),
            qp: Arc::new(qp),
            agent: None,
            allocator: Arc::clone(&self.allocator),
            conn_type: self.conn_type,
            raw: self.raw,
            clone_attr: self.clone_attr.clone(),
        })
    }

    /// get the queue pair endpoint information
    fn endpoint(&self) -> QueuePairEndpoint {
        self.qp.endpoint()
    }

    /// to hand shake the qp so that it works
    fn qp_handshake(&mut self, recv_attr: RQAttr, send_attr: SQAttr) -> io::Result<()> {
        self.qp.modify_to_rtr(recv_attr)?;
        self.qp.modify_to_rts(send_attr)?;
        debug!("rts");
        Ok(())
    }

    /// Agent init helper
    async fn init_agent(
        &mut self,
        max_message_length: usize,
        max_rmr_access: ibv_access_flags,
    ) -> io::Result<()> {
        if !self.raw {
            let agent = Arc::new(Agent::new(
                Arc::<QueuePair>::clone(&self.qp),
                Arc::<MrAllocator>::clone(&self.allocator),
                max_message_length,
                max_rmr_access,
            )?);
            self.agent = Some(agent);
            // wait for the remote agent to prepare
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Ok(())
    }

    /// Listen for new connections using the same `mr_allocator` and `event_listener` as parent `Rdma`
    ///
    /// Used with `connect` and `new_connect`
    ///
    /// # Examples
    ///
    /// ```
    /// use async_rdma::RdmaBuilder;
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let mut rdma = RdmaBuilder::default().connect(addr).await?;
    ///     for _ in 0..3 {
    ///         let _new_rdma = rdma.new_connect(addr).await?;
    ///     }
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let mut rdma = RdmaBuilder::default().listen(addr).await?;
    ///     for _ in 0..3 {
    ///         let _new_rdma = rdma.listen().await?;
    ///     }
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
    pub async fn listen(&mut self) -> io::Result<Self> {
        match self.conn_type {
            ConnectionType::RCSocket => {
                let mut rdma = self.clone()?;
                let remote = self
                    .clone_attr
                    .tcp_listener
                    .as_ref()
                    .map_or_else(
                        || Err(io::Error::new(io::ErrorKind::Other, "tcp_listener is None")),
                        |tcp_listener| {
                            Ok(async {
                                let tcp_listener = tcp_listener.lock().await;
                                tcp_listen(&tcp_listener, &rdma.endpoint()).await
                            })
                        },
                    )?
                    .await?;
                self.clone_attr.rq_attr_mut().reset_remote_info(&remote);
                let (recv_attr, send_attr) = self.get_rq_sq_attr()?;
                rdma.qp_handshake(recv_attr, send_attr)?;
                debug!("handshake done");
                rdma.init_agent(
                    self.clone_attr.agent_attr.max_message_length,
                    self.clone_attr.agent_attr.max_rmr_access,
                )
                .await?;
                Ok(rdma)
            }
            ConnectionType::RCCM | ConnectionType::RCIBV => Err(io::Error::new(
                io::ErrorKind::Other,
                "ConnectionType should be XXSocket",
            )),
        }
    }

    /// Establish new connections with RDMA server using the same `mr_allocator` and `event_listener` as parent `Rdma`
    ///
    /// Used with `listen`
    ///
    /// # Examples
    ///
    /// ```
    /// use async_rdma::RdmaBuilder;
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let mut rdma = RdmaBuilder::default().connect(addr).await?;
    ///     for _ in 0..3 {
    ///         let _new_rdma = rdma.new_connect(addr).await?;
    ///     }
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let mut rdma = RdmaBuilder::default().listen(addr).await?;
    ///     for _ in 0..3 {
    ///         let _new_rdma = rdma.listen().await?;
    ///     }
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
    pub async fn new_connect<A: ToSocketAddrs>(&mut self, addr: A) -> io::Result<Self> {
        match self.conn_type {
            ConnectionType::RCSocket => {
                let mut rdma = self.clone()?;
                let remote = tcp_connect_helper(addr, &rdma.endpoint()).await?;
                self.clone_attr.rq_attr_mut().reset_remote_info(&remote);
                let (recv_attr, send_attr) = self.get_rq_sq_attr()?;
                rdma.qp_handshake(recv_attr, send_attr)?;
                rdma.init_agent(
                    self.clone_attr.agent_attr.max_message_length,
                    self.clone_attr.agent_attr.max_rmr_access,
                )
                .await?;
                Ok(rdma)
            }
            ConnectionType::RCCM | ConnectionType::RCIBV => Err(io::Error::new(
                io::ErrorKind::Other,
                "ConnectionType should be XXSocket",
            )),
        }
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
    ///     unsafe { *(*lmr.as_mut_ptr() as *mut Data) = Data("hello world".to_string()) };
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
    ///             *(*(*lmr.as_ptr() as *const Data)).0
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

    /// Send raw data in the lm
    ///
    /// Used with `receive_raw`.
    ///
    /// # Examples
    ///
    ///  ```
    /// use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, RdmaBuilder};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io::{self, Write},
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// const RAW_DATA: [u8; 8] = [1_u8; 8];
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = RdmaBuilder::default().set_raw(true).connect(addr).await?;
    ///     let mut lmr = rdma.alloc_local_mr(Layout::for_value(&RAW_DATA))?;
    ///     // put data into lmr
    ///     let _num = lmr.as_mut_slice().write(&RAW_DATA)?;
    ///     // wait for serer to receive first
    ///     tokio::time::sleep(Duration::from_millis(100)).await;
    ///     // send the content of lmr to server
    ///     rdma.send_raw(&lmr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = RdmaBuilder::default().set_raw(true).listen(addr).await?;
    ///     // receive the data sent by client and put it into an mr
    ///     let lmr = rdma.receive_raw(Layout::for_value(&RAW_DATA)).await?;
    ///     // read data from mr
    ///     assert_eq!(*lmr.as_slice(), RAW_DATA);
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
    #[cfg(feature = "raw")]
    pub async fn send_raw(&self, lm: &LocalMr) -> io::Result<()> {
        self.qp.send_sge_raw(&[lm], None).await
    }

    /// Send raw data in the lm with imm
    ///
    /// Used with `receive_raw_with_imm`
    ///
    /// # Examples
    ///
    ///  ```
    /// use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, RdmaBuilder};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io::{self, Write},
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// const RAW_DATA: [u8; 8] = [1_u8; 8];
    /// const IMM: u32 = 1_u32;
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = RdmaBuilder::default().set_raw(true).connect(addr).await?;
    ///     let mut lmr = rdma.alloc_local_mr(Layout::for_value(&RAW_DATA))?;
    ///     // put data into lmr
    ///     let _num = lmr.as_mut_slice().write(&RAW_DATA)?;
    ///     // wait for serer to receive first
    ///     tokio::time::sleep(Duration::from_millis(100)).await;
    ///     // send the content of lmr to server
    ///     rdma.send_raw_with_imm(&lmr, IMM).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = RdmaBuilder::default().set_raw(true).listen(addr).await?;
    ///     // receive the data sent by client and put it into an mr
    ///     let (lmr, imm) = rdma
    ///         .receive_raw_with_imm(Layout::for_value(&RAW_DATA))
    ///         .await?;
    ///     // read data from mr
    ///     assert_eq!(*lmr.as_slice(), RAW_DATA);
    ///     assert_eq!(imm, Some(IMM));
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
    #[cfg(feature = "raw")]
    pub async fn send_raw_with_imm(&self, lm: &LocalMr, imm: u32) -> io::Result<()> {
        self.qp.send_sge_raw(&[lm], Some(imm)).await
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
    ///     unsafe { std::ptr::write(*lmr.as_mut_ptr() as *mut Data, Data(MSG.to_string())) };
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
    ///     unsafe { assert_eq!(MSG.to_string(), *(*(*lmr.as_ptr() as *const Data)).0) };
    ///     // receive the data in mr while avoiding the immediate data is ok.
    ///     let lmr = rdma.receive().await?;
    ///     unsafe { assert_eq!(MSG.to_string(), *(*(*lmr.as_ptr() as *const Data)).0) };
    ///     // `receive_with_imm` works well even if the client didn't send any immediate data.
    ///     // the imm received will be a `None`.
    ///     let (lmr, imm) = rdma.receive_with_imm().await?;
    ///     assert_eq!(imm, None);
    ///     unsafe { assert_eq!(MSG.to_string(), *(*(*lmr.as_ptr() as *const Data)).0) };
    ///     // compared to the above, using `receive` is a better choice.
    ///     let lmr = rdma.receive().await?;
    ///     unsafe { assert_eq!(MSG.to_string(), *(*(*lmr.as_ptr() as *const Data)).0) };
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
    ///     unsafe { *(*lmr.as_mut_ptr() as *mut Data) = Data("hello world".to_string()) };
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
    ///             *(*(*lmr.as_ptr() as *const Data)).0
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

    /// Receive raw data
    ///
    /// Used with `send_raw`.
    ///
    /// # Examples
    ///
    ///  ```
    /// use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, RdmaBuilder};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io::{self, Write},
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// const RAW_DATA: [u8; 8] = [1_u8; 8];
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = RdmaBuilder::default().set_raw(true).connect(addr).await?;
    ///     let mut lmr = rdma.alloc_local_mr(Layout::for_value(&RAW_DATA))?;
    ///     // put data into lmr
    ///     let _num = lmr.as_mut_slice().write(&RAW_DATA)?;
    ///     // wait for serer to receive first
    ///     tokio::time::sleep(Duration::from_millis(100)).await;
    ///     // send the content of lmr to server
    ///     rdma.send_raw(&lmr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = RdmaBuilder::default().set_raw(true).listen(addr).await?;
    ///     // receive the data sent by client and put it into an mr
    ///     let lmr = rdma.receive_raw(Layout::for_value(&RAW_DATA)).await?;
    ///     // read data from mr
    ///     assert_eq!(*lmr.as_slice(), RAW_DATA);
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
    #[cfg(feature = "raw")]
    pub async fn receive_raw(&self, layout: Layout) -> io::Result<LocalMr> {
        let mut lmr = self.alloc_local_mr(layout)?;
        let _imm = self.qp.receive_sge_raw(&[&mut lmr]).await?;
        Ok(lmr)
    }

    /// Receive raw data with imm
    ///
    /// Used with `send_raw_with_imm`
    ///
    /// # Examples
    ///
    ///  ```
    /// use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, RdmaBuilder};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io::{self, Write},
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// const RAW_DATA: [u8; 8] = [1_u8; 8];
    /// const IMM: u32 = 1_u32;
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = RdmaBuilder::default().set_raw(true).connect(addr).await?;
    ///     let mut lmr = rdma.alloc_local_mr(Layout::for_value(&RAW_DATA))?;
    ///     // put data into lmr
    ///     let _num = lmr.as_mut_slice().write(&RAW_DATA)?;
    ///     // wait for serer to receive first
    ///     tokio::time::sleep(Duration::from_millis(100)).await;
    ///     // send the content of lmr to server
    ///     rdma.send_raw_with_imm(&lmr, IMM).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = RdmaBuilder::default().set_raw(true).listen(addr).await?;
    ///     // receive the data sent by client and put it into an mr
    ///     let (lmr, imm) = rdma
    ///         .receive_raw_with_imm(Layout::for_value(&RAW_DATA))
    ///         .await?;
    ///     // read data from mr
    ///     assert_eq!(*lmr.as_slice(), RAW_DATA);
    ///     assert_eq!(imm, Some(IMM));
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
    #[cfg(feature = "raw")]
    pub async fn receive_raw_with_imm(&self, layout: Layout) -> io::Result<(LocalMr, Option<u32>)> {
        let mut lmr = self.alloc_local_mr(layout)?;
        let imm = self.qp.receive_sge_raw(&[&mut lmr]).await?;
        Ok((lmr, imm))
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
    ///     unsafe { std::ptr::write(*lmr.as_mut_ptr() as *mut Data, Data(MSG.to_string())) };
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
    ///     unsafe { assert_eq!(MSG.to_string(), *(*(*lmr.as_ptr() as *const Data)).0) };
    ///     // receive the data in mr while avoiding the immediate data is ok.
    ///     let lmr = rdma.receive().await?;
    ///     unsafe { assert_eq!(MSG.to_string(), *(*(*lmr.as_ptr() as *const Data)).0) };
    ///     // `receive_with_imm` works well even if the client didn't send any immediate data.
    ///     // the imm received will be a `None`.
    ///     let (lmr, imm) = rdma.receive_with_imm().await?;
    ///     assert_eq!(imm, None);
    ///     unsafe { assert_eq!(MSG.to_string(), *(*(*lmr.as_ptr() as *const Data)).0) };
    ///     // compared to the above, using `receive` is a better choice.
    ///     let lmr = rdma.receive().await?;
    ///     unsafe { assert_eq!(MSG.to_string(), *(*(*lmr.as_ptr() as *const Data)).0) };
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
    ///     unsafe { *(*lmr.as_mut_ptr() as *mut Data) = data };
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
    ///     unsafe { assert_eq!(MSG.to_string(), *(*(*lmr.as_ptr() as *const Data)).0) };
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
    ///     unsafe { *(*lmr.as_mut_ptr() as *mut Data) = Data("hello world".to_string()) };
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
    ///             *(*(*lmr.as_ptr() as *const Data)).0
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
    ///     unsafe { *(*lmr.as_mut_ptr() as *mut Data) = Data("hello world".to_string()) };
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
    ///             *(*(*lmr.as_ptr() as *const Data)).0
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
    ///     unsafe { *(*lmr.as_mut_ptr() as *mut Data) = data };
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
    ///     unsafe { assert_eq!(MSG.to_string(), *(*(*lmr.as_ptr() as *const Data)).0) };
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
            .set_gid_index(gid_index.cast())
            .build()?;
        assert!(
            rdma.conn_type == ConnectionType::RCSocket,
            "should set connection type to RCSocket"
        );
        let remote = tcp_connect_helper(addr, &rdma.endpoint()).await?;
        let recv_attr_builder = RQAttrBuilder::default();
        let send_attr_builder = SQAttrBuilder::default();
        let (recv_attr, send_attr) =
            builders_into_attrs(recv_attr_builder, send_attr_builder, &remote)?;
        rdma.qp_handshake(recv_attr, send_attr)?;
        rdma.init_agent(max_message_length, *DEFAULT_ACCESS).await?;
        // wait for server to initialize
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok(rdma)
    }

    /// Establish connection with RDMA CM server
    ///
    /// Application scenario can be seen in `[/example/cm_client.rs]`
    ///
    /// # Examples
    ///
    /// ```
    /// use async_rdma::Rdma;
    /// use local_ip_address::local_ip;
    /// use portpicker::pick_unused_port;
    /// use rdma_sys::*;
    /// use std::{io, ptr::null_mut, time::Duration};
    ///
    /// static SERVER_NODE: &str = "0.0.0.0\0";
    ///
    /// async fn client(node: &str, service: &str) -> io::Result<()> {
    ///     let _rdma = Rdma::cm_connect(node, service, 1, 1, 0).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(node: &str, service: &str) -> io::Result<()> {
    ///     let mut hints = unsafe { std::mem::zeroed::<rdma_addrinfo>() };
    ///     let mut res: *mut rdma_addrinfo = null_mut();
    ///     hints.ai_flags = RAI_PASSIVE.try_into().unwrap();
    ///     hints.ai_port_space = rdma_port_space::RDMA_PS_TCP.try_into().unwrap();
    ///     let mut ret = unsafe {
    ///         rdma_getaddrinfo(
    ///             node.as_ptr().cast(),
    ///             service.as_ptr().cast(),
    ///             &hints,
    ///             &mut res,
    ///         )
    ///     };
    ///     if ret != 0 {
    ///         println!("rdma_getaddrinfo");
    ///         return Err(io::Error::last_os_error());
    ///     }
    ///
    ///     let mut listen_id = null_mut();
    ///     let mut id = null_mut();
    ///     let mut init_attr = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
    ///     init_attr.cap.max_send_wr = 1;
    ///     init_attr.cap.max_recv_wr = 1;
    ///     ret = unsafe { rdma_create_ep(&mut listen_id, res, null_mut(), &mut init_attr) };
    ///     if ret != 0 {
    ///         println!("rdma_create_ep");
    ///         return Err(io::Error::last_os_error());
    ///     }
    ///
    ///     ret = unsafe { rdma_listen(listen_id, 0) };
    ///     if ret != 0 {
    ///         println!("rdma_listen");
    ///         return Err(io::Error::last_os_error());
    ///     }
    ///
    ///     ret = unsafe { rdma_get_request(listen_id, &mut id) };
    ///     if ret != 0 {
    ///         println!("rdma_get_request");
    ///         return Err(io::Error::last_os_error());
    ///     }
    ///
    ///     ret = unsafe { rdma_accept(id, null_mut()) };
    ///     if ret != 0 {
    ///         println!("rdma_get_request");
    ///         return Err(io::Error::last_os_error());
    ///     }
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let port = pick_unused_port().unwrap();
    ///     let server_service = port.to_string() + "\0";
    ///     let client_service = server_service.clone();
    ///     std::thread::spawn(move || server(SERVER_NODE, &server_service));
    ///     tokio::time::sleep(Duration::from_secs(1)).await;
    ///     let node = local_ip().unwrap().to_string() + "\0";
    ///     client(&node, &client_service)
    ///         .await
    ///         .map_err(|err| println!("{}", err))
    ///         .unwrap();
    /// }
    /// ```
    #[inline]
    #[cfg(feature = "cm")]
    pub async fn cm_connect(
        node: &str,
        service: &str,
        port_num: u8,
        gid_index: usize,
        max_message_length: usize,
    ) -> io::Result<Self> {
        let mut rdma = RdmaBuilder::default()
            .set_port_num(port_num)
            .set_gid_index(gid_index)
            .set_raw(true)
            .set_conn_type(ConnectionType::RCCM)
            .build()?;
        assert!(
            rdma.conn_type == ConnectionType::RCCM,
            "should set connection type to RCSocket"
        );
        cm_connect_helper(&mut rdma, node, service)?;
        rdma.init_agent(max_message_length, *DEFAULT_ACCESS).await?;
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
    ///     unsafe { *(*lmr.as_mut_ptr() as *mut Data) = Data("hello world".to_string()) };
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
    ///             *(*(*lmr.as_ptr() as *const Data)).0
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
        self.allocator
            .alloc_zeroed_default_access(&layout, &self.pd)
    }

    /// Allocate a local memory region that has not been initialized
    ///
    /// You can use local mr to `send`&`receive` or `read`&`write` with a remote mr.
    /// The parameter `layout` can be obtained by `Layout::new::<Data>()`.
    /// You can learn the way to write or read data in mr in the following example.
    ///
    /// # Safety
    ///
    /// The newly allocated memory in this `LocalMr` is uninitialized.
    /// Initialize it before using to make it safe.
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
    ///     let mut lmr = unsafe { rdma.alloc_local_mr_uninit(Layout::new::<Data>())? };
    ///     // put data into lmr
    ///     unsafe { *(*lmr.as_mut_ptr() as *mut Data) = Data("hello world".to_string()) };
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
    ///             *(*(*lmr.as_ptr() as *const Data)).0
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
    pub unsafe fn alloc_local_mr_uninit(&self, layout: Layout) -> io::Result<LocalMr> {
        self.allocator.alloc_default_access(&layout, &self.pd)
    }

    /// Allocate a local memory region with specified access
    ///
    /// Use `alloc_local_mr` if you want to alloc memory region with default access.
    ///
    /// If you want more information, please check the documentation and examples
    /// of `alloc_local_mr`.
    ///
    /// # Example
    ///
    /// ```
    /// use async_rdma::{AccessFlag, MrAccess, RdmaBuilder};
    /// use std::alloc::Layout;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let rdma = RdmaBuilder::default().build().unwrap();
    ///     let layout = Layout::new::<[u8; 4096]>();
    ///     let access = AccessFlag::LocalWrite | AccessFlag::RemoteRead;
    ///     let mr = rdma.alloc_local_mr_with_access(layout, access).unwrap();
    ///     assert_eq!(mr.access(), access);
    /// }
    ///
    /// ```
    #[inline]
    pub fn alloc_local_mr_with_access(
        &self,
        layout: Layout,
        access: BitFlags<AccessFlag>,
    ) -> io::Result<LocalMr> {
        self.allocator
            .alloc_zeroed(&layout, flags_into_ibv_access(access), &self.pd)
    }

    /// Allocate a local memory region with specified access that has not been initialized
    ///
    /// Use `alloc_local_mr_uninit` if you want to alloc memory region with default access.
    ///
    /// If you want more information, please check the documentation and examples
    /// of `alloc_local_mr_uninit`.
    ///
    /// # Safety
    ///
    /// The newly allocated memory in this `LocalMr` is uninitialized.
    /// Initialize it before using to make it safe.
    ///
    /// # Example
    ///
    /// ```
    /// use async_rdma::{AccessFlag, MrAccess, RdmaBuilder};
    /// use std::alloc::Layout;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let rdma = RdmaBuilder::default().build().unwrap();
    ///     let layout = Layout::new::<[u8; 4096]>();
    ///     let access = AccessFlag::LocalWrite | AccessFlag::RemoteRead;
    ///     let mr = unsafe {
    ///         rdma.alloc_local_mr_uninit_with_access(layout, access)
    ///             .unwrap()
    ///     };
    ///     assert_eq!(mr.access(), access);
    /// }
    ///
    /// ```
    #[inline]
    pub unsafe fn alloc_local_mr_uninit_with_access(
        &self,
        layout: Layout,
        access: BitFlags<AccessFlag>,
    ) -> io::Result<LocalMr> {
        self.allocator
            .alloc(&layout, flags_into_ibv_access(access), &self.pd)
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
    ///         .request_remote_mr_with_timeout(Layout::new::<Data>(), Duration::from_secs(10))
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
    ///     unsafe { *(*lmr.as_mut_ptr() as *mut Data) = Data("hello world".to_string()) };
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
    ///         *(*(*lmr.as_ptr() as *const Data)).0
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

    /// Set qp access for new `Rdma` that created by `clone`
    ///
    /// Used with `listen`, `new_connect`
    ///
    /// # Examples
    ///
    /// ```
    /// use async_rdma::{AccessFlag, RdmaBuilder};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = RdmaBuilder::default().connect(addr).await?;
    ///     let access = AccessFlag::LocalWrite | AccessFlag::RemoteRead;
    ///     let mut rdma = rdma.set_new_qp_access(access);
    ///     let _new_rdma = rdma.new_connect(addr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let mut rdma = RdmaBuilder::default().listen(addr).await?;
    ///         let _new_rdma = rdma.listen().await?;
    ///     Ok(())
    /// }
    ///
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
    #[must_use]
    pub fn set_new_qp_access(mut self, qp_access: BitFlags<AccessFlag>) -> Self {
        let _ = self
            .clone_attr
            .qp_init_attr
            .access(flags_into_ibv_access(qp_access));
        self
    }

    /// Set max access permission for remote mr requests for new `Rdma` that created by `clone`
    ///
    /// Used with `listen`, `new_connect`
    ///
    /// # Examples
    ///
    /// ```
    /// use async_rdma::{AccessFlag, RdmaBuilder, MrAccess};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let mut rdma = RdmaBuilder::default().connect(addr).await?;
    ///     let rmr = rdma.request_remote_mr(Layout::new::<char>()).await?;
    ///     let new_rdma = rdma.new_connect(addr).await?;
    ///     let new_rmr = new_rdma.request_remote_mr(Layout::new::<char>()).await?;
    ///     let access = AccessFlag::LocalWrite | AccessFlag::RemoteRead;
    ///     assert_eq!(new_rmr.access(), access);
    ///     assert_ne!(rmr.access(), new_rmr.access());
    ///     new_rdma.send_remote_mr(new_rmr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = RdmaBuilder::default().listen(addr).await?;
    ///     let access = AccessFlag::LocalWrite | AccessFlag::RemoteRead;
    ///     let mut rdma = rdma.set_new_max_rmr_access(access);
    ///     let new_rdma = rdma.listen().await?;
    ///     // receive the metadata of the lmr that had been requested by client
    ///     let _lmr = new_rdma.receive_local_mr().await?;
    ///     // wait for the agent thread to send all reponses to the remote.
    ///     tokio::time::sleep(Duration::from_secs(1)).await;
    ///     Ok(())
    /// }
    ///
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
    #[must_use]
    pub fn set_new_max_rmr_access(mut self, max_rmr_access: BitFlags<AccessFlag>) -> Self {
        let _ = self
            .clone_attr
            .agent_attr
            .set_max_rmr_access(flags_into_ibv_access(max_rmr_access));
        self
    }

    /// Set qp access for new `Rdma` that created by `clone`
    ///
    /// Used with `listen`, `new_connect`
    ///
    /// # Examples
    ///
    /// ```
    /// use async_rdma::RdmaBuilder;
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = RdmaBuilder::default().connect(addr).await?;
    ///     let mut rdma = rdma.set_new_port_num(1_u8);
    ///     let _new_rdma = rdma.new_connect(addr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let mut rdma = RdmaBuilder::default().listen(addr).await?;
    ///     let _new_rdma = rdma.listen().await?;
    ///     Ok(())
    /// }
    ///
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
    #[must_use]
    pub fn set_new_port_num(mut self, port_num: u8) -> Self {
        let _ = self.clone_attr.qp_init_attr.port_num(port_num);
        self
    }

    /// Set new `ProtectionDomain` for new `Rdma` that created by `clone` to provide isolation.
    ///
    /// Used with `listen`, `new_connect`
    ///
    /// # Examples
    ///
    /// ```
    /// use async_rdma::RdmaBuilder;
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = RdmaBuilder::default().connect(addr).await?;
    ///     let mut rdma = rdma.set_new_pd()?;
    ///     // then the `Rdma`s created by `new_connect` will have a new `ProtectionDomain`
    ///     let _new_rdma = rdma.new_connect(addr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let mut rdma = RdmaBuilder::default().listen(addr).await?;
    ///     let _new_rdma = rdma.listen().await?;
    ///     Ok(())
    /// }
    ///
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
    pub fn set_new_pd(mut self) -> io::Result<Self> {
        let _ = self
            .clone_attr
            .set_pd(Arc::new(self.ctx.create_protection_domain()?));
        Ok(self)
    }

    /// Get the real state of qp by quering
    ///
    /// # Examples
    ///
    /// ```
    /// use async_rdma::{RdmaBuilder, QueuePairState};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma_init = RdmaBuilder::default().build()?;
    ///     assert_eq!(rdma_init.get_cur_qp_state(), QueuePairState::Init);
    ///     let rdma_send = RdmaBuilder::default().connect(addr).await?;
    ///     assert_eq!(rdma_send.get_cur_qp_state(), QueuePairState::ReadyToSend);
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = RdmaBuilder::default().listen(addr).await?;
    ///     assert_eq!(rdma.get_cur_qp_state(), QueuePairState::ReadyToSend);
    ///     Ok(())
    /// }
    ///
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
    #[must_use]
    pub fn get_cur_qp_state(&self) -> QueuePairState {
        self.qp.cur_state().read().to_owned()
    }

    /// Get the real state of qp by quering
    ///
    /// # Examples
    ///
    /// ```
    /// use async_rdma::{RdmaBuilder, QueuePairState};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma_init = RdmaBuilder::default().build()?;
    ///     assert_eq!(rdma_init.query_qp_state()?, QueuePairState::Init);
    ///     let rdma_send = RdmaBuilder::default().connect(addr).await?;
    ///     assert_eq!(rdma_send.query_qp_state()?, QueuePairState::ReadyToSend);
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = RdmaBuilder::default().listen(addr).await?;
    ///     assert_eq!(rdma.query_qp_state()?, QueuePairState::ReadyToSend);
    ///     Ok(())
    /// }
    ///
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
    pub fn query_qp_state(&self) -> io::Result<QueuePairState> {
        self.qp.query_state()
    }

    /// Get information of this qp for establishing a connection.
    ///
    ///  
    #[inline]
    #[must_use]
    pub fn get_qp_endpoint(&self) -> QueuePairEndpoint {
        self.qp.endpoint()
    }

    /// Get the attrs of send queue and recv queue or create and return a pair of default attrs if
    /// they are none.
    #[inline]
    pub(crate) fn get_rq_sq_attr(&self) -> io::Result<(RQAttr, SQAttr)> {
        let recv_attr = self.clone_attr.rq_attr.build()?;
        let send_attr = self.clone_attr.sq_attr.build()?;
        Ok((recv_attr, send_attr))
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
            .set_gid_index(gid_index.cast())
            .build()?;
        assert!(
            rdma.conn_type == ConnectionType::RCSocket,
            "should set connection type to RCSocket"
        );
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

        let mut recv_attr = RQAttrBuilder::default();
        let send_attr = SQAttrBuilder::default();
        let _ = recv_attr
            .address_handler()
            .port_num(port_num)
            .grh()
            .sgid_index(gid_index.cast());
        let (recv_attr, send_attr) = builders_into_attrs(recv_attr, send_attr, &remote)?;
        rdma.qp_handshake(recv_attr, send_attr)?;

        debug!("handshake done");
        rdma.init_agent(max_message_length, *DEFAULT_ACCESS).await?;
        Ok(rdma)
    }
}
