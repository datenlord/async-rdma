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
//! use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, RdmaBuilder};
//! use portpicker::pick_unused_port;
//! use std::{
//!     alloc::Layout,
//!     io::{self, Write},
//!     net::{Ipv4Addr, SocketAddrV4},
//!     time::Duration,
//! };
//!
//! async fn client(addr: SocketAddrV4) -> io::Result<()> {
//!     let layout = Layout::new::<[u8; 8]>();
//!     let rdma = RdmaBuilder::default().connect(addr).await?;
//!     // alloc 8 bytes remote memory
//!     let mut rmr = rdma.request_remote_mr(layout).await?;
//!     // alloc 8 bytes local memory
//!     let mut lmr = rdma.alloc_local_mr(layout)?;
//!     // write data into lmr
//!     let _num = lmr.as_mut_slice().write(&[1_u8; 8])?;
//!     // write the second half of the data in lmr to the rmr
//!     rdma.write(&lmr.get(4..8).unwrap(), &mut rmr.get_mut(4..8).unwrap())
//!         .await?;
//!     // send rmr's meta data to the remote end
//!     rdma.send_remote_mr(rmr).await?;
//!     Ok(())
//! }
//!
//! #[tokio::main]
//! async fn server(addr: SocketAddrV4) -> io::Result<()> {
//!     let rdma = RdmaBuilder::default().listen(addr).await?;
//!     // receive mr's meta data from client
//!     let lmr = rdma.receive_local_mr().await?;
//!     let data = *lmr.as_slice();
//!     println!("Data written by the client using RDMA WRITE: {:?}", data);
//!     assert_eq!(data, [[0_u8; 4], [1_u8; 4]].concat());
//!     Ok(())
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
//!     std::thread::spawn(move || server(addr));
//!     tokio::time::sleep(Duration::new(1, 0)).await;
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
/// The event channel that notifies the completion or error of a request
mod cq_event_channel;
/// The driver to poll the completion queue
mod cq_event_listener;
/// Error handling utilities
mod error_utilities;
/// Gid for device
mod gid;
/// `HashMap` extension
mod hashmap_extension;
/// Ibv async event listener
mod ibv_event_listener;
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
use cq_event_listener::{CQEventListener, PollingTriggerInput, DEFAULT_CC_EVENT_TIMEOUT};
pub use cq_event_listener::{ManualTrigger, PollingTriggerType};
use derive_builder::Builder;
use enumflags2::BitFlags;
pub use ibv_event_listener::IbvEventType;
pub use memory_region::{
    local::{LocalMr, LocalMrReadAccess, LocalMrWriteAccess},
    remote::{RemoteMr, RemoteMrReadAccess, RemoteMrWriteAccess},
    MrAccess, MrToken, MrTokenBuilder, MrTokenBuilderError,
};
pub use mr_allocator::MRManageStrategy;
use mr_allocator::MrAllocator;
use protection_domain::ProtectionDomain;
use queue_pair::{
    QueuePair, QueuePairInitAttrBuilder, RQAttr, RQAttrBuilder, SQAttr, SQAttrBuilder,
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
use std::{alloc::Layout, fmt::Debug, io, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, ToSocketAddrs},
    sync::{mpsc, Mutex},
};
use tracing::debug;

use crate::queue_pair::builders_into_attrs;
use getset::{CopyGetters, Getters, MutGetters, Setters};
pub use gid::Gid;
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
    /// The timeout value for event listener to wait for the CC's notification.
    ///
    /// The listener will wait for the CC's notification to poll the related CQ until timeout.
    /// After timeout, listener will poll the CQ to make sure no cqe there, and wait again.
    ///
    /// For the devices or drivers not support notification mechanism, this value will be the polling
    /// period, and as a protective measure in other cases.
    cc_event_timeout: Duration,
    /// The type of the `PollingTrigger`
    pt_type: PollingTriggerType,
}

impl Default for AgentInitAttr {
    #[inline]
    fn default() -> Self {
        Self {
            max_message_length: MAX_MSG_LEN,
            max_rmr_access: *DEFAULT_ACCESS,
            cc_event_timeout: DEFAULT_CC_EVENT_TIMEOUT,
            pt_type: PollingTriggerType::default(),
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

    /// Set the max number of CQE in a polling.
    #[inline]
    #[must_use]
    pub fn set_max_cqe(mut self, max_ceq: i32) -> Self {
        self.cq_attr.max_cqe = max_ceq;
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
        let _ = self.qp_attr.rq_attr.address_handler().port_num(port_num);
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
        self.mr_attr.access = flags_into_ibv_access(flags);
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

    /// Set the immediate data flag in `ibv_wc` of rdma device.
    ///
    /// This value is `3` as default for `Soft-RoCE`, and may be `2` for other devices.
    #[inline]
    pub fn set_imm_flag_in_wc(self, imm_flag: u32) -> io::Result<Self> {
        use completion_queue::{IBV_WC_WITH_IMM, INIT_IMM_FLAG};

        let mut guard = INIT_IMM_FLAG.lock();
        if *guard {
            Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "IBV_WC_WITH_IMM is initialized or being initialized but has not yet completed",
            ))
        } else {
            // SAFETY: only init once before using it
            unsafe {
                IBV_WC_WITH_IMM = imm_flag;
            }
            *guard = true;
            Ok(self)
        }
    }

    /// Set the timeout value for event listener to wait for the notification of completion channel.
    ///
    /// When a completion queue entry (CQE) is placed on the CQ, a completion event will be sent to
    /// the completion channel (CC) associated with the CQ.
    ///
    /// The listener will wait for the CC's notification to poll the related CQ until timeout.
    /// After timeout, listener will poll the CQ to make sure no cqe there, and wait again.
    ///
    /// For the devices or drivers not support notification mechanism, this value will be the polling
    /// period, and as a protective measure in other cases.
    #[inline]
    #[must_use]
    pub fn set_cc_evnet_timeout(mut self, timeout: Duration) -> Self {
        self.agent_attr.cc_event_timeout = timeout;
        self
    }

    /// Set the polling trigger type of cq. Used with `Rdma.poll()` and `Rmda.get_manual_trigger()`.
    ///
    /// When a completion queue entry (CQE) is placed on the CQ, a completion event will be sent to
    /// the completion channel (CC) associated with the CQ.
    ///
    /// As default(`PollingTriggerType::AsyncFd`), the listener will wait for the CC's notification to
    /// poll the related CQ. If you want to ignore the notification and poll by yourself, you can input
    /// `pt_type` as `PollingTriggerType::Channel` and call `Rdma.poll()` to poll the CQ.
    #[inline]
    #[must_use]
    pub fn set_polling_trigger(mut self, pt_type: PollingTriggerType) -> Self {
        self.agent_attr.pt_type = pt_type;
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

    use error_utilities::log_ret_last_os_err;
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
        (*id).recv_cq_channel = rdma.qp.cq_event_listener().cq.event_channel().as_ptr();
        (*id).recv_cq_channel = rdma.qp.cq_event_listener().cq.event_channel().as_ptr();
        (*id).recv_cq = rdma.qp.cq_event_listener().cq.as_ptr();
        (*id).send_cq = rdma.qp.cq_event_listener().cq.as_ptr();
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
    /// The tx end of polling trigger
    trigger_tx: Option<mpsc::Sender<()>>,
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
            qp_attr.rq_attr.get_port_num(),
            qp_attr.rq_attr.get_sgid_index().cast(),
        )?);
        let ec = ctx.create_event_channel()?;
        let cq = Arc::new(ctx.create_completion_queue(cq_attr.cq_size, ec, cq_attr.max_cqe)?);

        let (pt_input, trigger_tx) = match agent_attr.pt_type {
            PollingTriggerType::Automatic => (PollingTriggerInput::AsyncFd(Arc::clone(&cq)), None),
            PollingTriggerType::Manual => {
                let (tx, rx) = mpsc::channel(2);
                (PollingTriggerInput::Channel(rx), Some(tx))
            }
        };

        let cq_event_listener = Arc::new(CQEventListener::new(
            Arc::clone(&cq),
            agent_attr.cc_event_timeout,
            pt_input,
        ));

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

        let qp = Arc::new(pd.create_qp(
            cq_event_listener,
            qp_attr.init_attr,
            qp_attr.rq_attr.get_port_num(),
            true,
        )?);

        Ok(Self {
            ctx,
            pd,
            qp,
            agent: None,
            allocator,
            conn_type: qp_attr.conn_type,
            raw: qp_attr.raw,
            clone_attr,
            trigger_tx,
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
    /// async fn client(mut client_rdma: Rdma, server_info: QueuePairEndpoint) -> io::Result<()> {
    ///     client_rdma.ibv_connect(server_info).await?;
    ///     // alloc 8 bytes local memory
    ///     let mut lmr = client_rdma.alloc_local_mr(Layout::new::<[u8; 8]>())?;
    ///     // write data into lmr
    ///     let _num = lmr.as_mut_slice().write(&[1_u8; 8])?;
    ///     // send data in mr to the remote end
    ///     client_rdma.send(&lmr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(mut server_rdma: Rdma, client_info: QueuePairEndpoint) -> io::Result<()> {
    ///     server_rdma.ibv_connect(client_info).await?;
    ///     // receive data
    ///     let lmr = server_rdma.receive().await?;
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
    pub async fn ibv_connect(&mut self, remote: QueuePairEndpoint) -> io::Result<()> {
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
                Ok(())
            }
            ConnectionType::RCCM | ConnectionType::RCSocket => Err(io::Error::new(
                io::ErrorKind::Other,
                "ConnectionType should be XXIBV",
            )),
        }
    }

    /// Create a new `Rdma` that has the same `mr_allocator` and `cq_event_listener` as parent.
    fn clone(&self) -> io::Result<Self> {
        let qp = Arc::new(self.clone_attr.pd.create_qp(
            Arc::clone(self.qp.cq_event_listener()),
            self.clone_attr.qp_init_attr,
            self.clone_attr.rq_attr.get_port_num(),
            true,
        )?);
        Ok(Self {
            ctx: Arc::clone(&self.ctx),
            pd: Arc::clone(self.clone_attr.pd()),
            qp,
            agent: None,
            allocator: Arc::clone(&self.allocator),
            conn_type: self.conn_type,
            raw: self.raw,
            clone_attr: self.clone_attr.clone(),
            trigger_tx: self.trigger_tx.clone(),
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
                Arc::clone(&self.ctx),
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
    ///     // wait for the agent thread to send all responses to the remote.
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

    /// A 64 bits value in a remote mr being read, compared with `old_value` and if they are equal,
    /// the `new_value` is being written to the remote mr in an atomic way.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_rdma::{LocalMrReadAccess, RdmaBuilder};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = RdmaBuilder::default().connect(addr).await?;
    ///     // alloc 8 bytes remote memory
    ///     let mut rmr = rdma.request_remote_mr(Layout::new::<[u8; 8]>()).await?;
    ///     let new_value = u64::from_le_bytes([1_u8; 8]);
    ///     // read, compare with rmr and swap `old_value` with `new_value`
    ///     rdma.atomic_cas(0, new_value, &mut rmr).await?;
    ///     // send rmr's meta data to the remote end
    ///     rdma.send_remote_mr(rmr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = RdmaBuilder::default().listen(addr).await?;
    ///     // receive mr's meta data from client
    ///     let lmr = rdma.receive_local_mr().await?;
    ///     // assert the content of lmr, which was write by cas
    ///     let data = *lmr.as_slice();
    ///     assert_eq!(data, [1_u8; 8]);
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
    pub async fn atomic_cas<RW>(
        &self,
        old_value: u64,
        new_value: u64,
        rm: &mut RW,
    ) -> io::Result<()>
    where
        RW: RemoteMrWriteAccess,
    {
        if rm.length() != 8 {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "The length of remote mr should be 8",
            ))
        } else if rm.addr() & 7 != 0 {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Atomic operations are legal only when the remote address is on a naturally-aligned 8-byte boundary",
            ))
        } else {
            let buf = self.alloc_local_mr(std::alloc::Layout::new::<[u8; 8]>())?;
            self.qp.atomic_cas(old_value, new_value, &buf, rm).await
        }
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
    ///     // wait for the agent thread to send all responses to the remote.
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
    pub async fn send_raw<LR>(&self, lm: &LR) -> io::Result<()>
    where
        LR: LocalMrReadAccess,
    {
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
    ///     // wait for the agent thread to send all responses to the remote.
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
    pub async fn send_raw_with_imm<LR>(&self, lm: &LR, imm: u32) -> io::Result<()>
    where
        LR: LocalMrReadAccess,
    {
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
    ///     // wait for the agent thread to send all responses to the remote.
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
    ///     // wait for the agent thread to send all responses to the remote.
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
    ///     // wait for the agent thread to send all responses to the remote.
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
    ///     // wait for the agent thread to send all responses to the remote.
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

    /// Receive raw data and excute an fn after `submit_receive` and before `poll`
    ///
    /// **This is an experimental API**
    ///
    /// # Examples
    ///
    /// ```
    /// use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, RdmaBuilder};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io::{self, Write},
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    /// use tokio::sync::oneshot;
    ///
    /// const RAW_DATA: [u8; 8] = [1_u8; 8];
    ///
    /// async fn client(addr: SocketAddrV4, rx: oneshot::Receiver<()>) -> io::Result<()> {
    ///     let rdma = RdmaBuilder::default().set_raw(true).connect(addr).await?;
    ///     let mut lmr = rdma.alloc_local_mr(Layout::for_value(&RAW_DATA))?;
    ///     // put data into lmr
    ///     let _num = lmr.as_mut_slice().write(&RAW_DATA)?;
    ///     // wait for serer to receive first
    ///     rx.await.unwrap();
    ///     // send the content of lmr to server
    ///     rdma.send_raw(&lmr).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4, tx: oneshot::Sender<()>) -> io::Result<()> {
    ///     let func = || {
    ///         println!("after post_recv and before poll");
    ///         tx.send(()).unwrap();
    ///     };
    ///     let rdma = RdmaBuilder::default().set_raw(true).listen(addr).await?;
    ///     // receive the data sent by client and put it into an mr
    ///     let lmr = rdma
    ///         .receive_raw_fn(Layout::for_value(&RAW_DATA), func)
    ///         .await?;
    ///     // read data from mr
    ///     assert_eq!(*lmr.as_slice(), RAW_DATA);
    ///     // wait for the agent thread to send all responses to the remote.
    ///     tokio::time::sleep(Duration::from_secs(1)).await;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    ///     let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
    ///     std::thread::spawn(move || server(addr, tx));
    ///     tokio::time::sleep(Duration::from_secs(3)).await;
    ///     client(addr, rx)
    ///         .await
    ///         .map_err(|err| println!("{}", err))
    ///         .unwrap();
    /// }
    ///
    /// ```
    #[inline]
    #[cfg(feature = "exp")]
    pub async fn receive_raw_fn<F>(&self, layout: Layout, func: F) -> io::Result<LocalMr>
    where
        F: FnOnce(),
    {
        let mut lmr = self.alloc_local_mr(layout)?;
        let _imm = self.qp.receive_sge_fn(&[&mut lmr], func).await?;
        Ok(lmr)
    }

    /// Receive raw data with imm and excute an fn after `submit_receive` and before `poll`
    ///
    /// **This is an experimental API**
    ///
    /// # Examples
    ///
    /// ```
    /// use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, RdmaBuilder};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io::{self, Write},
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    /// use tokio::sync::oneshot;
    ///
    /// const RAW_DATA: [u8; 8] = [1_u8; 8];
    /// const IMM: u32 = 1_u32;
    ///
    /// async fn client(addr: SocketAddrV4, rx: oneshot::Receiver<()>) -> io::Result<()> {
    ///     let rdma = RdmaBuilder::default().set_raw(true).connect(addr).await?;
    ///     let mut lmr = rdma.alloc_local_mr(Layout::for_value(&RAW_DATA))?;
    ///     // put data into lmr
    ///     let _num = lmr.as_mut_slice().write(&RAW_DATA)?;
    ///     // wait for serer to receive first
    ///     rx.await.unwrap();
    ///     // send the content of lmr to server
    ///     rdma.send_raw_with_imm(&lmr, IMM).await?;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4, tx: oneshot::Sender<()>) -> io::Result<()> {
    ///     let func = || {
    ///         println!("after post_recv and before poll");
    ///         tx.send(()).unwrap();
    ///     };
    ///     let rdma = RdmaBuilder::default().set_raw(true).listen(addr).await?;
    ///     // receive the data sent by client and put it into an mr
    ///     let (lmr, imm) = rdma
    ///         .receive_raw_with_imm_fn(Layout::for_value(&RAW_DATA), func)
    ///         .await?;
    ///     // read data from mr
    ///     assert_eq!(*lmr.as_slice(), RAW_DATA);
    ///     assert_eq!(imm, Some(IMM));
    ///     // wait for the agent thread to send all responses to the remote.
    ///     tokio::time::sleep(Duration::from_secs(1)).await;
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    ///     let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
    ///     std::thread::spawn(move || server(addr, tx));
    ///     tokio::time::sleep(Duration::from_secs(3)).await;
    ///     client(addr, rx)
    ///         .await
    ///         .map_err(|err| println!("{}", err))
    ///         .unwrap();
    /// }
    ///
    /// ```
    #[inline]
    #[cfg(feature = "exp")]
    pub async fn receive_raw_with_imm_fn<F>(
        &self,
        layout: Layout,
        func: F,
    ) -> io::Result<(LocalMr, Option<u32>)>
    where
        F: FnOnce(),
    {
        let mut lmr = self.alloc_local_mr(layout)?;
        let imm = self.qp.receive_sge_fn(&[&mut lmr], func).await?;
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
    ///     // wait for the agent thread to send all responses to the remote.
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
    ///     // wait for the agent thread to send all responses to the remote.
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
    ///     // wait for the agent thread to send all responses to the remote.
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
    ///     // wait for the agent thread to send all responses to the remote.
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
    ///     // wait for the agent thread to send all responses to the remote.
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
    ///     // wait for the agent thread to send all responses to the remote.
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
    ///     // wait for the agent thread to send all responses to the remote.
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
    ///     // wait for the agent thread to send all responses to the remote.
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
        let _ = self.clone_attr.rq_attr.address_handler().port_num(port_num);
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

    /// Get the tx end of polling trigger
    fn get_poll_trigger_tx(&self) -> io::Result<&mpsc::Sender<()>> {
        self.trigger_tx
            .as_ref()
            .ok_or_else(|| match *self.qp.cq_event_listener().pt_type() {
                PollingTriggerType::Automatic => io::Error::new(
                    io::ErrorKind::Other,
                    "this method can only used with PollingTriggerType::Manual",
                ),
                PollingTriggerType::Manual => io::Error::new(
                    io::ErrorKind::Other,
                    "this is a bug, trigger_tx is not initialized",
                ),
            })
    }

    /// User driven poll. Used with `RdmaBuilder.set_polling_trigger()`.
    /// See also `Rdma.get_manual_trigger()`.
    ///
    /// If you want to control CQ polling by yourself manually, you can set `PollingTriggerType::Manual`
    /// using `RdmaBuilder.set_polling_trigger()`. Then all async RDMA operations will not complete
    /// until you poll.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, RdmaBuilder};
    /// use minstant::Instant;
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io::{self, Write},
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     sync::Arc,
    ///     time::Duration,
    /// };
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     const POLLING_INTERVAL: Duration = Duration::from_millis(100);
    ///     let rdma = Arc::new(RdmaBuilder::default()
    ///         .set_polling_trigger(async_rdma::PollingTriggerType::Manual)
    ///         .set_cc_evnet_timeout(Duration::from_secs(10))
    ///         .connect(addr)
    ///         .await
    ///         .unwrap());
    ///
    ///     let rdma_trigger = rdma.clone();
    ///     // polling task
    ///     let _trigger_handle = tokio::spawn(async move {
    ///         loop {
    ///             tokio::time::sleep(POLLING_INTERVAL).await;
    ///             rdma_trigger.poll().await.unwrap();
    ///         }
    ///     });
    ///
    ///     let mut lmr = rdma.alloc_local_mr(Layout::new::<[u8; 8]>())?;
    ///     let _num = lmr.as_mut_slice().write(&[1_u8; 8])?;
    ///     let instant = Instant::now();
    ///     rdma.send(&lmr).await?;
    ///     assert!(instant.elapsed() >= POLLING_INTERVAL);
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = RdmaBuilder::default().listen(addr).await?;
    ///     let lmr = rdma.receive().await?;
    ///     let data = *lmr.as_slice();
    ///     assert_eq!(data, [1_u8; 8]);
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
    ///
    /// ```
    #[inline]
    pub async fn poll(&self) -> io::Result<()> {
        self.get_poll_trigger_tx()?.send(()).await.map_err(|_e| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "this is a bug, receiver is closed",
            )
        })
    }

    /// Get tx end of channel polling trigger.
    ///
    /// You can send a `()` message through tx like to trigger a polling like `Rdma.poll()`.
    /// This API is more convenient and flexible than `Rdma.poll()`, becasue you can clone more than
    /// one tx and send them to other threads or tasks.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, RdmaBuilder};
    /// use minstant::Instant;
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     io::{self, Write},
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration,
    /// };
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     const POLLING_INTERVAL: Duration = Duration::from_millis(100);
    ///     let rdma = RdmaBuilder::default()
    ///         .set_polling_trigger(async_rdma::PollingTriggerType::Manual)
    ///         .set_cc_evnet_timeout(Duration::from_secs(10))
    ///         .connect(addr)
    ///         .await
    ///         .unwrap();
    ///
    ///     let trigger = rdma.get_manual_trigger().unwrap();
    ///     // polling task
    ///     let _trigger_handle = tokio::spawn(async move {
    ///         loop {
    ///             tokio::time::sleep(POLLING_INTERVAL).await;
    ///             trigger.pull().await.unwrap();
    ///         }
    ///     });
    ///
    ///     let mut lmr = rdma.alloc_local_mr(Layout::new::<[u8; 8]>())?;
    ///     let _num = lmr.as_mut_slice().write(&[1_u8; 8])?;
    ///     let instant = Instant::now();
    ///     rdma.send(&lmr).await?;
    ///     assert!(instant.elapsed() >= POLLING_INTERVAL);
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = RdmaBuilder::default().listen(addr).await?;
    ///     let lmr = rdma.receive().await?;
    ///     let data = *lmr.as_slice();
    ///     assert_eq!(data, [1_u8; 8]);
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
    ///
    /// ```
    #[inline]
    pub fn get_manual_trigger(&self) -> io::Result<ManualTrigger> {
        Ok(ManualTrigger(self.get_poll_trigger_tx()?.clone()))
    }

    /// Get the last ibv async event type.
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// use async_rdma::{RdmaBuilder, IbvEventType};
    /// use portpicker::pick_unused_port;
    /// use std::{
    ///     alloc::Layout,
    ///     net::{Ipv4Addr, SocketAddrV4},
    ///     time::Duration, sync::Arc, io,
    /// };
    /// const POLLING_INTERVAL: Duration = Duration::from_secs(2);
    ///
    /// async fn client(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = Arc::new(RdmaBuilder::default()
    ///         .set_polling_trigger(async_rdma::PollingTriggerType::Manual)
    ///         .set_cc_evnet_timeout(POLLING_INTERVAL)
    ///         .set_cq_size(1)
    ///         .set_max_cqe(1)
    ///         .connect(addr)
    ///         .await
    ///         .unwrap());
    ///
    ///     // waiting for rdma send requests
    ///     tokio::time::sleep(POLLING_INTERVAL).await;
    ///     //  cq is full
    ///     assert_eq!(rdma.get_last_ibv_event_type().unwrap().unwrap(),IbvEventType::IBV_EVENT_CQ_ERR);
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn server(addr: SocketAddrV4) -> io::Result<()> {
    ///     let rdma = Arc::new(RdmaBuilder::default().listen(addr).await?);
    ///     let layout = Layout::new::<[u8; 8]>();
    ///     let mut handles = vec![];
    ///
    ///     for _ in 0..10{
    ///         let rdma_move = rdma.clone();
    ///         let lmr = rdma_move.alloc_local_mr(layout)?;
    ///         handles.push(tokio::spawn(async move {let _ = rdma_move.send(&lmr).await;}));
    ///     }
    ///     tokio::time::sleep(POLLING_INTERVAL).await;
    ///     for handle in handles{
    ///         handle.abort();
    ///     }
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
    ///
    /// ```
    #[inline]
    pub fn get_last_ibv_event_type(&self) -> io::Result<Option<IbvEventType>> {
        Ok(*self
            .agent
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Agent is not ready"))?
            .ibv_event_listener()
            .last_event_type()
            .lock())
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
