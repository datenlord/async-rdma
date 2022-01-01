//! RDMA high level abstraction, providing several useful APIs.
//! [Here's a placeholder]
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
    clippy::cargo
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
/// Work Request wrapper
mod work_request;

use agent::Agent;
use context::Context;
use enumflags2::{bitflags, BitFlags};
use event_listener::EventListener;
use memory_region::{LocalMemoryRegion, RemoteMemoryRegion};
use mr_allocator::MRAllocator;
use protection_domain::ProtectionDomain;
use queue_pair::{QueuePair, QueuePairEndpoint};
use rdma_sys::ibv_access_flags;
use std::{alloc::Layout, any::Any, fmt::Debug, io, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, ToSocketAddrs},
};
use tracing::debug;
use utilities::Cast;

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
    allocator: Arc<MRAllocator>,
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
        let allocator = Arc::new(MRAllocator::new(Arc::<ProtectionDomain>::clone(&pd))?);
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
        self.qp.modify_to_rts(0x12, 6, 7, 0, 1)?;
        debug!("rts");
        Ok(())
    }

    /// The send the content in the `lm`
    #[inline]
    pub async fn send(&self, lm: &LocalMemoryRegion) -> io::Result<()> {
        Arc::<Agent>::clone(
            self.agent
                .as_ref()
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Agent is not ready"))?,
        )
        .send_data(lm)
        .await
    }

    /// Receive the content and stored in the returned memory region
    #[inline]
    pub async fn receive(&self) -> io::Result<LocalMemoryRegion> {
        Arc::<Agent>::clone(
            self.agent
                .as_ref()
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Agent is not ready"))?,
        )
        .receive_data()
        .await
    }

    /// Read content in the `rm` and store the content in the `lm`
    #[inline]
    pub async fn read(
        &self,
        lm: &mut LocalMemoryRegion,
        rm: &RemoteMemoryRegion,
    ) -> io::Result<()> {
        self.qp.read(lm, rm).await
    }

    /// Write content in the `lm` to `rm`
    #[inline]
    pub async fn write(&self, lm: &LocalMemoryRegion, rm: &RemoteMemoryRegion) -> io::Result<()> {
        self.qp.write(lm, rm).await
    }

    /// Connect the remote endpoint build rmda queue pair by TCP connection
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
            Arc::<MRAllocator>::clone(&rdma.allocator),
            max_message_length,
        )?);
        rdma.agent = Some(agent);
        Ok(rdma)
    }

    /// Allocate a local memory region
    #[inline]
    pub fn alloc_local_mr(&self, layout: Layout) -> io::Result<LocalMemoryRegion> {
        self.allocator.alloc(&layout)
    }

    /// Request a remote memory region
    #[inline]
    pub async fn request_remote_mr(&self, layout: Layout) -> io::Result<RemoteMemoryRegion> {
        if let Some(ref agent) = self.agent {
            agent.request_remote_mr(layout).await
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Agent is not ready, please wait a while",
            ))
        }
    }

    /// Send a memory region, either local mr or remote mr
    #[inline]
    pub async fn send_mr(&self, mr: Arc<dyn Any + Send + Sync>) -> io::Result<()> {
        if let Some(ref agent) = self.agent {
            agent.send_mr(mr).await
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Agent is not ready, please wait a while",
            ))
        }
    }

    /// Receive a memory region
    #[inline]
    async fn receive_mr(&self) -> io::Result<Arc<dyn Any + Send + Sync>> {
        if let Some(ref agent) = self.agent {
            agent.receive_mr().await
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "Agent is not ready, please wait a while",
            ))
        }
    }

    /// Receive a local memory region
    #[inline]
    pub async fn receive_local_mr(&self) -> io::Result<Arc<LocalMemoryRegion>> {
        Ok(self.receive_mr().await?.downcast().map_err(|orig| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("failed to downcast {:?} to Arc<LocalMemoryRegion>", orig),
            )
        })?)
    }

    /// Receive a remote memory region
    #[inline]
    pub async fn receive_remote_mr(&self) -> io::Result<Arc<RemoteMemoryRegion>> {
        Ok(self.receive_mr().await?.downcast().map_err(|orig| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("failed to downcast {:?} to Arc<RemoteMemoryRegion>", orig),
            )
        })?)
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
    #[inline]
    pub async fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let tcp_listener = TcpListener::bind(addr).await?;
        Ok(Self { tcp_listener })
    }

    /// Wait for a connection from a remote host
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
            Arc::<MRAllocator>::clone(&rdma.allocator),
            max_message_length,
        )?);
        rdma.agent = Some(agent);
        Ok(rdma)
    }
}
