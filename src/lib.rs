// #![deny(
//     // The following are allowed by default lints according to
//     // https://doc.rust-lang.org/rustc/lints/listing/allowed-by-default.html
//     anonymous_parameters,
//     bare_trait_objects,
//     // box_pointers, // use box pointer to allocate on heap
//     // elided_lifetimes_in_paths, // allow anonymous lifetime
//     missing_copy_implementations,
//     missing_debug_implementations,
//     missing_docs, // TODO: add documents
//     single_use_lifetimes, // TODO: fix lifetime names only used once
//     trivial_casts, // TODO: remove trivial casts in code
//     trivial_numeric_casts,
//     // unreachable_pub, allow clippy::redundant_pub_crate lint instead
//     // unsafe_code,
//     unstable_features,
//     unused_extern_crates,
//     unused_import_braces,
//     unused_qualifications,
//     // unused_results, // TODO: fix unused results
//     variant_size_differences,

//     warnings, // treat all wanings as errors

//     clippy::all,
//     clippy::restriction,
//     clippy::pedantic,
//     clippy::nursery,
//     clippy::cargo
// )]

mod agent;
mod completion_queue;
mod context;
mod event_channel;
mod event_listener;
mod gid;
mod memory_region;
mod memory_window;
mod mr_allocator;
mod protection_domain;
mod queue_pair;
mod work_request;

use agent::Agent;
use context::Context;
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

#[macro_use]
extern crate lazy_static;

pub struct RdmaBuilder {
    dev_name: Option<String>,
    access: ibv_access_flags,
    cq_size: u32,
}

impl RdmaBuilder {
    pub fn build(&self) -> io::Result<Rdma> {
        Rdma::new(self.dev_name.as_deref(), self.access, self.cq_size)
    }

    pub fn set_dev(&mut self, dev: &str) {
        self.dev_name = Some(dev.to_string());
    }

    pub fn set_cq_size(&mut self, cq_size: u32) {
        self.cq_size = cq_size
    }
}

impl Default for RdmaBuilder {
    fn default() -> Self {
        Self {
            dev_name: None,
            access: ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
                | ibv_access_flags::IBV_ACCESS_REMOTE_READ
                | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC,
            cq_size: 16,
        }
    }
}

#[allow(dead_code)]
pub struct Rdma {
    ctx: Arc<Context>,
    pd: Arc<ProtectionDomain>,
    allocator: Arc<MRAllocator>,
    qp: Arc<QueuePair>,
    agent: Option<Arc<Agent>>,
}

impl Rdma {
    pub fn new(dev_name: Option<&str>, access: ibv_access_flags, cq_size: u32) -> io::Result<Self> {
        let ctx = Arc::new(Context::open(dev_name)?);
        let ec = ctx.create_event_channel()?;
        let cq = Arc::new(ctx.create_completion_queue(cq_size, Some(ec))?);
        let event_listener = EventListener::new(cq);
        let pd = Arc::new(ctx.create_protection_domain()?);
        let allocator = Arc::new(MRAllocator::new(pd.clone()));
        let qp = Arc::new(
            pd.create_queue_pair_builder()
                .set_event_listener(event_listener)
                .build()?,
        );
        qp.modify_to_init(access)?;
        Ok(Self {
            ctx,
            pd,
            qp,
            agent: None,
            allocator,
        })
    }

    pub fn endpoint(&self) -> QueuePairEndpoint {
        self.qp.endpoint()
    }

    pub fn handshake(&mut self, remote: QueuePairEndpoint) -> io::Result<()> {
        self.qp.modify_to_rtr(remote, 0, 1, 0x12)?;
        debug!("rtr");
        self.qp.modify_to_rts(0x12, 6, 7, 0, 1)?;
        debug!("rts");
        Ok(())
    }

    pub async fn send(&self, lm: &LocalMemoryRegion) -> io::Result<()> {
        self.agent.as_ref().unwrap().clone().send(lm).await
    }

    pub async fn receive(&self) -> LocalMemoryRegion {
        self.agent.as_ref().unwrap().clone().receive().await
    }

    pub async fn read(
        &self,
        lm: &mut LocalMemoryRegion,
        rm: &RemoteMemoryRegion,
    ) -> io::Result<()> {
        self.qp.read(lm, rm).await.map_err(|e| e.into())
    }

    pub async fn write(
        &self,
        local: &LocalMemoryRegion,
        remote: &RemoteMemoryRegion,
    ) -> io::Result<()> {
        self.qp.write(local, remote).await.map_err(|e| e.into())
    }

    pub async fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let mut rdma = RdmaBuilder::default().build()?;
        let mut stream = TcpStream::connect(addr).await?;
        let mut endpoint = bincode::serialize(&rdma.endpoint()).unwrap();
        stream.write_all(&endpoint).await?;
        stream.read_exact(endpoint.as_mut()).await?;
        let remote: QueuePairEndpoint = bincode::deserialize(&endpoint).unwrap();
        rdma.handshake(remote)?;
        let agent = Arc::new(Agent::new(rdma.qp.clone(), rdma.allocator.clone()));
        rdma.agent = Some(agent);
        Ok(rdma)
    }

    pub fn alloc_local_mr(&self, layout: Layout) -> io::Result<LocalMemoryRegion> {
        self.allocator.alloc(layout)
    }

    pub async fn alloc_remote_mr(&self, layout: Layout) -> io::Result<RemoteMemoryRegion> {
        if let Some(agent) = &self.agent {
            agent.alloc_mr(layout).await
        } else {
            panic!();
        }
    }

    pub async fn send_mr(&self, mr: Arc<dyn Any + Send + Sync>) -> io::Result<()> {
        if let Some(agent) = &self.agent {
            agent.send_mr(mr).await
        } else {
            panic!();
        }
    }

    pub async fn receive_mr(&self) -> io::Result<Arc<dyn Any + Send + Sync>> {
        if let Some(agent) = &self.agent {
            agent.receive_mr().await
        } else {
            panic!();
        }
    }

    pub async fn receive_local_mr(&self) -> io::Result<Arc<LocalMemoryRegion>> {
        Ok(self.receive_mr().await?.downcast().unwrap())
    }

    pub async fn receive_remote_mr(&self) -> io::Result<Arc<RemoteMemoryRegion>> {
        Ok(self.receive_mr().await?.downcast().unwrap())
    }
}

#[derive(Debug)]
pub struct RdmaListener {
    tcp_listener: TcpListener,
}

impl RdmaListener {
    pub async fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let tcp_listener = TcpListener::bind(addr).await?;
        Ok(Self { tcp_listener })
    }

    pub async fn accept(&self) -> io::Result<Rdma> {
        let (mut stream, _) = self.tcp_listener.accept().await?;
        let mut rdma = RdmaBuilder::default().build()?;
        let mut remote = vec![0_u8; 22];
        stream.read_exact(remote.as_mut()).await?;
        let remote: QueuePairEndpoint = bincode::deserialize(&remote).unwrap();
        let local = bincode::serialize(&rdma.endpoint()).unwrap();
        stream.write_all(&local).await?;
        rdma.handshake(remote)?;
        debug!("handshake done");
        let agent = Arc::new(Agent::new(rdma.qp.clone(), rdma.allocator.clone()));
        rdma.agent = Some(agent);
        Ok(rdma)
    }
}
