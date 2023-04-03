use crate::{
    completion_queue::{CompletionQueue, WorkCompletion, WorkRequestId},
    hashmap_extension::HashMapExtension,
    lock_utilities::ArcRwLockGuard,
    memory_region::local::RwLocalMrInner,
};
use async_trait::async_trait;
use clippy_utilities::Cast;
use getset::Getters;
use std::{collections::HashMap, io, os::unix::prelude::RawFd, sync::Arc, time::Duration};
use tokio::{
    io::unix::AsyncFd,
    // Using mpsc here bacause the `oneshot` Sender needs its own ownership when it performs a `send`.
    // But we cann't get the ownership from LockFreeCuckooHash because of the principle of it.
    sync::mpsc::{channel, Receiver, Sender},
    time::timeout,
};
use tracing::{debug, error, warn};

/// Provided by the requester and used by the manager task to send
/// the command response back to the requester.
type Responder = Sender<WorkCompletion>;

/// `Arc`s of `LocalMrInner`s that are being used by RDMA ops
pub(crate) type LmrInners = Vec<Arc<RwLocalMrInner>>;

/// `ArcRwLockGuard` of locked `RwLocalMrInner`.
///
/// Insert them into `ReqMap` before RDMA ops and remove when the corresponding
/// `LocalMrInner`'s RDMA operation is done to ensure that mr will not be misused during ops.
pub(crate) type LmrGuards = Vec<ArcRwLockGuard>;

/// Map holding the Request Id to `Responder`
type ReqMap = Arc<parking_lot::Mutex<HashMap<WorkRequestId, (Responder, LmrInners, LmrGuards)>>>;

/// The default timeout value for event listener to wait for the CC's notification.
pub(crate) static DEFAULT_CC_EVENT_TIMEOUT: Duration = Duration::from_millis(100);

/// Time to wait for being canceled.
/// Only used in `cancel_safety` test.
#[cfg(feature = "cancel_safety_test")]
static DELAY_FOR_CANCEL_SAFETY_TEST: Duration = Duration::from_secs(1);

/// The event listener that polls the completion queue events
#[derive(Debug, Getters)]
pub(crate) struct CQEventListener {
    /// The completion queue
    pub(crate) cq: Arc<CompletionQueue>,
    /// Request map from request id to responder
    req_map: ReqMap,
    /// The polling thread task handle
    poller_handle: tokio::task::JoinHandle<io::Result<()>>,
    /// The timeout value for event listener to wait for the CC's notification.
    ///
    /// The listener will wait for the CC's notification to poll the related CQ until timeout.
    /// After timeout, listener will poll the CQ to make sure no cqe there, and wait again.
    ///
    /// For the devices or drivers not support notification mechanism, this value will be the polling
    /// period, and as a protective measure in other cases.
    _cc_event_timeout: Duration,
    /// Polling trigger type
    #[getset(get = "pub")]
    pt_type: PollingTriggerType,
}

impl Drop for CQEventListener {
    fn drop(&mut self) {
        self.poller_handle.abort();
    }
}

impl CQEventListener {
    /// Create a `EventListner`
    pub(crate) fn new(
        cq: Arc<CompletionQueue>,
        cc_event_timeout: Duration,
        pt_input: PollingTriggerInput,
    ) -> CQEventListener {
        let req_map = ReqMap::default();
        let req_map_move = Arc::<
            parking_lot::Mutex<HashMap<WorkRequestId, (Responder, LmrInners, LmrGuards)>>,
        >::clone(&req_map);
        let (poller_handle, pt_type) = match pt_input {
            PollingTriggerInput::AsyncFd(inner_cq) => (
                Self::start::<AsyncFdTrigger>(
                    Arc::clone(&cq),
                    req_map_move,
                    cc_event_timeout,
                    PollingTriggerInput::AsyncFd(inner_cq),
                ),
                PollingTriggerType::Automatic,
            ),
            PollingTriggerInput::Channel(rx) => (
                Self::start::<ChannelTrigger>(
                    Arc::<CompletionQueue>::clone(&cq),
                    req_map_move,
                    cc_event_timeout,
                    PollingTriggerInput::Channel(rx),
                ),
                PollingTriggerType::Manual,
            ),
        };
        Self {
            req_map,
            // TODO: handle polling task error
            poller_handle,
            cq,
            _cc_event_timeout: cc_event_timeout,
            pt_type,
        }
    }

    /// Start the polling task
    #[allow(clippy::unreachable)]
    fn start<T: PollingTrigger>(
        cq: Arc<CompletionQueue>,
        req_map: ReqMap,
        cc_event_time: Duration,
        polling_trigger_input: PollingTriggerInput,
    ) -> tokio::task::JoinHandle<io::Result<()>> {
        tokio::spawn(async move {
            let mut wc_buf: Vec<WorkCompletion> = Vec::with_capacity((*cq.max_poll_cqe()).cast());
            let mut trigger = T::new(polling_trigger_input)?;

            loop {
                timeout(cc_event_time, trigger.call()).await.map_or_else(
                    |_| {
                        debug!("poll protectively");
                        Ok(())
                    },
                    |trigger_res| {
                        trigger_res.map_or_else(
                            |e| {
                                error! {"polling trigger error: {:?}", e};
                                Err(e)
                            },
                            |_| {
                                debug!("trigger a poll");
                                Ok(())
                            },
                        )
                    },
                )?;

                loop {
                    match cq.poll_cq_multiple(&mut wc_buf) {
                        Ok(_) => {
                            #[cfg(feature = "cancel_safety_test")]
                            // only used in cancel_safety test for waiting to be canceled
                            tokio::time::sleep(DELAY_FOR_CANCEL_SAFETY_TEST).await;
                            while let Some(wc) = wc_buf.pop() {
                                match req_map.lock().remove(&wc.wr_id()) {
                                    Some(v) => {
                                        debug!("polled wc wr_id {:?}", &wc.wr_id());
                                        v.0.try_send(wc).unwrap_or_else(|e| {
                                            warn!("The waiting task is dropped, {:?}", e);
                                        });
                                    }
                                    None => {
                                        error!(
                                            "Failed to get the responser for the request {:?}",
                                            &wc.wr_id()
                                        );
                                    }
                                };
                            }
                        }
                        Err(err) => {
                            if err.kind() == io::ErrorKind::WouldBlock {
                                break;
                            }
                            unreachable!("get unreachable error: {:?}", err);
                        }
                    }
                }
                cq.req_notify(false).map_err(|e| {
                    error!(
                        "Failed to request a notification on next cq arrival, {:?}",
                        e
                    );
                    e
                })?;
            }
        })
    }

    /// Register a new work request id and hold the `LocalMrInner`s to prevent mrs from being
    /// droped before the RDMA operations done.
    pub(crate) fn register(
        &self,
        inners: &[Arc<RwLocalMrInner>],
        is_write: bool,
    ) -> io::Result<(WorkRequestId, Receiver<WorkCompletion>)> {
        let (tx, rx) = channel(2);
        let mut guards = vec![];
        for inner in inners {
            if is_write {
                match inner.try_write_arc() {
                    Some(write_guard) => {
                        guards.push(ArcRwLockGuard::RwLockWriteGuard(write_guard));
                    }
                    None => {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("{:?} is locked by other ops", &inner),
                        ))
                    }
                }
            } else {
                match inner.try_read_arc() {
                    Some(read_guard) => {
                        guards.push(ArcRwLockGuard::RwLockReadGuard(read_guard));
                    }
                    None => {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("{:?} is locked by other ops", &inner),
                        ))
                    }
                }
            }
        }
        let wr_id = self
            .req_map
            .lock()
            .insert_until_success((tx, inners.to_owned(), guards), WorkRequestId::new);
        Ok((wr_id, rx))
    }

    /// Register `LocalMrInner`s before read data from them
    pub(crate) fn register_for_read(
        &self,
        inners: &[Arc<RwLocalMrInner>],
    ) -> io::Result<(WorkRequestId, Receiver<WorkCompletion>)> {
        self.register(inners, false)
    }

    /// Register `LocalMrInner`s before write data into them
    pub(crate) fn register_for_write(
        &self,
        inners: &[Arc<RwLocalMrInner>],
    ) -> io::Result<(WorkRequestId, Receiver<WorkCompletion>)> {
        self.register(inners, true)
    }
}

/// The trigger of polling.
#[async_trait]
pub(crate) trait PollingTrigger: Sized + Sync + Send {
    /// Create a new Trigger
    fn new(input: PollingTriggerInput) -> io::Result<Self>;
    /// Wait for the trigger.
    /// Return `Ok` to poll.
    /// Return `Err` to record error message and stop polling
    async fn call(&mut self) -> io::Result<()>;
}

/// A trigger that listen to the event channel of cq asynchronously.
pub(crate) struct AsyncFdTrigger {
    /// Dependented cq
    _cq: Arc<CompletionQueue>,
    /// Associates the file descriptor of cq with the tokio reactor,
    /// allowing for readiness to be polled.
    async_fd: AsyncFd<RawFd>,
}

#[async_trait]
impl PollingTrigger for AsyncFdTrigger {
    fn new(input: PollingTriggerInput) -> io::Result<Self> {
        match input {
            PollingTriggerInput::AsyncFd(cq) => Ok(Self {
                async_fd: cq.event_channel().async_fd()?,
                _cq: cq,
            }),
            PollingTriggerInput::Channel(_) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "need `AsyncFdTriggerInput`",
            )),
        }
    }

    async fn call(&mut self) -> io::Result<()> {
        self.async_fd.readable().await?.clear_ready();
        Ok(())
    }
}

/// A trigger that uses the mpsc receiver to wait for the sender's msg before polling.
pub(crate) struct ChannelTrigger {
    /// Receiver that wait for the sender's msg
    rx: Receiver<()>,
}

#[async_trait]
impl PollingTrigger for ChannelTrigger {
    fn new(input: PollingTriggerInput) -> io::Result<Self> {
        match input {
            PollingTriggerInput::Channel(rx) => Ok(Self { rx }),
            PollingTriggerInput::AsyncFd(_) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "need `AsyncFdTriggerInput`",
            )),
        }
    }

    async fn call(&mut self) -> io::Result<()> {
        self.rx.recv().await.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "the channel of `ChannelTrigger` was closed",
            )
        })
    }
}

/// Type of polling trigger.
#[derive(Debug, Clone, Copy)]
pub enum PollingTriggerType {
    /// A trigger that listen to the event channel of cq and drive polling asynchronously and automatically.
    Automatic,
    /// A trigger that uses the mpsc receiver to wait for the sender's msg before polling.
    /// Only trigger polling when user send `()` through tx end manully.
    Manual,
}

impl Default for PollingTriggerType {
    #[inline]
    fn default() -> Self {
        Self::Automatic
    }
}

/// Input data of polling triggers
pub(crate) enum PollingTriggerInput {
    /// Used by `PollingTriggerType::AsyncFd`
    AsyncFd(Arc<CompletionQueue>),
    /// Used by `PollingTriggerType::Channel`
    Channel(Receiver<()>),
}

/// Manual polling trigger.
///
/// Pull this trigger to trigger a polling.
#[derive(Debug, Clone)]
pub struct ManualTrigger(pub Sender<()>);

impl ManualTrigger {
    /// Pull the tirgger to do a polling. Used with `get_manual_trigger`.
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
    ///     let mut lmr = rdma.alloc_local_mr(Layout::new::<[u8; 8]>())?;
    ///     let _num = lmr.as_mut_slice().write(&[1_u8; 8])?;
    ///     let instant = Instant::now();
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
    pub async fn pull(&self) -> io::Result<()> {
        self.0.send(()).await.map_err(|_e| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "this is a bug, receiver is closed",
            )
        })
    }
}
