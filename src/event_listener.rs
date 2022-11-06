use crate::{
    completion_queue::{CompletionQueue, WorkCompletion, WorkRequestId},
    hashmap_extension::HashMapExtension,
    lock_utilities::ArcRwLockGuard,
    memory_region::local::RwLocalMrInner,
};
use clippy_utilities::Cast;
use std::{collections::HashMap, io, sync::Arc, time::Duration};
use tokio::{
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
#[derive(Debug)]
pub(crate) struct EventListener {
    /// The completion queue
    pub(crate) cq: Arc<CompletionQueue>,
    /// Request map from request id to responder
    req_map: ReqMap,
    /// The polling thread task handle
    _poller_handle: tokio::task::JoinHandle<()>,
    /// The timeout value for event listener to wait for the CC's notification.
    ///
    /// The listener will wait for the CC's notification to poll the related CQ until timeout.
    /// After timeout, listener will poll the CQ to make sure no cqe there, and wait again.
    ///
    /// For the devices or drivers not support notification mechanism, this value will be the polling
    /// period, and as a protective measure in other cases.
    _cc_event_timeout: Duration,
}

impl EventListener {
    /// Create a `EventListner`
    pub(crate) fn new(cq: Arc<CompletionQueue>, cc_event_timeout: Duration) -> EventListener {
        let req_map = ReqMap::default();
        let req_map_move = Arc::<
            parking_lot::Mutex<HashMap<WorkRequestId, (Responder, LmrInners, LmrGuards)>>,
        >::clone(&req_map);
        Self {
            req_map,
            _poller_handle: Self::start(
                Arc::<CompletionQueue>::clone(&cq),
                req_map_move,
                cc_event_timeout,
            ),
            cq,
            _cc_event_timeout: cc_event_timeout,
        }
    }

    /// Start the polling task
    #[allow(clippy::unreachable)]
    fn start(
        cq: Arc<CompletionQueue>,
        req_map: ReqMap,
        cc_event_time: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::task::spawn(async move {
            let mut wc_buf: Vec<WorkCompletion> = Vec::with_capacity((*cq.max_poll_cqe()).cast());
            let async_fd = match cq.event_channel().async_fd() {
                Ok(fd) => fd,
                Err(e) => {
                    error!("getting event channel failed, {:?}", e);
                    return;
                }
            };

            loop {
                if let Ok(readable) = timeout(cc_event_time, async_fd.readable()).await {
                    match readable {
                        Ok(mut readable) => {
                            readable.clear_ready();
                        }
                        Err(e) => {
                            error!("event channel closed, {:?}", e);
                            break;
                        }
                    }
                }
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
                if let Err(e) = cq.req_notify(false) {
                    error!(
                        "Failed to request a notification on next cq arrival, {:?}",
                        e
                    );
                    return;
                }
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
