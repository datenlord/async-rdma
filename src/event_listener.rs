use crate::completion_queue::{CompletionQueue, WorkCompletion, WorkRequestId};
use clippy_utilities::Cast;
use lockfree_cuckoohash::{pin, LockFreeCuckooHash};
use std::{io, sync::Arc, time::Duration};
use tokio::{
    // Using mpsc here bacause the `oneshot` Sender needs its own ownership when it performs a `send`.
    // But we cann't get the ownership from LockFreeCuckooHash because of the principle of it.
    sync::mpsc::{channel, Receiver, Sender},
    time::timeout,
};
use tracing::{error, warn};

/// Provided by the requester and used by the manager task to send
/// the command response back to the requester.
type Responder = Sender<WorkCompletion>;

/// Map holding the Request Id to `Responder`
type ReqMap = Arc<LockFreeCuckooHash<WorkRequestId, Responder>>;

/// Event listener timeout, default is 1 sec
static EVENT_LISTENER_TIMEOUT: Duration = Duration::from_secs(1);

/// The event listener that polls the completion queue events
#[derive(Debug)]
pub(crate) struct EventListener {
    /// The completion queue
    pub(crate) cq: Arc<CompletionQueue>,
    /// Request map from request id to responder
    req_map: ReqMap,
    /// The polling thread task handle
    _poller_handle: tokio::task::JoinHandle<()>,
}

impl EventListener {
    /// Create a `EventListner`
    pub(crate) fn new(cq: Arc<CompletionQueue>) -> EventListener {
        let req_map = Arc::new(LockFreeCuckooHash::new());
        let req_map_move = Arc::<LockFreeCuckooHash<WorkRequestId, Responder>>::clone(&req_map);
        Self {
            req_map,
            _poller_handle: Self::start(Arc::<CompletionQueue>::clone(&cq), req_map_move),
            cq,
        }
    }

    /// Start the polling task
    #[allow(clippy::unreachable)]
    fn start(cq: Arc<CompletionQueue>, req_map: ReqMap) -> tokio::task::JoinHandle<()> {
        tokio::task::spawn(async move {
            let mut wc_buf: Vec<WorkCompletion> = Vec::with_capacity(cq.max_cqe().cast());
            let async_fd = match cq.event_channel().async_fd() {
                Ok(fd) => fd,
                Err(e) => {
                    error!("getting event channel failed, {:?}", e);
                    return;
                }
            };

            loop {
                if let Ok(readable) = timeout(EVENT_LISTENER_TIMEOUT, async_fd.readable()).await {
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
                            while let Some(wc) = wc_buf.pop() {
                                match req_map.remove_with_guard(&wc.wr_id(), &pin()) {
                                    Some(resp) => {
                                        resp.try_send(wc).unwrap_or_else(|e| {
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

    /// Register a new work request id
    pub(crate) fn register(&self) -> (WorkRequestId, Receiver<WorkCompletion>) {
        let (tx, rx) = channel(2);
        let mut wr_id = WorkRequestId::new();
        loop {
            if self.req_map.insert_if_not_exists(wr_id, tx.clone()) {
                break;
            }
            wr_id = WorkRequestId::new();
        }
        (wr_id, rx)
    }
}
