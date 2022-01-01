use crate::completion_queue::{CompletionQueue, WorkCompletion, WorkRequestId};
use lockfree_cuckoohash::{pin, LockFreeCuckooHash};
use std::{sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::timeout};
use tracing::{error, warn};

/// Provided by the requester and used by the manager task to send
/// the command response back to the requester.
type Responder = mpsc::Sender<WorkCompletion>;

/// Map holding the Request Id to `Responder`
type ReqMap = Arc<LockFreeCuckooHash<WorkRequestId, Responder>>;

/// Event listener timeout, default is 1 sec
static EVENT_LISTENER_TIMEOUT: Duration = Duration::from_secs(1);

/// The event listener that polls the completion queue events
#[derive(Debug)]
pub struct EventListener {
    /// The completion queue
    pub cq: Arc<CompletionQueue>,
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
    fn start(cq: Arc<CompletionQueue>, req_map: ReqMap) -> tokio::task::JoinHandle<()> {
        tokio::task::spawn(async move {
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
                while let Ok(wc) = cq.poll_single() {
                    if let Some(resp) = req_map.remove_with_guard(&wc.wr_id(), &pin()) {
                        resp
                    } else {
                        error!(
                            "Failed to get the responser for the request {:?}",
                            &wc.wr_id()
                        );
                        return;
                    }
                    .try_send(wc)
                    .unwrap_or_else(|e| {
                        warn!("The waiting task is dropped, {:?}", e);
                    });
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
    pub(crate) fn register(&self) -> (WorkRequestId, mpsc::Receiver<WorkCompletion>) {
        let (tx, rx) = mpsc::channel(2);
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
