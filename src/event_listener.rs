use crate::completion_queue::{CompletionQueue, WorkCompletion, WorkRequestId};
use lockfree_cuckoohash::{pin, LockFreeCuckooHash};
use std::{os::unix::prelude::AsRawFd, sync::Arc};
use tokio::{io::unix::AsyncFd, sync::mpsc};

/// Provided by the requester and used by the manager task to send
/// the command response back to the requester.
type Responder = mpsc::Sender<WorkCompletion>;
type ReqMap = Arc<LockFreeCuckooHash<WorkRequestId, Responder>>;
pub struct EventListener {
    pub cq: Arc<CompletionQueue>,
    req_map: ReqMap,
    _poller_handle: tokio::task::JoinHandle<()>,
}

impl EventListener {
    pub fn new(cq: Arc<CompletionQueue>) -> EventListener {
        let req_map = Arc::new(LockFreeCuckooHash::new());
        let req_map_move = req_map.clone();
        Self {
            req_map,
            _poller_handle: Self::start(cq.clone(), req_map_move),
            cq,
        }
    }

    pub fn start(cq: Arc<CompletionQueue>, req_map: ReqMap) -> tokio::task::JoinHandle<()> {
        tokio::task::spawn(async move {
            let async_fd = AsyncFd::new(cq.event_channel().as_raw_fd()).unwrap();
            loop {
                async_fd.readable().await.unwrap().clear_ready();
                cq.req_notify(false).unwrap();
                while let Ok(wc) = cq.poll_single() {
                    req_map
                        .remove_with_guard(&wc.wr_id(), &pin())
                        .unwrap()
                        .clone()
                        .try_send(wc)
                        .unwrap();
                }
            }
        })
    }

    pub fn register(&self) -> (WorkRequestId, mpsc::Receiver<WorkCompletion>) {
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
