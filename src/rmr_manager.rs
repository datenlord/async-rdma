use crate::{memory_region::MrToken, protection_domain::ProtectionDomain, LocalMr};
use parking_lot::Mutex;
use rdma_sys::ibv_access_flags;
use std::{collections::HashMap, io, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use tracing::{debug, error, warn};
/// The maximum value of `RemoteMr` managed
static TIMER_CHANNEL_BUF_SIZE: usize = 1000;

/// Default Maximum alive time of a `RemoteMr`
pub(crate) static DEFAULT_RMR_TIMEOUT: Duration = Duration::from_secs(60);

/// Map uesd to record `LocalMr` used by remote ends
///
/// `oneshot::Sender` is used to cancel timer task if the corresponding mr
/// is going to be released before timeout.
type RmrMap = Arc<Mutex<HashMap<MrToken, (LocalMr, oneshot::Sender<()>)>>>;
/// Remote memory region manager
#[derive(Debug)]
pub(crate) struct RemoteMrManager {
    /// The Mrs owned by this agent
    mr_map: RmrMap,
    /// Sender used by Timer tasks to wake up `RemoteMrManager`
    timer_tx: mpsc::Sender<MrToken>,
    /// `RemoteMrManager` task handler
    handler: JoinHandle<()>,
    /// `Protection Domain` of remote mr
    pub(crate) pd: Arc<ProtectionDomain>,
    /// Max access permission for remote mr requests
    pub(crate) max_rmr_access: ibv_access_flags,
}

impl RemoteMrManager {
    /// New a `RemoteMrManager`
    pub(crate) fn new(pd: Arc<ProtectionDomain>, max_rmr_access: ibv_access_flags) -> Self {
        let mr_own = Arc::new(Mutex::new(HashMap::new()));
        let (timer_tx, timer_rx) = mpsc::channel::<MrToken>(TIMER_CHANNEL_BUF_SIZE);
        let handler = tokio::task::spawn(timeout_monitor(timer_rx, RmrMap::clone(&mr_own)));
        Self {
            mr_map: mr_own,
            timer_tx,
            handler,
            pd,
            max_rmr_access,
        }
    }

    /// Record `LocalMr` used by remote.
    pub(crate) async fn record_mr(
        &self,
        token: MrToken,
        mr: LocalMr,
        timeout: Duration,
    ) -> io::Result<()> {
        let (release_tx, release_rx) = oneshot::channel::<()>();
        self.mr_map
            .lock()
            .insert(token, (mr, release_tx))
            .map_or_else(
                || {
                    let timer_tx = self.timer_tx.clone();
                    // spawn a timer task to wake up `RemoteMrManager` when this mr timeout
                    let _task = tokio::spawn(mr_timer(timeout, release_rx, timer_tx, token));
                    debug!("record {:?}", token);
                    Ok(())
                },
                |lmr| {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("the MR {:?} used multiple times", lmr),
                    ))
                },
            )
    }

    /// Remove mr from map and cancel the corresponding timer task
    pub(crate) fn release_mr(&self, token: &MrToken) -> io::Result<LocalMr> {
        debug!("release_mr {:?}", token);
        remove_mr_from_map(&self.mr_map, token).map_or_else(Err, |(lmr, release_tx)| {
            // cancel timer task
            release_tx.send(()).map_or_else(
                |_| {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "timer task already droped and the related mr was droped {:?}",
                            token
                        ),
                    ))
                },
                |_| Ok(lmr),
            )
        })
    }
}

/// Timer task used to wake up `RemoteMrManager` when the mr timeout
async fn mr_timer(
    timeout: Duration,
    release_rx: oneshot::Receiver<()>,
    timer_tx: mpsc::Sender<MrToken>,
    token: MrToken,
) {
    let is_timeout = tokio::time::timeout(timeout, release_rx).await.map_or_else(
        |_| {
            debug!(
                "mr_timer is going to wake up rmr_manager to drop timeout mr {:?}",
                token
            );
            true
        },
        |_| {
            debug!(
                "mr_timer received cancel signal, the corresponding mr is dopped {:?}",
                token
            );
            false
        },
    );
    if is_timeout {
        timer_tx.send(token).await.map_or_else(
            |err| {
                error!("rmr_manager timer receiver droped {:?}", err);
            },
            |_| debug!("mr_timer waked up rmr_manager successfully"),
        );
    }
}

/// Remove mr from `RmrMap`
fn remove_mr_from_map(map: &RmrMap, token: &MrToken) -> io::Result<(LocalMr, oneshot::Sender<()>)> {
    map.lock().remove(token).map_or_else(
        || {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("can not find MR {:?}", token),
            ))
        },
        |res| {
            debug!("removed {:?}", token);
            Ok(res)
        },
    )
}

/// Main function used to free timeout mrs.
#[allow(clippy::unreachable)]
async fn timeout_monitor(mut timer_rx: mpsc::Receiver<MrToken>, map: RmrMap) {
    loop {
        let token = timer_rx.recv().await.map_or_else(
            || {
                unreachable!("timer sender was closed");
            },
            |token| token,
        );
        remove_mr_from_map(&map, &token).map_or_else(
            |err| {
                warn!("already droped or invalid token{:?}", err);
            },
            |_| {
                debug!("timeout and droped {:?}", token);
            },
        );
    }
}

impl Drop for RemoteMrManager {
    fn drop(&mut self) {
        self.handler.abort();
    }
}
