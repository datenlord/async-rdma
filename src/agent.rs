use crate::context::Context;
use crate::hashmap_extension::HashMapExtension;
use crate::ibv_event_listener::IbvEventListener;
use crate::queue_pair::MAX_RECV_WR;
use crate::rmr_manager::RemoteMrManager;
use crate::RemoteMrReadAccess;
use crate::{
    id,
    memory_region::{
        local::{LocalMr, LocalMrReadAccess, LocalMrSlice, LocalMrWriteAccess},
        remote::RemoteMr,
        MrAccess, MrToken,
    },
    mr_allocator::MrAllocator,
    queue_pair::QueuePair,
};
use clippy_utilities::Cast;
use getset::Getters;
use rdma_sys::ibv_access_flags;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;
use std::{
    alloc::Layout,
    fmt::Debug,
    io::{self, Cursor},
    mem,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{
        // Using mpsc here bacause the `oneshot` Sender needs its own ownership when it performs a `send`.
        // But we cann't get the ownership from LockFreeCuckooHash because of the principle of it.
        mpsc::{channel, Receiver, Sender},
        Mutex,
    },
    task::JoinHandle,
};
use tracing::{debug, error, trace};

/// Maximum time for waiting for a response
static RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum length of message send/recv by Agent
pub(crate) static MAX_MSG_LEN: usize = 512;

/// An agent for handling the dirty rdma request and async events
#[derive(Debug, Getters)]
pub(crate) struct Agent {
    /// The agent inner implementation, which may be shared in many MRs
    inner: Arc<AgentInner>,
    /// Local MR information receiver from agent thread
    local_mr_recv: Mutex<Receiver<LocalMr>>,
    /// Remote MR information receiver from agent thread
    remote_mr_recv: Mutex<Receiver<RemoteMr>>,
    /// Data receiver from agent thread
    data_recv: Mutex<Receiver<(LocalMr, usize, Option<u32>)>>,
    /// Immediate data receiver from agent thread
    imm_recv: Mutex<Receiver<u32>>,
    /// Join handle for the agent background thread
    handles: Handles,
    /// Agent thread resource
    #[allow(dead_code)]
    agent_thread: Arc<AgentThread>,
    /// Context async event listener
    #[get = "pub"]
    ibv_event_listener: IbvEventListener,
}

impl Drop for Agent {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

impl Agent {
    /// Create a new agent thread
    pub(crate) fn new(
        qp: Arc<QueuePair>,
        allocator: Arc<MrAllocator>,
        max_sr_data_len: usize,
        max_rmr_access: ibv_access_flags,
        ctx: Arc<Context>,
    ) -> io::Result<Self> {
        let response_waits = Arc::new(parking_lot::Mutex::new(HashMap::new()));
        let rmr_manager = RemoteMrManager::new(Arc::clone(qp.pd()), max_rmr_access);
        let (local_mr_send, local_mr_recv) = channel(1024);
        let (remote_mr_send, remote_mr_recv) = channel(1024);
        let (data_send, data_recv) = channel(1024);
        let (imm_send, imm_recv) = channel(1024);
        let local_mr_recv = Mutex::new(local_mr_recv);
        let remote_mr_recv = Mutex::new(remote_mr_recv);
        let data_recv = Mutex::new(data_recv);
        let imm_recv = Mutex::new(imm_recv);
        let inner = Arc::new(AgentInner {
            qp,
            response_waits,
            rmr_manager,
            allocator,
            max_sr_data_len,
        });
        let (agent_thread, handles) = AgentThread::run(
            Arc::<AgentInner>::clone(&inner),
            local_mr_send,
            remote_mr_send,
            data_send,
            imm_send,
            max_sr_data_len,
        )?;
        let ibv_event_listener = IbvEventListener::new(ctx);
        Ok(Self {
            inner,
            local_mr_recv,
            remote_mr_recv,
            data_recv,
            imm_recv,
            handles,
            agent_thread,
            ibv_event_listener,
        })
    }

    /// Allocate a remote memory region with timeout
    pub(crate) async fn request_remote_mr_with_timeout(
        &self,
        layout: Layout,
        timeout: Duration,
    ) -> io::Result<RemoteMr> {
        self.inner
            .request_remote_mr_with_timeout(layout, timeout)
            .await
    }

    /// Send a local memory region metadata to the other side
    pub(crate) async fn send_local_mr_with_timeout(
        &self,
        mr: LocalMr,
        timeout: Duration,
    ) -> io::Result<()> {
        // SAFETY: no date race here
        let token = unsafe { mr.token_with_timeout_unchecked(timeout) }.map_or_else(
            || {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "wrong timeout value, duration is too long",
                ))
            },
            Ok,
        )?;
        self.inner.rmr_manager.record_mr(token, mr, timeout).await?;
        let kind = RequestKind::SendMR(SendMRRequest {
            kind: SendMRKind::Local(token),
        });
        // this response is not important
        let _ = self.inner.send_request(kind).await?;
        Ok(())
    }

    /// Send a remote memory region metadata to the other side
    pub(crate) async fn send_remote_mr(&self, mr: RemoteMr) -> io::Result<()> {
        let request = RequestKind::SendMR(SendMRRequest {
            kind: SendMRKind::Remote(mr.token()),
        });
        // this response is not important
        let resp_kind = self.inner.send_request(request).await?;
        // using `mem::forget` here to skip the destructor of `mr` to prevent double free.
        // both of the destructor of remote mr and `send_remote_mr` will free this mr at the remote.
        // it's safe because the space taken by the variable `mr` will be reclaimed after forget.
        #[allow(clippy::mem_forget)]
        mem::forget(mr);
        match resp_kind {
            ResponseKind::SendMR(smr) => match smr.kind {
                SendMRResponseKind::Success => Ok(()),
                SendMRResponseKind::Timeout => {
                    Err(io::Error::new(io::ErrorKind::Other, "this rmr is timeout"))
                }
                SendMRResponseKind::RemoteAgentErr => Err(io::Error::new(
                    io::ErrorKind::Other,
                    "remote agent is in an error state",
                )),
            },
            ResponseKind::AllocMR(_) | ResponseKind::ReleaseMR(_) | ResponseKind::SendData(_) => {
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "received wrong response, expect SendMR response",
                ))
            }
        }
    }

    /// Receive a local memory region metadata from the other side
    pub(crate) async fn receive_local_mr(&self) -> io::Result<LocalMr> {
        self.local_mr_recv
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "mr channel closed"))
    }

    /// Receive a local memory region metadata from the other side
    pub(crate) async fn receive_remote_mr(&self) -> io::Result<RemoteMr> {
        self.remote_mr_recv
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "mr channel closed"))
    }

    /// Send the content in the `lm` to the other side
    pub(crate) async fn send_data(&self, lm: &LocalMr, imm: Option<u32>) -> io::Result<()> {
        let mut start = 0;
        let lm_len = lm.length();
        while start < lm_len {
            let end = (start.saturating_add(self.max_msg_len())).min(lm_len);
            let kind = RequestKind::SendData(SendDataRequest {
                len: end.wrapping_sub(start),
            });
            let response = self
                .inner
                // SAFETY: The input range is always valid
                .send_request_append_data(kind, &[&unsafe { lm.get_unchecked(start..end) }], imm)
                .await?;
            if let ResponseKind::SendData(send_data_resp) = response {
                if send_data_resp.status > 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "send data failed, response status is {}",
                            send_data_resp.status
                        ),
                    ));
                }
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!(
                        "send data failed, due to unexpected response type {:?}",
                        response
                    ),
                ));
            }
            start = end;
        }
        Ok(())
    }

    /// Receive content sent from the other side and stored in the `LocalMr`
    pub(crate) async fn receive_data(&self) -> io::Result<(LocalMr, Option<u32>)> {
        let (lmr, len, imm) = self
            .data_recv
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "data channel closed"))?;
        let lmr = lmr.take(0..len).ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "this is a bug, received wrong len")
        })?;
        Ok((lmr, imm))
    }

    /// Receive content sent from the other side and stored in the `LocalMr`
    pub(crate) async fn receive_imm(&self) -> io::Result<u32> {
        self.imm_recv
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "imm data channel closed"))
    }

    /// Get the max length of message for send/recv
    pub(crate) fn max_msg_len(&self) -> usize {
        self.inner.max_sr_data_len
    }
}

/// Agent thread data structure, actually it spawn a task on the tokio thread pool
#[derive(Debug)]
struct AgentThread {
    /// The agent part that may be shared in the memory region
    inner: Arc<AgentInner>,
    /// The channel sender to send local mr meta from the other side
    local_mr_send: Sender<LocalMr>,
    /// The channel sender to send remote mr meta from the other side
    remote_mr_send: Sender<RemoteMr>,
    /// The channel sender to send data from the other side
    data_send: Sender<(LocalMr, usize, Option<u32>)>,
    /// The channel sender to send imm data from the other side
    imm_send: Sender<u32>,
    /// Max send/recv message length
    max_sr_data_len: usize,
}

/// handles of receiver tasks
type Handles = Vec<JoinHandle<io::Result<()>>>;

impl AgentThread {
    /// Spawn a main task to tokio thread pool
    fn run(
        inner: Arc<AgentInner>,
        local_mr_send: Sender<LocalMr>,
        remote_mr_send: Sender<RemoteMr>,
        data_send: Sender<(LocalMr, usize, Option<u32>)>,
        imm_send: Sender<u32>,
        max_sr_data_len: usize,
    ) -> io::Result<(Arc<Self>, Handles)> {
        let agent = Arc::new(Self {
            inner,
            local_mr_send,
            remote_mr_send,
            data_send,
            imm_send,
            max_sr_data_len,
        });
        if max_sr_data_len == 0 {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "max message length is {:?}, it should be greater than 0",
                    max_sr_data_len
                ),
            ))
        } else {
            let mut handles = Vec::new();
            for _ in 0..MAX_RECV_WR {
                handles.push(tokio::spawn(Arc::<AgentThread>::clone(&agent).main()));
            }
            Ok((Arc::<AgentThread>::clone(&agent), handles))
        }
    }

    /// The main agent function that handles messages sent from the other side
    async fn main(self: Arc<Self>) -> io::Result<()> {
        let mut header_buf = self
            .inner
            .allocator
            // SAFETY: alignment 1 is always correct
            .alloc_zeroed_default(unsafe {
                &Layout::from_size_align_unchecked(*REQUEST_HEADER_MAX_LEN, 1)
            })?;
        let mut data_buf = self
            .inner
            .allocator
            // SAFETY: alignment 1 is always correct
            .alloc_zeroed_default(unsafe {
                &Layout::from_size_align_unchecked(self.max_sr_data_len, 1)
            })?;
        loop {
            // SAFETY: ?
            // TODO: check safety
            unsafe {
                std::ptr::write_bytes(header_buf.as_mut_ptr_unchecked(), 0_u8, header_buf.length());
            }
            let (sz, imm) = self
                .inner
                .qp
                .receive_sge(&[&mut header_buf, &mut data_buf])
                .await?;
            // SAFETY: the mr is readable here without cancel safety issue
            if imm.is_some() && unsafe { header_buf.as_slice_unchecked() } == CLEAN_STATE {
                debug!("write with immediate data : {:?}", imm);
                // imm was checked by `is_some()`
                #[allow(clippy::unwrap_used)]
                let _task = tokio::spawn(Arc::<Self>::clone(&self).handle_write_imm(imm.unwrap()));
                continue;
            }
            let message =
                // SAFETY: the mr is readable here without cancel safety issue
                bincode::deserialize(unsafe {header_buf.as_slice_unchecked()}.get(..).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "{:?} is out of range, the length is {:?}",
                            0..sz,
                            header_buf.length()
                        ),
                    )
                })?)
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("failed to deserialize {:?}", e),
                    )
                })?;
            debug!("message: {:?}", message);
            match message {
                Message::Request(request) => match request.kind {
                    RequestKind::SendData(_) => {
                        // detach the task
                        let _task = tokio::spawn(
                            Arc::<Self>::clone(&self).handle_send_req(request, data_buf, imm),
                        );
                        // alignment 1 is always correct
                        // SAFETY: ?
                        // TODO: check safety
                        data_buf = self.inner.allocator.alloc_zeroed_default(unsafe {
                            &Layout::from_size_align_unchecked(self.max_sr_data_len, 1)
                        })?;
                    }
                    RequestKind::AllocMR(_)
                    | RequestKind::ReleaseMR(_)
                    | RequestKind::SendMR(_) => {
                        // detach the task
                        let _task = tokio::spawn(Arc::<Self>::clone(&self).handle_request(request));
                    }
                },
                Message::Response(response) => {
                    // detach the task
                    let _task = tokio::spawn(Arc::<Self>::clone(&self).handle_response(response));
                }
            };
        }
    }

    /// hadnle `write_with_imm` requests.
    async fn handle_write_imm(self: Arc<Self>, imm: u32) -> io::Result<()> {
        self.imm_send.send(imm).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Data receiver has stopped, {:?}", e),
            )
        })
    }

    /// request handler
    async fn handle_request(self: Arc<Self>, request: Request) -> io::Result<()> {
        debug!("handle request");
        let response = match request.kind {
            RequestKind::AllocMR(param) => {
                // TODO: error handling
                let mr = self.inner.allocator.alloc_zeroed(
                    &Layout::from_size_align(param.size, param.align)
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
                    self.inner.rmr_manager.max_rmr_access,
                    &self.inner.rmr_manager.pd,
                )?;
                // SAFETY: no date race here
                let token = unsafe { mr.token_with_timeout_unchecked(param.timeout) }.map_or_else(
                    || {
                        Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "wrong timeout value, duration is too long",
                        ))
                    },
                    Ok,
                )?;
                // the MR is newly created, it's impossible to find it in the map
                #[allow(clippy::unreachable)]
                self.inner
                    .rmr_manager
                    .record_mr(token, mr, param.timeout)
                    .await
                    .map_or_else(
                        |err| unreachable!("{:?}", err),
                        |_| debug!("record mr requested by remote end {:?}", token),
                    );
                let response = AllocMRResponse { token };
                ResponseKind::AllocMR(response)
            }
            RequestKind::ReleaseMR(param) => {
                self.inner.rmr_manager.release_mr(&param.token).map_or_else(
                    |err| {
                        debug!(
                            "{:?} already released by `rmr_manager`. {:?}",
                            param.token, err
                        );
                    },
                    |lmr| debug!("release {:?}", lmr),
                );
                // TODO: error handling
                ResponseKind::ReleaseMR(ReleaseMRResponse { status: 0 })
            }
            RequestKind::SendMR(param) => {
                let kind = match param.kind {
                    SendMRKind::Local(token) => self
                        .remote_mr_send
                        .send(RemoteMr::new_from_token(
                            token,
                            Some(Arc::<AgentInner>::clone(&self.inner)),
                        ))
                        .await
                        .map_or_else(
                            |err| {
                                error!("Agent remote_mr channel error {:?}", err);
                                SendMRResponseKind::RemoteAgentErr
                            },
                            |_| SendMRResponseKind::Success,
                        ),
                    SendMRKind::Remote(token) => match self.inner.rmr_manager.release_mr(&token) {
                        Ok(mr) => self.local_mr_send.send(mr).await.map_or_else(
                            |err| {
                                error!("Agent local_mr channel error {:?}", err);
                                SendMRResponseKind::RemoteAgentErr
                            },
                            |_| SendMRResponseKind::Success,
                        ),
                        Err(err) => {
                            debug!("{:?}", err);
                            SendMRResponseKind::Timeout
                        }
                    },
                };
                ResponseKind::SendMR(SendMRResponse { kind })
            }
            RequestKind::SendData(_) => {
                // TODO: error handling
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Should not reach here, SendData is handled separately",
                ));
            }
        };
        let response = Response {
            request_id: request.request_id,
            kind: response,
        };
        self.inner.send_response(response).await?;
        trace!("handle request done");
        Ok(())
    }

    /// response handler
    async fn handle_response(self: Arc<Self>, response: Response) -> io::Result<()> {
        trace!("handle response");
        let sender = self
            .inner
            .response_waits
            .lock()
            .remove(&response.request_id)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!(
                        "request id {:?} is missing in waiting list",
                        &response.request_id
                    ),
                )
            })?;
        match sender.try_send(Ok(response.kind)) {
            Ok(_) => Ok(()),
            Err(_) => Err(io::Error::new(
                io::ErrorKind::Other,
                "The waiting task has dropped",
            )),
        }
    }

    /// handle the send request separately as we need to prepare a buf
    async fn handle_send_req(
        self: Arc<Self>,
        request: Request,
        buf: LocalMr,
        imm: Option<u32>,
    ) -> io::Result<()> {
        if let RequestKind::SendData(param) = request.kind {
            self.data_send
                .send((buf, param.len, imm))
                .await
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("Data receiver has stopped, {:?}", e),
                    )
                })?;
            let response = Response {
                request_id: request.request_id,
                kind: ResponseKind::SendData(SendDataResponse { status: 0 }),
            };
            self.inner.send_response(response).await?;
        } else {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "This function only handles send request",
            ));
        }
        Ok(())
    }
}

/// The inner agent that may be shared in different MRs
#[derive(Debug)]
pub(crate) struct AgentInner {
    /// The Queue Pair used to communicate with other side
    qp: Arc<QueuePair>,
    /// The map holding the waiters that waits the response
    response_waits: ResponseWaitsMap,
    /// Remote memory region manager
    rmr_manager: RemoteMrManager,
    /// MR allocator that creating new memory regions
    allocator: Arc<MrAllocator>,
    /// Max message length
    max_sr_data_len: usize,
}

impl AgentInner {
    /// Request a `RemoteMr` with timeout from the other side
    pub(crate) async fn request_remote_mr_with_timeout(
        self: &Arc<Self>,
        layout: Layout,
        timeout: Duration,
    ) -> io::Result<RemoteMr> {
        let request = AllocMRRequest {
            size: layout.size(),
            align: layout.align(),
            timeout,
        };
        let kind = RequestKind::AllocMR(request);
        let response = self.send_request(kind).await?;
        if let ResponseKind::AllocMR(alloc_mr_response) = response {
            Ok(RemoteMr::new_from_token(
                alloc_mr_response.token,
                Some(Arc::<Self>::clone(self)),
            ))
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Should not be here, we're expecting AllocMR response",
            ))
        }
    }

    /// Release a remote MR got from the other side
    pub(crate) async fn release_mr(&self, token: MrToken) -> io::Result<()> {
        let kind = RequestKind::ReleaseMR(ReleaseMRRequest { token });
        let _response = self.send_request(kind).await?;
        Ok(())
    }

    /// Send a request to the agent on the other side
    async fn send_request(&self, kind: RequestKind) -> io::Result<ResponseKind> {
        self.send_request_append_data(kind, &[], None).await
    }

    /// Send a request with data appended
    async fn send_request_append_data(
        &self,
        kind: RequestKind,
        data: &[&LocalMrSlice<'_>],
        imm: Option<u32>,
    ) -> io::Result<ResponseKind> {
        let data_len: usize = data.iter().map(|l| l.length()).sum();
        assert!(data_len <= self.max_sr_data_len);
        let (tx, mut rx) = channel(2);
        let req_id = self
            .response_waits
            .lock()
            .insert_until_success(tx, AgentRequestId::new);
        let req = Request {
            request_id: req_id,
            kind,
        };
        // SAFETY: ?
        // TODO: check safety
        let mut header_buf = self
            .allocator
            // alignment 1 is always correct
            .alloc_zeroed_default(unsafe {
                &Layout::from_size_align_unchecked(*REQUEST_HEADER_MAX_LEN, 1)
            })?;
        // SAFETY: the mr is writeable here without cancel safety issue
        let cursor = Cursor::new(unsafe { header_buf.as_mut_slice_unchecked() });
        let message = Message::Request(req);
        // FIXME: serialize udpate
        bincode::serialize_into(cursor, &message)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        // SAFETY: The input range is always valid
        let header_buf = &unsafe { header_buf.get_unchecked(0..header_buf.length()) };
        let mut lms: Vec<&LocalMrSlice> = vec![header_buf];
        lms.extend(data);
        self.qp.send_sge(&lms, imm).await?;
        match tokio::time::timeout(RESPONSE_TIMEOUT, rx.recv()).await {
            Ok(resp) => {
                resp.ok_or_else(|| io::Error::new(io::ErrorKind::Other, "agent is dropped"))?
            }
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "Timeout for waiting for a response.",
            )),
        }
    }

    /// Send a response to the other side
    async fn send_response(&self, response: Response) -> io::Result<()> {
        // SAFETY: ?
        // TODO: check safety
        let mut header = self
            .allocator
            // alignment 1 is always correct
            .alloc_zeroed_default(unsafe {
                &Layout::from_size_align_unchecked(*RESPONSE_HEADER_MAX_LEN, 1)
            })
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        // SAFETY: the mr is readable here without cancel safety issue
        let cursor = Cursor::new(unsafe { header.as_mut_slice_unchecked() });
        let message = Message::Response(response);
        let msz = bincode::serialized_size(&message)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            .cast();
        bincode::serialize_into(cursor, &message)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let buf = header.get_mut(0..msz).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Other,
                "this is a bug, get a wrong serialized size",
            )
        })?;
        self.qp.send(&buf).await?;
        Ok(())
    }
}

lazy_static! {
    static ref SEND_DATA_OFFSET: usize = {
        let request = Request {
            request_id: AgentRequestId::new(),
            kind: RequestKind::SendData(SendDataRequest { len: 0 }),
        };
        let message = Message::Request(request);
        // This is the easiest serialize, should not fail
        #[allow(clippy::unwrap_used)]
        bincode::serialized_size(&message).unwrap().cast()
    };
}
/// Used for checking if `header_buf` is clean.
const CLEAN_STATE: [u8; 56] = [0_u8; 56];

lazy_static! {
    static ref REQUEST_HEADER_MAX_LEN: usize = {
        let request_kind = vec![
            RequestKind::AllocMR(AllocMRRequest {
                size: 0,
                align: 0,
                timeout: Duration::from_secs(1),
            }),
            RequestKind::ReleaseMR(ReleaseMRRequest {
                token: MrToken {
                    addr: 0,
                    len: 0,
                    rkey: 0,
                    ddl: SystemTime::now(),
                    access:0,
                },
            }),
            RequestKind::SendMR(SendMRRequest {
                kind: SendMRKind::Local(MrToken {
                    addr: 0,
                    len: 0,
                    rkey: 0,
                    ddl: SystemTime::now(),
                    access:0,
                }),
            }),
            RequestKind::SendMR(SendMRRequest {
                kind: SendMRKind::Remote(MrToken {
                    addr: 0,
                    len: 0,
                    rkey: 0,
                    ddl: SystemTime::now(),
                    access:0,
                }),
            }),
            RequestKind::SendData(SendDataRequest { len: 0 }),
        ];

        #[allow(clippy::unwrap_used)]
        let max = request_kind
            .into_iter()
            .map(|kind| {
                #[allow(clippy::unwrap_used)]
                bincode::serialized_size(&Message::Request(Request {
                    request_id: AgentRequestId::new(),
                    kind,
                }))
                .unwrap()
                .cast()
            })
            .max()
            .unwrap();
        // check CLEAN_STATE len
        assert_eq!(max, CLEAN_STATE.len(), "make sure the length of CLEAN_STATE equals to max");
        max
    };
    static ref RESPONSE_HEADER_MAX_LEN: usize = {
        let response_kind = vec![
            ResponseKind::AllocMR(AllocMRResponse {
                token: MrToken {
                    addr: 0,
                    len: 0,
                    rkey: 0,
                    ddl: SystemTime::now(),
                    access:0,
                },
            }),
            ResponseKind::ReleaseMR(ReleaseMRResponse { status: 0 }),
            ResponseKind::SendMR(SendMRResponse { kind: SendMRResponseKind::Success }),
            ResponseKind::SendData(SendDataResponse { status: 0 }),
        ];
        #[allow(clippy::unwrap_used)]
        response_kind
            .into_iter()
            .map(|kind| {
                #[allow(clippy::unwrap_used)]
                bincode::serialized_size(&Message::Response(Response {
                    request_id: AgentRequestId::new(),
                    kind,
                }))
                .unwrap()
                .cast()
            })
            .max()
            .unwrap()
    };
}

/// The map for the task waiters, these tasks have submitted the RDMA request but haven't got the result
type ResponseWaitsMap =
    Arc<parking_lot::Mutex<HashMap<AgentRequestId, Sender<io::Result<ResponseKind>>>>>;

/// The Id for each RDMA request
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct AgentRequestId(u64);

impl AgentRequestId {
    /// Randomly generate a request id
    fn new() -> Self {
        Self(id::random_u64())
    }
}

/// Request to alloc a remote MR
#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
struct AllocMRRequest {
    /// Memory Region size
    size: usize,
    /// Alignment
    align: usize,
    /// Validity period
    timeout: Duration,
}

/// Response to the alloc MR request
#[derive(Debug, Serialize, Deserialize)]
struct AllocMRResponse {
    /// The token to access the MR
    token: MrToken,
}

/// Request to release a MR
#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
struct ReleaseMRRequest {
    /// Token of the MR
    token: MrToken,
}

/// Response to the release MR request
#[derive(Debug, Serialize, Deserialize)]
struct ReleaseMRResponse {
    /// The status of the operation
    status: usize,
}

/// MR's kind enumeration that tells it's local or remote
#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
enum SendMRKind {
    /// Local MR
    Local(MrToken),
    /// Remote MR
    Remote(MrToken),
}

/// Reqeust to send MR metadata
#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
struct SendMRRequest {
    /// The information of the MR including the token
    kind: SendMRKind,
}

/// Response to the request of sending MR
#[derive(Debug, Serialize, Deserialize)]
struct SendMRResponse {
    /// The kinds of Response to the request of sending MR
    kind: SendMRResponseKind,
}

/// The kinds of Response to the request of sending MR
#[derive(Debug, Serialize, Deserialize)]
enum SendMRResponseKind {
    /// Remote end received `Mr` successfully
    Success,
    /// The `Mr` sent to remote was timeout and has been dropped
    Timeout,
    /// The RDMA Agent of remote end is in an error state
    RemoteAgentErr,
}
/// Request to send data
#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
struct SendDataRequest {
    /// The length of the data
    len: usize,
}

/// Response to the request of sending data
#[derive(Debug, Serialize, Deserialize)]
struct SendDataResponse {
    /// response status
    status: usize,
}

/// Request type enumeration
#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
enum RequestKind {
    /// Allocate MR
    AllocMR(AllocMRRequest),
    /// Release MR
    ReleaseMR(ReleaseMRRequest),
    /// Send MR metadata
    SendMR(SendMRRequest),
    /// Send Data
    SendData(SendDataRequest),
}

/// Request between agents
#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
struct Request {
    /// Request id
    request_id: AgentRequestId,
    /// The type of the request
    kind: RequestKind,
}

/// Response type enumeration
#[derive(Serialize, Deserialize, Debug)]
#[allow(variant_size_differences)]
enum ResponseKind {
    /// Allocate MR
    AllocMR(AllocMRResponse),
    /// Release MR
    ReleaseMR(ReleaseMRResponse),
    /// Send MR
    SendMR(SendMRResponse),
    /// Send Data
    SendData(SendDataResponse),
}

/// Response between agents
#[derive(Serialize, Deserialize, Debug)]
struct Response {
    /// Request id
    request_id: AgentRequestId,
    /// The type of the response
    kind: ResponseKind,
}

/// Message enumeration
#[derive(Serialize, Deserialize, Debug)]
enum Message {
    /// Request
    Request(Request),
    /// Response
    Response(Response),
}
