use crate::{
    id,
    memory_region::{LocalMr, LocalMrAccess, MrAccess, MrToken, RemoteMr},
    mr_allocator::MrAllocator,
    queue_pair::QueuePair,
};
use lockfree_cuckoohash::{pin, LockFreeCuckooHash};
use serde::{Deserialize, Serialize};
use std::{
    alloc::Layout,
    collections::HashMap,
    fmt::Debug,
    io::{self, Cursor},
    mem,
    sync::Arc,
};
use tokio::{
    sync::{
        mpsc::{channel, Receiver, Sender},
        Mutex,
    },
    task::JoinHandle,
};
use tracing::{debug, trace};
use utilities::{Cast, OverflowArithmetic};
/// An agent for handling the dirty rdma request and async events
#[derive(Debug)]
pub(crate) struct Agent {
    /// The agent inner implementation, which may be shared in many MRs
    inner: Arc<AgentInner>,
    /// Local MR information receiver from agent thread
    local_mr_recv: Mutex<Receiver<LocalMr>>,
    /// Remote MR information receiver from agent thread
    remote_mr_recv: Mutex<Receiver<RemoteMr>>,
    /// Data receiver from agent thread
    data_recv: Mutex<Receiver<(LocalMr, usize)>>,
    /// Join handle for the agent background thread
    handle: JoinHandle<io::Result<()>>,
    /// Max message length
    max_sr_data_len: usize,
}

impl Drop for Agent {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl Agent {
    /// Create a new agent thread
    pub(crate) fn new(
        qp: Arc<QueuePair>,
        allocator: Arc<MrAllocator>,
        max_sr_data_len: usize,
    ) -> io::Result<Self> {
        let response_waits = Arc::new(LockFreeCuckooHash::new());
        let mr_own = Arc::new(Mutex::new(HashMap::new()));
        let (local_mr_send, local_mr_recv) = channel(1024);
        let (remote_mr_send, remote_mr_recv) = channel(1024);
        let (data_send, data_recv) = channel(1024);
        let local_mr_recv = Mutex::new(local_mr_recv);
        let remote_mr_recv = Mutex::new(remote_mr_recv);
        let data_recv_mutex = Mutex::new(data_recv);
        let inner = Arc::new(AgentInner {
            qp,
            response_waits,
            mr_own,
            allocator,
            max_sr_data_len,
        });
        let handle = AgentThread::run(
            Arc::<AgentInner>::clone(&inner),
            local_mr_send,
            remote_mr_send,
            data_send,
            max_sr_data_len,
        )?;

        Ok(Self {
            inner,
            data_recv: data_recv_mutex,
            handle,
            max_sr_data_len,
            local_mr_recv,
            remote_mr_recv,
        })
    }

    /// Allocate a remote memory region
    pub(crate) async fn request_remote_mr(&self, layout: Layout) -> io::Result<RemoteMr> {
        self.inner.request_remote_mr(layout).await
    }

    /// Send a local memory region metadata to the other side
    pub(crate) async fn send_local_mr(&self, mr: LocalMr) -> io::Result<()> {
        // Have checked the type in the if condition
        let token = mr.token();
        if self.inner.mr_own.lock().await.insert(token, mr).is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("the MR {:?} should be send multiple times", token),
            ));
        }
        let kind = SendMRKind::Local(token);
        let request = RequestKind::SendMR(SendMRRequest { kind });
        // this response is not important
        let _ = self.inner.send_request(request).await?;
        Ok(())
    }

    /// Send a remote memory region metadata to the other side
    pub(crate) async fn send_remote_mr(&self, mr: RemoteMr) -> io::Result<()> {
        let kind = SendMRKind::Remote(mr.token());
        #[allow(clippy::mem_forget)]
        mem::forget(mr);
        let request = RequestKind::SendMR(SendMRRequest { kind });
        // this response is not important
        let _ = self.inner.send_request(request).await?;
        Ok(())
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
    pub(crate) async fn send_data(&self, lm: &LocalMr) -> io::Result<()> {
        let mut start = 0;
        let lm_len = lm.length();
        while start < lm_len {
            let end = (start.overflow_add(self.max_sr_data_len)).min(lm_len);
            let request = RequestKind::SendData(SendDataRequest {
                len: end.overflow_sub(start),
            });
            let response = self
                .inner
                .send_request_append_data(request, &[&&lm[start..end]])
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

    /// Receive content sent from the other side and stored in the return value
    pub(crate) async fn receive_data(&self) -> io::Result<(LocalMr, usize)> {
        self.data_recv
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "data channel closed"))
    }
}

/// Agent thread data structure, actually it spawn a task on the tokio thread pool
struct AgentThread {
    /// The agent part that may be shared in the memory region
    inner: Arc<AgentInner>,
    /// The channel sender to send local mr meta from the other side
    local_mr_send: Sender<LocalMr>,
    /// The channel sender to send remote mr meta from the other side
    remote_mr_send: Sender<RemoteMr>,
    /// The channel sender to send data from the other side
    data_send: Sender<(LocalMr, usize)>,
    /// Max send/recv message length
    max_sr_data_len: usize,
}

impl AgentThread {
    /// Spawn a main task to tokio thread pool
    fn run(
        inner: Arc<AgentInner>,
        local_mr_send: Sender<LocalMr>,
        remote_mr_send: Sender<RemoteMr>,
        data_send: Sender<(LocalMr, usize)>,
        max_sr_data_len: usize,
    ) -> io::Result<JoinHandle<io::Result<()>>> {
        let agent = Arc::new(Self {
            inner,
            local_mr_send,
            remote_mr_send,
            data_send,
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
            Ok(tokio::spawn(agent.main()))
        }
    }

    /// The main agent function that handles messages sent from the other side
    async fn main(self: Arc<Self>) -> io::Result<()> {
        let mut header_buf = self
            .inner
            .allocator
            // alignment 1 is always correct
            .alloc(unsafe { &Layout::from_size_align_unchecked(*REQUEST_HEADER_MAX_LEN, 1) })?;
        let mut data_buf = self
            .inner
            .allocator
            // alignment 1 is always correct
            .alloc(unsafe { &Layout::from_size_align_unchecked(self.max_sr_data_len, 1) })?;
        loop {
            debug!("receiving message");
            let sz = self
                .inner
                .qp
                .receive_sge(&[&mut header_buf, &mut data_buf])
                .await?;
            debug!("received message, size = {}", sz);
            let header_sz = sz.min(*REQUEST_HEADER_MAX_LEN);
            let message =
                bincode::deserialize(header_buf.as_slice().get(0..header_sz).ok_or_else(|| {
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
            match message {
                Message::Request(request) => match request.kind {
                    RequestKind::SendData(_) => {
                        // detach the task
                        let _task = tokio::spawn(
                            Arc::<Self>::clone(&self).handle_send_req(request, data_buf),
                        );
                        // alignment 1 is always correct
                        data_buf = self.inner.allocator.alloc(unsafe {
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

    /// request handler
    async fn handle_request(self: Arc<Self>, request: Request) -> io::Result<()> {
        debug!("handle request");
        let response = match request.kind {
            RequestKind::AllocMR(param) => {
                let mr = self.inner.allocator.alloc(
                    &Layout::from_size_align(param.size, param.align)
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
                )?;
                let token = mr.token();
                let response = AllocMRResponse { token };
                // the MR is newly created, it's impossible to find it in the map
                let _old = self.inner.mr_own.lock().await.insert(token, mr);
                ResponseKind::AllocMR(response)
            }
            RequestKind::ReleaseMR(param) => {
                assert!(self
                    .inner
                    .mr_own
                    .lock()
                    .await
                    .remove(&param.token)
                    .is_some());
                ResponseKind::ReleaseMR(ReleaseMRResponse { status: 0 })
            }
            RequestKind::SendMR(param) => {
                match param.kind {
                    SendMRKind::Local(token) => {
                        assert!(self
                            .remote_mr_send
                            .send(RemoteMr::new_from_token(
                                token,
                                Arc::<AgentInner>::clone(&self.inner)
                            ))
                            .await
                            .is_ok());
                    }
                    SendMRKind::Remote(token) => {
                        let mr =
                            self.inner
                                .mr_own
                                .lock()
                                .await
                                .remove(&token)
                                .ok_or_else(|| {
                                    io::Error::new(
                                        io::ErrorKind::Other,
                                        format!("the token {:?} is not registered", token),
                                    )
                                })?;
                        assert!(self.local_mr_send.send(mr).await.is_ok());
                    }
                }
                ResponseKind::SendMR(SendMRResponse {})
            }
            RequestKind::SendData(_) => {
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
        let guard = pin();
        let sender = self
            .inner
            .response_waits
            .remove_with_guard(&response.request_id, &guard)
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
    async fn handle_send_req(self: Arc<Self>, request: Request, data: LocalMr) -> io::Result<()> {
        if let RequestKind::SendData(param) = request.kind {
            self.data_send.send((data, param.len)).await.map_err(|e| {
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
    /// The Mrs owned by this agent
    mr_own: Arc<Mutex<HashMap<MrToken, LocalMr>>>,
    /// MR allocator that creating new memory regions
    allocator: Arc<MrAllocator>,
    /// Max message length
    max_sr_data_len: usize,
}

impl AgentInner {
    /// Request a remote MR from the other side
    pub(crate) async fn request_remote_mr(
        self: &Arc<Self>,
        layout: Layout,
    ) -> io::Result<RemoteMr> {
        let request = AllocMRRequest {
            size: layout.size(),
            align: layout.align(),
        };
        let request = RequestKind::AllocMR(request);
        let response = self.send_request(request).await?;
        if let ResponseKind::AllocMR(alloc_mr_response) = response {
            Ok(RemoteMr::new_from_token(
                alloc_mr_response.token,
                Arc::<Self>::clone(self),
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
        let request = RequestKind::ReleaseMR(ReleaseMRRequest { token });
        let _response = self.send_request(request).await?;
        Ok(())
    }

    /// Send a request to the agent on the other side
    async fn send_request(&self, request: RequestKind) -> io::Result<ResponseKind> {
        self.send_request_append_data(request, &[]).await
    }

    /// Send a request with data appended
    async fn send_request_append_data(
        &self,
        request: RequestKind,
        data: &[&dyn LocalMrAccess],
    ) -> io::Result<ResponseKind> {
        let data_len: usize = data.iter().map(|l| l.length()).sum();
        assert!(data_len <= self.max_sr_data_len);
        // Using mpsc here bacause `oneshot` tx need it's ownership when `send`.
        // But we cann't get it from LockFreeCuckooHash because of the principle of LockFreeCuckooHash.
        let (tx, mut rx) = channel(2);
        let mut req = Request {
            request_id: AgentRequestId::new(),
            kind: request,
        };
        while !self
            .response_waits
            .insert_if_not_exists(req.request_id, tx.clone())
        {
            req = Request {
                request_id: AgentRequestId::new(),
                kind: request,
            };
        }
        let mut header_buf = self
            .allocator
            // alignment 1 is always correct
            .alloc(unsafe { &Layout::from_size_align_unchecked(*REQUEST_HEADER_MAX_LEN, 1) })?;
        let cursor = Cursor::new(header_buf.as_mut_slice());
        let message = Message::Request(req);
        // FIXME: serialize udpate
        let mut msz = bincode::serialized_size(&message)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            .cast();
        bincode::serialize_into(cursor, &message)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        if let RequestKind::SendMR(_) = req.kind {
            msz = *REQUEST_HEADER_MAX_LEN;
        }
        let header_buf = &&header_buf[0..msz];
        let mut lms: Vec<&dyn LocalMrAccess> = vec![header_buf];
        lms.extend(data);
        self.qp.send_sge(&lms).await?;
        rx.recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "agent is dropped"))?
    }

    /// Send a response to the other side
    async fn send_response(&self, response: Response) -> io::Result<()> {
        let mut header = self
            .allocator
            // alignment 1 is always correct
            .alloc(unsafe { &Layout::from_size_align_unchecked(*RESPONSE_HEADER_MAX_LEN, 1) })
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let cursor = Cursor::new(header.as_mut_slice());
        let message = Message::Response(response);
        let msz = bincode::serialized_size(&message)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            .cast();
        bincode::serialize_into(cursor, &message)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let buf = &header[0..msz];
        self.qp.send(&buf).await?;
        Ok(())
    }
}

lazy_static! {
    static ref REQUEST_HEADER_MAX_LEN: usize = {
        let mut request_kind = vec![];
        request_kind.push(RequestKind::AllocMR(AllocMRRequest { size: 0, align: 0 }));
        request_kind.push(RequestKind::ReleaseMR(ReleaseMRRequest {
            token: MrToken {
                addr: 0,
                len: 0,
                rkey: 0,
            },
        }));
        request_kind.push(RequestKind::SendMR(SendMRRequest {
            kind: SendMRKind::Local(MrToken {
                addr: 0,
                len: 0,
                rkey: 0,
            }),
        }));
        request_kind.push(RequestKind::SendMR(SendMRRequest {
            kind: SendMRKind::Remote(MrToken {
                addr: 0,
                len: 0,
                rkey: 0,
            }),
        }));
        request_kind.push(RequestKind::SendData(SendDataRequest { len: 0 }));
        #[allow(clippy::unwrap_used)]
        request_kind
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
            .unwrap()
    };
    static ref RESPONSE_HEADER_MAX_LEN: usize = {
        let mut response_kind = vec![];
        response_kind.push(ResponseKind::AllocMR(AllocMRResponse {
            token: MrToken {
                addr: 0,
                len: 0,
                rkey: 0,
            },
        }));
        response_kind.push(ResponseKind::ReleaseMR(ReleaseMRResponse { status: 0 }));
        response_kind.push(ResponseKind::SendMR(SendMRResponse {}));
        response_kind.push(ResponseKind::SendData(SendDataResponse { status: 0 }));
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
type ResponseWaitsMap = Arc<LockFreeCuckooHash<AgentRequestId, Sender<io::Result<ResponseKind>>>>;

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
#[derive(Serialize, Deserialize, Clone, Copy)]
struct AllocMRRequest {
    /// Memory Region size
    size: usize,
    /// Alignment
    align: usize,
}

/// Response to the alloc MR request
#[derive(Debug, Serialize, Deserialize)]
struct AllocMRResponse {
    /// The token to access the MR
    token: MrToken,
}

/// Request to release a MR
#[derive(Serialize, Deserialize, Clone, Copy)]
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
#[derive(Serialize, Deserialize, Clone, Copy)]
enum SendMRKind {
    /// Local MR
    Local(MrToken),
    /// Remote MR
    Remote(MrToken),
}

/// Reqeust to send MR metadata
#[derive(Serialize, Deserialize, Clone, Copy)]
struct SendMRRequest {
    /// The information of the MR including the token
    kind: SendMRKind,
}

/// Response to the request of sending MR
#[derive(Debug, Serialize, Deserialize)]
struct SendMRResponse {}

/// Request to send data
#[derive(Serialize, Deserialize, Clone, Copy)]
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
#[derive(Serialize, Deserialize, Clone, Copy)]
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
#[derive(Serialize, Deserialize, Clone, Copy)]
struct Request {
    /// Request id
    request_id: AgentRequestId,
    /// The type of the request
    kind: RequestKind,
}

/// Response type enumeration
#[derive(Serialize, Deserialize, Debug)]
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
#[derive(Serialize, Deserialize)]
struct Response {
    /// Request id
    request_id: AgentRequestId,
    /// The type of the response
    kind: ResponseKind,
}

/// Message enumeration
#[derive(Serialize, Deserialize)]
enum Message {
    /// Request
    Request(Request),
    /// Response
    Response(Response),
}
