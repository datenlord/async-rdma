use crate::{
    memory_region::{
        Local, LocalMemoryRegion, MemoryRegion, MemoryRegionToken, RemoteMemoryRegion,
    },
    mr_allocator::MRAllocator,
    queue_pair::QueuePair,
};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    alloc::Layout,
    any::Any,
    collections::HashMap,
    fmt::Debug,
    io::{self, Cursor},
    sync::Arc,
};
use tokio::{
    sync::{
        mpsc::{channel, Receiver, Sender},
        oneshot, Mutex,
    },
    task::JoinHandle,
};
use tracing::{debug, trace};
use utilities::{Cast, OverflowArithmetic};

/// An agent for handling the dirty rdma request and async events
#[derive(Debug)]
pub struct Agent {
    /// The agent inner implementation, which may be shared in many MRs
    inner: Arc<AgentInner>,
    /// MR information receiver from agent thread
    mr_recv: Mutex<Receiver<io::Result<Arc<dyn Any + Send + Sync>>>>,
    /// Data receiver from agent thread
    data_recv: Mutex<Receiver<LocalMemoryRegion>>,
    /// Join handle for the agent background thread
    handle: JoinHandle<io::Result<()>>,
    /// Max message length
    max_message_len: usize,
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
        allocator: Arc<MRAllocator>,
        max_message_len: usize,
    ) -> io::Result<Self> {
        let response_waits = Arc::new(Mutex::new(HashMap::new()));
        let mr_own = Arc::new(Mutex::new(HashMap::new()));
        let (mr_send, mr_recv) = channel(1024);
        let (data_send, data_recv) = channel(1024);
        let mr_recv_mutex = Mutex::new(mr_recv);
        let data_recv_mutex = Mutex::new(data_recv);
        let inner = Arc::new(AgentInner {
            qp,
            response_waits,
            mr_own,
            allocator,
            max_message_len,
        });
        let handle = AgentThread::run(
            Arc::<AgentInner>::clone(&inner),
            mr_send,
            data_send,
            max_message_len,
        )?;

        Ok(Self {
            inner,
            mr_recv: mr_recv_mutex,
            data_recv: data_recv_mutex,
            handle,
            max_message_len,
        })
    }

    /// Allocate a remote memory region
    pub(crate) async fn request_remote_mr(&self, layout: Layout) -> io::Result<RemoteMemoryRegion> {
        self.inner.request_remote_mr(layout).await
    }

    /// Send a memory region metadata to the other side
    pub(crate) async fn send_mr(&self, mr: Arc<dyn Any + Send + Sync>) -> io::Result<()> {
        let request = if mr.is::<LocalMemoryRegion>() {
            // Have checked the type in the if condition
            #[allow(clippy::unwrap_used)]
            let mr = mr.downcast::<LocalMemoryRegion>().unwrap();
            let token = mr.token();
            let ans = SendMRKind::Local(token);
            if self.inner.mr_own.lock().await.insert(token, mr).is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("the MR {:?} should be send multiple times", token),
                ));
            }
            ans
        } else {
            let mr = mr.downcast::<RemoteMemoryRegion>().map_err(|m| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "this Mr {:?} can not be downcasted to RemoteMemoryRegion",
                        m
                    ),
                )
            })?;
            SendMRKind::Remote(mr.token())
        };
        let request = Request {
            request_id: RequestId::new(),
            kind: RequestKind::SendMR(SendMRRequest { kind: request }),
        };
        // this response is not important
        let _ = self.inner.send_request(request).await?;
        Ok(())
    }

    /// Receive a memory region metadata from the other side
    pub(crate) async fn receive_mr(&self) -> io::Result<Arc<dyn Any + Send + Sync>> {
        self.mr_recv.lock().await.recv().await.map_or_else(
            || Err(io::Error::new(io::ErrorKind::Other, "mr channel closed")),
            |mr| mr,
        )
    }

    /// Send the content in the `lm` to the other side
    pub async fn send_data(&self, lm: &LocalMemoryRegion) -> io::Result<()> {
        let mut start = 0;
        let lm_len = lm.length();
        let max_content_len = self.max_message_len.overflow_sub(*SEND_DATA_OFFSET);
        while start < lm_len {
            let end = (start.overflow_add(max_content_len)).min(lm_len);
            let request = Request {
                request_id: RequestId::new(),
                kind: RequestKind::SendData(SendDataRequest {
                    len: end.overflow_sub(start),
                }),
            };
            let response = self
                .inner
                .send_request_append_data(request, &[&lm.slice(start..end)?])
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
    pub async fn receive_data(&self) -> io::Result<LocalMemoryRegion> {
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
    /// The channel sender to send mr meta from the other side
    mr_send: Sender<io::Result<Arc<dyn Any + Send + Sync>>>,
    /// The channel sender to send data from the other side
    data_send: Sender<LocalMemoryRegion>,
    /// Max message length
    max_message_len: usize,
}

impl AgentThread {
    /// Spawn a main task to tokio thread pool
    fn run(
        inner: Arc<AgentInner>,
        mr_send: Sender<io::Result<Arc<dyn Any + Send + Sync>>>,
        data_send: Sender<LocalMemoryRegion>,
        max_message_len: usize,
    ) -> io::Result<JoinHandle<io::Result<()>>> {
        let agent = Arc::new(Self {
            inner,
            mr_send,
            data_send,
            max_message_len,
        });
        if max_message_len < *SEND_DATA_OFFSET {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "max message length is {:?}, it should be at leaset {:?}",
                    max_message_len, *SEND_DATA_OFFSET
                ),
            ))
        } else {
            Ok(tokio::spawn(agent.main()))
        }
    }

    /// The main agent function that handles messages sent from the other side
    async fn main(self: Arc<Self>) -> io::Result<()> {
        let mut buf = self
            .inner
            .allocator
            // alignment 1 is always correct
            .alloc(unsafe { &Layout::from_size_align_unchecked(self.max_message_len, 1) })?;
        loop {
            debug!("receiving message");
            let sz = self.inner.qp.receive(&buf).await?;
            debug!("received message, size = {}", sz);
            let message = bincode::deserialize(buf.as_slice().get(0..sz).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!(
                        "{:?} is out of range, the length is {:?}",
                        0..sz,
                        buf.length()
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
                        let _task =
                            tokio::spawn(Arc::<Self>::clone(&self).handle_send_req(request, buf));
                        // alignment 1 is always correct
                        buf = self.inner.allocator.alloc(unsafe {
                            &Layout::from_size_align_unchecked(self.max_message_len, 1)
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
                let mr = Arc::new(
                    self.inner.allocator.alloc(
                        &Layout::from_size_align(param.size, param.align)
                            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
                    )?,
                );
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
                            .mr_send
                            .send(Ok(Arc::new(RemoteMemoryRegion::new_from_token(
                                token,
                                Arc::<AgentInner>::clone(&self.inner)
                            ))))
                            .await
                            .is_ok());
                    }
                    SendMRKind::Remote(token) => {
                        let mr = Arc::<MemoryRegion<Local>>::clone(
                            self.inner.mr_own.lock().await.get(&token).ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::Other,
                                    format!("the token {:?} is not registered", token),
                                )
                            })?,
                        );
                        assert!(self.mr_send.send(Ok(mr)).await.is_ok());
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
        let sender = self
            .inner
            .response_waits
            .lock()
            .await
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
        match sender.send(Ok(response.kind)) {
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
        buf: LocalMemoryRegion,
    ) -> io::Result<()> {
        if let RequestKind::SendData(param) = request.kind {
            let buf = buf.slice(*SEND_DATA_OFFSET..(SEND_DATA_OFFSET.overflow_add(param.len)))?;
            self.data_send.send(buf).await.map_err(|e| {
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
pub struct AgentInner {
    /// The Queue Pair used to communicate with other side
    qp: Arc<QueuePair>,
    /// The map holding the waiters that waits the response
    response_waits: Arc<Mutex<ResponseWaitsMap>>,
    /// The Mrs owned by this agent
    mr_own: Arc<Mutex<HashMap<MemoryRegionToken, Arc<LocalMemoryRegion>>>>,
    /// MR allocator that creating new memory regions
    allocator: Arc<MRAllocator>,
    /// Max message length
    max_message_len: usize,
}

impl AgentInner {
    /// Request a remote MR from the other side
    pub async fn request_remote_mr(
        self: &Arc<Self>,
        layout: Layout,
    ) -> io::Result<RemoteMemoryRegion> {
        let request = AllocMRRequest {
            size: layout.size(),
            align: layout.align(),
        };
        let request = Request {
            request_id: RequestId::new(),
            kind: RequestKind::AllocMR(request),
        };
        let response = self.send_request(request).await?;
        if let ResponseKind::AllocMR(alloc_mr_response) = response {
            Ok(RemoteMemoryRegion::new_from_token(
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
    pub async fn release_mr(&self, token: MemoryRegionToken) -> io::Result<()> {
        let request = Request {
            request_id: RequestId::new(),
            kind: RequestKind::ReleaseMR(ReleaseMRRequest { token }),
        };
        let _response = self.send_request(request).await?;
        Ok(())
    }

    /// Send a request to the agent on the other side
    async fn send_request(&self, request: Request) -> io::Result<ResponseKind> {
        self.send_request_append_data(request, &[]).await
    }

    /// Send a request with data appended
    async fn send_request_append_data(
        &self,
        request: Request,
        lm: &[&LocalMemoryRegion],
    ) -> io::Result<ResponseKind> {
        let (send, recv) = oneshot::channel();
        // As request is always newly created, it's impossible to find it in the response_waits
        let _old = self
            .response_waits
            .lock()
            .await
            .insert(request.request_id, send);

        let mut buf = self
            .allocator
            // alignment 1 is always correct
            .alloc(unsafe { &Layout::from_size_align_unchecked(self.max_message_len, 1) })?;
        let cursor = Cursor::new(buf.as_mut_slice());
        let message = Message::Request(request);
        // FIXME: serialize udpate
        let msz = bincode::serialized_size(&message)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            .cast();
        bincode::serialize_into(cursor, &message)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let buf = buf.slice(0..msz)?;
        let mut lms = vec![&buf];
        lms.extend(lm);
        let lms_len: usize = lms.iter().map(|l| l.length()).sum();
        assert!(lms_len <= self.max_message_len);
        self.qp.send_sge(&lms).await?;
        let ans = recv
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        ans
    }

    /// Send a response to the other side
    async fn send_response(&self, response: Response) -> io::Result<()> {
        let mut buf = self
            .allocator
            // alignment 1 is always correct
            .alloc(unsafe { &Layout::from_size_align_unchecked(self.max_message_len, 1) })
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let cursor = Cursor::new(buf.as_mut_slice());
        let message = Message::Response(response);
        let msz = bincode::serialized_size(&message)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            .cast();
        bincode::serialize_into(cursor, &message)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let buf = buf.slice(0..msz)?;
        self.qp.send(&buf).await?;
        Ok(())
    }
}

lazy_static! {
    static ref SEND_DATA_OFFSET: usize = {
        let request = Request {
            request_id: RequestId::new(),
            kind: RequestKind::SendData(SendDataRequest { len: 0 }),
        };
        let message = Message::Request(request);
        // This is the easiest serialize, should not fail
        #[allow(clippy::unwrap_used)]
        bincode::serialized_size(&message).unwrap().cast()
    };
}

/// The map for the task waiters, these tasks have submitted the RDMA request but haven't got the result
type ResponseWaitsMap = HashMap<RequestId, oneshot::Sender<io::Result<ResponseKind>>>;

/// The Id for each RDMA request
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct RequestId(usize);

impl RequestId {
    /// Randomly generate a request id
    fn new() -> Self {
        Self(rand::thread_rng().gen())
    }
}

/// Request to alloc a remote MR
#[derive(Serialize, Deserialize)]
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
    token: MemoryRegionToken,
}

/// Request to release a MR
#[derive(Serialize, Deserialize)]
struct ReleaseMRRequest {
    /// Token of the MR
    token: MemoryRegionToken,
}

/// Response to the release MR request
#[derive(Debug, Serialize, Deserialize)]
struct ReleaseMRResponse {
    /// The status of the operation
    status: usize,
}

/// MR's kind enumeration that tells it's local or remote
#[derive(Serialize, Deserialize)]
enum SendMRKind {
    /// Local MR
    Local(MemoryRegionToken),
    /// Remote MR
    Remote(MemoryRegionToken),
}

/// Reqeust to send MR metadata
#[derive(Serialize, Deserialize)]
struct SendMRRequest {
    /// The information of the MR including the token
    kind: SendMRKind,
}

/// Response to the request of sending MR
#[derive(Debug, Serialize, Deserialize)]
struct SendMRResponse {}

/// Request to send data
#[derive(Serialize, Deserialize)]
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
#[derive(Serialize, Deserialize)]
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
#[derive(Serialize, Deserialize)]
struct Request {
    /// Request id
    request_id: RequestId,
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
    request_id: RequestId,
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
