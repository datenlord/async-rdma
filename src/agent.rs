use crate::{
    memory_region::{LocalMemoryRegion, MemoryRegionToken, RemoteMemoryRegion},
    mr_allocator::MRAllocator,
    queue_pair::QueuePair,
};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    alloc::Layout,
    any::Any,
    collections::HashMap,
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
use tracing::debug;

pub struct Agent {
    inner: Arc<AgentInner>,
    mr_recv: Mutex<Receiver<io::Result<Arc<dyn Any + Send + Sync>>>>,
    data_recv: Mutex<Receiver<LocalMemoryRegion>>,
    _handle: JoinHandle<io::Result<()>>,
}

impl Agent {
    pub fn new(qp: Arc<QueuePair>, allocator: Arc<MRAllocator>) -> Self {
        let response_waits = Arc::new(Mutex::new(HashMap::new()));
        let mr_own = Arc::new(Mutex::new(HashMap::new()));
        let (mr_send, mr_recv) = channel(1024);
        let (data_send, data_recv) = channel(1024);
        let mr_recv = Mutex::new(mr_recv);
        let data_recv = Mutex::new(data_recv);
        let inner = Arc::new(AgentInner {
            qp,
            response_waits,
            mr_own,
            allocator,
        });
        let _handle = AgentThread::run(inner.clone(), mr_send, data_send);
        Self {
            inner,
            mr_recv,
            data_recv,
            _handle,
        }
    }

    pub async fn alloc_mr(&self, layout: Layout) -> io::Result<RemoteMemoryRegion> {
        self.inner.alloc_mr(layout).await
    }

    pub async fn _release_mr(&self, token: MemoryRegionToken) -> io::Result<()> {
        self.inner.release_mr(token).await
    }

    pub async fn send_mr(&self, mr: Arc<dyn Any + Send + Sync>) -> io::Result<()> {
        let request = if mr.is::<LocalMemoryRegion>() {
            let mr = mr.downcast::<LocalMemoryRegion>().unwrap();
            let ans = SendMRKind::Local(mr.token());
            self.inner.mr_own.lock().await.insert(mr.token(), mr);
            ans
        } else {
            let mr = mr.downcast::<RemoteMemoryRegion>().unwrap();
            SendMRKind::Remote(mr.token())
        };
        let request = Request {
            request_id: RequestId::new(),
            kind: RequestKind::SendMR(SendMRRequest { kind: request }),
        };
        let _response = self.inner.send_request(request).await.unwrap();
        Ok(())
    }

    pub async fn receive_mr(&self) -> io::Result<Arc<dyn Any + Send + Sync>> {
        self.mr_recv.lock().await.recv().await.unwrap()
    }

    pub async fn send(&self, lm: &LocalMemoryRegion) -> io::Result<()> {
        let mut start = 0;
        let lm_len = lm.length();
        while start < lm_len {
            let end = (start + *SEND_RECV_MAX_LEN).min(lm_len);
            let request = Request {
                request_id: RequestId::new(),
                kind: RequestKind::SendData(SendDataRequest { len: end - start }),
            };
            let response = self
                .inner
                .send_request_append_data(request, vec![&lm.slice(start..end).unwrap()])
                .await
                .unwrap();
            if let ResponseKind::SendData(response) = response {
                if response.status > 0 {
                    todo!()
                }
            } else {
                panic!();
            }
            start = end;
        }
        Ok(())
    }

    pub async fn receive(&self) -> LocalMemoryRegion {
        self.data_recv.lock().await.recv().await.unwrap()
    }
}

struct AgentThread {
    inner: Arc<AgentInner>,
    mr_send: Sender<io::Result<Arc<dyn Any + Send + Sync>>>,
    data_send: Sender<LocalMemoryRegion>,
}

impl AgentThread {
    fn run(
        inner: Arc<AgentInner>,
        mr_send: Sender<io::Result<Arc<dyn Any + Send + Sync>>>,
        data_send: Sender<LocalMemoryRegion>,
    ) -> JoinHandle<io::Result<()>> {
        let agent = Arc::new(Self {
            inner,
            mr_send,
            data_send,
        });
        tokio::spawn(agent.main())
    }

    async fn main(self: Arc<Self>) -> io::Result<()> {
        let mut buf = self
            .inner
            .allocator
            .alloc(Layout::new::<[u8; MESSAGE_MAX_SIZE]>())
            .unwrap();
        loop {
            debug!("receiving message");
            let sz = self.inner.qp.receive(&buf).await.unwrap();
            debug!("received message, size = {}", sz);
            let message = bincode::deserialize(&buf.as_slice()[0..sz]).unwrap();
            match message {
                Message::Request(request) => match &request.kind {
                    RequestKind::SendData(_) => {
                        tokio::spawn(self.clone().handle_send(request, buf));
                        buf = self
                            .inner
                            .allocator
                            .alloc(Layout::new::<[u8; MESSAGE_MAX_SIZE]>())
                            .unwrap();
                    }
                    _ => {
                        tokio::spawn(self.clone().handle_request(request));
                    }
                },
                Message::Response(response) => {
                    tokio::spawn(self.clone().handle_response(response));
                }
            };
        }
    }

    async fn handle_request(self: Arc<Self>, request: Request) {
        debug!("handle request");
        let response = match request.kind {
            RequestKind::AllocMR(param) => {
                let mr = Arc::new(
                    self.inner
                        .allocator
                        .alloc(Layout::from_size_align(param.size, param.align).unwrap())
                        .unwrap(),
                );
                let token = mr.token();
                let response = AllocMRResponse { token };
                self.inner.mr_own.lock().await.insert(token, mr);
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
                                self.inner.clone()
                            ))))
                            .await
                            .is_ok());
                    }
                    SendMRKind::Remote(token) => {
                        let mr = self.inner.mr_own.lock().await.get(&token).unwrap().clone();
                        assert!(self.mr_send.send(Ok(mr)).await.is_ok());
                    }
                }
                ResponseKind::SendMR(SendMRResponse {})
            }
            RequestKind::ReceiveData => todo!(),
            _ => panic!(),
        };
        let response = Response {
            request_id: request.request_id,
            kind: response,
        };
        self.inner.send_response(response).await;
        debug!("handle request done");
    }

    async fn handle_response(self: Arc<Self>, response: Response) {
        debug!("handle response");
        let sender = self
            .inner
            .response_waits
            .lock()
            .await
            .remove(&response.request_id)
            .unwrap();
        match sender.send(Ok(response.kind)) {
            Ok(_) => (),
            Err(_) => todo!(),
        }
    }

    async fn handle_send(self: Arc<Self>, request: Request, buf: LocalMemoryRegion) {
        if let RequestKind::SendData(param) = request.kind {
            let buf = buf
                .slice(*SEND_DATA_OFFSET..*SEND_DATA_OFFSET + param.len)
                .unwrap();
            self.data_send.send(buf).await.unwrap();
            let response = Response {
                request_id: request.request_id,
                kind: ResponseKind::SendData(SendDataResponse { status: 0 }),
            };
            self.inner.send_response(response).await
        } else {
            panic!()
        }
    }
}

pub struct AgentInner {
    qp: Arc<QueuePair>,
    response_waits: Arc<Mutex<ResponseWaitsMap>>,
    mr_own: Arc<Mutex<HashMap<MemoryRegionToken, Arc<LocalMemoryRegion>>>>,
    allocator: Arc<MRAllocator>,
}

impl AgentInner {
    pub async fn alloc_mr(self: &Arc<Self>, layout: Layout) -> io::Result<RemoteMemoryRegion> {
        let request = AllocMRRequest {
            size: layout.size(),
            align: layout.align(),
        };
        let request = Request {
            request_id: RequestId::new(),
            kind: RequestKind::AllocMR(request),
        };
        let response = self.send_request(request).await.unwrap();
        if let ResponseKind::AllocMR(response) = response {
            Ok(RemoteMemoryRegion::new_from_token(
                response.token,
                self.clone(),
            ))
        } else {
            panic!()
        }
    }

    pub async fn release_mr(&self, token: MemoryRegionToken) -> io::Result<()> {
        let request = Request {
            request_id: RequestId::new(),
            kind: RequestKind::ReleaseMR(ReleaseMRRequest { token }),
        };
        let _response = self.send_request(request).await.unwrap();
        Ok(())
    }

    async fn send_request(&self, request: Request) -> io::Result<ResponseKind> {
        self.send_request_append_data(request, vec![]).await
    }

    async fn send_request_append_data(
        &self,
        request: Request,
        lm: Vec<&LocalMemoryRegion>,
    ) -> io::Result<ResponseKind> {
        let (send, recv) = oneshot::channel();
        self.response_waits
            .lock()
            .await
            .insert(request.request_id, send);
        let mut buf = self
            .allocator
            .alloc(Layout::new::<[u8; MESSAGE_MAX_SIZE]>())
            .unwrap();
        let cursor = Cursor::new(buf.as_mut_slice());
        let message = Message::Request(request);
        let msz = bincode::serialized_size(&message).unwrap() as usize;
        bincode::serialize_into(cursor, &message).unwrap();
        let buf = buf.slice(0..msz).unwrap();
        let mut lms = vec![&buf];
        lms.extend(lm);
        let lms_len: usize = lms.iter().map(|lm| lm.length()).sum();
        assert!(lms_len <= MESSAGE_MAX_SIZE);
        self.qp.send_sge(lms).await.unwrap();
        let ans = recv.await.unwrap();
        ans
    }

    async fn send_response(&self, response: Response) {
        let mut buf = self
            .allocator
            .alloc(Layout::new::<[u8; MESSAGE_MAX_SIZE]>())
            .unwrap();
        let cursor = Cursor::new(buf.as_mut_slice());
        let message = Message::Response(response);
        let msz = bincode::serialized_size(&message).unwrap() as usize;
        bincode::serialize_into(cursor, &message).unwrap();
        let buf = buf.slice(0..msz).unwrap();
        self.qp.send(&buf).await.unwrap();
    }
}

const MESSAGE_MAX_SIZE: usize = 4096;

lazy_static! {
    static ref SEND_DATA_OFFSET: usize = {
        let request = Request {
            request_id: RequestId::new(),
            kind: RequestKind::SendData(SendDataRequest { len: 0 }),
        };
        let message = Message::Request(request);
        let ans = bincode::serialize(&message).unwrap();
        ans.len()
    };
    static ref SEND_RECV_MAX_LEN: usize = MESSAGE_MAX_SIZE - *SEND_DATA_OFFSET;
}

type ResponseWaitsMap = HashMap<RequestId, oneshot::Sender<io::Result<ResponseKind>>>;

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct RequestId(usize);

impl RequestId {
    fn new() -> Self {
        Self(rand::thread_rng().gen())
    }
}

#[derive(Serialize, Deserialize)]
struct AllocMRRequest {
    size: usize,
    align: usize,
}

#[derive(Serialize, Deserialize)]
struct AllocMRResponse {
    token: MemoryRegionToken,
}

#[derive(Serialize, Deserialize)]
struct ReleaseMRRequest {
    token: MemoryRegionToken,
}

#[derive(Serialize, Deserialize)]
struct ReleaseMRResponse {
    status: usize,
}

#[derive(Serialize, Deserialize)]
enum SendMRKind {
    Local(MemoryRegionToken),
    Remote(MemoryRegionToken),
}

#[derive(Serialize, Deserialize)]
struct SendMRRequest {
    kind: SendMRKind,
}

#[derive(Serialize, Deserialize)]
struct SendMRResponse {}

#[derive(Serialize, Deserialize)]
struct ReceiveMRRequest {}

#[derive(Serialize, Deserialize)]
struct ReceiveMRResponse {}

#[derive(Serialize, Deserialize)]
struct SendDataRequest {
    len: usize,
}

#[derive(Serialize, Deserialize)]
struct SendDataResponse {
    status: usize,
}

#[derive(Serialize, Deserialize)]
enum RequestKind {
    AllocMR(AllocMRRequest),
    ReleaseMR(ReleaseMRRequest),
    SendMR(SendMRRequest),
    ReceiveMR,
    SendData(SendDataRequest),
    ReceiveData,
}

#[derive(Serialize, Deserialize)]
struct Request {
    request_id: RequestId,
    kind: RequestKind,
}

#[derive(Serialize, Deserialize)]
enum ResponseKind {
    AllocMR(AllocMRResponse),
    ReleaseMR(ReleaseMRResponse),
    SendMR(SendMRResponse),
    ReceiveMR,
    SendData(SendDataResponse),
    ReceiveData,
}

#[derive(Serialize, Deserialize)]
struct Response {
    request_id: RequestId,
    kind: ResponseKind,
}

#[derive(Serialize, Deserialize)]
enum Message {
    Request(Request),
    Response(Response),
}
