use crate::{agent::AgentInner, protection_domain::ProtectionDomain};
use rdma_sys::{ibv_access_flags, ibv_dereg_mr, ibv_mr, ibv_reg_mr};
use serde::{Deserialize, Serialize};
use std::{
    alloc::Layout,
    fmt::Debug,
    io,
    ops::Range,
    ptr::NonNull,
    slice,
    sync::{Arc, Mutex, MutexGuard},
};

#[derive(Debug)]
pub struct MemoryRegion<T: LocalRemoteMR> {
    inner: Arc<InnerMr<T>>,
}

impl<T: LocalRemoteMR> MemoryRegion<T> {
    fn new_root(addr: usize, len: usize, t: T) -> Self {
        Self {
            inner: Arc::new(InnerMr::new_root(addr, len, t)),
        }
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.inner.as_ptr()
    }

    pub fn length(&self) -> usize {
        self.inner.length()
    }

    pub fn rkey(&self) -> u32 {
        self.inner.rkey()
    }

    pub fn token(&self) -> MemoryRegionToken {
        MemoryRegionToken {
            addr: self.inner.addr,
            len: self.inner.len,
            rkey: self.rkey(),
        }
    }

    pub fn slice(&self, range: Range<usize>) -> io::Result<Self> {
        Ok(Self {
            inner: Arc::new(self.inner.slice(range)?),
        })
    }

    pub fn alloc(&self, layout: Layout) -> io::Result<Self> {
        Ok(Self {
            inner: Arc::new(self.inner.alloc(layout)?),
        })
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Copy)]
pub struct MemoryRegionToken {
    pub addr: usize,
    pub len: usize,
    pub rkey: u32,
}

pub trait LocalRemoteMR {
    fn rkey(&self) -> u32;
}
#[derive(Debug)]
struct Node<T: LocalRemoteMR> {
    fa: Arc<InnerMr<T>>,
    root: Arc<InnerMr<T>>,
}

#[derive(Debug)]
enum MemoryRegionKind<T: LocalRemoteMR> {
    Root(T),
    Node(Node<T>),
}

impl<T: LocalRemoteMR> MemoryRegionKind<T> {
    fn rkey(&self) -> u32 {
        match self {
            MemoryRegionKind::Root(root) => root.rkey(),
            MemoryRegionKind::Node(node) => node.root.rkey(),
        }
    }
}

#[derive(Debug)]
pub struct InnerMr<T: LocalRemoteMR> {
    addr: usize,
    len: usize,
    kind: MemoryRegionKind<T>,
    sub: SubMemoryRegion,
}

impl<T: LocalRemoteMR> InnerMr<T> {
    fn as_ptr(&self) -> *const u8 {
        self.addr as _
    }

    fn length(&self) -> usize {
        self.len
    }

    fn rkey(&self) -> u32 {
        self.kind.rkey()
    }

    fn new_root(addr: usize, len: usize, t: T) -> Self {
        Self {
            addr,
            len,
            kind: MemoryRegionKind::Root(t),
            sub: SubMemoryRegion::new(len),
        }
    }

    fn new_node(self: &Arc<Self>, addr: usize, len: usize) -> Self {
        let new_node = Node {
            fa: self.clone(),
            root: self.root(),
        };
        let kind = MemoryRegionKind::Node(new_node);
        Self {
            addr,
            len,
            kind,
            sub: SubMemoryRegion::new(len),
        }
    }

    fn root(self: &Arc<Self>) -> Arc<Self> {
        match &self.kind {
            MemoryRegionKind::Root(_) => self.clone(),
            MemoryRegionKind::Node(node) => node.root.clone(),
        }
    }

    fn slice(self: &Arc<Self>, range: Range<usize>) -> io::Result<Self> {
        self.sub.slice(&range)?;
        Ok(self.new_node(self.addr + range.start, range.len()))
    }

    fn alloc(self: &Arc<Self>, layout: Layout) -> io::Result<Self> {
        let range = self.sub.alloc(layout)?;
        Ok(self.new_node(self.addr + range.start, range.len()))
    }
}

impl<T: LocalRemoteMR> Drop for InnerMr<T> {
    fn drop(&mut self) {
        if let MemoryRegionKind::Node(node) = &self.kind {
            node.fa
                .sub
                .free(self.addr - node.fa.addr..self.len + self.addr - node.fa.addr)
                .unwrap();
        }
    }
}

pub struct Local {
    inner_mr: NonNull<ibv_mr>,
    _pd: Arc<ProtectionDomain>,
    _data: Vec<u8>,
}

impl Debug for Local {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Local")
            .field("inner_mr", &self.inner_mr)
            .finish()
    }
}

impl Drop for Local {
    fn drop(&mut self) {
        let errno = unsafe { ibv_dereg_mr(self.inner_mr.as_ptr()) };
        assert_eq!(errno, 0);
    }
}

impl Local {
    fn lkey(&self) -> u32 {
        unsafe { self.inner_mr.as_ref() }.lkey
    }
}

impl LocalRemoteMR for Local {
    fn rkey(&self) -> u32 {
        unsafe { self.inner_mr.as_ref() }.rkey
    }
}

unsafe impl Sync for Local {}

unsafe impl Send for Local {}

impl InnerMr<Local> {
    fn lkey(&self) -> u32 {
        match &self.kind {
            MemoryRegionKind::Root(root) => root.lkey(),
            MemoryRegionKind::Node(node) => node.root.lkey(),
        }
    }
}

pub type LocalMemoryRegion = MemoryRegion<Local>;

impl LocalMemoryRegion {
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.as_ptr() as _
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.as_ptr(), self.length()) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr(), self.length()) }
    }

    pub fn lkey(&self) -> u32 {
        self.inner.lkey()
    }

    pub fn new_from_pd(
        pd: &Arc<ProtectionDomain>,
        layout: Layout,
        access: ibv_access_flags,
    ) -> io::Result<Self> {
        let data = vec![0_u8; layout.size()];
        let inner_mr = NonNull::new(unsafe {
            ibv_reg_mr(pd.as_ptr(), data.as_ptr() as _, data.len(), access.0 as _)
        })
        .ok_or_else(io::Error::last_os_error)?;
        let addr = data.as_ptr() as _;
        let len = data.len();
        let local = Local {
            inner_mr,
            _pd: pd.clone(),
            _data: data,
        };
        Ok(MemoryRegion::new_root(addr, len, local))
    }
}

pub struct Remote {
    token: MemoryRegionToken,
    agent: Arc<AgentInner>,
}

impl LocalRemoteMR for Remote {
    fn rkey(&self) -> u32 {
        self.token.rkey
    }
}

impl Drop for Remote {
    fn drop(&mut self) {
        let agent = self.agent.clone();
        let token = self.token;
        tokio::spawn(async move { AgentInner::release_mr(&agent, token).await });
    }
}

pub type RemoteMemoryRegion = MemoryRegion<Remote>;

impl RemoteMemoryRegion {
    pub fn new_from_token(token: MemoryRegionToken, agent: Arc<AgentInner>) -> Self {
        let addr = token.addr;
        let len = token.len;
        let remote = Remote { token, agent };
        let inner = Arc::new(InnerMr::new_root(addr, len, remote));
        Self { inner }
    }
}
#[derive(Debug)]
struct SubMemoryRegion {
    length: usize,
    sub: Mutex<Vec<Range<usize>>>,
}

impl SubMemoryRegion {
    fn new(length: usize) -> Self {
        Self {
            sub: Mutex::new(Vec::new()),
            length,
        }
    }

    fn get_slice(mut locked_sub: MutexGuard<Vec<Range<usize>>>, range: &Range<usize>) {
        let pos = locked_sub
            .binary_search_by(|r| r.start.cmp(&range.start))
            .err()
            .unwrap();
        locked_sub.insert(pos, range.clone());
    }

    fn slice(&self, range: &Range<usize>) -> io::Result<()> {
        if range.start >= range.end || range.end > self.length {
            return Err(io::Error::new(io::ErrorKind::Other, "Invalid Range"));
        }
        let locked_sub = self.sub.lock().unwrap();
        if !locked_sub
            .iter()
            .all(|sub_range| range.end <= sub_range.start || range.start >= sub_range.end)
        {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Memory slice Has been used",
            ));
        }
        Self::get_slice(locked_sub, range);
        Ok(())
    }

    fn alloc(&self, layout: Layout) -> io::Result<Range<usize>> {
        let mut last = 0;
        let locked_sub = self.sub.lock().unwrap();
        let mut ans = Err(io::Error::new(
            io::ErrorKind::Other,
            "No Enough Memory".to_string(),
        ));
        for range in locked_sub.iter() {
            if last + layout.size() <= range.start {
                ans = Ok(last..last + layout.size());
                break;
            }
            last = range.end
        }
        if ans.is_err() && last + layout.size() <= self.length {
            ans = Ok(last..last + layout.size());
        }
        let ans = ans?;
        Self::get_slice(locked_sub, &ans);
        Ok(ans)
    }

    fn free(&self, range: Range<usize>) -> io::Result<()> {
        let mut locked_sub = self.sub.lock().unwrap();
        let pos = locked_sub
            .binary_search_by(|r| r.start.cmp(&range.start))
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Invalid Range".to_string()))?;
        locked_sub.remove(pos);
        Ok(())
    }
}
