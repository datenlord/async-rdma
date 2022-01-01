use crate::{agent::AgentInner, protection_domain::ProtectionDomain};
use rdma_sys::{ibv_access_flags, ibv_dereg_mr, ibv_mr, ibv_reg_mr};
use serde::{Deserialize, Serialize};
use std::{
    alloc::{alloc, Layout},
    fmt::Debug,
    io,
    ops::Range,
    ptr::NonNull,
    slice,
    sync::{Arc, Mutex, MutexGuard},
};
use tracing::error;
use utilities::{mut_ptr_to_usize, Cast, OverflowArithmetic};

/// Memory Region
/// It's managed in the hierarchy way
#[derive(Debug)]
pub struct MemoryRegion<T: RemoteKey> {
    /// Inner that can be shared
    inner: Arc<InnerMr<T>>,
}

impl<T: RemoteKey> MemoryRegion<T> {
    /// Create a new memory region, usually it's the root
    fn new_root(addr: usize, len: usize, t: T) -> Self {
        Self {
            inner: Arc::new(InnerMr::new_root(addr, len, t)),
        }
    }

    /// Get the start pointer of the memory region
    pub fn as_ptr(&self) -> *const u8 {
        self.inner.as_ptr()
    }

    /// Get the length of the memory region
    pub fn length(&self) -> usize {
        self.inner.length()
    }

    /// Get the remote key
    pub fn rkey(&self) -> u32 {
        self.inner.rkey()
    }

    /// Get the token for the remote access
    pub fn token(&self) -> MemoryRegionToken {
        MemoryRegionToken {
            addr: self.inner.addr,
            len: self.inner.len,
            rkey: self.rkey(),
        }
    }

    /// Get a slice from the memory region, the return value is also
    /// a `MemoryRegion` type if success.
    ///
    /// Return error if there's no enough resource
    pub fn slice(&self, range: Range<usize>) -> io::Result<Self> {
        Ok(Self {
            inner: Arc::new(self.inner.slice(range)?),
        })
    }

    /// Get a slice from the memory region considering the `layout`,
    /// the return value is also a `MemoryRegion` type if success.
    ///
    /// Return error if there's no enough resource
    pub fn alloc(&self, layout: &Layout) -> io::Result<Self> {
        Ok(Self {
            inner: Arc::new(self.inner.alloc(layout)?),
        })
    }
}

/// Memory region token used for the remote access
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Copy, Debug)]
pub struct MemoryRegionToken {
    /// The start address
    pub addr: usize,
    /// The length
    pub len: usize,
    /// The rkey
    pub rkey: u32,
}

/// Remote key for remote access
pub trait RemoteKey {
    /// get rkey
    fn rkey(&self) -> u32;
}

/// The node in memory region hierarchy tree, used in all
/// memory regions except the roots
#[derive(Debug)]
struct Node<T: RemoteKey> {
    /// The parent of current node
    parent: Arc<InnerMr<T>>,
    /// The root of current node
    root: Arc<InnerMr<T>>,
}

/// The kind of this memory region
#[derive(Debug)]
enum MemoryRegionKind<T: RemoteKey> {
    /// The root node
    Root(T),
    /// The node except root
    Node(Node<T>),
}

impl<T: RemoteKey> MemoryRegionKind<T> {
    /// Get remote key
    fn rkey(&self) -> u32 {
        match *self {
            MemoryRegionKind::Root(ref root) => root.rkey(),
            MemoryRegionKind::Node(ref node) => node.root.rkey(),
        }
    }
}

/// The real memory region structure, maybe shared
#[derive(Debug)]
pub struct InnerMr<T: RemoteKey> {
    /// The start address of thie MR
    addr: usize,
    /// the length of this MR
    len: usize,
    /// kind, root or not?
    kind: MemoryRegionKind<T>,
    /// the struct tracking the regions allocated to the children
    sub: AllocManager,
}

impl<T: RemoteKey> InnerMr<T> {
    /// Get start address of the memory region
    #[allow(clippy::as_conversions)]
    fn as_ptr(&self) -> *const u8 {
        // it's usize to addr conversion which should be safe
        self.addr as _
    }

    /// The length of the memory region
    fn length(&self) -> usize {
        self.len
    }

    /// Remote key
    fn rkey(&self) -> u32 {
        self.kind.rkey()
    }

    /// Create a root memory region
    fn new_root(addr: usize, len: usize, t: T) -> Self {
        Self {
            addr,
            len,
            kind: MemoryRegionKind::Root(t),
            sub: AllocManager::new(addr, len),
        }
    }

    /// Create a non-root memory region
    fn new_node(self: &Arc<Self>, addr: usize, len: usize) -> Self {
        let new_node = Node {
            parent: Arc::<Self>::clone(self),
            root: self.root(),
        };
        let kind = MemoryRegionKind::Node(new_node);
        Self {
            addr,
            len,
            kind,
            sub: AllocManager::new(addr, len),
        }
    }

    /// Get the root memory region
    fn root(self: &Arc<Self>) -> Arc<Self> {
        match self.kind {
            MemoryRegionKind::Root(_) => Arc::<Self>::clone(self),
            MemoryRegionKind::Node(ref node) => Arc::<Self>::clone(&node.root),
        }
    }

    /// Get a range of the memory region
    fn slice(self: &Arc<Self>, range: Range<usize>) -> io::Result<Self> {
        self.sub.slice(&range)?;
        Ok(self.new_node(self.addr.overflow_add(range.start), range.len()))
    }

    /// Get a block of the memory region considering the size and alignment
    fn alloc(self: &Arc<Self>, layout: &Layout) -> io::Result<Self> {
        let range = self.sub.alloc(layout)?;
        Ok(self.new_node(self.addr.overflow_add(range.start), range.len()))
    }
}

impl<T: RemoteKey> Drop for InnerMr<T> {
    fn drop(&mut self) {
        if let MemoryRegionKind::Node(ref node) = self.kind {
            if let Err(e) = node.parent.sub.free(
                self.addr.overflow_sub(node.parent.addr)
                    ..self
                        .len
                        .overflow_add(self.addr)
                        .overflow_sub(node.parent.addr),
            ) {
                error!("Faild to drop a memory region, {:?}", e);
            }
        }
    }
}

/// The information for a local memory region
pub struct Local {
    /// the internal `ibv_mr` pointer
    inner_mr: NonNull<ibv_mr>,
    /// the protection domain the memory region belongs to
    _pd: Arc<ProtectionDomain>,
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
        assert_eq!(errno, 0_i32);
    }
}

impl Local {
    /// Local key
    fn lkey(&self) -> u32 {
        unsafe { self.inner_mr.as_ref() }.lkey
    }
}

impl RemoteKey for Local {
    fn rkey(&self) -> u32 {
        unsafe { self.inner_mr.as_ref() }.rkey
    }
}

unsafe impl Sync for Local {}

unsafe impl Send for Local {}

impl InnerMr<Local> {
    /// Local key
    fn lkey(&self) -> u32 {
        match self.kind {
            MemoryRegionKind::Root(ref root) => root.lkey(),
            MemoryRegionKind::Node(ref node) => node.root.lkey(),
        }
    }
}

/// Local Memory Region type
pub type LocalMemoryRegion = MemoryRegion<Local>;

impl LocalMemoryRegion {
    /// get the memory region start mut addr
    #[allow(clippy::as_conversions)]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        // const pointer to mut pointer is safe
        self.as_ptr() as _
    }

    /// get the memory region as slice
    pub fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.as_ptr(), self.length()) }
    }

    /// get the memory region as mut slice
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr(), self.length()) }
    }

    /// get local key
    pub fn lkey(&self) -> u32 {
        self.inner.lkey()
    }

    /// Create a local memory region from protection domain
    pub fn new_from_pd(
        pd: &Arc<ProtectionDomain>,
        layout: Layout,
        access: ibv_access_flags,
    ) -> io::Result<Self> {
        let addr = unsafe { alloc(layout) };
        let inner_mr = NonNull::new(unsafe {
            ibv_reg_mr(pd.as_ptr(), addr.cast(), layout.size(), access.0.cast())
        })
        .ok_or_else(io::Error::last_os_error)?;
        let len = layout.size();
        let local = Local {
            inner_mr,
            _pd: Arc::<ProtectionDomain>::clone(pd),
        };
        Ok(MemoryRegion::new_root(mut_ptr_to_usize(addr), len, local))
    }
}

/// Remote memory region specific information
#[derive(Debug)]
pub struct Remote {
    /// The token
    token: MemoryRegionToken,
    /// Local agent
    agent: Arc<AgentInner>,
}

impl RemoteKey for Remote {
    fn rkey(&self) -> u32 {
        self.token.rkey
    }
}

impl Drop for Remote {
    fn drop(&mut self) {
        let agent = Arc::<AgentInner>::clone(&self.agent);
        let token = self.token;
        // detach the task
        let _task = tokio::spawn(async move { AgentInner::release_mr(&agent, token).await });
    }
}

/// Remote memory region type
pub type RemoteMemoryRegion = MemoryRegion<Remote>;

impl RemoteMemoryRegion {
    /// Create a remote memory region from the `token`
    pub fn new_from_token(token: MemoryRegionToken, agent: Arc<AgentInner>) -> Self {
        let addr = token.addr;
        let len = token.len;
        let remote = Remote { token, agent };
        let inner = Arc::new(InnerMr::new_root(addr, len, remote));
        Self { inner }
    }
}

/// The memory region allocation management structure
#[derive(Debug)]
struct AllocManager {
    /// The start address
    addr: usize,
    /// The length
    length: usize,
    /// The ranges that have been allocated
    occupy: Mutex<Vec<Range<usize>>>,
}

impl AllocManager {
    /// Createa new allocation management
    fn new(addr: usize, length: usize) -> Self {
        Self {
            addr,
            occupy: Mutex::new(Vec::new()),
            length,
        }
    }

    /// Insert a slice into occupy
    fn insert_slice(
        mut locked_sub: MutexGuard<Vec<Range<usize>>>,
        range: &Range<usize>,
    ) -> io::Result<()> {
        let pos = locked_sub
            .binary_search_by(|r| r.start.cmp(&range.start))
            .err()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "The slice is there"))?;
        locked_sub.insert(pos, range.clone());
        Ok(())
    }

    /// Allocate a slice according to a `range`
    fn slice(&self, range: &Range<usize>) -> io::Result<()> {
        // Invalid range or range is too large
        #[allow(clippy::suspicious_operation_groupings)]
        if range.start >= range.end || range.end > self.length {
            return Err(io::Error::new(io::ErrorKind::Other, "Invalid Range"));
        }
        let locked_sub = self.occupy.lock().map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Cannot lock occupy in alloc manager, {:?}", e),
            )
        })?;
        if !locked_sub
            .iter()
            .all(|sub_range| range.end <= sub_range.start || range.start >= sub_range.end)
        {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Memory slice Has been used",
            ));
        }
        Self::insert_slice(locked_sub, range)?;
        Ok(())
    }

    /// Allocate a slice according to a `layout`
    fn alloc(&self, layout: &Layout) -> io::Result<Range<usize>> {
        let mut last = 0;
        let locked_sub = self.occupy.lock().map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Cannot lock occupy in alloc manager, {:?}", e),
            )
        })?;

        let mut ans = Err(io::Error::new(io::ErrorKind::Other, "No Enough Memory"));

        for range in locked_sub.iter() {
            last = self.celling(last, layout);
            if last.overflow_add(layout.size()) <= range.start {
                ans = Ok(last..last.overflow_add(layout.size()));
                break;
            }
            last = range.end;
        }

        if ans.is_err() {
            // check last to the end
            last = self.celling(last, layout);
            if last.overflow_add(layout.size()) <= self.length {
                ans = Ok(last..last.overflow_add(layout.size()));
            }
        }

        let ans = ans?;
        Self::insert_slice(locked_sub, &ans)?;
        Ok(ans)
    }

    /// round up the `offset` according to the `layout`
    fn celling(&self, offset: usize, layout: &Layout) -> usize {
        let align = layout.align();
        self.addr
            .overflow_add(offset)
            .overflow_add(align)
            .overflow_sub(1)
            .overflow_div(align)
            .overflow_mul(align)
            .overflow_sub(self.addr)
    }

    /// free a allocated slice
    fn free(&self, range: Range<usize>) -> io::Result<()> {
        let mut locked_sub = self.occupy.lock().map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Cannot lock occupy in alloc manager, {:?}", e),
            )
        })?;
        let pos = locked_sub
            .binary_search_by(|r| r.start.cmp(&range.start))
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("Free an invalid range, {:?}", e),
                )
            })?;

        // the value is not important, just skip it
        let _ = locked_sub.remove(pos);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::AllocManager;
    use std::io;

    #[test]
    #[allow(clippy::indexing_slicing)]
    fn test_sub_mr_slice() -> io::Result<()> {
        let sub_mr = AllocManager::new(0, 1024);
        let res = sub_mr.slice(&(0..1));
        assert!(res.is_ok());
        let occupy_list = sub_mr.occupy.lock().map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Cannot lock occupy in alloc manager, {:?}", e),
            )
        })?;
        assert_eq!(occupy_list.len(), 1);
        // the length is checked
        assert_eq!(occupy_list[0], (0..1));
        Ok(())
    }
}
