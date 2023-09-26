use crate::{
    lock_utilities::MappedMutex,
    memory_region::{
        local::{LocalMr, LocalMrInner},
        RawMemoryRegion,
    },
    protection_domain::ProtectionDomain,
    MRInitAttr,
};
use clippy_utilities::Cast;
use libc::{c_void, size_t};
use parking_lot::{Mutex, MutexGuard};
use rdma_sys::ibv_access_flags;
use std::{
    alloc::{alloc, alloc_zeroed, Layout},
    collections::{BTreeMap, HashMap},
    io, mem,
    ops::Bound::Included,
    ptr,
    sync::Arc,
};
use tikv_jemalloc_sys::{
    self, extent_hooks_t, MALLOCX_ALIGN, MALLOCX_ARENA, MALLOCX_TCACHE_NONE, MALLOCX_ZERO,
};
use tracing::{debug, error};

/// Get default extent hooks from arena0
static DEFAULT_ARENA_INDEX: u32 = 0;

/// Custom extent alloc hook used by jemalloc
/// Alloc extent memory with `ibv_reg_mr()`
static RDMA_ALLOC_EXTENT_HOOK: unsafe extern "C" fn(
    extent_hooks: *mut extent_hooks_t,
    new_addr: *mut c_void,
    size: usize,
    alignment: usize,
    zero: *mut i32,
    commit: *mut i32,
    arena_ind: u32,
) -> *mut c_void = extent_alloc_hook;

/// Custom extent dalloc hook used by jemalloc
/// Dalloc extent memory with `ibv_dereg_mr()`
static RDMA_DALLOC_EXTENT_HOOK: unsafe extern "C" fn(
    extent_hooks: *mut extent_hooks_t,
    addr: *mut c_void,
    size: usize,
    committed: i32,
    arena_ind: u32,
) -> i32 = extent_dalloc_hook;

/// Custom extent merge hook used by jemalloc
/// Merge two adjacent extents to a bigger one
static RDMA_MERGE_EXTENT_HOOK: unsafe extern "C" fn(
    extent_hooks: *mut extent_hooks_t,
    addr_a: *mut c_void,
    size_a: usize,
    addr_b: *mut c_void,
    size_b: usize,
    committed: i32,
    arena_ind: u32,
) -> i32 = extent_merge_hook;

/// Custom extent hooks
static mut RDMA_EXTENT_HOOKS: extent_hooks_t = extent_hooks_t {
    alloc: Some(RDMA_ALLOC_EXTENT_HOOK),
    dalloc: Some(RDMA_DALLOC_EXTENT_HOOK),
    destroy: None,
    commit: None,
    decommit: None,
    purge_lazy: None,
    purge_forced: None,
    split: None,
    merge: Some(RDMA_MERGE_EXTENT_HOOK),
};

/// The tuple of `ibv_access_falgs` and `ProtectionDomain` as a key.
#[derive(PartialEq, Eq, Hash)]
pub(crate) struct AccessPDKey(u32, usize);

impl AccessPDKey {
    /// Create a new `AccessPDKey`
    #[allow(clippy::as_conversions)]
    pub(crate) fn new(pd: &Arc<ProtectionDomain>, access: ibv_access_flags) -> Self {
        Self(access.0, pd.as_ptr() as usize)
    }
}

/// The map that records correspondence between extent metadata and `raw_mr`
type ExtentTokenMap = Arc<Mutex<BTreeMap<usize, Item>>>;
/// The map that records correspondence between `arena_ind` and `ProtectionDomain`
type ArenaPdMap = Arc<Mutex<HashMap<u32, (Arc<ProtectionDomain>, ibv_access_flags)>>>;
/// The map that records correspondence between (`ibv_access_flags`, &pd) and `arena_ind`
type AccessPDArenaMap = Arc<Mutex<HashMap<AccessPDKey, u32>>>;

lazy_static! {
    /// Default extent hooks of jemalloc
    static ref ORIGIN_HOOKS: extent_hooks_t = get_default_hooks_impl(DEFAULT_ARENA_INDEX);
    /// The correspondence between extent metadata and `raw_mr`
    #[derive(Debug)]
    pub(crate) static ref EXTENT_TOKEN_MAP: ExtentTokenMap = Arc::new(Mutex::new(BTreeMap::<usize, Item>::new()));
    /// The correspondence between `arena_ind` and `ProtectionDomain`
    pub(crate) static ref ARENA_PD_MAP:  ArenaPdMap = Arc::new(Mutex::new(HashMap::new()));
    /// The correspondence between `ibv_access_flags` and `arena_ind`
    pub(crate) static ref ACCESS_PD_ARENA_MAP: AccessPDArenaMap = Arc::new(Mutex::new(HashMap::new()));
}

/// Combination between extent metadata and `raw_mr`
#[derive(Debug)]
pub(crate) struct Item {
    /// addr of an extent(memory region)
    addr: usize,
    /// length of an extent(memory region)
    len: usize,
    /// Reference of RawMemoryRegion
    raw_mr: Arc<RawMemoryRegion>,
    /// arena index of this extent
    arena_ind: u32,
}

/// Strategies to manage `MR`s
#[derive(Debug, Clone, Copy)]
pub enum MRManageStrategy {
    /// Use `Jemalloc` to manage `MRs`
    Jemalloc,
    /// Alloc raw `MR`
    Raw,
}

/// Memory region allocator
#[derive(Debug)]
pub(crate) struct MrAllocator {
    /// Protection domain that holds the allocator
    default_pd: Arc<ProtectionDomain>,
    /// Default arena index
    default_arena_ind: u32,
    /// Strategy to manage `MR`s
    strategy: MRManageStrategy,
    /// Default memory region access
    default_access: ibv_access_flags,
}

impl MrAllocator {
    /// Create a new MR allocator
    pub(crate) fn new(pd: Arc<ProtectionDomain>, mr_attr: MRInitAttr) -> Self {
        match mr_attr.strategy {
            MRManageStrategy::Jemalloc => {
                debug!("new mr_allocator using jemalloc strategy");
                #[allow(clippy::expect_used)]
                let arena_ind =
                    init_je_statics(Arc::<ProtectionDomain>::clone(&pd), mr_attr.access)
                        .expect("init je statics failed");
                Self {
                    default_pd: pd,
                    default_arena_ind: arena_ind,
                    strategy: mr_attr.strategy,
                    default_access: mr_attr.access,
                }
            }
            MRManageStrategy::Raw => {
                debug!("new mr_allocator using raw strategy");
                Self {
                    default_pd: pd,
                    default_arena_ind: 0_u32,
                    strategy: mr_attr.strategy,
                    default_access: mr_attr.access,
                }
            }
        }
    }

    /// Allocate an uninitialized `LocalMr` according to the `layout`
    ///
    /// # Safety
    ///
    /// The newly allocated memory in this `LocalMr` is uninitialized.
    /// Initialize it before using to make it safe.
    #[allow(clippy::as_conversions)]
    pub(crate) unsafe fn alloc(
        self: &Arc<Self>,
        layout: &Layout,
        access: ibv_access_flags,
        pd: &Arc<ProtectionDomain>,
    ) -> io::Result<LocalMr> {
        let inner = self.alloc_inner(layout, 0, access, pd)?;
        Ok(LocalMr::new(inner))
    }

    /// Allocate a `LocalMr` according to the `layout`
    #[allow(clippy::as_conversions)]
    pub(crate) fn alloc_zeroed(
        self: &Arc<Self>,
        layout: &Layout,
        access: ibv_access_flags,
        pd: &Arc<ProtectionDomain>,
    ) -> io::Result<LocalMr> {
        // SAFETY: alloc zeroed memory is safe
        let inner = unsafe { self.alloc_inner(layout, MALLOCX_ZERO, access, pd)? };
        Ok(LocalMr::new(inner))
    }

    /// Allocate a `LocalMr` according to the `layout` with default access and pd
    #[allow(clippy::as_conversions)]
    pub(crate) fn alloc_zeroed_default(self: &Arc<Self>, layout: &Layout) -> io::Result<LocalMr> {
        self.alloc_zeroed(layout, self.default_access, &self.default_pd)
    }

    /// Allocate an uninitialized `LocalMr` according to the `layout` with default access
    ///
    /// # Safety
    ///
    /// The newly allocated memory in this `LocalMr` is uninitialized.
    /// Initialize it before using to make it safe.
    #[allow(clippy::as_conversions)]
    pub(crate) unsafe fn alloc_default_access(
        self: &Arc<Self>,
        layout: &Layout,
        pd: &Arc<ProtectionDomain>,
    ) -> io::Result<LocalMr> {
        self.alloc(layout, self.default_access, pd)
    }

    /// Allocate a `LocalMr` according to the `layout` with default access
    #[allow(clippy::as_conversions)]
    pub(crate) fn alloc_zeroed_default_access(
        self: &Arc<Self>,
        layout: &Layout,
        pd: &Arc<ProtectionDomain>,
    ) -> io::Result<LocalMr> {
        self.alloc_zeroed(layout, self.default_access, pd)
    }

    /// Allocate a `LocalMrInner` according to the `layout`
    ///
    /// # Safety
    ///
    /// This func is safe when `zeroed == true`, otherwise newly allocated memory in the `LocalMrInner` is uninitialized.
    #[allow(clippy::as_conversions)]
    pub(crate) unsafe fn alloc_inner(
        self: &Arc<Self>,
        layout: &Layout,
        flag: i32,
        access: ibv_access_flags,
        pd: &Arc<ProtectionDomain>,
    ) -> io::Result<LocalMrInner> {
        // check to ensure the safety requirements of `slice` methords
        if layout.size() > isize::MAX.cast() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the size of mr must be no larger than isize::MAX",
            ));
        }

        match self.strategy {
            MRManageStrategy::Jemalloc => {
                let arena_id = if access == self.default_access && pd.inner_pd == self.default_pd.inner_pd {
                    self.default_arena_ind
                }else {
                    let access_pd_key = AccessPDKey::new(pd, access);
                    let arena_id = ACCESS_PD_ARENA_MAP.lock().get(&access_pd_key).copied();
                    arena_id.map_or_else(||{
                        // didn't use this access before, so create an arena to mange this kind of MR
                        let ind = init_je_statics(Arc::<ProtectionDomain>::clone(pd), access)?;
                        if ACCESS_PD_ARENA_MAP.lock().insert(access_pd_key, ind).is_some(){
                            return Err(io::Error::new(
                                io::ErrorKind::Other,
                                format!("this is a bug: insert ACCESS_ARENA_MAP failed, access:{:?} has been recorded", access),
                            ))
                        };
                        Ok(ind)
                    }, |ind|{Ok(ind)})?
                };
                alloc_from_je(arena_id, layout, flag).map_or_else(||{
                    Err(io::Error::new(io::ErrorKind::OutOfMemory, "insufficient contiguous memory was available to service the allocation request"))
                }, |addr|{
                    #[allow(clippy::unreachable)]
                    let raw_mr = lookup_raw_mr(arena_id, addr as usize).map_or_else(
                        || {
                            unreachable!("can not find raw mr with arena_id: {} by addr: {}",arena_id, addr as usize);
                        },
                        |raw_mr| raw_mr,
                    );
                    Ok(LocalMrInner::new(addr as usize, *layout, raw_mr, self.strategy))
                })
            },
            MRManageStrategy::Raw => {
                alloc_raw_mem(layout, flag).map_or_else(||{
                        Err(io::Error::new(io::ErrorKind::OutOfMemory, "insufficient contiguous memory was available to service the allocation request"))
                    }, |addr|{
                        let raw_mr = Arc::new(RawMemoryRegion::register_from_pd(pd, addr, layout.size(), access)?);
                        Ok(LocalMrInner::new(addr as usize, *layout, raw_mr, self.strategy))
                    })
            },
        }
    }
}

/// Alloc memory for RDMA operations from jemalloc
///
/// # Safety
///
/// This func is safe when `zeroed == true`, otherwise newly allocated memory is uninitialized.
unsafe fn alloc_from_je(arena_ind: u32, layout: &Layout, flag: i32) -> Option<*mut u8> {
    // set align 、 arena_id and flags for this allocation, and disable tcache for lookup_raw_mr check
    let flags = (MALLOCX_ALIGN(layout.align())
        | MALLOCX_ARENA(arena_ind.cast())
        | MALLOCX_TCACHE_NONE()
        | flag)
        .cast();
    debug!(
        "alloc mr from je, arena_ind: {:?}, layout: {:?}, flags: {:?}",
        arena_ind, layout, flags
    );
    let addr = { tikv_jemalloc_sys::mallocx(layout.size(), flags) };
    if addr.is_null() {
        None
    } else {
        Some(addr.cast::<u8>())
    }
}

/// Get default extent hooks of jemalloc
#[allow(clippy::unreachable)]
fn get_default_hooks_impl(arena_ind: u32) -> extent_hooks_t {
    // read default alloc impl
    let mut hooks: *mut extent_hooks_t = ptr::null_mut();
    let hooks_ptr: *mut *mut extent_hooks_t = &mut hooks;
    let key = format!("arena.{}.extent_hooks\0", arena_ind);
    let mut hooks_len = mem::size_of_val(&hooks);
    // SAFETY: ?
    // TODO: check safety
    let errno = unsafe {
        tikv_jemalloc_sys::mallctl(
            key.as_ptr().cast(),
            hooks_ptr.cast(),
            &mut hooks_len,
            ptr::null_mut(),
            0,
        )
    };
    if errno != 0_i32 {
        unreachable!(
            "get default hooks failed {:?}",
            io::Error::from_raw_os_error(errno)
        );
    }
    // SAFETY: ?
    // TODO: check safety
    unsafe { *hooks }
}

/// Alloc raw memory region for RDMA operations
///
/// # Safety
///
/// This func is safe when:
/// * `flag == MALLOCX_ZERO`, otherwise newly allocated memory is uninitialized.
/// * Dealloc `addr` after dropping `raw_mr`.
unsafe fn alloc_raw_mem(layout: &Layout, flag: i32) -> Option<*mut u8> {
    let addr = if flag == MALLOCX_ZERO {
        alloc_zeroed(*layout)
    } else {
        alloc(*layout)
    };

    if addr.is_null() {
        None
    } else {
        Some(addr)
    }
}

/// Set custom extent hooks
fn set_extent_hooks(arena_ind: u32) -> io::Result<()> {
    let key = format!("arena.{}.extent_hooks\0", arena_ind);
    // SAFETY: ?
    // TODO: check safety
    let mut hooks_ptr: *mut extent_hooks_t = unsafe { &mut RDMA_EXTENT_HOOKS };
    let hooks_ptr_ptr: *mut *mut extent_hooks_t = &mut hooks_ptr;
    let hooks_len: size_t = mem::size_of_val(&hooks_ptr_ptr);
    // SAFETY: ?
    // TODO: check safety
    let errno = unsafe {
        tikv_jemalloc_sys::mallctl(
            key.as_ptr().cast(),
            ptr::null_mut(),
            ptr::null_mut(),
            hooks_ptr_ptr.cast(),
            hooks_len,
        )
    };
    if errno != 0_i32 {
        error!("set extent hooks failed");
        return Err(io::Error::from_raw_os_error(errno));
    }
    debug!("arena<{}> set hooks success", arena_ind);
    Ok(())
}

/// Create an arena to manage `MR`s memory
fn create_arena() -> io::Result<u32> {
    let key = "arenas.create\0";
    let mut aid = 0_u32;
    let aid_ptr: *mut u32 = &mut aid;
    let mut aid_len: size_t = mem::size_of_val(&aid);
    // SAFETY: ?
    // TODO: check safety
    let errno = unsafe {
        tikv_jemalloc_sys::mallctl(
            key.as_ptr().cast(),
            aid_ptr.cast(),
            &mut aid_len,
            ptr::null_mut(),
            0,
        )
    };
    if errno != 0_i32 {
        error!("set extent hooks failed");
        return Err(io::Error::from_raw_os_error(errno));
    }
    debug!("create arena success aid : {}", aid);
    Ok(aid)
}

/// Create arena and init statics
fn init_je_statics(pd: Arc<ProtectionDomain>, access: ibv_access_flags) -> io::Result<u32> {
    let ind = create_arena()?;
    if ARENA_PD_MAP.lock().insert(ind, (pd, access)).is_some() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "insert ARENA_PD_MAP failed",
        ));
    }
    set_extent_hooks(ind)?;
    Ok(ind)
}

/// Look up `raw_mr` info by addr
fn lookup_raw_mr(arena_ind: u32, addr: usize) -> Option<Arc<RawMemoryRegion>> {
    EXTENT_TOKEN_MAP.map_read(|map| {
        map.range((Included(&0), Included(&addr)))
            .next_back()
            .map_or_else(
                || {
                    debug!("no related item. addr {}, arena_ind {}", addr, arena_ind);
                    None
                },
                |(_, item)| {
                    debug!("lookup addr {}, item {:?}", addr, item);
                    assert_eq!(arena_ind, item.arena_ind);
                    assert!(addr >= item.addr && addr < item.addr.wrapping_add(item.len));
                    Some(Arc::<RawMemoryRegion>::clone(&item.raw_mr))
                },
            )
    })
}

/// Custom extent alloc hook enable jemalloc manage rdma memory region
#[allow(clippy::expect_used)]
#[allow(clippy::unreachable)]
unsafe extern "C" fn extent_alloc_hook(
    extent_hooks: *mut extent_hooks_t,
    new_addr: *mut c_void,
    size: usize,
    alignment: usize,
    zero: *mut i32,
    commit: *mut i32,
    arena_ind: u32,
) -> *mut c_void {
    let origin_alloc = (*ORIGIN_HOOKS)
        .alloc
        .expect("can not get default alloc hook");
    let addr = origin_alloc(
        extent_hooks,
        new_addr,
        size,
        alignment,
        zero,
        commit,
        arena_ind,
    );
    if addr.is_null() {
        return addr;
    }
    register_extent_mr(addr, size, arena_ind).map_or_else(
        || {
            error!("register_extent_mr failed. If this is the first registration, it may be caused by invalid parameters, otherwise it is OOM");
            let origin_dalloc_hook = (*ORIGIN_HOOKS)
            .dalloc
            .expect("can not get default dalloc hook");
        if origin_dalloc_hook(extent_hooks, addr, size, *commit, arena_ind) == 1_i32 {
            unreachable!("dalloc failed, check if JEMALLOC_RETAIN defined. If so, then please undefined it");
        }
            ptr::null_mut()
        },
        |item| {
            debug!("alloc item {:?} lkey {}", &item, item.raw_mr.lkey());
            insert_item(item);
            addr
        },
    )
}

/// Custom extent dalloc hook enable jemalloc manage rdma memory region
#[allow(clippy::as_conversions)]
#[allow(clippy::expect_used)]
unsafe extern "C" fn extent_dalloc_hook(
    extent_hooks: *mut extent_hooks_t,
    addr: *mut c_void,
    size: usize,
    committed: i32,
    arena_ind: u32,
) -> i32 {
    debug!("dalloc addr {}, size {}", addr as usize, size);
    remove_item(addr);
    let origin_dalloc = (*ORIGIN_HOOKS)
        .dalloc
        .expect("can not get default dalloc hook");
    origin_dalloc(extent_hooks, addr, size, committed, arena_ind)
}

/// Custom extent merge hook enable jemalloc manage rdma memory region
#[allow(clippy::as_conversions)]
#[allow(clippy::expect_used)]
unsafe extern "C" fn extent_merge_hook(
    extent_hooks: *mut extent_hooks_t,
    addr_a: *mut c_void,
    size_a: usize,
    addr_b: *mut c_void,
    size_b: usize,
    committed: i32,
    arena_ind: u32,
) -> i32 {
    debug!(
        "merge addr_a {}, size_a {}; addr_b {}, size_b {}",
        addr_a as usize, size_a, addr_b as usize, size_b
    );
    let origin_merge = (*ORIGIN_HOOKS)
        .merge
        .expect("can not get default merge hook");
    let err = origin_merge(
        extent_hooks,
        addr_a,
        size_a,
        addr_b,
        size_b,
        committed,
        arena_ind,
    );
    // merge failed
    if err == 1_i32 {
        debug!(
            "merge failed. addr_a {}, size_a {}; addr_b {}, size_b {}",
            addr_a as usize, size_a, addr_b as usize, size_b
        );
        return err;
    }
    EXTENT_TOKEN_MAP.map_write(|map| {
        let arena_a = get_arena_ind_after_lock(map, addr_a);
        let arena_b = get_arena_ind_after_lock(map, addr_b);
        // make sure the extents belong to the same pd(arena).
        if !(arena_a == arena_b && arena_a.is_some()) {
            return 1_i32;
        }
        // the old mrs will be deregistered after `raw_mr` drop(after item removed)
        remove_item_after_lock(map, addr_a);
        remove_item_after_lock(map, addr_b);
        // so we only need to register a new `raw_mr`
        register_extent_mr(addr_a, size_a.wrapping_add(size_b), arena_ind).map_or_else(
            || {
                error!("register_extent_mr failed");
                1_i32
            },
            |item| {
                insert_item_after_lock(map, item);
                0_i32
            },
        )
    })
}

/// get arena index after lock
#[allow(clippy::as_conversions)]
fn get_arena_ind_after_lock(
    map: &mut MutexGuard<BTreeMap<usize, Item>>,
    addr: *mut c_void,
) -> Option<u32> {
    map.get(&(addr as usize)).map_or_else(
        || {
            error!(
                "can not get item from EXTENT_TOKEN_MAP. addr : {}",
                addr as usize
            );
            None
        },
        |item| Some(item.arena_ind),
    )
}

/// Insert item into `EXTENT_TOKEN_MAP`
fn insert_item(item: Item) {
    EXTENT_TOKEN_MAP.map_write(|map| insert_item_after_lock(map, item));
}

/// Insert item into `EXTENT_TOKEN_MAP` after lock
#[allow(clippy::unreachable)]
fn insert_item_after_lock(map: &mut MutexGuard<BTreeMap<usize, Item>>, item: Item) {
    let addr = item.addr;
    let _ = map.insert(addr, item).map(|old| {
        unreachable!("alloc the same addr double time {:?}", old);
    });
}

/// remove item from `EXTENT_TOKEN_MAP`
fn remove_item(addr: *mut c_void) {
    EXTENT_TOKEN_MAP.map_write(|map| remove_item_after_lock(map, addr));
}

/// Insert item into `EXTENT_TOKEN_MAP` after lock
#[allow(clippy::as_conversions)]
#[allow(clippy::unreachable)]
fn remove_item_after_lock(map: &mut MutexGuard<BTreeMap<usize, Item>>, addr: *mut c_void) {
    map.remove(&(addr as usize)).map_or_else(
        || {
            unreachable!(
                "can not get item from EXTENT_TOKEN_MAP. addr : {}",
                addr as usize
            );
        },
        |item| {
            debug!("remove item {:?}", item);
        },
    );
}

/// Register extent memory region
#[allow(clippy::as_conversions)]
pub(crate) fn register_extent_mr(addr: *mut c_void, size: usize, arena_ind: u32) -> Option<Item> {
    assert_ne!(addr, ptr::null_mut());
    ARENA_PD_MAP.lock().get(&arena_ind).map_or_else(
        || {
            error!("can not get pd from ARENA_PD_MAP");
            None
        },
        |&(ref pd, ref access)| {
            debug!(
                "reg_mr addr {}, size {}, arena_ind {}, access {:?}",
                addr as usize, size, arena_ind, access
            );
            RawMemoryRegion::register_from_pd(pd, addr.cast::<u8>(), size, *access).map_or_else(
                |err| {
                    error!("RawMemoryRegion::register_from_pd failed {:?}", err);
                    None
                },
                |raw_mr| {
                    Some(Item {
                        addr: addr as usize,
                        len: size,
                        raw_mr: Arc::new(raw_mr),
                        arena_ind,
                    })
                },
            )
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        access::ibv_access_into_flags, context::Context, AccessFlag, LocalMrReadAccess, MrAccess,
        RdmaBuilder, DEFAULT_ACCESS,
    };
    use std::{alloc::Layout, io, thread};
    use tikv_jemalloc_sys::MALLOCX_ALIGN;

    #[tokio::test]
    async fn alloc_mr_from_rdma() -> io::Result<()> {
        let rdma = RdmaBuilder::default()
            .set_port_num(1)
            .set_gid_index(1)
            .build()?;
        let mut mrs = vec![];
        let layout = Layout::new::<[u8; 4096]>();
        for _ in 0_i32..2_i32 {
            let mr = rdma.alloc_local_mr(layout)?;
            mrs.push(mr);
        }
        Ok(())
    }

    #[test]
    fn alloc_mr_from_allocator() -> io::Result<()> {
        let ctx = Arc::new(Context::open(None, 1, 1)?);
        let pd = Arc::new(ctx.create_protection_domain()?);
        let allocator = Arc::new(MrAllocator::new(pd, MRInitAttr::default()));
        let layout = Layout::new::<char>();
        let lmr = allocator.alloc_zeroed_default(&layout)?;
        debug!("lmr info :{:?}", &lmr);
        Ok(())
    }

    #[allow(clippy::as_conversions)]
    #[allow(clippy::unwrap_used)]
    unsafe extern "C" fn extent_alloc_hook_for_test(
        extent_hooks: *mut extent_hooks_t,
        new_addr: *mut c_void,
        size: usize,
        alignment: usize,
        zero: *mut i32,
        commit: *mut i32,
        arena_ind: u32,
    ) -> *mut c_void {
        let addr = extent_alloc_hook(
            extent_hooks,
            new_addr,
            size,
            alignment,
            zero,
            commit,
            arena_ind,
        );
        let item = lookup_raw_mr(arena_ind, addr as usize).unwrap();
        assert_eq!(item.addr(), addr as usize);
        assert_eq!(item.length(), size);
        addr
    }

    #[allow(clippy::as_conversions)]
    unsafe extern "C" fn extent_dalloc_hook_for_test(
        extent_hooks: *mut extent_hooks_t,
        addr: *mut c_void,
        size: usize,
        committed: i32,
        arena_ind: u32,
    ) -> i32 {
        let res = extent_dalloc_hook(extent_hooks, addr, size, committed, arena_ind);
        assert!(lookup_raw_mr(arena_ind, addr as usize).is_none());
        res
    }

    #[allow(clippy::as_conversions)]
    #[allow(clippy::unwrap_used)]
    unsafe extern "C" fn extent_merge_hook_for_test(
        extent_hooks: *mut extent_hooks_t,
        addr_a: *mut c_void,
        size_a: usize,
        addr_b: *mut c_void,
        size_b: usize,
        committed: i32,
        arena_ind: u32,
    ) -> i32 {
        let res = extent_merge_hook(
            extent_hooks,
            addr_a,
            size_a,
            addr_b,
            size_b,
            committed,
            arena_ind,
        );
        assert_eq!(res, 0_i32);
        assert_eq!(
            lookup_raw_mr(arena_ind, addr_b as usize).unwrap().addr(),
            addr_a as usize
        );
        let item = lookup_raw_mr(arena_ind, addr_a as usize).unwrap();
        assert_eq!(item.addr(), addr_a as usize);
        assert_eq!(item.length(), size_a.wrapping_add(size_b));
        res
    }

    /// Check if the default extent hooks are the same as custom hooks
    fn check_hooks(arena_ind: u32) {
        let hooks = get_default_hooks_impl(arena_ind);
        // SAFETY: ?
        // TODO: check safety
        unsafe {
            assert_eq!(hooks.alloc, RDMA_EXTENT_HOOKS.alloc);
            assert_eq!(hooks.dalloc, RDMA_EXTENT_HOOKS.dalloc);
            assert_eq!(hooks.merge, RDMA_EXTENT_HOOKS.merge);
        }
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_extent_hooks() -> io::Result<()> {
        let ctx = Arc::new(Context::open(None, 1, 1)?);
        let pd = Arc::new(ctx.create_protection_domain()?);
        let allocator = Arc::new(MrAllocator::new(pd, MRInitAttr::default()));

        check_hooks(allocator.default_arena_ind);
        // set test hooks
        // SAFETY: ?
        // TODO: check safety
        unsafe {
            RDMA_EXTENT_HOOKS = extent_hooks_t {
                alloc: Some(extent_alloc_hook_for_test),
                dalloc: Some(extent_dalloc_hook_for_test),
                destroy: None,
                commit: None,
                decommit: None,
                purge_lazy: None,
                purge_forced: None,
                split: None,
                merge: Some(extent_merge_hook_for_test),
            };
        }
        set_extent_hooks(allocator.default_arena_ind).unwrap();
        // check custom test hooks
        check_hooks(allocator.default_arena_ind);

        // some tests to make it possible to observe the opreations of jemalloc's
        // memory management operatios such as alloc, dalloc, merge.
        let mut layout = Layout::new::<char>();
        // alloc and drop one by one
        for _ in 0_u32..100_u32 {
            let _lmr = allocator.alloc_zeroed_default(&layout)?;
        }
        // alloc all and drop all
        layout = Layout::new::<[u8; 16 * 1024]>();
        let mut lmrs = vec![];
        for _ in 0_u32..100_u32 {
            lmrs.push(allocator.alloc_zeroed_default(&layout)?);
        }
        // jemalloc will merge extents after drop all lmr in lmrs.
        lmrs.clear();
        // alloc big extent and dalloc it immediately after _lmr's dropping
        layout = Layout::new::<[u8; 1024 * 1024 * 32]>();
        let _lmr = allocator.alloc_zeroed_default(&layout)?;
        Ok(())
    }

    #[test]
    #[allow(clippy::as_conversions)]
    #[allow(clippy::unwrap_used)]
    fn je_malloxc_test() {
        let ctx = Arc::new(Context::open(None, 1, 1).unwrap());
        let pd = Arc::new(ctx.create_protection_domain().unwrap());
        let ind = init_je_statics(pd, *DEFAULT_ACCESS).unwrap();
        let thread = thread::spawn(move || {
            let layout = Layout::new::<char>();
            // SAFETY: ?
            // TODO: check safety
            let addr = unsafe {
                tikv_jemalloc_sys::mallocx(
                    layout.size(),
                    MALLOCX_ALIGN(layout.align()) | MALLOCX_ARENA(ind.cast()),
                )
            };
            assert_ne!(addr, ptr::null_mut());
            // SAFETY: ?
            // TODO: check safety
            unsafe {
                *(addr.cast::<char>()) = 'c';
                assert_eq!(*(addr.cast::<char>()), 'c');
                debug!(
                    "addr : {}, char : {}",
                    addr as usize,
                    *(addr.cast::<char>())
                );
            }
        });
        let layout = Layout::new::<char>();
        // SAFETY: ?
        // TODO: check safety
        let addr = unsafe {
            tikv_jemalloc_sys::mallocx(
                layout.size(),
                MALLOCX_ALIGN(layout.align()) | MALLOCX_ARENA(ind.cast()),
            )
        };
        assert_ne!(addr, ptr::null_mut());
        // SAFETY: ?
        // TODO: check safety
        unsafe {
            *(addr.cast::<char>()) = 'c';
            assert_eq!(*(addr.cast::<char>()), 'c');
            debug!(
                "addr : {}, char : {}",
                addr as usize,
                *(addr.cast::<char>())
            );
        }
        thread.join().unwrap();
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn zeroed_test() -> io::Result<()> {
        let ctx = Arc::new(Context::open(None, 1, 1)?);
        let pd = Arc::new(ctx.create_protection_domain()?);
        let allocator = Arc::new(MrAllocator::new(pd, MRInitAttr::default()));
        let lmr = allocator
            .alloc_zeroed_default(&Layout::new::<[u8; 10]>())
            .unwrap();
        assert_eq!(*lmr.as_slice(), [0_u8; 10]);
        Ok(())
    }

    #[tokio::test]
    async fn alloc_raw_mr_from_rdma() -> io::Result<()> {
        let rdma = RdmaBuilder::default()
            .set_mr_strategy(MRManageStrategy::Raw)
            .build()?;
        let mut mrs = vec![];
        let layout = Layout::new::<[u8; 4096]>();
        for _ in 0_i32..2_i32 {
            let mr = rdma.alloc_local_mr(layout)?;
            mrs.push(mr);
        }
        mrs.clear();
        Ok(())
    }

    #[test]
    fn alloc_raw_mr_from_allocator() -> io::Result<()> {
        let ctx = Arc::new(Context::open(None, 1, 1)?);
        let pd = Arc::new(ctx.create_protection_domain()?);
        let mr_attr = MRInitAttr {
            access: *DEFAULT_ACCESS,
            strategy: MRManageStrategy::Raw,
        };
        let allocator = Arc::new(MrAllocator::new(pd, mr_attr));
        let layout = Layout::new::<char>();
        let lmr = allocator.alloc_zeroed_default(&layout)?;
        debug!("lmr info :{:?}", &lmr);
        Ok(())
    }

    #[tokio::test]
    async fn alloc_raw_mr_with_access() -> io::Result<()> {
        let rdma = RdmaBuilder::default()
            .set_mr_strategy(MRManageStrategy::Raw)
            .build()?;
        let layout = Layout::new::<[u8; 4096]>();
        let access = AccessFlag::LocalWrite | AccessFlag::RemoteRead;
        let mr = rdma.alloc_local_mr_with_access(layout, access)?;
        assert_eq!(mr.access(), access);
        assert_ne!(mr.access(), ibv_access_into_flags(*DEFAULT_ACCESS));
        Ok(())
    }

    #[tokio::test]
    async fn alloc_mr_with_access_from_je() -> io::Result<()> {
        let rdma = RdmaBuilder::default().build()?;
        let layout = Layout::new::<[u8; 4096]>();
        let access = AccessFlag::LocalWrite | AccessFlag::RemoteRead;
        let mr = rdma.alloc_local_mr_with_access(layout, access)?;
        assert_eq!(mr.access(), access);
        assert_ne!(mr.access(), ibv_access_into_flags(*DEFAULT_ACCESS));
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::as_conversions)]
    async fn check_je_mr_layout() -> io::Result<()> {
        let rdma = RdmaBuilder::default().build()?;
        let layouts = vec![
            Layout::new::<char>(),
            Layout::new::<u8>(),
            Layout::new::<u32>(),
            Layout::new::<u64>(),
            Layout::new::<String>(),
            Layout::new::<[u8; 8]>(),
        ];
        for lay in layouts {
            let mr = rdma.alloc_local_mr(lay)?;
            // size check
            assert_eq!(lay.size(), mr.length());
            // alignment check
            assert_eq!((*mr.as_ptr() as usize) % lay.align(), 0);
        }
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::as_conversions)]
    async fn check_raw_mr_layout() -> io::Result<()> {
        let rdma = RdmaBuilder::default()
            .set_mr_strategy(MRManageStrategy::Raw)
            .build()?;
        let layouts = vec![
            Layout::new::<char>(),
            Layout::new::<u8>(),
            Layout::new::<u32>(),
            Layout::new::<u64>(),
            Layout::new::<String>(),
            Layout::new::<[u8; 8]>(),
        ];
        for lay in layouts {
            let mr = rdma.alloc_local_mr(lay)?;
            // size check
            assert_eq!(lay.size(), mr.length());
            // alignment check
            assert_eq!((*mr.as_ptr() as usize) % lay.align(), 0);
        }
        Ok(())
    }

    /// `Jemalloc` may alloc `MR` with wrong `arena_index` from `tcache` when we
    /// create more than one `Jemalloc` enabled `mr_allocator`s.
    /// So we disable `tcache` by default and make this test.
    /// If you want to enable `tcache` and make sure safety by yourself, change
    /// `JEMALLOC_SYS_WITH_MALLOC_CONF` from `tcache:false` to `tcache:true`.
    #[tokio::test]
    async fn multi_je_allocator() -> io::Result<()> {
        let rdma_1 = RdmaBuilder::default().build()?;
        let layout = Layout::new::<char>();
        let _mr_1 = rdma_1.alloc_local_mr(layout);
        let rdma_2 = RdmaBuilder::default().build()?;
        let _mr_2 = rdma_2.alloc_local_mr(layout);
        Ok(())
    }

    #[test]
    fn alloc_raw_multi_pd_mr() -> io::Result<()> {
        let ctx = Arc::new(Context::open(None, 1, 1)?);
        let pd_1 = Arc::new(ctx.create_protection_domain()?);
        let pd_2 = Arc::new(ctx.create_protection_domain()?);
        let mr_attr = MRInitAttr {
            access: *DEFAULT_ACCESS,
            strategy: MRManageStrategy::Raw,
        };
        let allocator = Arc::new(MrAllocator::new(Arc::clone(&pd_1), mr_attr));
        let layout = Layout::new::<char>();
        let mr_1 = allocator.alloc_zeroed(&layout, *DEFAULT_ACCESS, &pd_1)?;
        println!("{:?}", mr_1);
        assert_eq!(&mr_1.read_inner().pd().inner_pd, &pd_1.inner_pd);
        let mr_2 = allocator.alloc_zeroed(&layout, *DEFAULT_ACCESS, &pd_2)?;
        println!("{:?}", mr_2);
        assert_eq!(&mr_2.read_inner().pd().inner_pd, &pd_2.inner_pd);
        Ok(())
    }

    #[test]
    fn alloc_multi_pd_mr_from_je() -> io::Result<()> {
        let ctx = Arc::new(Context::open(None, 1, 1)?);
        let pd_1 = Arc::new(ctx.create_protection_domain()?);
        let pd_2 = Arc::new(ctx.create_protection_domain()?);
        let mr_attr = MRInitAttr {
            access: *DEFAULT_ACCESS,
            strategy: MRManageStrategy::Jemalloc,
        };
        let allocator = Arc::new(MrAllocator::new(Arc::clone(&pd_1), mr_attr));
        let layout = Layout::new::<char>();
        let mr_1 = allocator.alloc_zeroed(&layout, *DEFAULT_ACCESS, &pd_1)?;
        assert_eq!(&mr_1.read_inner().pd().inner_pd, &pd_1.inner_pd);
        let mr_2 = allocator.alloc_zeroed(&layout, *DEFAULT_ACCESS, &pd_2)?;
        assert_eq!(&mr_2.read_inner().pd().inner_pd, &pd_2.inner_pd);
        Ok(())
    }
}

/// Test `LocalMr` APIs with multi-pd & multi-connection
#[cfg(test)]
mod mr_with_multi_pd_test {
    use crate::AccessFlag;
    use crate::{LocalMrReadAccess, RdmaBuilder};
    use portpicker::pick_unused_port;
    use std::{
        alloc::Layout,
        io,
        net::{Ipv4Addr, SocketAddrV4},
        time::Duration,
    };

    async fn client(addr: SocketAddrV4) -> io::Result<()> {
        let rdma = RdmaBuilder::default().connect(addr).await?;
        let mut rdma = rdma.set_new_pd()?;
        let layout = Layout::new::<char>();
        // then the `Rdma`s created by `new_connect` will have a new `ProtectionDomain`
        let new_rdma = rdma.new_connect(addr).await?;

        let mr_1 = rdma.alloc_local_mr(layout)?;
        let mr_2 = new_rdma.alloc_local_mr(layout)?;
        assert_ne!(
            &mr_1.read_inner().pd().inner_pd,
            &mr_2.read_inner().pd().inner_pd
        );

        // SAFETY: test without memory access
        let mr_3 = unsafe { rdma.alloc_local_mr_uninit(layout)? };
        // SAFETY: test without memory access
        let mr_4 = unsafe { new_rdma.alloc_local_mr_uninit(layout)? };
        assert_ne!(
            &mr_3.read_inner().pd().inner_pd,
            &mr_4.read_inner().pd().inner_pd
        );

        let access = AccessFlag::LocalWrite | AccessFlag::RemoteRead | AccessFlag::RemoteWrite;
        let mr_5 = rdma.alloc_local_mr_with_access(layout, access)?;
        let mr_6 = new_rdma.alloc_local_mr_with_access(layout, access)?;
        assert_ne!(
            &mr_5.read_inner().pd().inner_pd,
            &mr_6.read_inner().pd().inner_pd
        );

        // SAFETY: test without memory access
        let mr_7 = unsafe { rdma.alloc_local_mr_uninit_with_access(layout, access)? };
        // SAFETY: test without memory access
        let mr_8 = unsafe { new_rdma.alloc_local_mr_uninit_with_access(layout, access)? };
        assert_ne!(
            &mr_7.read_inner().pd().inner_pd,
            &mr_8.read_inner().pd().inner_pd
        );

        Ok(())
    }

    #[tokio::main]
    async fn server(addr: SocketAddrV4) -> io::Result<()> {
        let mut rdma = RdmaBuilder::default().listen(addr).await?;
        let _new_rdma = rdma.listen().await?;
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn main() {
        let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), pick_unused_port().unwrap());
        let server_handle = std::thread::spawn(move || server(addr));
        tokio::time::sleep(Duration::from_secs(3)).await;
        client(addr)
            .await
            .map_err(|err| println!("{}", err))
            .unwrap();
        server_handle.join().unwrap().unwrap();
    }
}
