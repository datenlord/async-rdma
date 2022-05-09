use crate::{
    lock_utilities::MappedMutex,
    memory_region::{
        local::{LocalMr, LocalMrInner},
        RawMemoryRegion,
    },
    protection_domain::ProtectionDomain,
};
use clippy_utilities::Cast;
use libc::{c_void, size_t};
use parking_lot::{Mutex, MutexGuard};
use rdma_sys::ibv_access_flags;
use std::collections::{BTreeMap, HashMap};
use std::mem;
use std::ops::Bound::Included;
use std::{alloc::Layout, io, ptr, sync::Arc};
use tikv_jemalloc_sys::{self, extent_hooks_t, MALLOCX_ALIGN, MALLOCX_ARENA};
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

lazy_static! {
    /// Default extent hooks of jemalloc
    static ref ORIGIN_HOOKS: extent_hooks_t = get_default_hooks_impl(DEFAULT_ARENA_INDEX);
    /// The correspondence between extent metadata and `raw_mr`
    #[derive(Debug)]
    pub(crate) static ref EXTENT_TOKEN_MAP: Arc<Mutex<BTreeMap<usize, Item>>> = Arc::new(Mutex::new(BTreeMap::<usize, Item>::new()));
    /// The correspondence between `arena_ind` and `ProtectionDomain`
    pub(crate) static ref ARENA_PD_MAP: Arc<Mutex<HashMap<u32, Arc<ProtectionDomain>>>> = Arc::new(Mutex::new(HashMap::new()));
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

/// Memory region allocator
#[derive(Debug)]
pub(crate) struct MrAllocator {
    /// Protection domain that holds the allocator
    _pd: Arc<ProtectionDomain>,
    /// Arena index
    arena_ind: u32,
}

impl MrAllocator {
    /// Create a new MR allocator
    pub(crate) fn new(pd: Arc<ProtectionDomain>) -> Self {
        #[allow(clippy::expect_used)]
        let arena_ind =
            init_je_statics(Arc::<ProtectionDomain>::clone(&pd)).expect("init je statics failed");
        Self { _pd: pd, arena_ind }
    }

    /// Allocate a `LocalMr` according to the `layout`
    #[allow(clippy::as_conversions)]
    pub(crate) fn alloc(self: &Arc<Self>, layout: &Layout) -> io::Result<LocalMr> {
        let inner = self.alloc_inner(layout)?;
        Ok(LocalMr::new(inner))
    }

    /// Allocate a `LocalMrInner` according to the `layout`
    #[allow(clippy::as_conversions)]
    pub(crate) fn alloc_inner(self: &Arc<Self>, layout: &Layout) -> io::Result<LocalMrInner> {
        self.alloc_from_je(layout).map_or_else(||{
                Err(io::Error::new(io::ErrorKind::OutOfMemory, "insufficient contiguous memory was available to service the allocation request"))
            }, |addr|{
                let raw_mr = self.lookup_raw_mr(addr as usize);
                Ok(LocalMrInner::new(addr as usize, layout.size(), raw_mr))
            })
    }

    /// Alloc memory for RDMA operations from jemalloc
    fn alloc_from_je(&self, layout: &Layout) -> Option<*mut u8> {
        let addr = unsafe {
            tikv_jemalloc_sys::mallocx(
                layout.size(),
                (MALLOCX_ALIGN(layout.align()) | MALLOCX_ARENA(self.arena_ind.cast())).cast(),
            )
        };
        if addr.is_null() {
            None
        } else {
            Some(addr.cast::<u8>())
        }
    }

    /// Look up `raw_mr` info by addr
    #[allow(clippy::unreachable)]
    fn lookup_raw_mr(&self, addr: usize) -> Arc<RawMemoryRegion> {
        lookup_raw_mr(self.arena_ind, addr).map_or_else(
            || {
                unreachable!("can not find raw mr by addr {}", addr);
            },
            |raw_mr| raw_mr,
        )
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
    unsafe { *hooks }
}

/// Set custom extent hooks
fn set_extent_hooks(arena_ind: u32) -> io::Result<()> {
    let key = format!("arena.{}.extent_hooks\0", arena_ind);
    let mut hooks_ptr: *mut extent_hooks_t = unsafe { &mut RDMA_EXTENT_HOOKS };
    let hooks_ptr_ptr: *mut *mut extent_hooks_t = &mut hooks_ptr;
    let hooks_len: size_t = mem::size_of_val(&hooks_ptr_ptr);
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
fn init_je_statics(pd: Arc<ProtectionDomain>) -> io::Result<u32> {
    let ind = create_arena()?;
    if ARENA_PD_MAP.lock().insert(ind, pd).is_some() {
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
    register_extent_mr_default(addr, size, arena_ind).map_or_else(
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
        register_extent_mr_default(addr_a, size_a.wrapping_add(size_b), arena_ind).map_or_else(
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
    map.remove(&(addr as _)).map_or_else(
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

/// Register extent memory region with default access flags
pub(crate) fn register_extent_mr_default(
    addr: *mut c_void,
    size: usize,
    arena_ind: u32,
) -> Option<Item> {
    let access = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
        | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
        | ibv_access_flags::IBV_ACCESS_REMOTE_READ
        | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;
    register_extent_mr(addr, size, arena_ind, access)
}

/// Register extent memory region
#[allow(clippy::as_conversions)]
pub(crate) fn register_extent_mr(
    addr: *mut c_void,
    size: usize,
    arena_ind: u32,
    access: ibv_access_flags,
) -> Option<Item> {
    assert_ne!(addr, ptr::null_mut());
    debug!(
        "reg_mr addr {}, size {}, arena_ind {}, access {:?}",
        addr as usize, size, arena_ind, access
    );
    ARENA_PD_MAP.lock().get(&arena_ind).map_or_else(
        || {
            error!("can not get pd from ARENA_PD_MAP");
            None
        },
        |pd| {
            RawMemoryRegion::register_from_pd(pd, addr.cast::<u8>(), size, access).map_or_else(
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
    use crate::{context::Context, MrAccess, RdmaBuilder};
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
        let allocator = Arc::new(MrAllocator::new(pd));
        let layout = Layout::new::<char>();
        let lmr = allocator.alloc(&layout)?;
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
        unsafe {
            assert_eq!(hooks.alloc, RDMA_EXTENT_HOOKS.alloc);
            assert_eq!(hooks.dalloc, RDMA_EXTENT_HOOKS.dalloc);
            assert_eq!(hooks.merge, RDMA_EXTENT_HOOKS.merge);
        }
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_extent_hooks() -> io::Result<()> {
        tracing_subscriber::fmt::init();
        let ctx = Arc::new(Context::open(None, 1, 1)?);
        let pd = Arc::new(ctx.create_protection_domain()?);
        let allocator = Arc::new(MrAllocator::new(pd));

        check_hooks(allocator.arena_ind);
        // set test hooks
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
        set_extent_hooks(allocator.arena_ind).unwrap();
        // check custom test hooks
        check_hooks(allocator.arena_ind);

        // some tests to make it possible to observe the opreations of jemalloc's
        // memory management operatios such as alloc, dalloc, merge.
        let mut layout = Layout::new::<char>();
        // alloc and drop one by one
        for _ in 0_u32..100_u32 {
            let _lmr = allocator.alloc(&layout)?;
        }
        // alloc all and drop all
        layout = Layout::new::<[u8; 16 * 1024]>();
        let mut lmrs = vec![];
        for _ in 0_u32..100_u32 {
            lmrs.push(allocator.alloc(&layout)?);
        }
        // jemalloc will merge extents after drop all lmr in lmrs.
        lmrs.clear();
        // alloc big extent and dalloc it immediately after _lmr's dropping
        layout = Layout::new::<[u8; 1024 * 1024 * 32]>();
        let _lmr = allocator.alloc(&layout)?;
        Ok(())
    }

    #[test]
    #[allow(clippy::as_conversions)]
    #[allow(clippy::unwrap_used)]
    fn je_malloxc_test() {
        let ctx = Arc::new(Context::open(None, 1, 1).unwrap());
        let pd = Arc::new(ctx.create_protection_domain().unwrap());
        let ind = init_je_statics(pd).unwrap();
        let thread = thread::spawn(move || {
            let layout = Layout::new::<char>();
            let addr = unsafe {
                tikv_jemalloc_sys::mallocx(
                    layout.size(),
                    MALLOCX_ALIGN(layout.align()) | MALLOCX_ARENA(ind.cast()),
                )
            };
            assert_ne!(addr, ptr::null_mut());
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
        let addr = unsafe {
            tikv_jemalloc_sys::mallocx(
                layout.size(),
                MALLOCX_ALIGN(layout.align()) | MALLOCX_ARENA(ind.cast()),
            )
        };
        assert_ne!(addr, ptr::null_mut());
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
}
