use crate::{
    memory_region::{local::LocalMr, RawMemoryRegion},
    protection_domain::ProtectionDomain,
    LocalMrReadAccess,
};
use libc::{c_void, size_t};
use num_traits::ToPrimitive;
use rdma_sys::ibv_access_flags;
use std::mem;
use std::sync::Mutex;
use std::{alloc::Layout, io, ptr, sync::Arc};
use tikv_jemalloc_sys::{self, extent_hooks_t, MALLOCX_ALIGN, MALLOCX_ARENA};
use tracing::error;

/// Get default extent hooks from arena0
static DEFAULT_ARENA_INDEX: u32 = 0;

/// Get mr from this arena
static mut MR_ARENA_INDEX: u32 = 0;

/// Used by custom hooks to register memory region
static mut DEFAULT_PD: Option<Arc<ProtectionDomain>> = None;

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
    merge: None,
};

lazy_static! {
    /// Default extent hooks of jemalloc
    static ref ORIGIN_HOOKS: extent_hooks_t = get_default_hooks_impl(DEFAULT_ARENA_INDEX).unwrap();
    /// The correspondence between extent metadata and `raw_mr`
    static ref EXTENT_TOKEN_MAP: Arc<Mutex<Vec<Item>>> = Arc::new(Mutex::new(Vec::<Item>::new()));
}

/// Combination between extent metadata and `raw_mr`
#[derive(Debug)]
pub(crate) struct Item {
    addr: usize,
    len: usize,
    raw_mr: Arc<RawMemoryRegion>,
}

/// Memory region allocator
#[derive(Debug)]
pub(crate) struct MrAllocator {
    /// Protection domain that holds the allocator
    _pd: Arc<ProtectionDomain>,
}

impl MrAllocator {
    /// Create a new MR allocator
    pub(crate) fn new(pd: Arc<ProtectionDomain>) -> Self {
        get_je_stats();
        init_je_statics(pd.clone()).unwrap();
        Self { _pd: pd }
    }

    /// Allocate a MR according to the `layout`
    pub(crate) fn alloc(self: &Arc<Self>, layout: &Layout) -> io::Result<LocalMr> {
        let addr = alloc_from_je(layout) as usize;
        let raw_mr = lookup_raw_mr(addr).unwrap();
        Ok(LocalMr::new(addr, layout.size(), raw_mr))
    }
}

/// Look up `raw_mr` info by addr
/// TODO: Need to optimize
fn lookup_raw_mr(addr: usize) -> Option<Arc<RawMemoryRegion>> {
    for item in EXTENT_TOKEN_MAP.lock().unwrap().iter() {
        if addr >= item.addr && addr < item.addr + item.len {
            println!("LOOK addr {}, item {:?}", addr, item);
            return Some(item.raw_mr.clone());
        }
    }
    error!("can not find raw mr by addr {}", addr);
    None
}

/// Alloc memory for RDMA operations from jemalloc
fn alloc_from_je(layout: &Layout) -> *mut u8 {
    let addr = unsafe {
        tikv_jemalloc_sys::mallocx(
            layout.size(),
            (MALLOCX_ALIGN(layout.align()) | MALLOCX_ARENA(MR_ARENA_INDEX.to_usize().unwrap()))
                .to_i32()
                .unwrap(),
        )
    };
    assert_ne!(addr, ptr::null_mut());
    addr as *mut u8
}

/// Get default extent hooks of jemalloc
#[allow(trivial_casts)] // `cast() doesn't work here
fn get_default_hooks_impl(arena_ind: u32) -> io::Result<extent_hooks_t> {
    // read default alloc impl
    let mut hooks: *mut extent_hooks_t = ptr::null_mut();
    let key = format!("arena.{}.extent_hooks\0", arena_ind);
    mem::forget(hooks);
    let mut hooks_len = mem::size_of_val(&hooks);
    let errno = unsafe {
        tikv_jemalloc_sys::mallctl(
            key.as_ptr() as *const _,
            &mut hooks as *mut _ as *mut c_void,
            &mut hooks_len,
            ptr::null_mut(),
            0,
        )
    };

    let hooksd = unsafe { *hooks };
    if errno != 0_i32 {
        return Err(io::Error::from_raw_os_error(errno));
    }

    Ok(hooksd)
}

/// Set custom extent hooks
#[allow(trivial_casts)] // `cast() doesn't work here
fn set_extent_hooks(arena_ind: u32) -> io::Result<()> {
    let key = format!("arena.{}.extent_hooks\0", arena_ind);
    let hooks_len: size_t = unsafe { mem::size_of_val(&&RDMA_EXTENT_HOOKS) };
    let errno = unsafe {
        tikv_jemalloc_sys::mallctl(
            key.as_ptr() as *const _,
            ptr::null_mut(),
            ptr::null_mut(),
            &mut &mut RDMA_EXTENT_HOOKS as *mut _ as *mut c_void,
            hooks_len,
        )
    };
    println!("arena<{}> set hooks success", arena_ind);
    if errno != 0_i32 {
        error!("set extent hooks failed");
        return Err(io::Error::from_raw_os_error(errno));
    }
    Ok(())
}

/// Create an arena to manage `MR`s memory
#[allow(trivial_casts)] // `cast() doesn't work here
fn create_arena() -> io::Result<u32> {
    let key = "arenas.create\0";
    let mut aid = 0_u32;
    let mut aid_len: size_t = mem::size_of_val(&aid);
    let errno = unsafe {
        tikv_jemalloc_sys::mallctl(
            key.as_ptr() as *const _,
            &mut aid as *mut _ as *mut c_void,
            &mut aid_len,
            ptr::null_mut(),
            0,
        )
    };
    if errno != 0_i32 {
        error!("set extent hooks failed");
        return Err(io::Error::from_raw_os_error(errno));
    }
    println!("create arena success aid : {}", aid);
    Ok(aid)
}

/// Get stats of je
/// TODO: Need to optimize
#[allow(trivial_casts)] // `cast() doesn't work here
fn get_je_stats() {
    let mut allocated: usize = 0;
    let mut val_len = mem::size_of_val(&allocated);
    let field = "stats.allocated\0";
    let errno = unsafe {
        tikv_jemalloc_sys::mallctl(
            field.as_ptr() as *const _,
            &mut allocated as *mut _ as *mut c_void,
            &mut val_len,
            ptr::null_mut(),
            0,
        )
    };
    if errno != 0_i32 {
        error!("can not get je");
    }
    println!("allocated {}", allocated);
}

/// Create arena and init statics
fn init_je_statics(pd: Arc<ProtectionDomain>) -> io::Result<()> {
    unsafe { DEFAULT_PD = Some(pd) }
    let ind = create_arena()?;
    unsafe { MR_ARENA_INDEX = ind };
    set_extent_hooks(ind)?;
    Ok(())
}

unsafe extern "C" fn extent_alloc_hook(
    extent_hooks: *mut extent_hooks_t,
    new_addr: *mut c_void,
    size: usize,
    alignment: usize,
    zero: *mut i32,
    commit: *mut i32,
    arena_ind: u32,
) -> *mut c_void {
    let origin_alloc = (*ORIGIN_HOOKS).alloc.unwrap();
    let addr = origin_alloc(
        extent_hooks,
        new_addr,
        size,
        alignment,
        zero,
        commit,
        arena_ind,
    );
    assert_ne!(addr, ptr::null_mut());
    let access = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
        | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
        | ibv_access_flags::IBV_ACCESS_REMOTE_READ
        | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;
    let raw_mr = Arc::new(
        RawMemoryRegion::register_from_pd(
            &DEFAULT_PD.clone().unwrap(),
            addr as *mut u8,
            size,
            access,
        )
        .unwrap(),
    );
    let item = Item {
        addr: addr as usize,
        len: size,
        raw_mr,
    };
    println!("ALLOC item {:?} lkey {}", &item, item.raw_mr.lkey());
    EXTENT_TOKEN_MAP.as_ref().lock().unwrap().push(item);
    addr
}

unsafe extern "C" fn extent_dalloc_hook(
    extent_hooks: *mut extent_hooks_t,
    addr: *mut c_void,
    size: usize,
    committed: i32,
    arena_ind: u32,
) -> i32 {
    println!("DALLOC addr {}, size{}", addr as usize, size);
    // todo!("remove item from EXTENT_TOKEN_MAP");
    let origin_dalloc = (*ORIGIN_HOOKS).dalloc.unwrap();
    origin_dalloc(extent_hooks, addr, size, committed, arena_ind)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{context::Context, RdmaBuilder};
    use std::{alloc::Layout, io, thread};
    use tikv_jemalloc_sys::MALLOCX_ALIGN;

    #[tokio::test]
    async fn alloc_mr_from_rdma() -> io::Result<()> {
        let rdma = RdmaBuilder::default()
            .set_port_num(1)
            .set_gid_index(1)
            .build()?;
        let mut mrs = vec![];
        //Layout::new::<[u8; 4096]>()
        let layout = Layout::new::<char>();
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
        println!("lmr info :{:?}", &lmr);
        Ok(())
    }

    #[test]
    fn je_malloxc_test() {
        let ctx = Arc::new(Context::open(None, 1, 1).unwrap());
        let pd = Arc::new(ctx.create_protection_domain().unwrap());
        init_je_statics(pd.clone()).unwrap();
        let thread = thread::spawn(move || {
            let layout = Layout::new::<char>();
            let addr = unsafe {
                tikv_jemalloc_sys::mallocx(
                    layout.size(),
                    MALLOCX_ALIGN(layout.align())
                        | MALLOCX_ARENA(MR_ARENA_INDEX.to_usize().unwrap()),
                )
            };
            assert_ne!(addr, ptr::null_mut());
            unsafe {
                *(addr as *mut char) = 'c';
                assert_eq!(*(addr as *mut char), 'c');
                println!("addr : {}, char : {}", addr as usize, *(addr as *mut char));
            }
        });
        let layout = Layout::new::<char>();
        let addr = unsafe {
            tikv_jemalloc_sys::mallocx(
                layout.size(),
                MALLOCX_ALIGN(layout.align()) | MALLOCX_ARENA(MR_ARENA_INDEX.to_usize().unwrap()),
            )
        };
        assert_ne!(addr, ptr::null_mut());
        unsafe {
            *(addr as *mut char) = 'c';
            assert_eq!(*(addr as *mut char), 'c');
            println!("addr : {}, char : {}", addr as usize, *(addr as *mut char));
        }
        thread.join().unwrap();
    }
}
