use crate::{
    memory_region::{local::LocalMr, MrAccess, RawMemoryRegion},
    protection_domain::ProtectionDomain,
};
use libc::{c_void, size_t};
use num_traits::ToPrimitive;
use rdma_sys::ibv_access_flags;
use std::mem;
use std::{alloc::Layout, io, ptr, sync::Arc};
use tikv_jemalloc_sys::{self, extent_hooks_t, MALLOCX_ALIGN, MALLOCX_ARENA};
use tracing::error;

/// Get default extent hooks from arena0
static DEFAULT_ARENA_INDEX: u32 = 0;

/// Get mr from this arena
static mut MR_ARENA_INDEX: u32 = 0;

/// Custom extent alloc hook used by jemalloc
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
static RDMA_DALLOC_EXTENT_HOOK: unsafe extern "C" fn(
    extent_hooks: *mut extent_hooks_t,
    addr: *mut c_void,
    size: usize,
    committed: i32,
    arena_ind: u32,
) -> i32 = extent_dalloc_hook;

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
    static ref ORIGIN_HOOKS: extent_hooks_t = get_default_hooks_impl(DEFAULT_ARENA_INDEX).unwrap();
}
// static MALLCTL_ARENAS_ALL:c_int	= 4096;

/// Memory region allocator
#[derive(Debug)]
pub(crate) struct MrAllocator {
    /// Protection domain that holds the allocator
    pd: Arc<ProtectionDomain>,
}

impl MrAllocator {
    /// Create a new MR allocator
    pub(crate) fn new(pd: Arc<ProtectionDomain>) -> Self {
        get_je();
        let ind = create_arena().unwrap();
        set_extent_hooks(ind).unwrap();
        unsafe { MR_ARENA_INDEX = ind };
        let layout = Layout::new::<char>();
        let _ttt = unsafe {
            tikv_jemalloc_sys::mallocx(
                layout.size(),
                MALLOCX_ARENA(MR_ARENA_INDEX.to_usize().unwrap()),
            )
        };
        println!("alloc ttt success");
        Self { pd }
    }

    /// Allocate a MR according to the `layout`
    pub(crate) fn alloc(self: &Arc<Self>, layout: &Layout) -> io::Result<LocalMr> {
        unsafe { println!("alloc lmr from arena {} {:?}", MR_ARENA_INDEX, layout) };
        let addr = unsafe {
            tikv_jemalloc_sys::mallocx(
                layout.size(),
                (MALLOCX_ALIGN(layout.align()) | MALLOCX_ARENA(MR_ARENA_INDEX.to_usize().unwrap()))
                    .to_i32()
                    .unwrap(),
            )
        };
        assert_ne!(addr, ptr::null_mut());
        let access = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
            | ibv_access_flags::IBV_ACCESS_REMOTE_READ
            | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC;
        let raw_mr = Arc::new(RawMemoryRegion::register_from_pd(
            &self.pd,
            addr as *mut u8,
            layout.size(),
            access,
        )?);
        Ok(LocalMr::new(raw_mr.addr(), raw_mr.length(), raw_mr))
    }
}

#[allow(trivial_casts)]
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
    println!("get original hooks  {:?}", hooksd.alloc);
    if errno != 0_i32 {
        return Err(io::Error::from_raw_os_error(errno));
    }

    Ok(hooksd)
}

#[allow(trivial_casts)]
fn set_extent_hooks(arena_ind: u32) -> io::Result<()> {
    let key = format!("arena.{}.extent_hooks\0", arena_ind);
    // let mut hooks = extent_hooks_t {
    //     alloc: Some(RDMA_ALLOC_EXTENT_HOOK),
    //     dalloc: Some(RDMA_DALLOC_EXTENT_HOOK),
    //     destroy: None,
    //     commit: None,
    //     decommit: None,
    //     purge_lazy: None,
    //     purge_forced: None,
    //     split: None,
    //     merge: None,
    // };
    // mem::forget(hooks);
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

#[allow(trivial_casts)]
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

#[allow(trivial_casts)]
fn get_je() {
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

unsafe extern "C" fn extent_alloc_hook(
    extent_hooks: *mut extent_hooks_t,
    new_addr: *mut c_void,
    size: usize,
    alignment: usize,
    zero: *mut i32,
    commit: *mut i32,
    arena_ind: u32,
) -> *mut c_void {
    println!("my extent alloc hook {:?}", (*extent_hooks).alloc);
    let origin_alloc = (*ORIGIN_HOOKS).alloc.unwrap();
    origin_alloc(
        extent_hooks,
        new_addr,
        size,
        alignment,
        zero,
        commit,
        arena_ind,
    )
}

unsafe extern "C" fn extent_dalloc_hook(
    extent_hooks: *mut extent_hooks_t,
    addr: *mut c_void,
    size: usize,
    committed: i32,
    arena_ind: u32,
) -> i32 {
    println!("my extent dalloc hook");
    let origin_dalloc = (*ORIGIN_HOOKS).dalloc.unwrap();
    origin_dalloc(extent_hooks, addr, size, committed, arena_ind)
}

#[cfg(test)]
mod tests {
    use tikv_jemalloc_sys::MALLOCX_ALIGN;

    use super::*;
    use crate::RdmaBuilder;
    use std::{alloc::Layout, io, thread};

    #[tokio::test]
    async fn test1() -> io::Result<()> {
        let rdma = RdmaBuilder::default()
            .set_port_num(1)
            .set_gid_index(1)
            .build()?;
        let mut mrs = vec![];
        for _ in 0_i32..2_i32 {
            let mr = rdma.alloc_local_mr(Layout::new::<[u8; 4096]>())?;
            mrs.push(mr);
        }
        Ok(())
    }

    #[test]
    fn je() {
        get_je();
        let ind = create_arena().unwrap();
        unsafe { MR_ARENA_INDEX = ind };
        set_extent_hooks(ind).unwrap();
        
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
                println!("char : {}", *(addr as *mut char));
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
            println!("char : {}", *(addr as *mut char));
        }
        thread.join().unwrap();
    }
}
