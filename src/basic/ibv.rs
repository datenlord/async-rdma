// The network byte order is defined to always be big-endian.
// X86 is little-endian.

use nix::sys::epoll;
use rdma_sys::{
    ___ibv_query_port, ibv_access_flags, ibv_ack_async_event, ibv_ack_cq_events, ibv_alloc_pd,
    ibv_async_event, ibv_close_device, ibv_comp_channel, ibv_context, ibv_cq,
    ibv_create_comp_channel, ibv_create_cq, ibv_create_qp, ibv_dealloc_pd, ibv_dereg_mr,
    ibv_destroy_comp_channel, ibv_destroy_cq, ibv_destroy_qp, ibv_free_device_list,
    ibv_get_async_event, ibv_get_cq_event, ibv_get_device_list, ibv_get_device_name, ibv_gid,
    ibv_modify_qp, ibv_mr, ibv_mtu, ibv_open_device, ibv_pd, ibv_poll_cq, ibv_port_attr,
    ibv_post_recv, ibv_post_send, ibv_qp, ibv_qp_attr, ibv_qp_attr_mask, ibv_qp_init_attr,
    ibv_qp_state, ibv_qp_type, ibv_query_gid, ibv_query_qp, ibv_recv_wr, ibv_reg_mr,
    ibv_req_notify_cq, ibv_send_flags, ibv_send_wr, ibv_sge, ibv_wc, ibv_wc_status,
    ibv_wc_status_str, ibv_wr_opcode,
};
// use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::ffi::{CStr, CString};
use std::os::raw::{c_int, c_uint, c_void};
use std::pin::Pin;
use std::thread::{self, JoinHandle};
use utilities::{Cast, OverflowArithmetic};

use super::socket::Udp;
use super::util;

///
const MSG: &str = "SEND operation";
///
const RDMAMSGR: &str = "RDMA read operation";
///
const RDMAMSGW: &str = "RDMA write operation";
///
const MSG_SIZE: usize = 720;
///
const MAX_POLL_CQ_TIMEOUT: i64 = 2000;
///
const INVALID_SIZE: isize = -1;

/// The data needed to connect QP
#[derive(Deserialize, Serialize)]
struct CmConData {
    /// Buffer address
    addr: u64,
    /// Remote key
    rkey: u32,
    /// QP number
    qp_num: u32,
    /// LID of the IB port
    lid: u16,
    /// gid
    gid: u128,
}

impl CmConData {
    ///
    const fn into_be(self) -> Self {
        Self {
            addr: u64::to_be(self.addr),
            rkey: u32::to_be(self.rkey),
            qp_num: u32::to_be(self.qp_num),
            lid: u16::to_be(self.lid),
            gid: u128::to_be(self.gid),
        }
    }
    ///
    const fn into_le(self) -> Self {
        Self {
            addr: u64::from_be(self.addr),
            rkey: u32::from_be(self.rkey),
            qp_num: u32::from_be(self.qp_num),
            lid: u16::from_be(self.lid),
            gid: u128::from_be(self.gid),
        }
    }
}

impl std::fmt::Display for CmConData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        //unsafe {
        write!(
            f,
            "addr={:x}, rkey={:x}, qp_num={:x}, lid={:x}, gid={:x}",
            self.addr, self.rkey, self.qp_num, self.lid, self.gid,
        )
        //}
    }
}

///
#[derive(Debug, Deserialize, Serialize)]
enum State {
    ///
    ReceiveReady,
    ///
    SendSize(isize),
    ///
    ReadSize(isize),
    ///
    WriteSize(isize),
    ///
    WriteImm,
    ///
    WriteDone,
    ///
    AtomicReady,
    ///
    AtomicDone,
}

macro_rules! impl_send_sync {
    ($t: ident) => {
        unsafe impl Send for $t {}
        unsafe impl Sync for $t {}
    };
}

/// ibv context wrapper structure
#[derive(Clone, Copy)]
pub struct IbvCtx {
    /// inner ibv_context pointer
    pub inner: *mut ibv_context,
}

/// ibv event channel wrapper structure
#[derive(Clone, Copy)]
pub struct IbvEventChannel {
    /// inner ibv event channel pointer
    pub inner: *mut ibv_comp_channel,
}

/// ibv complete queue wrapper structure
#[derive(Clone, Copy)]
pub struct IbvCq {
    /// inner ibv_cq pointer
    pub inner: *mut ibv_cq,
}

/// ibv queue pair wrapper structure
#[derive(Clone, Copy)]
pub struct IbvQp {
    /// inner ibv_qp pointer
    pub inner: *mut ibv_qp,
}

/// ibv protect domain wrapper structure
#[derive(Clone, Copy)]
pub struct IbvPd {
    /// inner ibv_qp pointer
    pub inner: *mut ibv_pd,
}

/// ibv memory region wrapper structure
#[derive(Clone, Copy)]
pub struct IbvMr {
    /// inner ibv_qp pointer
    pub inner: *mut ibv_mr,
}

impl_send_sync!(IbvCtx);
impl_send_sync!(IbvEventChannel);
impl_send_sync!(IbvCq);
impl_send_sync!(IbvQp);
impl_send_sync!(IbvPd);

/// RDMA resources
pub struct Resources {
    ///
    remote_props: CmConData,
    /// Device handle
    ib_ctx: IbvCtx,
    /// Event channel
    event_channel: IbvEventChannel,
    /// PD handle
    pd: IbvPd,
    /// CQ handle
    cq: IbvCq,
    /// QP handle
    qp: IbvQp,
    /// MR handle for buf
    mr: IbvMr,
    /// memory buffer pointer, used for RDMA and send ops
    buf: Pin<Vec<u8>>,
    /// The socket to the remote peer of QP
    sock: Udp,
}

impl Drop for Resources {
    fn drop(&mut self) {
        let mut rc: c_int;
        rc = unsafe { ibv_destroy_qp(self.qp.inner) };
        debug_assert_eq!(rc, 0, "failed to destroy QP");
        rc = unsafe { ibv_dereg_mr(self.mr.inner) };
        debug_assert_eq!(rc, 0, "failed to deregister MR");

        rc = unsafe { ibv_destroy_cq(self.cq.inner) };
        debug_assert_eq!(rc, 0, "failed to destroy CQ");
        rc = unsafe { ibv_dealloc_pd(self.pd.inner) };
        debug_assert_eq!(rc, 0, "failed to deallocate PD");
        rc = unsafe { ibv_destroy_comp_channel(self.event_channel.inner) };
        debug_assert_eq!(rc, 0, "failed to destroy event completion channel");
        rc = unsafe { ibv_close_device(self.ib_ctx.inner) };
        debug_assert_eq!(rc, 0, "failed to close device context");
    }
}

impl Resources {
    ///
    pub fn buf_slice(&self) -> &[u8] {
        (*self.buf).as_ref()
    }

    ///
    pub fn buf_slice_mut(&mut self) -> &mut [u8] {
        (*self.buf).as_mut()
    }

    ///
    pub fn async_poll_completion(&self) -> JoinHandle<c_int> {
        async_poll_completion(self.cq, self.event_channel)
    }

    ///
    pub fn poll_async_event_non_blocking(&self) -> JoinHandle<c_int> {
        poll_async_event_non_blocking(self.ib_ctx)
    }

    ///
    pub fn poll_completion(&self) -> c_int {
        let (status, _) = poll_completion(self.cq);
        status
    }

    ///
    pub fn post_write_imm(&self) -> c_int {
        remote_write_with_imm(
            self.remote_props.addr,
            self.remote_props.rkey,
            0x1234,
            self.qp,
            self.cq,
        )
    }

    ///
    pub fn post_send(&self, opcode: c_uint, len: u32) -> c_int {
        match opcode {
            ibv_wr_opcode::IBV_WR_RDMA_READ => remote_read(
                util::ptr_to_usize(self.buf.as_ptr()).cast(),
                len,
                unsafe { (*self.mr.inner).lkey },
                self.remote_props.addr,
                self.remote_props.rkey,
                self.qp,
                self.cq,
            ),
            ibv_wr_opcode::IBV_WR_RDMA_WRITE => remote_write(
                util::ptr_to_usize(self.buf.as_ptr()).cast(),
                len,
                unsafe { (*self.mr.inner).lkey },
                self.remote_props.addr,
                self.remote_props.rkey,
                self.qp,
                self.cq,
            ),
            ibv_wr_opcode::IBV_WR_RDMA_WRITE_WITH_IMM => remote_write_with_imm(
                self.remote_props.addr,
                self.remote_props.rkey,
                0x1234,
                self.qp,
                self.cq,
            ),
            ibv_wr_opcode::IBV_WR_SEND_WITH_IMM => remote_send_with_imm(
                self.remote_props.addr,
                self.remote_props.rkey,
                0x1234,
                self.qp,
                self.cq,
            ),
            ibv_wr_opcode::IBV_WR_SEND => remote_send(
                util::ptr_to_usize(self.buf.as_ptr()).cast(),
                len,
                unsafe { (*self.mr.inner).lkey },
                self.qp,
                self.cq,
            ),
            ibv_wr_opcode::IBV_WR_ATOMIC_CMP_AND_SWP => remote_atomic_cas(
                util::ptr_to_usize(self.buf.as_ptr()).cast(),
                8,
                unsafe { (*self.mr.inner).lkey },
                self.remote_props.addr,
                self.remote_props.rkey,
                0,
                1,
                self.qp,
                self.cq,
            ),
            _ => -1,
        }
    }

    ///
    pub fn post_receive(&self) -> c_int {
        post_receive(
            util::ptr_to_usize(self.buf.as_ptr()).cast(),
            self.buf_slice().len().cast(),
            unsafe { (*self.mr.inner).lkey },
            self.qp,
        )
    }

    ///
    pub fn new(input_dev_name: &str, gid_idx: c_int, ib_port: u8, sock: Udp) -> Self {
        // Searching for IB devices in host
        let (ib_ctx, _) = open_ib_ctx(input_dev_name);

        // Query port properties
        let port_attr = query_port(ib_ctx, ib_port);

        // Get GID
        let my_gid = query_gid(ib_ctx, ib_port, gid_idx);

        // Allocate Protection Domain
        let pd = create_pd(ib_ctx);

        // Allocate Complete Queue
        let (cq, event_channel) = create_cq(ib_ctx, 10);

        // Create Memory Region
        let (mr, buf) = create_mr(
            MSG_SIZE,
            pd,
            ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                | ibv_access_flags::IBV_ACCESS_REMOTE_READ
                | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
                | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC,
        );

        // Create the Queue Pair
        let qp = create_qp(cq, pd);

        // Exchange using UDP sockets info required to connect QPs
        let remote_con_data = exchange_info(&buf, mr, qp, &port_attr, &my_gid, &sock);

        Self {
            remote_props: remote_con_data,
            ib_ctx,
            event_channel,
            pd,
            cq,
            qp,
            mr,
            buf,
            sock,
        }
    }

    ///
    pub fn new_and_connect_qp(
        input_dev_name: &str,
        gid_idx: c_int,
        ib_port: u8,
        sock: Udp,
    ) -> Self {
        let this = Self::new(input_dev_name, gid_idx, ib_port, sock);
        let rc = this.connect_qp(gid_idx, ib_port);
        debug_assert_eq!(rc, 0, "failed to connect QPs");
        this
    }

    ///
    fn connect_qp(&self, gid_idx: c_int, ib_port: u8) -> c_int {
        let mut rc: c_int;
        // modify the QP to init
        rc = modify_qp_to_init(
            ib_port,
            self.qp,
            ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                | ibv_access_flags::IBV_ACCESS_REMOTE_READ
                | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE
                | ibv_access_flags::IBV_ACCESS_REMOTE_ATOMIC,
        );
        if rc != 0 {
            panic!("change QP state to INIT failed");
        }
        // modify the QP to RTR
        rc = modify_qp_to_rtr(
            gid_idx,
            ib_port,
            self.qp,
            self.remote_props.qp_num,
            self.remote_props.lid,
            self.remote_props.gid,
            1024,
            0,
            1,
            0x12,
        );
        if rc != 0 {
            panic!("failed to modify QP state to RTR");
        }
        rc = modify_qp_to_rts(self.qp, 0x12, 6, 0, 0, 1);
        if rc != 0 {
            panic!("failed to modify QP state to RTS");
        }
        println!("QP state was change to RTS");
        rc
    }

    ///
    fn query_qp(&self) {
        query_qp(self.qp);
    }
}

/// Get ib context based on the dev name, if the name is empty get the first one
pub fn open_ib_ctx(dev_name: &str) -> (IbvCtx, String) {
    let mut num_devs: c_int = 0;
    let dev_list_ptr = unsafe { ibv_get_device_list(&mut num_devs) };
    // if there isn't any IB device in host
    debug_assert_ne!(num_devs, 0, "found {} device(s)", num_devs);
    println!("found {} device(s)", num_devs);
    let dev_list = unsafe { std::slice::from_raw_parts(dev_list_ptr, num_devs.cast()) };
    debug_assert!(
        !dev_list.is_empty(),
        "ibv_get_device_list return empty list",
    );

    let dev_name_list = dev_list
        .iter()
        .map(|dev| {
            let dev_name_cstr = unsafe {
                CString::from_raw(libc::strdup(ibv_get_device_name(util::const_ptr_cast_mut(
                    *dev,
                ))))
            };
            println!("available device name: {:?}", dev_name_cstr);
            dev_name_cstr
        })
        .collect::<Vec<_>>();
    // search for the specific device we want to work with
    let (dev_name_cstr, ib_dev) = if dev_name.is_empty() {
        let dev = dev_list.get(0).unwrap_or_else(|| panic!("no device found"));
        let dname = dev_name_list
            .get(0)
            .unwrap_or_else(|| panic!("no device name found"));
        println!(
            "no device name input, select first available device: {:?}",
            dname
        );
        (dname, *dev)
    } else {
        let dev_name_cstr = CString::new(dev_name.as_bytes()).unwrap_or_else(|err| {
            panic!(
                "failed to convert \"{}\" to CString, the error is: {}",
                dev_name, err
            )
        });
        let mut itr = dev_name_list.iter().zip(dev_list).filter(|&(dn, _dev)| {
            println!("filter device by name {:?} == {:?}", dn, dev_name_cstr);
            dn == &dev_name_cstr
        });
        let (dn, d) = itr
            .next()
            .unwrap_or_else(|| panic!("IB device {} wasn't found", dev_name));
        (dn, *d)
    };

    // get device handle
    let ib_ctx = unsafe { ibv_open_device(util::const_ptr_cast_mut(ib_dev)) };
    debug_assert!(
        !util::is_null_mut_ptr(ib_ctx),
        "failed to open device {:?}, the error is: {}",
        dev_name_cstr,
        util::get_last_error(),
    );
    // We are now done with device list, free it
    unsafe { ibv_free_device_list(dev_list_ptr) };
    (
        IbvCtx { inner: ib_ctx },
        dev_name_cstr.to_str().unwrap_or_default().to_owned(),
    )
}

/// Query port attribute based on the port number
pub fn query_port(ib_ctx: IbvCtx, ib_port: u8) -> ibv_port_attr {
    let mut port_attr = unsafe { std::mem::zeroed::<ibv_port_attr>() };
    let rc = unsafe { ___ibv_query_port(ib_ctx.inner, ib_port, &mut port_attr) };
    debug_assert_eq!(rc, 0, "ibv_query_port on port {} failed", ib_port);
    port_attr
}

/// Query gid based on the gid index
pub fn query_gid(ib_ctx: IbvCtx, ib_port: u8, gid_idx: c_int) -> ibv_gid {
    let mut my_gid = unsafe { std::mem::zeroed::<ibv_gid>() };
    if gid_idx >= 0 {
        let rc = unsafe { ibv_query_gid(ib_ctx.inner, ib_port, gid_idx, &mut my_gid) };
        debug_assert_eq!(
            rc, 0,
            "could not get gid for index={}, port={}",
            ib_port, gid_idx,
        );
    }
    my_gid
}

/// Create a new protect domain on a device
pub fn create_pd(ib_ctx: IbvCtx) -> IbvPd {
    let pd = unsafe { ibv_alloc_pd(ib_ctx.inner) };
    if util::is_null_mut_ptr(pd) {
        panic!("ibv_alloc_pd failed");
    }
    IbvPd { inner: pd }
}

/// Create a complete queue on a device
pub fn create_cq(ib_ctx: IbvCtx, cq_size: u32) -> (IbvCq, IbvEventChannel) {
    let cq_context = std::ptr::null_mut::<c_void>();
    let event_channel = unsafe { ibv_create_comp_channel(ib_ctx.inner) };
    if util::is_null_mut_ptr(event_channel) {
        panic!("failed to create event completion channel");
    }

    let comp_vector = 0;
    let cq = unsafe {
        ibv_create_cq(
            ib_ctx.inner,
            cq_size.cast(),
            cq_context,
            event_channel,
            comp_vector,
        )
    };
    if util::is_null_mut_ptr(cq) {
        panic!("failed to create CQ with {} entries", cq_size);
    }
    (
        IbvCq { inner: cq },
        IbvEventChannel {
            inner: event_channel,
        },
    )
}

/// create memory region for `pd` protect domain,
/// the length of the region is `size` and the access flag is `mr_flag`.
pub fn create_mr(size: usize, pd: IbvPd, mr_flag: ibv_access_flags) -> (IbvMr, Pin<Vec<u8>>) {
    // Allocate the memory buffer that will hold the data
    let mut buf = Pin::new(vec![0; size]);

    // Register the memory buffer
    let mr_flags = mr_flag.0;
    let mr = unsafe {
        ibv_reg_mr(
            pd.inner,
            util::mut_ptr_cast(buf.as_mut_ptr()),
            buf.len(),
            mr_flags.cast(),
        )
    };
    if util::is_null_mut_ptr(mr) {
        panic!("ibv_reg_mr failed with mr_flags={}", mr_flags);
    }
    println!(
        "MR was registered with addr={:x}, lkey={:x}, rkey={:x}, flags={}",
        util::ptr_to_usize(buf.as_ptr()),
        unsafe { (*mr).lkey },
        unsafe { (*mr).rkey },
        mr_flags
    );

    (IbvMr { inner: mr }, buf)
}

/// create queue pair under `pd` protect domain, bind `cq` complete queue to the queue pair
pub fn create_qp(cq: IbvCq, pd: IbvPd) -> IbvQp {
    let mut qp_init_attr = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
    qp_init_attr.qp_type = ibv_qp_type::IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 0; // set to 0 to avoid CQE for every SR
    qp_init_attr.send_cq = cq.inner;
    qp_init_attr.recv_cq = cq.inner;
    qp_init_attr.cap.max_send_wr = 10;
    qp_init_attr.cap.max_recv_wr = 10;
    qp_init_attr.cap.max_send_sge = 10;
    qp_init_attr.cap.max_recv_sge = 10;
    let qp = unsafe { ibv_create_qp(pd.inner, &mut qp_init_attr) };
    if util::is_null_mut_ptr(qp) {
        panic!("failed to create QP, errno {}", errno::errno());
    }
    println!("QP was created, QP number={:x}", unsafe { (*qp).qp_num });
    IbvQp { inner: qp }
}

/// Exechange information between two peers
fn exchange_info(
    buf: &Pin<Vec<u8>>,
    mr: IbvMr,
    qp: IbvQp,
    port_attr: &ibv_port_attr,
    gid: &ibv_gid,
    sock: &Udp,
) -> CmConData {
    let mut local_con_data = unsafe { std::mem::zeroed::<CmConData>() };
    local_con_data.addr = util::ptr_to_usize(buf.as_ptr()).cast();
    local_con_data.rkey = unsafe { (*mr.inner).rkey };
    local_con_data.qp_num = unsafe { (*qp.inner).qp_num };
    local_con_data.lid = port_attr.lid;
    local_con_data.gid = u128::from_be_bytes(unsafe { gid.raw });
    println!("local connection data: {}", local_con_data);
    let local_con_data_be = local_con_data.into_be();
    let remote_con_data_be: CmConData = sock.exchange_data(&local_con_data_be);
    let remote_con_data = remote_con_data_be.into_le();
    println!("remote connection data: {}", remote_con_data);
    remote_con_data
}

/// Modify the queue pair to init state
pub fn modify_qp_to_init(ib_port: u8, qp: IbvQp, flag: ibv_access_flags) -> c_int {
    let mut attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };

    attr.pkey_index = 0;
    attr.port_num = ib_port;
    attr.qp_state = ibv_qp_state::IBV_QPS_INIT;
    attr.qp_access_flags = flag.0;

    let flags = ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
        | ibv_qp_attr_mask::IBV_QP_STATE
        | ibv_qp_attr_mask::IBV_QP_PORT
        | ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;

    let rc = unsafe { ibv_modify_qp(qp.inner, &mut attr, flags.0.cast()) };
    if rc != 0 {
        panic!("failed to modify QP state to INIT");
    }
    rc
}

#[allow(clippy::too_many_arguments)]
/// Modify the queue pair to rtr state
pub fn modify_qp_to_rtr(
    gid_idx: c_int,
    ib_port: u8,
    qp: IbvQp,
    remote_qp_num: u32,
    remote_lid: u16,
    remote_global_id: u128,
    mtu: u32,
    start_psn: u32,
    max_dest_rd_atomic: u8,
    min_rnr_timer: u8,
) -> c_int {
    let mut attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
    attr.qp_state = ibv_qp_state::IBV_QPS_RTR;
    attr.path_mtu = match mtu {
        256 => ibv_mtu::IBV_MTU_256,
        512 => ibv_mtu::IBV_MTU_512,
        1024 => ibv_mtu::IBV_MTU_1024,
        2048 => ibv_mtu::IBV_MTU_2048,
        4096 => ibv_mtu::IBV_MTU_4096,
        _ => panic!("{} is not a valid MTU size", mtu),
    };
    attr.dest_qp_num = remote_qp_num;
    attr.rq_psn = start_psn;
    attr.max_dest_rd_atomic = max_dest_rd_atomic;
    attr.min_rnr_timer = min_rnr_timer;
    attr.ah_attr.is_global = 0;
    attr.ah_attr.dlid = remote_lid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = ib_port;
    if gid_idx >= 0 {
        attr.ah_attr.is_global = 1;
        attr.ah_attr.port_num = 1;
        attr.ah_attr.grh.dgid.raw = remote_global_id.to_be_bytes();
        attr.ah_attr.grh.flow_label = 0;
        attr.ah_attr.grh.hop_limit = 1;
        attr.ah_attr.grh.sgid_index = gid_idx.cast();
        attr.ah_attr.grh.traffic_class = 0;
    }
    let flags = ibv_qp_attr_mask::IBV_QP_STATE
        | ibv_qp_attr_mask::IBV_QP_AV
        | ibv_qp_attr_mask::IBV_QP_PATH_MTU
        | ibv_qp_attr_mask::IBV_QP_DEST_QPN
        | ibv_qp_attr_mask::IBV_QP_RQ_PSN
        | ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC
        | ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER;
    let rc = unsafe { ibv_modify_qp(qp.inner, &mut attr, flags.0.cast()) };
    if rc != 0 {
        panic!("failed to modify QP state to RTR, errno is {}", rc);
    }
    rc
}

#[allow(clippy::too_many_arguments)]
/// modify queue pair to rts
pub fn modify_qp_to_rts(
    qp: IbvQp,
    timeout: u8,
    retry_cnt: u8,
    rnr_retry: u8,
    start_psn: u32,
    max_rd_atomic: u8,
) -> c_int {
    let mut attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
    attr.qp_state = ibv_qp_state::IBV_QPS_RTS;
    attr.timeout = timeout;
    attr.retry_cnt = retry_cnt;
    attr.rnr_retry = rnr_retry;
    attr.sq_psn = start_psn;
    attr.max_rd_atomic = max_rd_atomic;
    let flags = ibv_qp_attr_mask::IBV_QP_STATE
        | ibv_qp_attr_mask::IBV_QP_TIMEOUT
        | ibv_qp_attr_mask::IBV_QP_RETRY_CNT
        | ibv_qp_attr_mask::IBV_QP_RNR_RETRY
        | ibv_qp_attr_mask::IBV_QP_SQ_PSN
        | ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC;
    let rc = unsafe { ibv_modify_qp(qp.inner, &mut attr, flags.0.cast()) };
    if rc != 0 {
        panic!("failed to modify QP state to RTS, errno {}", rc);
    }
    rc
}

///
pub fn query_qp(qp: IbvQp) -> (ibv_qp_init_attr, ibv_qp_attr) {
    let mut attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
    let mut init_attr = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };

    let rc = unsafe {
        ibv_query_qp(
            qp.inner,
            &mut attr,
            ibv_qp_attr_mask::IBV_QP_STATE.0.cast(),
            &mut init_attr,
        )
    };
    println!(
        "QP state: {}, cur state: {}",
        attr.qp_state, attr.cur_qp_state
    );
    if rc != 0 {
        panic!("failed to query QP state");
    }

    (init_attr, attr)
}

///
pub fn async_poll_completion(cq: IbvCq, channel: IbvEventChannel) -> JoinHandle<c_int> {
    thread::spawn(move || poll_completion_non_blocking(cq, channel))
}

///
pub fn poll_async_event_non_blocking(ibv_ctx: IbvCtx) -> JoinHandle<c_int> {
    thread::spawn(move || {
        let mut event = unsafe { std::mem::zeroed::<ibv_async_event>() };
        let mut rc: c_int;
        loop {
            rc = unsafe { ibv_get_async_event(ibv_ctx.inner, &mut event) };
            if rc != 0 {
                panic!("Failed to get async event");
            }
            println!("get async event type={}", event.event_type);
            unsafe { ibv_ack_async_event(&mut event) };
        }
    })
}

///
fn poll_completion_non_blocking(cq_addr: IbvCq, channel_addr: IbvEventChannel) -> c_int {
    let mut cq = cq_addr.inner;
    let event_channel = channel_addr.inner;

    let flags = unsafe { libc::fcntl((*event_channel).fd, libc::F_GETFL) };
    let mut rc =
        unsafe { libc::fcntl((*event_channel).fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
    if rc < 0 {
        panic!("Failed to change file descriptor of Completion Event Channel");
    }

    let epoll_fd = epoll::epoll_create()
        .unwrap_or_else(|err| panic!("epoll_create failed, the error is: {}", err));
    let channel_fd = unsafe { (*event_channel).fd };
    let mut epoll_ev = epoll::EpollEvent::new(
        epoll::EpollFlags::EPOLLIN | epoll::EpollFlags::EPOLLET,
        channel_fd.cast(),
    );
    epoll::epoll_ctl(
        epoll_fd,
        epoll::EpollOp::EpollCtlAdd,
        channel_fd,
        &mut epoll_ev,
    )
    .unwrap_or_else(|err| panic!("epoll_ctl failed, the error is: {}", err));

    // let epoll_size = 1;
    // let mut epoll_ev = unsafe { std::mem::zeroed::<libc::epoll_event>() };
    // epoll_ev.u64 = channel_fd.cast();
    // println!("EPOLLIN={:x}, EPOLLET={:x}", libc::EPOLLIN, libc::EPOLLET);
    // epoll_ev.events = (libc::EPOLLIN | libc::EPOLLET).cast();
    // unsafe { libc::epoll_ctl(epoll_fd, libc::EPOLL_CTL_ADD, channel_fd, &mut epoll_ev) };

    println!("start epoll...");
    let timeout_ms = 10;
    let mut event_list = [epoll_ev];
    let mut nfds: usize;
    loop {
        nfds = epoll::epoll_wait(epoll_fd, &mut event_list, timeout_ms)
            .unwrap_or_else(|err| panic!("epoll_wait failed, the error is: {}", err));
        if nfds > 0 {
            println!("end epoll");
            break;
        }
    }
    unsafe { libc::close(epoll_fd) };
    let mut cq_context = std::ptr::null_mut::<c_void>();
    rc = unsafe { ibv_get_cq_event(event_channel, &mut cq, &mut cq_context) };
    if rc != 0 {
        panic!("Failed to get cq_event");
    }
    unsafe { ibv_ack_cq_events(cq, 1) };

    let mut wc = unsafe { std::mem::zeroed::<ibv_wc>() };
    let mut cur_time_msec: i64;
    let mut cur_time = unsafe { std::mem::zeroed::<libc::timeval>() };
    let mut poll_result: c_int;
    // poll the completion for a while before giving up of doing it ..
    let time_zone = std::ptr::null_mut();
    unsafe { libc::gettimeofday(&mut cur_time, time_zone) };
    let start_time_msec =
        (cur_time.tv_sec.overflow_mul(1000)).overflow_add(cur_time.tv_usec.overflow_div(1000));
    println!("start ibv_poll_cq");
    loop {
        poll_result = unsafe { ibv_poll_cq(cq, 1, &mut wc) };
        unsafe { libc::gettimeofday(&mut cur_time, time_zone) };
        cur_time_msec =
            (cur_time.tv_sec.overflow_mul(1000)).overflow_add(cur_time.tv_usec.overflow_div(1000));
        if (poll_result != 0)
            || ((cur_time_msec.overflow_sub(start_time_msec)) >= MAX_POLL_CQ_TIMEOUT)
        {
            println!("end ibv_poll_cq");
            break;
        }
    }

    match poll_result.cmp(&0) {
        Ordering::Less => {
            // poll CQ failed
            // rc = 1;
            panic!("poll CQ failed");
        }
        Ordering::Equal => {
            // the CQ is empty
            // rc = 1;
            panic!("completion wasn't found in the CQ after timeout");
        }
        Ordering::Greater => {
            // CQE found
            println!("completion was found in CQ with status={}", wc.status);
            // check the completion status (here we don't care about the completion opcode
            debug_assert_eq!(
                wc.status,
                ibv_wc_status::IBV_WC_SUCCESS,
                "got bad completion with status={}, vendor syndrome={}, the error is: {:?}",
                wc.status,
                wc.vendor_err,
                unsafe { CStr::from_ptr(ibv_wc_status_str(wc.status)) },
            );
        }
    }
    0
}

///
pub fn poll_completion(cq: IbvCq) -> (c_int, ibv_wc) {
    // let qp_addr = util::ptr_to_usize(self.qp);
    // let qp = unsafe { util::usize_to_mut_ptr::<ibv_qp>(qp_addr) };
    let mut wc = unsafe { std::mem::zeroed::<ibv_wc>() };
    // let start_time_msec: u64;
    let mut cur_time_msec: i64;
    let mut cur_time = unsafe { std::mem::zeroed::<libc::timeval>() };
    let mut poll_result: c_int;
    // poll the completion for a while before giving up of doing it ..
    let time_zone = std::ptr::null_mut();
    unsafe { libc::gettimeofday(&mut cur_time, time_zone) };
    let start_time_msec =
        (cur_time.tv_sec.overflow_mul(1000)).overflow_add(cur_time.tv_usec.overflow_div(1000));
    loop {
        poll_result = unsafe { ibv_poll_cq(cq.inner, 1, &mut wc) };
        unsafe { libc::gettimeofday(&mut cur_time, time_zone) };
        cur_time_msec =
            (cur_time.tv_sec.overflow_mul(1000)).overflow_add(cur_time.tv_usec.overflow_div(1000));
        if (poll_result != 0)
            || ((cur_time_msec.overflow_sub(start_time_msec)) >= MAX_POLL_CQ_TIMEOUT)
        {
            break;
        }
    }

    match poll_result.cmp(&0) {
        Ordering::Less => {
            // poll CQ failed
            // rc = 1;
            panic!("poll CQ failed");
        }
        Ordering::Equal => {
            // the CQ is empty
            // rc = 1;
            // println!("completion wasn't found in the CQ after timeout");
            // Self::query_qp_cb(qp);
            panic!("completion wasn't found in the CQ after timeout");
        }
        Ordering::Greater => {
            // CQE found
            println!("completion was found in CQ with status={}", wc.status);
            // check the completion status (here we don't care about the completion opcode
            debug_assert_eq!(
                wc.status,
                ibv_wc_status::IBV_WC_SUCCESS,
                "got bad completion with status={}, vendor syndrome={}, the error is: {:?}",
                wc.status,
                wc.vendor_err,
                unsafe { CStr::from_ptr(ibv_wc_status_str(wc.status)) },
            );
        }
    }
    (0, wc)
}

/// Read remote data
pub fn remote_read(
    addr: u64,
    len: u32,
    local_key: u32,
    remote_addr: u64,
    remote_key: u32,
    qp: IbvQp,
    cq: IbvCq,
) -> c_int {
    remote_read_write(
        addr,
        len,
        local_key,
        remote_addr,
        remote_key,
        cq,
        qp,
        ibv_wr_opcode::IBV_WR_RDMA_READ,
    )
}

/// Write data to remote
pub fn remote_write(
    addr: u64,
    len: u32,
    local_key: u32,
    remote_addr: u64,
    remote_key: u32,
    qp: IbvQp,
    cq: IbvCq,
) -> c_int {
    remote_read_write(
        addr,
        len,
        local_key,
        remote_addr,
        remote_key,
        cq,
        qp,
        ibv_wr_opcode::IBV_WR_RDMA_WRITE,
    )
}

/// Send data to remote
pub fn remote_send(addr: u64, len: u32, local_key: u32, qp: IbvQp, cq: IbvCq) -> c_int {
    let mut sr = unsafe { std::mem::zeroed::<ibv_send_wr>() };
    let mut sge = unsafe { std::mem::zeroed::<ibv_sge>() };
    let mut bad_wr = std::ptr::null_mut::<ibv_send_wr>();

    // prepare the scatter/gather entry
    sge.addr = addr;
    sge.length = len.cast();
    sge.lkey = local_key;

    // prepare the send work request
    sr.next = std::ptr::null_mut();
    sr.wr_id = 0;
    sr.sg_list = &mut sge;
    sr.num_sge = 1;
    sr.opcode = ibv_wr_opcode::IBV_WR_SEND;
    sr.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0; // TODO: might use unsignaled SR

    req_cq_notify(cq);
    // there is a Receive Request in the responder side, so we won't get any into RNR flow
    let rc = unsafe { ibv_post_send(qp.inner, &mut sr, &mut bad_wr) };
    if rc == 0 {
        println!("RDMA send request was posted");
    } else {
        panic!("failed to post SR, the error code is:{}", rc);
    }
    rc
}

/// Write data to remote or read data from remote, should not be used directly
#[allow(clippy::too_many_arguments)]
fn remote_read_write(
    addr: u64,
    len: u32,
    local_key: u32,
    remote_addr: u64,
    remote_key: u32,
    cq: IbvCq,
    qp: IbvQp,
    opcode: c_uint,
) -> c_int {
    let mut sr = unsafe { std::mem::zeroed::<ibv_send_wr>() };
    let mut sge = unsafe { std::mem::zeroed::<ibv_sge>() };
    let mut bad_wr = std::ptr::null_mut::<ibv_send_wr>();

    // prepare the scatter/gather entry
    sge.addr = addr;
    sge.length = len.cast();
    sge.lkey = local_key;

    // prepare the send work request
    sr.next = std::ptr::null_mut();
    sr.wr_id = 0;
    sr.sg_list = &mut sge;
    sr.num_sge = 1;
    sr.opcode = opcode;
    sr.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0; // TODO: might use unsignaled SR
    sr.wr.rdma.remote_addr = remote_addr;
    sr.wr.rdma.rkey = remote_key;

    req_cq_notify(cq);
    // there is a Receive Request in the responder side, so we won't get any into RNR flow
    let rc = unsafe { ibv_post_send(qp.inner, &mut sr, &mut bad_wr) };
    if rc == 0 {
        match opcode {
            ibv_wr_opcode::IBV_WR_RDMA_READ => println!("RDMA read request was posted"),
            ibv_wr_opcode::IBV_WR_RDMA_WRITE => println!("RDMA write request was posted"),
            _ => (),
        }
    } else {
        panic!("failed to post SR, the error code is:{}", rc);
    }
    rc
}

/// Write to remote with immediate data
pub fn remote_write_with_imm(
    remote_addr: u64,
    remote_key: u32,
    imm_data: u32,
    qp: IbvQp,
    cq: IbvCq,
) -> c_int {
    remote_write_send_with_imm(
        remote_addr,
        remote_key,
        imm_data,
        qp,
        cq,
        ibv_wr_opcode::IBV_WR_RDMA_WRITE_WITH_IMM,
    )
}

/// Send to remote with immediate data
pub fn remote_send_with_imm(
    remote_addr: u64,
    remote_key: u32,
    imm_data: u32,
    qp: IbvQp,
    cq: IbvCq,
) -> c_int {
    remote_write_send_with_imm(
        remote_addr,
        remote_key,
        imm_data,
        qp,
        cq,
        ibv_wr_opcode::IBV_WR_SEND_WITH_IMM,
    )
}

/// Write to remote with immediate data
fn remote_write_send_with_imm(
    // We don't need the remote address and length in send operation
    _remote_addr: u64,
    _remote_key: u32,
    imm_data: u32,
    qp: IbvQp,
    cq: IbvCq,
    opcode: c_uint,
) -> c_int {
    let mut sr = unsafe { std::mem::zeroed::<ibv_send_wr>() };
    let mut bad_wr = std::ptr::null_mut::<ibv_send_wr>();

    // prepare the send work request
    sr.next = std::ptr::null_mut();
    sr.wr_id = 0;
    sr.sg_list = std::ptr::null_mut();
    sr.num_sge = 0;
    sr.opcode = opcode;
    sr.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0; // TODO: might use unsignaled SR
    sr.imm_data_invalidated_rkey_union.imm_data = imm_data;

    req_cq_notify(cq);
    // there is a Receive Request in the responder side, so we won't get any into RNR flow
    let rc = unsafe { ibv_post_send(qp.inner, &mut sr, &mut bad_wr) };
    if rc == 0 {
        match opcode {
            ibv_wr_opcode::IBV_WR_SEND_WITH_IMM => {
                println!("RDMA send with immediate request was posted")
            }
            ibv_wr_opcode::IBV_WR_RDMA_WRITE_WITH_IMM => {
                println!("RDMA write with immediate request was posted")
            }
            _ => (),
        }
    } else {
        panic!("failed to post SR, the error code is:{}", rc);
    }
    rc
}

/// Operate compare and swap in remote addr
#[allow(clippy::too_many_arguments)]
pub fn remote_atomic_cas(
    addr: u64,
    len: u32,
    local_key: u32,
    remote_addr: u64,
    remote_key: u32,
    old_value: u64,
    new_value: u64,
    qp: IbvQp,
    cq: IbvCq,
) -> c_int {
    let mut sr = unsafe { std::mem::zeroed::<ibv_send_wr>() };
    let mut sge = unsafe { std::mem::zeroed::<ibv_sge>() };
    let mut bad_wr = std::ptr::null_mut::<ibv_send_wr>();

    // prepare the scatter/gather entry
    sge.addr = addr;
    sge.length = len.cast();
    sge.lkey = local_key;

    // prepare the send work request
    sr.next = std::ptr::null_mut();
    sr.wr_id = 0;
    sr.sg_list = &mut sge;
    sr.num_sge = 1;
    sr.opcode = ibv_wr_opcode::IBV_WR_ATOMIC_CMP_AND_SWP;
    sr.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0; // TODO: might use unsignaled SR
    let aligned_remote_addr = remote_addr.overflow_add(7).overflow_shr(3).overflow_shl(3);
    println!(
        "remote addr={:x}, aligned remote addr={:x}",
        remote_addr, aligned_remote_addr
    );

    //println!("remote addr={:x}", self.remote_props.addr);
    sr.wr.atomic.remote_addr = aligned_remote_addr;
    sr.wr.atomic.rkey = remote_key;
    sr.wr.atomic.compare_add = old_value;
    sr.wr.atomic.swap = new_value;

    req_cq_notify(cq);
    // there is a Receive Request in the responder side, so we won't get any into RNR flow
    let rc = unsafe { ibv_post_send(qp.inner, &mut sr, &mut bad_wr) };
    if rc == 0 {
        println!("RDMA atomic compare and swap was posted");
    } else {
        panic!("failed to post SR, the error code is:{}", rc);
    }
    rc
}

/// Post ibv receive request
pub fn post_receive(addr: u64, len: u32, local_key: u32, qp: IbvQp) -> c_int {
    let mut rr = unsafe { std::mem::zeroed::<ibv_recv_wr>() };
    let mut sge = unsafe { std::mem::zeroed::<ibv_sge>() };
    let mut bad_wr = std::ptr::null_mut::<ibv_recv_wr>();

    // prepare the scatter/gather entry
    sge.addr = addr.cast();
    sge.length = len.cast();
    sge.lkey = local_key;
    // prepare the receive work request
    rr.next = std::ptr::null_mut();
    rr.wr_id = 0;
    rr.sg_list = &mut sge;
    rr.num_sge = 1;
    // post the Receive Request to the RQ
    let rc = unsafe { ibv_post_recv(qp.inner, &mut rr, &mut bad_wr) };
    if rc == 0 {
        println!("Receive Request was posted");
    } else {
        panic!("failed to post RR");
    }
    rc
}

///
fn req_cq_notify(cq: IbvCq) {
    let solicited_only = 0;
    let rc = unsafe { ibv_req_notify_cq(cq.inner, solicited_only) };
    if rc != 0 {
        panic!("Failed to request CQ notification");
    }
}

///
#[allow(clippy::too_many_lines)]
pub fn run_client(
    server_name: &str,
    input_dev_name: &str,
    gid_idx: c_int,
    ib_port: u8,
    sock_port: u16,
) -> c_int {
    let mut rc: c_int;
    // client side
    //let client_sock = TcpSock::connect(server_name, sock_port);
    let client_sock = Udp::connect(format!("{}:{}", server_name, sock_port));

    // Create resources before using them
    let mut res = Resources::new_and_connect_qp(input_dev_name, gid_idx, ib_port, client_sock);
    res.poll_async_event_non_blocking();

    // Client post RR to be prepared for incoming messages
    rc = res.post_receive();
    debug_assert_eq!(rc, 0, "failed to post RR");
    // Notify server to send
    let resp_recv_ready: State = res.sock.exchange_data(&State::ReceiveReady);
    if let State::ReceiveReady = resp_recv_ready {
        println!("receive ready: {:?}", resp_recv_ready);
    } else {
        panic!("failed to receive ready");
    }
    // Exchange send size with server
    let resp_send_size: State = res.sock.exchange_data(&State::SendSize(INVALID_SIZE));
    let send_size = if let State::SendSize(send_size) = resp_send_size {
        println!("receive send size from server: {}", send_size);
        send_size
    } else {
        panic!(
            "failed to receive send size, the state is: {:?}",
            resp_send_size
        )
    };
    // Both sides expect to get a completion
    rc = res.poll_completion();
    debug_assert_eq!(rc, 0, "poll completion failed");

    // After polling the completion we have the message in the client buffer too
    let recv_msg = std::str::from_utf8(
        res.buf_slice()
            .get(0..(send_size.cast()))
            .unwrap_or_else(|| panic!("failed to slice to send size {}", send_size)),
    )
    .unwrap_or_else(|err| panic!("failed to build str from bytes, the error is: {}", err));
    println!("client received send message is: {:?}", recv_msg);

    // Sync with server the size of the data to read
    let resp_read_size: State = res.sock.exchange_data(&State::ReadSize(INVALID_SIZE));
    let read_size = if let State::ReadSize(read_size) = resp_read_size {
        println!("receive read size from server: {}", read_size);
        read_size
    } else {
        panic!(
            "failed to receive read size, the state is: {:?}",
            resp_read_size
        )
    };

    // Now the client performs an RDMA read and then write on server.
    // Note that the server has no idea these events have occured
    // First client read contens of server's buffer
    rc = res.post_send(
        ibv_wr_opcode::IBV_WR_RDMA_READ,
        res.buf_slice().len().cast(),
    );
    debug_assert_eq!(rc, 0, "failed to post SR 2");
    rc = res.poll_completion();
    debug_assert_eq!(rc, 0, "poll completion failed 2");

    let read_msg = std::str::from_utf8(
        res.buf_slice()
            .get(0..(read_size.cast()))
            .unwrap_or_else(|| panic!("failed to slice to read size {}", read_size)),
    )
    .unwrap_or_else(|err| panic!("failed to build str from bytes, the error is: {}", err));
    println!("client read server buffer: {}", read_msg);

    // client performs a zero sized rdma read
    rc = res.post_send(ibv_wr_opcode::IBV_WR_RDMA_READ, 0);
    debug_assert_eq!(rc, 0, "failed to post SR 2.5");
    rc = res.poll_completion();
    debug_assert_eq!(rc, 0, "poll completion failed 2.5");

    // Sync with server the size of write data
    let resp_write_size: State = res
        .sock
        .exchange_data(&State::WriteSize(res.buf_slice().len().cast()));
    if let State::WriteSize(write_size) = resp_write_size {
        println!("receive write size from server: {}", write_size);
    } else {
        panic!(
            "failed to receive write size, the state is: {:?}",
            resp_write_size
        );
    }
    // Next client write data to server's buffer
    util::copy_to_buf_pad(res.buf_slice_mut(), RDMAMSGW);
    println!("write to server with data: {}", RDMAMSGW);
    rc = res.post_send(
        ibv_wr_opcode::IBV_WR_RDMA_WRITE,
        res.buf_slice().len().cast(),
    );
    debug_assert_eq!(rc, 0, "failed to post SR 3");
    // rc = res.poll_completion();
    // debug_assert_eq!(rc, 0, "poll completion failed 3");
    let poll_handler = res.async_poll_completion();
    let poll_res = poll_handler.join();
    if let Err(err) = poll_res {
        panic!("async poll completion failed, the error is: {:?}", err);
    }

    // Prepare to receive write with imm
    res.post_receive();
    // Sync with server about write with imm
    let resp_write_imm: State = res.sock.exchange_data(&State::WriteImm);
    if let State::WriteImm = resp_write_imm {
        println!("receive write with imm from server");
    } else {
        panic!(
            "failed to receive write with imm, the state is: {:?}",
            resp_write_imm
        );
    }
    // Sync with server about write done
    let resp_write_done: State = res.sock.exchange_data(&State::WriteDone);
    if let State::WriteDone = resp_write_done {
        println!("receive write done from server");
    } else {
        panic!(
            "failed to receive write done, the state is: {:?}",
            resp_write_done
        );
    }

    // Notify server for atomic operation
    util::copy_to_buf_pad(res.buf_slice_mut(), "");
    let pre_atomic_msg = std::str::from_utf8(
        res.buf_slice()
            .get(0..(res.buf_slice().len()))
            .unwrap_or_else(|| {
                panic!(
                    "failed to read atomic buf with size {}",
                    res.buf_slice().len()
                )
            }),
    )
    .unwrap_or_else(|err| panic!("failed to build str from bytes, the error is: {}", err));
    println!(
        "client atomic data before server atomic operation: {:?}",
        pre_atomic_msg
    );

    println!(
        "client atomic mr begin addr={:x}, end addr={:x}",
        util::ptr_to_usize(res.buf_slice().as_ptr()),
        util::ptr_to_usize(res.buf_slice().as_ptr()).overflow_add(res.buf_slice().len())
    );
    // Pre atomic operation
    let resp_atomic_ready: State = res.sock.exchange_data(&State::AtomicReady);
    if let State::AtomicReady = resp_atomic_ready {
        println!("atomic ready: {:?}", resp_atomic_ready);
    } else {
        panic!("failed to atomic ready");
    }
    // let two_secs = std::time::Duration::from_millis(2000);
    // thread::sleep(two_secs);
    res.query_qp();
    // Post atomic operation
    let resp_atomic_done: State = res.sock.exchange_data(&State::AtomicDone);
    if let State::AtomicDone = resp_atomic_done {
        println!("atomic done: {:?}", resp_atomic_done);
    } else {
        panic!("failed to atomic done");
    }
    let post_atomic_msg = std::str::from_utf8(
        res.buf_slice()
            .get(0..(res.buf_slice().len()))
            .unwrap_or_else(|| {
                panic!(
                    "failed to read atomic buf with size {}",
                    res.buf_slice().len()
                )
            }),
    )
    .unwrap_or_else(|err| panic!("failed to build str from bytes, the error is: {}", err));
    println!(
        "client atomic data after server atomic operation: {:?}",
        post_atomic_msg
    );

    res.query_qp();
    println!("\ntest result is: {}", rc);
    rc
}

///
#[allow(clippy::too_many_lines)]
pub fn run_server(input_dev_name: &str, gid_idx: c_int, ib_port: u8, sock_port: u16) -> c_int {
    let mut rc: c_int;

    println!("waiting on port {} for TCP connection", sock_port);
    // let listen_sock = TcpSock::bind(sock_port);
    // let client_sock = listen_sock.accept();
    let listen_sock = Udp::bind(sock_port);
    let client_sock = listen_sock.accept();

    // Create resources
    let mut res = Resources::new_and_connect_qp(input_dev_name, gid_idx, ib_port, client_sock);

    // Only in the server side put the message in the memory buffer
    util::copy_to_buf_pad(res.buf_slice_mut(), MSG);
    // Sync with client before send
    let resp_recv_ready: State = res.sock.exchange_data(&State::ReceiveReady);
    if let State::ReceiveReady = resp_recv_ready {
        println!("receive ready: {:?}", resp_recv_ready);
    } else {
        panic!("failed to receive ready");
    }
    // Exchange send size with client
    let resp_send_size: State = res
        .sock
        .exchange_data(&State::SendSize(res.buf_slice().len().cast()));
    if let State::SendSize(send_size) = resp_send_size {
        println!("receive send size from client: {}", send_size);
    } else {
        panic!(
            "failed to receive send size, the state is:{:?}",
            resp_send_size
        );
    }
    // Post send request
    println!("going to send the message: \"{}\"", MSG);
    rc = res.post_send(ibv_wr_opcode::IBV_WR_SEND_WITH_IMM, 0);
    debug_assert_eq!(rc, 0, "failed to post send");
    // Both sides expect to get a completion
    rc = res.poll_completion();
    debug_assert_eq!(rc, 0, "poll completion failed");

    // Setup server buffer with read message
    util::copy_to_buf_pad(res.buf_slice_mut(), RDMAMSGR);
    // Sync with client the size of read data from server
    let resp_read_size: State = res
        .sock
        .exchange_data(&State::ReadSize(res.buf_slice().len().cast()));
    if let State::ReadSize(read_size) = resp_read_size {
        println!("receive read size from client: {}", read_size);
    } else {
        panic!(
            "failed to receive read size, the code is: {:?}",
            resp_read_size
        );
    }

    // Sync with client the size of write data to server
    let resp_write_size: State = res.sock.exchange_data(&State::WriteSize(INVALID_SIZE));
    let write_size = if let State::WriteSize(write_size) = resp_write_size {
        println!("receive write size from client: {}", write_size);
        write_size
    } else {
        panic!(
            "failed to receive write size, the state is: {:?}",
            resp_write_size
        )
    };

    // Sync with client about write with imm
    let resp_write_imm: State = res.sock.exchange_data(&State::WriteImm);
    if let State::WriteImm = resp_write_imm {
        println!("receive write with imm from server");
    } else {
        panic!(
            "failed to receive write with imm, the state is: {:?}",
            resp_write_imm
        );
    }
    // Send write with imm
    println!("going to post write with imm");
    res.post_write_imm();
    rc = res.poll_completion();
    debug_assert_eq!(rc, 0, "poll completion failed");
    // Sync with client about write done
    let resp_write_done: State = res.sock.exchange_data(&State::WriteDone);
    if let State::WriteDone = resp_write_done {
        println!("receive write done from client");
    } else {
        panic!(
            "failed to receive write done, the state is: {:?}",
            resp_write_done
        )
    };
    let write_msg = std::str::from_utf8(
        res.buf_slice()
            .get(0..(write_size.cast()))
            .unwrap_or_else(|| panic!("failed to slice to write size {}", write_size)),
    )
    .unwrap_or_else(|err| panic!("failed to build str from bytes, the error is: {}", err));
    println!("client write data to server buffer: {:?}", write_msg);

    // Sync with client before atomic
    let resp_atomic_ready: State = res.sock.exchange_data(&State::AtomicReady);
    if let State::AtomicReady = resp_atomic_ready {
        println!("atomic ready: {:?}", resp_atomic_ready);
    } else {
        panic!(
            "failed to atomic ready, the state is: {:?}",
            resp_atomic_ready
        );
    }
    // Atomic opteration
    println!("going to request atomic operation");
    rc = res.post_send(ibv_wr_opcode::IBV_WR_ATOMIC_CMP_AND_SWP, 0);
    debug_assert_eq!(rc, 0, "failed to post atomic");
    rc = res.poll_completion();
    debug_assert_eq!(rc, 0, "poll completion failed");
    // Sync with client after atomic
    let resp_atomic_done: State = res.sock.exchange_data(&State::AtomicDone);
    if let State::AtomicDone = resp_atomic_done {
        println!("atomic done: {:?}", resp_atomic_done);
    } else {
        panic!(
            "failed to atomic done, the state is: {:?}",
            resp_atomic_done
        );
    }

    res.query_qp();
    println!("\ntest result is: {}", rc);
    rc
}
