// The network byte order is defined to always be big-endian.
// X86 is little-endian.

use rdma_sys::*;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::cmp::Ordering;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_uint, c_void};
use std::os::unix::io::RawFd;
use utilities::{Cast, OverflowArithmetic};

use super::util;

///
const MSG: &str = "SEND operation ";
///
const RDMAMSGR: &str = "RDMA read operation ";
///
const RDMAMSGW: &str = "RDMA write operation";
///
const MSG_SIZE: usize = MSG.len() + 1;
///
const MAX_POLL_CQ_TIMEOUT: i64 = 2000;
// ///
// const TCP_PORT: u16 = 19875;

/// structure to exchange data which is needed to connect the QPs
//#[repr(packed)]
#[derive(Copy, Clone, Deserialize, Serialize)]
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
// #[allow(dead_code)]
// impl CmConData {
//     ///
//     const fn into_be(self) -> Self {
//         Self {
//             addr: u64::to_be(self.addr),
//             rkey: u32::to_be(self.rkey),
//             qp_num: u32::to_be(self.qp_num),
//             lid: u16::to_be(self.lid),
//             gid: u128::to_be(self.gid),
//         }
//     }
//     ///
//     const fn into_le(self) -> Self {
//         Self {
//             addr: u64::from_be(self.addr),
//             rkey: u32::from_be(self.rkey),
//             qp_num: u32::from_be(self.qp_num),
//             lid: u16::from_be(self.lid),
//             gid: u128::from_be(self.gid),
//         }
//     }
// }

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

/// structure of system resources
struct Resources {
    // /// Device attributes
    // device_attr: ibv_device_attr,
    // /// IB port attributes
    // port_attr: ibv_port_attr,
    /// values to connect to remote side
    remote_props: CmConData,
    /// device handle
    ib_ctx: *mut ibv_context,
    /// PD handle
    pd: *mut ibv_pd,
    /// CQ handle
    cq: *mut ibv_cq,
    /// QP handle
    qp: *mut ibv_qp,
    /// MR handle for buf
    mr: *mut ibv_mr,
    /// memory buffer pointer, used for RDMA and send ops
    buf: std::pin::Pin<Box<[u8; MSG_SIZE]>>,
    /// TCP socket file descriptor
    sock: RawFd,
}

///
fn resolve_addr(server_name: &str, port: u16, resolved_addr: *mut *mut libc::addrinfo) -> c_int {
    let server_addr_cstr = CString::new(server_name).unwrap_or_else(|err| {
        panic!(format!(
            "failed to build server address CString, the error is: {}",
            err,
        ))
    });
    let server_port_cstr = CString::new(port.to_string()).unwrap_or_else(|err| {
        panic!(format!(
            "failed to build port CString, the error is: {}",
            err,
        ))
    });
    let mut hints = unsafe { std::mem::zeroed::<libc::addrinfo>() };
    hints.ai_flags = libc::AI_PASSIVE;
    hints.ai_family = libc::AF_INET;
    hints.ai_socktype = libc::SOCK_STREAM;

    // Resolve DNS address
    let rc = unsafe {
        libc::getaddrinfo(
            if server_name.is_empty() {
                std::ptr::null::<c_char>()
            } else {
                server_addr_cstr.as_ptr()
            },
            server_port_cstr.as_ptr(),
            &hints,
            resolved_addr,
        )
    };
    debug_assert_eq!(rc, 0, "getaddrinfo failed, the error is: {:?}", unsafe {
        CStr::from_ptr(libc::gai_strerror(rc))
    });
    rc
}

///
fn sock_connect(server_name: &str, port: u16) -> c_int {
    let mut rc: c_int;
    let mut resolved_addr = std::ptr::null_mut::<libc::addrinfo>();
    rc = resolve_addr(server_name, port, &mut resolved_addr);
    debug_assert_eq!(rc, 0, "resolve_addr failed");

    // Search through results and find the one we want
    let mut iterator = resolved_addr;
    let mut sockfd = -1;
    while !util::is_null_mut_ptr(iterator) {
        let addr = unsafe { &*iterator };
        sockfd = unsafe { libc::socket(addr.ai_family, addr.ai_socktype, addr.ai_protocol) };
        if sockfd >= 0 {
            // Client mode. Initiate connection to remote
            rc = unsafe { libc::connect(sockfd, addr.ai_addr, addr.ai_addrlen) };
            debug_assert_eq!(rc, 0, "socket connect failed");
        }

        iterator = addr.ai_next;
    }
    if !util::is_null_mut_ptr(resolved_addr) {
        unsafe { libc::freeaddrinfo(resolved_addr) };
    }
    sockfd
}

///
fn sock_bind(port: u16) -> c_int {
    let mut rc: c_int;
    let mut resolved_addr = std::ptr::null_mut::<libc::addrinfo>();
    let empty_server_name = "";
    rc = resolve_addr(empty_server_name, port, &mut resolved_addr);
    debug_assert_eq!(rc, 0, "resolve_addr failed");

    // Search through results and find the one we want
    let mut iterator = resolved_addr;
    let mut listenfd = -1;
    while !util::is_null_mut_ptr(iterator) {
        let addr = unsafe { &*iterator };
        listenfd = unsafe { libc::socket(addr.ai_family, addr.ai_socktype, addr.ai_protocol) };
        if listenfd >= 0 {
            // Server mode. Set up listening socket an accept a connection
            rc = unsafe { libc::bind(listenfd, addr.ai_addr, addr.ai_addrlen) };
            debug_assert_eq!(rc, 0, "socket bind failed");
        }

        iterator = addr.ai_next;
    }
    if !util::is_null_mut_ptr(resolved_addr) {
        unsafe { libc::freeaddrinfo(resolved_addr) };
    }
    listenfd
}

///
fn sock_accept(listenfd: RawFd) -> c_int {
    let backlog = 1;
    let rc = unsafe { libc::listen(listenfd, backlog) };
    if rc != 0 {
        panic!("socket listen failed");
    }
    let client_addr = std::ptr::null_mut::<libc::sockaddr>();
    let mut addr_len = 0;
    let sockfd = unsafe { libc::accept(listenfd, client_addr, &mut addr_len) };
    if sockfd < 0 {
        panic!("socket accept failed");
    }
    sockfd
}

///
fn sync_data<T: Serialize + DeserializeOwned>(sock: RawFd, data: &T) -> T {
    let xfer_size = std::mem::size_of::<T>();
    let mut encoded: Vec<u8> = bincode::serialize(data)
        .unwrap_or_else(|err| panic!("failed to encode, the error is: {}", err));
    let mut decode_buf = Vec::with_capacity(xfer_size);
    let rc = unsafe {
        libc::write(
            sock,
            util::mut_ptr_cast(encoded.as_mut_ptr()),
            encoded.len(),
        )
    };
    debug_assert_eq!(rc, encoded.len().cast(), "failed to send data via socket",);
    let recv_size = unsafe {
        libc::read(
            sock,
            util::mut_ptr_cast(decode_buf.as_mut_ptr()),
            decode_buf.capacity(),
        )
    };
    unsafe {
        decode_buf.set_len(recv_size.cast());
    }
    debug_assert!(recv_size > 0, "failed to receive data from socket");

    bincode::deserialize(&decode_buf)
        .unwrap_or_else(|err| panic!("failed to decode, the error is: {}", err))
}

/*
///
fn sock_sync_data(
    sock: RawFd,
    xfer_size: usize,
    local_data: &[u8],
    remote_data: &mut Vec<u8>,
) -> isize {
    debug_assert!(
        xfer_size <= remote_data.capacity(),
        "transfer data size should not be larger than buffer size",
    );
    let rc = unsafe { libc::write(sock, util::const_ptr_cast(local_data), local_data.len()) };
    debug_assert_eq!(
        rc,
        local_data.len().cast(),
        "Failed writing data during sock_sync_data",
    );
    let total_read_bytes = unsafe {
        libc::read(
            sock,
            util::const_ptr_cast_mut(remote_data.as_ptr()),
            remote_data.capacity(),
        )
    };
    // let mut total_read_bytes = 0;
    // loop {
    //     rc = unsafe {
    //         // TODO: here is a bug, multiple read will overwrite previous read data
    //         libc::read(
    //             sock,
    //             util::const_ptr_cast_mut(remote_data.as_ptr()),
    //             remote_data.capacity(),
    //         )
    //     };
    //     match rc.cmp(&0) {
    //         Ordering::Equal => break,
    //         Ordering::Less => panic!("read socket failed"),
    //         Ordering::Greater => {
    //             total_read_bytes = total_read_bytes.overflow_add(rc);
    //             if xfer_size <= total_read_bytes.cast() {
    //                 // Remote data buffer size too small, stop loop and return non-zero
    //                 break;
    //             }
    //         }
    //     }
    // }
    unsafe {
        remote_data.set_len(total_read_bytes.cast());
    }
    total_read_bytes
}
*/

///
fn poll_completion(res: &Resources) -> c_int {
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
        poll_result = unsafe { ibv_poll_cq(res.cq, 1, &mut wc) };
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
            panic!("completion wasn't found in the CQ after timeout");
        }
        Ordering::Greater => {
            // CQE found
            println!("completion was found in CQ with status={}", wc.status);
            // check the completion status (here we don't care about the completion opcode
            debug_assert_eq!(
                wc.status,
                ibv_wc_status::IBV_WC_SUCCESS,
                "got bad completion with status={}, vendor syndrome={}",
                wc.status,
                wc.vendor_err
            );
        }
    }
    // if poll_result < 0 {
    //     // poll CQ failed
    //     // rc = 1;
    //     panic!("poll CQ failed");
    // } else if poll_result == 0 {
    //     // the CQ is empty
    //     // rc = 1;
    //     panic!("completion wasn't found in the CQ after timeout");
    // } else {
    //     // CQE found
    //     println!("completion was found in CQ with status={}", wc.status);
    //     // check the completion status (here we don't care about the completion opcode
    //     debug_assert_eq!(
    //         wc.status,
    //         ibv_wc_status::IBV_WC_SUCCESS,
    //         "got bad completion with status={}, vendor syndrome={}",
    //         wc.status,
    //         wc.vendor_err
    //     );
    // }
    0
}

///
fn post_send(res: &Resources, opcode: c_uint) -> c_int {
    let mut sr = unsafe { std::mem::zeroed::<ibv_send_wr>() };
    let mut sge = unsafe { std::mem::zeroed::<ibv_sge>() };
    let mut bad_wr = std::ptr::null_mut::<ibv_send_wr>();
    // prepare the scatter/gather entry
    sge.addr = util::ptr_to_usize(res.buf.as_ptr()).cast();
    sge.length = MSG_SIZE.cast();
    sge.lkey = unsafe { (*res.mr).lkey };
    // prepare the send work request
    sr.next = std::ptr::null_mut();
    sr.wr_id = 0;
    sr.sg_list = &mut sge;
    sr.num_sge = 1;
    sr.opcode = opcode;
    sr.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
    if opcode != ibv_wr_opcode::IBV_WR_SEND {
        sr.wr.rdma.remote_addr = res.remote_props.addr;
        sr.wr.rdma.rkey = res.remote_props.rkey;
    }
    // there is a Receive Request in the responder side, so we won't get any into RNR flow
    let rc = unsafe { ibv_post_send(res.qp, &mut sr, &mut bad_wr) };
    if rc == 0 {
        match opcode {
            ibv_wr_opcode::IBV_WR_SEND => println!("RDMA send request was posted"),
            ibv_wr_opcode::IBV_WR_RDMA_READ => println!("RDMA read Request was posted"),
            ibv_wr_opcode::IBV_WR_RDMA_WRITE => println!("RDMA write Request was posted"),
            _ => println!("Unknown Request was posted"),
        }
    } else {
        panic!("failed to post SR");
    }
    rc
}

///
fn post_receive(res: &Resources) -> c_int {
    let mut rr = unsafe { std::mem::zeroed::<ibv_recv_wr>() };
    let mut sge = unsafe { std::mem::zeroed::<ibv_sge>() };
    let mut bad_wr = std::ptr::null_mut::<ibv_recv_wr>();

    // prepare the scatter/gather entry
    sge.addr = util::ptr_to_usize(res.buf.as_ptr()).cast();
    sge.length = MSG_SIZE.cast();
    sge.lkey = unsafe { (*res.mr).lkey };
    // prepare the receive work request
    rr.next = std::ptr::null_mut();
    rr.wr_id = 0;
    rr.sg_list = &mut sge;
    rr.num_sge = 1;
    // post the Receive Request to the RQ
    let rc = unsafe { ibv_post_recv(res.qp, &mut rr, &mut bad_wr) };
    if rc == 0 {
        println!("Receive Request was posted");
    } else {
        panic!("failed to post RR");
    }
    rc
}

///
fn open_ib_ctx(dev_name: &str) -> *mut ibv_context {
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
    ib_ctx
}

///
fn resources_create(
    server_name: &str,
    input_dev_name: &str,
    gid_idx: c_int,
    ib_port: u8,
    sock: RawFd,
) -> Resources {
    let mut rc: c_int;
    // Searching for IB devices in host
    let ib_ctx = open_ib_ctx(input_dev_name);

    // Query port properties
    let mut port_attr = unsafe { std::mem::zeroed::<ibv_port_attr>() };
    rc = unsafe { ___ibv_query_port(ib_ctx, ib_port, &mut port_attr) };
    debug_assert_eq!(rc, 0, "ibv_query_port on port {} failed", ib_port);
    // Get GID
    let mut my_gid = unsafe { std::mem::zeroed::<ibv_gid>() };
    if gid_idx >= 0 {
        rc = unsafe { ibv_query_gid(ib_ctx, ib_port, gid_idx, &mut my_gid) };
        debug_assert_eq!(
            rc, 0,
            "could not get gid for index={}, port={}",
            ib_port, gid_idx,
        );
    }

    // Allocate Protection Domain
    let pd = unsafe { ibv_alloc_pd(ib_ctx) };
    if util::is_null_mut_ptr(pd) {
        // rc = 1;
        // goto resources_create_exit;
        panic!("ibv_alloc_pd failed");
    }
    // Each side will send only one WR, so Completion Queue with 1 entry is enough
    let cq_size = 1;
    let cq_context = std::ptr::null_mut::<c_void>();
    let comp_channel = std::ptr::null_mut::<ibv_comp_channel>();
    let comp_vector = 0;
    let cq = unsafe { ibv_create_cq(ib_ctx, cq_size, cq_context, comp_channel, comp_vector) };
    if util::is_null_mut_ptr(cq) {
        // rc = 1;
        // goto resources_create_exit;
        panic!("failed to create CQ with {} entries", cq_size);
    }
    // Allocate the memory buffer that will hold the data
    let mut buf = Box::pin([0; MSG_SIZE]);
    // if (!res->buf)
    // {
    // 	fprintf(stderr, "failed to malloc %Zu bytes to memory buffer\n", size);
    // 	rc = 1;
    // 	goto resources_create_exit;
    // }
    // memset(res->buf, 0, size);
    // Only in the server side put the message in the memory buffer
    if server_name.is_empty() {
        buf.copy_from_slice(format!("{}{}", MSG, "\0").as_bytes());
        println!("going to send the message: \"{}\"", MSG);
    }
    // Register the memory buffer
    let mr_flags = (ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
        | ibv_access_flags::IBV_ACCESS_REMOTE_READ
        | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE)
        .0;
    let mr = unsafe {
        ibv_reg_mr(
            pd,
            util::mut_ptr_cast(buf.as_mut_ptr()),
            MSG_SIZE,
            mr_flags.cast(),
        )
    };
    if util::is_null_mut_ptr(mr) {
        // rc = 1;
        // goto resources_create_exit;
        panic!("ibv_reg_mr failed with mr_flags={}", mr_flags);
    }
    println!(
        "MR was registered with addr={:?}, lkey={:x}, rkey={:x}, flags={}",
        buf.as_mut_ptr(),
        unsafe { (*mr).lkey },
        unsafe { (*mr).rkey },
        mr_flags
    );
    // Create the Queue Pair
    let mut qp_init_attr = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
    qp_init_attr.qp_type = ibv_qp_type::IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 1;
    qp_init_attr.send_cq = cq;
    qp_init_attr.recv_cq = cq;
    qp_init_attr.cap.max_send_wr = 1;
    qp_init_attr.cap.max_recv_wr = 1;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;
    let qp = unsafe { ibv_create_qp(pd, &mut qp_init_attr) };
    if util::is_null_mut_ptr(qp) {
        // rc = 1;
        // goto resources_create_exit;
        panic!("failed to create QP");
    }
    println!("QP was created, QP number={:x}", unsafe { (*qp).qp_num });

    // Exchange using TCP sockets info required to connect QPs
    let mut local_con_data = unsafe { std::mem::zeroed::<CmConData>() };
    local_con_data.addr = util::ptr_to_usize(buf.as_mut_ptr()).cast();
    local_con_data.rkey = unsafe { (*mr).rkey };
    local_con_data.qp_num = unsafe { (*qp).qp_num };
    local_con_data.lid = port_attr.lid;
    local_con_data.gid = u128::from_be_bytes(unsafe { my_gid.raw });
    println!("local connection data: {}", local_con_data);
    let remote_con_data = sync_data(sock, &local_con_data);
    // let local_con_data_be = local_con_data.into_be();
    // let remote_con_data_be = sync_data(sock, &local_con_data_be);
    // let remote_con_data = remote_con_data_be.into_le();
    println!("remote connection data: {}", remote_con_data);

    Resources {
        // device_attr: unsafe { std::mem::zeroed() },
        // port_attr,
        remote_props: remote_con_data,
        ib_ctx,
        pd,
        cq,
        qp,
        mr,
        buf,
        sock,
    }
}

///
fn modify_qp_to_init(qp: *mut ibv_qp, ib_port: u8) -> c_int {
    let mut attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
    attr.qp_state = ibv_qp_state::IBV_QPS_INIT;
    attr.port_num = ib_port;
    attr.pkey_index = 0;
    attr.qp_access_flags = (ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
        | ibv_access_flags::IBV_ACCESS_REMOTE_READ
        | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE)
        .0;
    let flags = ibv_qp_attr_mask::IBV_QP_STATE
        | ibv_qp_attr_mask::IBV_QP_PKEY_INDEX
        | ibv_qp_attr_mask::IBV_QP_PORT
        | ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
    let rc = unsafe { ibv_modify_qp(qp, &mut attr, flags.0.cast()) };
    if rc != 0 {
        panic!("failed to modify QP state to INIT");
    }
    rc
}

///
fn modify_qp_to_rtr(
    qp: *mut ibv_qp,
    remote_qpn: u32,
    dlid: u16,
    d_gid: &u128,
    gid_idx: c_int,
    ib_port: u8,
) -> c_int {
    let mut attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
    attr.qp_state = ibv_qp_state::IBV_QPS_RTR;
    attr.path_mtu = ibv_mtu::IBV_MTU_256;
    attr.dest_qp_num = remote_qpn;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 0x12;
    attr.ah_attr.is_global = 0;
    attr.ah_attr.dlid = dlid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = ib_port;
    if gid_idx >= 0 {
        attr.ah_attr.is_global = 1;
        attr.ah_attr.port_num = 1;
        attr.ah_attr.grh.dgid.raw = d_gid.to_be_bytes();
        attr.ah_attr.grh.flow_label = 0;
        attr.ah_attr.grh.hop_limit = 1;
        attr.ah_attr.grh.sgid_index = gid_idx.cast(); // TODO: gid_idx integer type?
        attr.ah_attr.grh.traffic_class = 0;
    }
    let flags = ibv_qp_attr_mask::IBV_QP_STATE
        | ibv_qp_attr_mask::IBV_QP_AV
        | ibv_qp_attr_mask::IBV_QP_PATH_MTU
        | ibv_qp_attr_mask::IBV_QP_DEST_QPN
        | ibv_qp_attr_mask::IBV_QP_RQ_PSN
        | ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC
        | ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER;
    let rc = unsafe { ibv_modify_qp(qp, &mut attr, flags.0.cast()) };
    if rc != 0 {
        panic!("failed to modify QP state to RTR");
    }
    rc
}

///
fn modify_qp_to_rts(qp: *mut ibv_qp) -> c_int {
    let mut attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
    attr.qp_state = ibv_qp_state::IBV_QPS_RTS;
    attr.timeout = 0x12;
    attr.retry_cnt = 6;
    attr.rnr_retry = 0;
    attr.sq_psn = 0;
    attr.max_rd_atomic = 1;
    let flags = ibv_qp_attr_mask::IBV_QP_STATE
        | ibv_qp_attr_mask::IBV_QP_TIMEOUT
        | ibv_qp_attr_mask::IBV_QP_RETRY_CNT
        | ibv_qp_attr_mask::IBV_QP_RNR_RETRY
        | ibv_qp_attr_mask::IBV_QP_SQ_PSN
        | ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC;
    let rc = unsafe { ibv_modify_qp(qp, &mut attr, flags.0.cast()) };
    if rc != 0 {
        panic!("failed to modify QP state to RTS");
    }
    rc
}

///
fn connect_qp(res: &Resources, server_name: &str, gid_idx: c_int, ib_port: u8) -> c_int {
    let mut rc: c_int;
    /*
        // let temp_char: char;
        let mut my_gid = unsafe { std::mem::zeroed::<ibv_gid>() };
        if gid_idx >= 0 {
            rc = unsafe { ibv_query_gid(res.ib_ctx, ib_port, gid_idx, &mut my_gid) };
            if rc != 0 {
                // return rc;
                panic!("could not get gid for port={}, index={}", ib_port, gid_idx);
            }
        }
        // exchange using TCP sockets info required to connect QPs
        let mut local_con_data = unsafe { std::mem::zeroed::<CmConData>() };
        local_con_data.addr = util::ptr_to_usize(res.buf.as_mut_ptr()).cast();
        local_con_data.rkey = unsafe { (*res.mr).rkey };
        local_con_data.qp_num = unsafe { (*res.qp).qp_num };
        local_con_data.lid = res.port_attr.lid;
        local_con_data.gid = u128::from_be_bytes(unsafe { my_gid.raw });
        // println!("tmp connection data: {}", tmp_con_data);
        // let tmp_con_data_be = tmp_con_data.to_be();
        // println!("tmp connection data: BE: {}", tmp_con_data_be);
        // println!("tmp connection data: LE: {}", tmp_con_data_from_be);
        // let mut local_con_data = unsafe { std::mem::zeroed::<CmConData>() };
        // local_con_data.addr = u64::to_be(util::ptr_to_usize(res.buf.as_mut_ptr()).cast());
        // local_con_data.rkey = u32::to_be(unsafe { (*res.mr).rkey });
        // local_con_data.qp_num = u32::to_be(unsafe { (*res.qp).qp_num });
        // local_con_data.lid = u16::to_be(res.port_attr.lid);
        // local_con_data.gid = u128::from_be_bytes(unsafe { my_gid.raw }).to_be();
        println!("local connection data: {}", local_con_data);
        let local_con_data_be = local_con_data.into_be();

        // let encoded: Vec<u8> = bincode::serialize(&local_con_data_be)
        //     .unwrap_or_else(|err| panic!("failed to encode, the error is: {}", err));
        // let buf_size = std::mem::size_of::<CmConData>();
        // let mut decode_buf = Vec::with_capacity(buf_size);
        // rc = sock_sync_data(res.sock, buf_size, &encoded, &mut decode_buf).cast();
        // if rc < 0 {
        //     // TODO: should be rc < 0 ?
        //     //rc = 1;
        //     //goto connect_qp_exit;
        //     panic!("failed to exchange connection data between sides");
        // }
        // let remote_con_data_be: CmConData = bincode::deserialize(&decode_buf)
        //     .unwrap_or_else(|err| panic!("failed to decode, the error is: {}", err));
        let remote_con_data_be = sync_data(res.sock, &local_con_data_be);

        // let mut remote_con_data = unsafe { std::mem::zeroed::<CmConData>() };
        // let tmp_con_data: CmConData = bincode::deserialize(&decode_buf)
        //     .unwrap_or_else(|err| panic!("failed to decode, the error is: {}", err));
        // remote_con_data.addr = u64::from_be(tmp_con_data.addr);
        // remote_con_data.rkey = u32::from_be(tmp_con_data.rkey);
        // remote_con_data.qp_num = u32::from_be(tmp_con_data.qp_num);
        // remote_con_data.lid = u16::from_be(tmp_con_data.lid);
        // remote_con_data.gid = u128::from_be(tmp_con_data.gid);
        // save the remote side attributes, we will need it for the post SR
        let remote_con_data = remote_con_data_be.into_le();
        res.remote_props = remote_con_data;
        println!("remote connection data: {}", remote_con_data);
    */
    // modify the QP to init
    rc = modify_qp_to_init(res.qp, ib_port);
    if rc != 0 {
        // goto connect_qp_exit;
        panic!("change QP state to INIT failed");
    }
    // let the client post RR to be prepared for incoming messages
    if !server_name.is_empty() {
        rc = post_receive(res);
        if rc != 0 {
            // goto connect_qp_exit;
            panic!("failed to post RR");
        }
    }
    // modify the QP to RTR
    rc = modify_qp_to_rtr(
        res.qp,
        res.remote_props.qp_num,
        res.remote_props.lid,
        &{ res.remote_props.gid },
        gid_idx,
        ib_port,
    );
    if rc != 0 {
        // goto connect_qp_exit;
        panic!("failed to modify QP state to RTR");
    }
    rc = modify_qp_to_rts(res.qp);
    if rc != 0 {
        // goto connect_qp_exit;
        panic!("failed to modify QP state to RTS");
    }
    println!("QP state was change to RTS");
    // sync to make sure that both sides are in states that they can connect to prevent packet loose
    // just send a dummy char back and forth
    let resp_msg = sync_data(res.sock, &"QP ready".to_owned());
    println!("received message: {}", resp_msg);
    // let char_buf_size = 1;
    // let mut temp_char_buf = Vec::with_capacity(char_buf_size);
    // rc = sock_sync_data(res.sock, char_buf_size, b"Q", &mut temp_char_buf).cast();
    // if rc < 0 {
    //     //rc = 1;
    //     panic!("sync error after QPs are were moved to RTS");
    // } else {
    //     rc = 0;
    // }
    rc
}

///
fn resources_destroy(res: &Resources) -> c_int {
    let mut rc: c_int;
    rc = unsafe { ibv_destroy_qp(res.qp) };
    debug_assert_eq!(rc, 0, "failed to destroy QP");
    rc = unsafe { ibv_dereg_mr(res.mr) };
    debug_assert_eq!(rc, 0, "failed to deregister MR");

    rc = unsafe { ibv_destroy_cq(res.cq) };
    debug_assert_eq!(rc, 0, "failed to destroy CQ");
    rc = unsafe { ibv_dealloc_pd(res.pd) };
    debug_assert_eq!(rc, 0, "failed to deallocate PD");
    rc = unsafe { ibv_close_device(res.ib_ctx) };
    debug_assert_eq!(rc, 0, "failed to close device context");

    rc = unsafe { libc::close(res.sock) };
    debug_assert_eq!(rc, 0, "failed to close socket");

    rc
}

///
#[allow(clippy::cognitive_complexity)]
pub fn run(
    server_name: &str,
    input_dev_name: &str,
    gid_idx: c_int,
    ib_port: u8,
    sock_port: u16,
) -> c_int {
    let mut rc: c_int;
    let sock = if server_name.is_empty() {
        // server side
        println!("waiting on port {} for TCP connection", sock_port);
        let listen_sock = sock_bind(sock_port);
        debug_assert!(listen_sock >= 0, "failed to bind TCP port {}", sock_port);
        let sock = sock_accept(listen_sock);
        debug_assert!(
            sock >= 0,
            "failed to accept TCP connection on port {}",
            sock_port,
        );
        sock
    } else {
        // client side
        let sock = sock_connect(server_name, sock_port);
        debug_assert!(
            sock >= 0,
            "failed to establish TCP connection to server {}, port {}",
            server_name,
            sock_port,
        );
        sock
    };

    // create resources before using them
    let mut res = resources_create(server_name, input_dev_name, gid_idx, ib_port, sock);
    // connect the QPs
    rc = connect_qp(&mut res, server_name, gid_idx, ib_port);
    debug_assert_eq!(rc, 0, "failed to connect QPs");
    // let the server post the sr
    if server_name.is_empty() {
        rc = post_send(&mut res, ibv_wr_opcode::IBV_WR_SEND);
        debug_assert_eq!(rc, 0, "failed to post sr");
    }
    // in both sides we expect to get a completion
    rc = poll_completion(&mut res);
    debug_assert_eq!(rc, 0, "poll completion failed");
    if server_name.is_empty() {
        // setup server buffer with read message
        res.buf.copy_from_slice(
            RDMAMSGR
                .as_bytes()
                .get(0..MSG_SIZE)
                .unwrap_or_else(|| panic!("failed to slicing")),
        );
    } else {
        // after polling the completion we have the message in the client buffer too
        let recv_msg = String::from_utf8_lossy(&*res.buf);
        println!("client side message is: {:?}", recv_msg);
    }
    // Sync so we are sure server side has data ready before client tries to read it
    // just send a dummy char back and forth
    let resp_msg = sync_data(res.sock, &"ready to read".to_owned());
    println!("received message: {}", resp_msg);
    // let char_buf_size = 1;
    // let mut temp_char_buf = Vec::with_capacity(char_buf_size);
    // rc = sock_sync_data(res.sock, char_buf_size, b"R", &mut temp_char_buf).cast();
    // if rc < 0 {
    //     //rc = 1;
    //     //goto main_exit;
    //     panic!("sync error before RDMA ops");
    // }

    // Now the client performs an RDMA read and then write on server.
    // Note that the server has no idea these events have occured
    if !server_name.is_empty() {
        // First we read contens of server's buffer
        rc = post_send(&mut res, ibv_wr_opcode::IBV_WR_RDMA_READ);
        debug_assert_eq!(rc, 0, "failed to post SR 2");
        rc = poll_completion(&mut res);
        debug_assert_eq!(rc, 0, "poll completion failed 2");
        let srv_msg = String::from_utf8_lossy(&*res.buf);
        println!("contents of server's buffer: {}", srv_msg);
        // Now we replace what's in the server's buffer
        res.buf.copy_from_slice(
            RDMAMSGW
                .as_bytes()
                .get(0..MSG_SIZE)
                .unwrap_or_else(|| panic!("failed to slicing")),
        );
        println!("now replacing it with: {}", RDMAMSGW);
        rc = post_send(&mut res, ibv_wr_opcode::IBV_WR_RDMA_WRITE);
        debug_assert_eq!(rc, 0, "failed to post SR 3");
        rc = poll_completion(&mut res);
        debug_assert_eq!(rc, 0, "poll completion failed 3");
    }
    // Sync so server will know that client is done mucking with its memory
    // just send a dummy char back and forth
    let resp_msg = sync_data(res.sock, &"done".to_owned());
    println!("received message: {}", resp_msg);
    // rc = sock_sync_data(res.sock, char_buf_size, b"W", &mut temp_char_buf).cast();
    // if rc < 0 {
    //     //rc = 1;
    //     //goto main_exit;
    //     panic!("sync error after RDMA ops");
    // }
    if server_name.is_empty() {
        let recv_msg = String::from_utf8_lossy(&*res.buf);
        println!("Contents of server buffer: {:?}", recv_msg);
    }
    // main_exit:
    rc = resources_destroy(&mut res);
    debug_assert_eq!(rc, 0, "failed to destroy resources");
    // if (config.dev_name)
    // 	free((char *)config.dev_name);
    println!("\ntest result is: {}", rc);
    rc
}
