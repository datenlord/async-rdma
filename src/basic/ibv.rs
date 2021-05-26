// The network byte order is defined to always be big-endian.
// X86 is little-endian.

use rdma_sys::*;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::cmp::Ordering;
use std::ffi::{CStr, CString};
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};
use std::os::raw::{c_int, c_uint, c_void};
use utilities::{Cast, OverflowArithmetic};

use super::util;

///
const MSG: &str = "SEND operation";
///
const RDMAMSGR: &str = "RDMA read operation";
///
const RDMAMSGW: &str = "RDMA write operation";
///
const MSG_SIZE: usize = 32;
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

pub enum TcpSock {
    Listener(TcpListener),
    Stream(TcpStream),
}

impl TcpSock {
    ///
    pub fn bind(port: u16) -> Self {
        let sock_addr = format!("0.0.0.0:{}", port);
        let tcp_listener = TcpListener::bind(&sock_addr)
            .unwrap_or_else(|err| panic!("failed to bind to {}, the error is: {}", sock_addr, err));
        Self::Listener(tcp_listener)
    }

    fn listener(&self) -> &TcpListener {
        match self {
            Self::Listener(srv) => srv,
            Self::Stream(_) => panic!("cannot return TcpListener from TcpSock::CLIENT"),
        }
    }

    fn stream(&self) -> &TcpStream {
        match self {
            Self::Listener(_) => panic!("cannot return TcpListener from TcpSock::CLIENT"),
            Self::Stream(clnt) => clnt,
        }
    }

    ///
    pub fn accept(&self) -> Self {
        match self.listener().accept() {
            Ok((tcp_stream, addr)) => {
                println!("new client: {:?}", addr);
                Self::Stream(tcp_stream)
            }
            Err(e) => panic!("couldn't get client: {:?}", e),
        }
    }

    ///
    pub fn connect(server_name: &str, port: u16) -> Self {
        let sock_addr = format!("{}:{}", server_name, port);
        let tcp_stream = TcpStream::connect(&sock_addr).unwrap_or_else(|err| {
            panic!("failed to connect to {}, the error is: {}", sock_addr, err)
        });
        Self::Stream(tcp_stream)
    }

    ///
    fn exchange_data<T: Serialize, U: DeserializeOwned>(&self, data: &T) -> U {
        let xfer_size = std::mem::size_of::<T>();
        let encoded: Vec<u8> = bincode::serialize(data)
            .unwrap_or_else(|err| panic!("failed to encode, the error is: {}", err));
        let send_size = self
            .stream()
            .write(&encoded)
            .unwrap_or_else(|err| panic!("failed to send data via socket, the error is: {}", err));
        debug_assert_eq!(send_size, encoded.len(), "socket send data size not match");
        let mut decode_buf = Vec::with_capacity(xfer_size);
        unsafe {
            decode_buf.set_len(xfer_size);
        }
        let recv_size = self.stream().read(&mut decode_buf).unwrap_or_else(|err| {
            panic!("failed to receive data via socket, the error is:{}", err)
        });
        unsafe {
            decode_buf.set_len(recv_size.cast());
        }
        debug_assert!(recv_size > 0, "failed to receive data from socket");

        bincode::deserialize(&decode_buf)
            .unwrap_or_else(|err| panic!("failed to decode, the error is: {}", err))
    }
}

/*
/// Tcp socket wrapper
pub struct TcpSocket {
    /// Socket handler
    sock: std::os::unix::io::RawFd,
}

impl Drop for TcpSocket {
    fn drop(&mut self) {
        unsafe { libc::close(self.sock) };
    }
}

impl TcpSocket {
    ///
    fn resolve_addr(
        server_name: &str,
        port: u16,
        resolved_addr: *mut *mut libc::addrinfo,
    ) -> c_int {
        let server_addr_cstr = CString::new(server_name).unwrap_or_else(|err| {
            panic!(
                "failed to build server address CString, the error is: {}",
                err,
            )
        });
        let server_port_cstr = CString::new(port.to_string())
            .unwrap_or_else(|err| panic!("failed to build port CString, the error is: {}", err,));
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
    pub fn connect(server_name: &str, port: u16) -> Self {
        let mut rc: c_int;
        let mut resolved_addr = std::ptr::null_mut::<libc::addrinfo>();
        rc = Self::resolve_addr(server_name, port, &mut resolved_addr);
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
        Self { sock: sockfd }
    }

    ///
    pub fn bind(port: u16) -> Self {
        let mut rc: c_int;
        let mut resolved_addr = std::ptr::null_mut::<libc::addrinfo>();
        let empty_server_name = "";
        rc = Self::resolve_addr(empty_server_name, port, &mut resolved_addr);
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
        Self { sock: listenfd }
    }

    ///
    pub fn accept(&self) -> Self {
        let backlog = 1;
        let rc = unsafe { libc::listen(self.sock, backlog) };
        if rc != 0 {
            panic!("socket listen failed");
        }
        let client_addr = std::ptr::null_mut::<libc::sockaddr>();
        let mut addr_len = 0;
        let rmt_sockfd = unsafe { libc::accept(self.sock, client_addr, &mut addr_len) };
        if rmt_sockfd < 0 {
            panic!("socket accept failed");
        }
        Self { sock: rmt_sockfd }
    }

    /// TODO: use system approach for state sync
    fn exchange_data<T: Serialize, U: DeserializeOwned>(&self, data: &T) -> U {
        let xfer_size = std::mem::size_of::<T>();
        let mut encoded: Vec<u8> = bincode::serialize(data)
            .unwrap_or_else(|err| panic!("failed to encode, the error is: {}", err));
        let mut decode_buf = Vec::with_capacity(xfer_size);
        let rc = unsafe {
            libc::write(
                self.sock,
                util::mut_ptr_cast(encoded.as_mut_ptr()),
                encoded.len(),
            )
        };
        debug_assert_eq!(rc, encoded.len().cast(), "failed to send data via socket",);
        let recv_size = unsafe {
            libc::read(
                self.sock,
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
}
*/

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
}

/// RDMA resources
pub struct Resources {
    ///
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
    /// TCP socket to the remote peer of QP
    sock: TcpSock,
}

impl Drop for Resources {
    fn drop(&mut self) {
        let mut rc: c_int;
        rc = unsafe { ibv_destroy_qp(self.qp) };
        debug_assert_eq!(rc, 0, "failed to destroy QP");
        rc = unsafe { ibv_dereg_mr(self.mr) };
        debug_assert_eq!(rc, 0, "failed to deregister MR");

        rc = unsafe { ibv_destroy_cq(self.cq) };
        debug_assert_eq!(rc, 0, "failed to destroy CQ");
        rc = unsafe { ibv_dealloc_pd(self.pd) };
        debug_assert_eq!(rc, 0, "failed to deallocate PD");
        rc = unsafe { ibv_close_device(self.ib_ctx) };
        debug_assert_eq!(rc, 0, "failed to close device context");
    }
}

impl Resources {
    ///
    pub fn buf_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.buf.as_ptr(), self.buf.len()) }
    }

    ///
    pub fn buf_slice_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.buf.as_mut_ptr(), self.buf.len()) }
    }

    ///
    pub fn poll_completion(&self) -> c_int {
        let cq_addr = util::ptr_to_usize(self.cq);
        Self::poll_completion_cb(cq_addr)
    }

    ///
    pub fn async_poll_completion(&self) -> std::thread::JoinHandle<c_int> {
        let cq_addr = util::ptr_to_usize(self.cq);

        std::thread::spawn(move || Self::poll_completion_cb(cq_addr))
    }

    ///
    fn poll_completion_cb(cq_addr: usize) -> c_int {
        let cq = unsafe { util::usize_to_mut_ptr::<ibv_cq>(cq_addr) };
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
            poll_result = unsafe { ibv_poll_cq(cq, 1, &mut wc) };
            unsafe { libc::gettimeofday(&mut cur_time, time_zone) };
            cur_time_msec = (cur_time.tv_sec.overflow_mul(1000))
                .overflow_add(cur_time.tv_usec.overflow_div(1000));
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
    pub fn post_send(&self, opcode: c_uint) -> c_int {
        let mut sr = unsafe { std::mem::zeroed::<ibv_send_wr>() };
        let mut sge = unsafe { std::mem::zeroed::<ibv_sge>() };
        let mut bad_wr = std::ptr::null_mut::<ibv_send_wr>();
        // prepare the scatter/gather entry
        sge.addr = util::ptr_to_usize(self.buf.as_ptr()).cast();
        sge.length = MSG_SIZE.cast();
        sge.lkey = unsafe { (*self.mr).lkey };
        // prepare the send work request
        sr.next = std::ptr::null_mut();
        sr.wr_id = 0;
        sr.sg_list = &mut sge;
        sr.num_sge = 1;
        sr.opcode = opcode;
        sr.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
        if opcode != ibv_wr_opcode::IBV_WR_SEND {
            sr.wr.rdma.remote_addr = self.remote_props.addr;
            sr.wr.rdma.rkey = self.remote_props.rkey;
        }
        // there is a Receive Request in the responder side, so we won't get any into RNR flow
        let rc = unsafe { ibv_post_send(self.qp, &mut sr, &mut bad_wr) };
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
    pub fn post_receive(&self) -> c_int {
        let mut rr = unsafe { std::mem::zeroed::<ibv_recv_wr>() };
        let mut sge = unsafe { std::mem::zeroed::<ibv_sge>() };
        let mut bad_wr = std::ptr::null_mut::<ibv_recv_wr>();

        // prepare the scatter/gather entry
        sge.addr = util::ptr_to_usize(self.buf.as_ptr()).cast();
        sge.length = MSG_SIZE.cast();
        sge.lkey = unsafe { (*self.mr).lkey };
        // prepare the receive work request
        rr.next = std::ptr::null_mut();
        rr.wr_id = 0;
        rr.sg_list = &mut sge;
        rr.num_sge = 1;
        // post the Receive Request to the RQ
        let rc = unsafe { ibv_post_recv(self.qp, &mut rr, &mut bad_wr) };
        if rc == 0 {
            println!("Receive Request was posted");
        } else {
            panic!("failed to post RR");
        }
        rc
    }

    ///
    pub fn new(input_dev_name: &str, gid_idx: c_int, ib_port: u8, sock: TcpSock) -> Self {
        let mut rc: c_int;
        // Searching for IB devices in host
        let ib_ctx = Self::open_ib_ctx(input_dev_name);

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
            panic!("ibv_reg_mr failed with mr_flags={}", mr_flags);
        }
        println!(
            "MR was registered with addr={:x}, lkey={:x}, rkey={:x}, flags={}",
            util::ptr_to_usize(buf.as_ptr()),
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
            panic!("failed to create QP");
        }
        println!("QP was created, QP number={:x}", unsafe { (*qp).qp_num });

        // Exchange using TCP sockets info required to connect QPs
        let mut local_con_data = unsafe { std::mem::zeroed::<CmConData>() };
        local_con_data.addr = util::ptr_to_usize(buf.as_ptr()).cast();
        local_con_data.rkey = unsafe { (*mr).rkey };
        local_con_data.qp_num = unsafe { (*qp).qp_num };
        local_con_data.lid = port_attr.lid;
        local_con_data.gid = u128::from_be_bytes(unsafe { my_gid.raw });
        println!("local connection data: {}", local_con_data);
        let local_con_data_be = local_con_data.into_be();
        let remote_con_data_be: CmConData = sock.exchange_data(&local_con_data_be);
        let remote_con_data = remote_con_data_be.into_le();
        println!("remote connection data: {}", remote_con_data);

        let res = Self {
            remote_props: remote_con_data,
            ib_ctx,
            pd,
            cq,
            qp,
            mr,
            buf,
            sock,
        };

        // Connect the QPs
        rc = res.connect_qp(gid_idx, ib_port);
        debug_assert_eq!(rc, 0, "failed to connect QPs");
        res
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
    fn connect_qp(&self, gid_idx: c_int, ib_port: u8) -> c_int {
        let mut rc: c_int;
        // modify the QP to init
        rc = self.modify_qp_to_init(ib_port);
        if rc != 0 {
            panic!("change QP state to INIT failed");
        }
        // modify the QP to RTR
        rc = self.modify_qp_to_rtr(
            // self.qp,
            // self.remote_props.qp_num,
            // self.remote_props.lid,
            // &{ self.remote_props.gid },
            gid_idx, ib_port,
        );
        if rc != 0 {
            panic!("failed to modify QP state to RTR");
        }
        rc = self.modify_qp_to_rts();
        if rc != 0 {
            panic!("failed to modify QP state to RTS");
        }
        println!("QP state was change to RTS");
        rc
    }

    ///
    fn modify_qp_to_init(&self, ib_port: u8) -> c_int {
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
        let rc = unsafe { ibv_modify_qp(self.qp, &mut attr, flags.0.cast()) };
        if rc != 0 {
            panic!("failed to modify QP state to INIT");
        }
        rc
    }

    ///
    fn modify_qp_to_rtr(
        &self,
        // qp: *mut ibv_qp,
        // remote_qpn: u32,
        // dlid: u16,
        // d_gid: &u128,
        gid_idx: c_int,
        ib_port: u8,
    ) -> c_int {
        let mut attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
        attr.qp_state = ibv_qp_state::IBV_QPS_RTR;
        attr.path_mtu = ibv_mtu::IBV_MTU_256;
        attr.dest_qp_num = self.remote_props.qp_num;
        attr.rq_psn = 0;
        attr.max_dest_rd_atomic = 1;
        attr.min_rnr_timer = 0x12;
        attr.ah_attr.is_global = 0;
        attr.ah_attr.dlid = self.remote_props.lid;
        attr.ah_attr.sl = 0;
        attr.ah_attr.src_path_bits = 0;
        attr.ah_attr.port_num = ib_port;
        if gid_idx >= 0 {
            attr.ah_attr.is_global = 1;
            attr.ah_attr.port_num = 1;
            attr.ah_attr.grh.dgid.raw = self.remote_props.gid.to_be_bytes();
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
        let rc = unsafe { ibv_modify_qp(self.qp, &mut attr, flags.0.cast()) };
        if rc != 0 {
            panic!("failed to modify QP state to RTR");
        }
        rc
    }

    ///
    fn modify_qp_to_rts(&self) -> c_int {
        let mut attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
        attr.qp_state = ibv_qp_state::IBV_QPS_RTS;
        attr.timeout = 0x12; // TODO: use input arg
        attr.retry_cnt = 6; // TODO: use input arg
        attr.rnr_retry = 0; // TODO: use input arg
        attr.sq_psn = 0;
        attr.max_rd_atomic = 1;
        let flags = ibv_qp_attr_mask::IBV_QP_STATE
            | ibv_qp_attr_mask::IBV_QP_TIMEOUT
            | ibv_qp_attr_mask::IBV_QP_RETRY_CNT
            | ibv_qp_attr_mask::IBV_QP_RNR_RETRY
            | ibv_qp_attr_mask::IBV_QP_SQ_PSN
            | ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC;
        let rc = unsafe { ibv_modify_qp(self.qp, &mut attr, flags.0.cast()) };
        if rc != 0 {
            panic!("failed to modify QP state to RTS");
        }
        rc
    }
}

///
fn copy_to_buf_pad(dst: &mut [u8], src: &str) {
    let src_str = if dst.len() <= src.len() {
        format!(
            "{}\0",
            src.get(0..(dst.len().overflow_sub(1)))
                .unwrap_or_else(|| panic!("failed to slice src: {}", src))
        )
    } else {
        let padding = std::iter::repeat("\0")
            .take(dst.len().overflow_sub(src.len()))
            .collect::<String>();
        format!("{}{}", src, padding)
    };
    debug_assert_eq!(dst.len(), src_str.len(), "src str size not match dst");
    dst.copy_from_slice(src_str.as_bytes());
}
/*
///
pub fn run(
    server_name: &str,
    input_dev_name: &str,
    gid_idx: c_int,
    ib_port: u8,
    sock_port: u16,
) -> c_int {
    let mut rc: c_int;
    let client_sock = if server_name.is_empty() {
        // server side
        println!("waiting on port {} for TCP connection", sock_port);
        let listen_sock = TcpSocket::bind(sock_port);
        listen_sock.accept()
    } else {
        // client side
        TcpSocket::connect(server_name, sock_port)
    };

    // create resources before using them
    let mut res = Resources::new(server_name, input_dev_name, gid_idx, ib_port, client_sock);
    // let the server post the sr
    if server_name.is_empty() {
        // Only in the server side put the message in the memory buffer
        copy_to_buf_pad(res.buf_slice_mut(), MSG);
        // res.buf_slice_mut()
        //     .copy_from_slice(format!("{}{}", MSG, "\0").as_bytes());
        println!("going to send the message: \"{}\"", MSG);
        rc = res.post_send(ibv_wr_opcode::IBV_WR_SEND);
        debug_assert_eq!(rc, 0, "failed to post sr");
    }
    // in both sides we expect to get a completion
    rc = res.poll_completion();
    debug_assert_eq!(rc, 0, "poll completion failed");
    let resp_send_size: State = res.sock.exchange_data(&State::SendSize(MSG.len()));
    let send_size = if let State::SendSize(send_size) = resp_send_size {
        println!("receive send size: {}", send_size);
        send_size
    } else {
        panic!("failed to receive send size")
    };

    if server_name.is_empty() {
        // setup server buffer with read message
        copy_to_buf_pad(res.buf_slice_mut(), RDMAMSGR);
        // res.buf_slice_mut().copy_from_slice(
        //     RDMAMSGR
        //         .as_bytes()
        //         .get(0..MSG_SIZE)
        //         .unwrap_or_else(|| panic!("failed to slicing")),
        // );
    } else {
        // after polling the completion we have the message in the client buffer too
        // let recv_msg = String::from_utf8_lossy(res.buf_slice());
        let recv_msg = std::str::from_utf8(
            res.buf_slice()
                .get(0..send_size)
                .unwrap_or_else(|| panic!("failed to slice to send size {}", send_size)),
        )
        .unwrap_or_else(|err| panic!("failed to build str from bytes, the error is: {}", err));
        println!("client received send message is: {:?}", recv_msg);
    }
    // Sync so we are sure server side has data ready before client tries to read it
    // just send a dummy char back and forth
    let resp_read_size: State = res.sock.exchange_data(&State::ReadSize(RDMAMSGR.len()));
    let read_size = if let State::ReadSize(read_size) = resp_read_size {
        println!("receive read size: {}", read_size);
        read_size
    } else {
        panic!("failed to receive read size")
    };
    // let resp_msg: String = res.sock.exchange_data(&"ready to read".to_owned());
    // println!("received message: {}", resp_msg);

    // Now the client performs an RDMA read and then write on server.
    // Note that the server has no idea these events have occured
    if !server_name.is_empty() {
        // First we read contens of server's buffer
        rc = res.post_send(ibv_wr_opcode::IBV_WR_RDMA_READ);
        debug_assert_eq!(rc, 0, "failed to post SR 2");
        rc = res.poll_completion();
        debug_assert_eq!(rc, 0, "poll completion failed 2");
        // let read_msg = String::from_utf8_lossy(res.buf_slice());

        let read_msg = std::str::from_utf8(
            res.buf_slice()
                .get(0..read_size)
                .unwrap_or_else(|| panic!("failed to slice to read size {}", read_size)),
        )
        .unwrap_or_else(|err| panic!("failed to build str from bytes, the error is: {}", err));
        println!("client read server buffer: {}", read_msg);
        // Now we replace what's in the server's buffer
        copy_to_buf_pad(res.buf_slice_mut(), RDMAMSGW);
        // res.buf_slice_mut().copy_from_slice(
        //     RDMAMSGW
        //         .as_bytes()
        //         .get(0..MSG_SIZE)
        //         .unwrap_or_else(|| panic!("failed to slicing")),
        // );
        println!("now replacing it with: {}", RDMAMSGW);
        rc = res.post_send(ibv_wr_opcode::IBV_WR_RDMA_WRITE);
        debug_assert_eq!(rc, 0, "failed to post SR 3");
        rc = res.poll_completion();
        debug_assert_eq!(rc, 0, "poll completion failed 3");
    }
    // Sync so server will know that client is done mucking with its memory
    // just send a dummy char back and forth
    let resp_write_size: State = res.sock.exchange_data(&State::WriteSize(RDMAMSGW.len()));
    let write_size = if let State::WriteSize(write_size) = resp_write_size {
        println!("receive write size: {}", write_size);
        write_size
    } else {
        panic!("failed to receive write size")
    };
    // let resp_done: String = res.sock.exchange_data(&"done".to_owned());
    // println!("received message: {:?}", resp_done);
    if server_name.is_empty() {
        let write_msg = std::str::from_utf8(
            res.buf_slice()
                .get(0..write_size)
                .unwrap_or_else(|| panic!("failed to slice to write size {}", write_size)),
        )
        .unwrap_or_else(|err| panic!("failed to build str from bytes, the error is: {}", err));
        println!("client write to server buffer: {:?}", write_msg);
    }
    println!("\ntest result is: {}", rc);
    rc
}
*/
///
pub fn run_client(
    server_name: &str,
    input_dev_name: &str,
    gid_idx: c_int,
    ib_port: u8,
    sock_port: u16,
) -> c_int {
    let mut rc: c_int;
    // client side
    let client_sock = TcpSock::connect(server_name, sock_port);

    // Create resources before using them
    let mut res = Resources::new(input_dev_name, gid_idx, ib_port, client_sock);
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
    // Both sides expect to get a completion
    rc = res.poll_completion();
    debug_assert_eq!(rc, 0, "poll completion failed");
    let resp_send_size: State = res.sock.exchange_data(&State::SendSize(INVALID_SIZE));
    let send_size = if let State::SendSize(send_size) = resp_send_size {
        println!("receive send size from server: {}", send_size);
        send_size
    } else {
        panic!("failed to receive send size")
    };

    // after polling the completion we have the message in the client buffer too
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
        panic!("failed to receive read size")
    };

    // Now the client performs an RDMA read and then write on server.
    // Note that the server has no idea these events have occured
    // First client read contens of server's buffer
    rc = res.post_send(ibv_wr_opcode::IBV_WR_RDMA_READ);
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

    // Next client write data to server's buffer
    copy_to_buf_pad(res.buf_slice_mut(), RDMAMSGW);
    println!("now to server with data: {}", RDMAMSGW);
    rc = res.post_send(ibv_wr_opcode::IBV_WR_RDMA_WRITE);
    debug_assert_eq!(rc, 0, "failed to post SR 3");
    // rc = res.poll_completion();
    // debug_assert_eq!(rc, 0, "poll completion failed 3");
    let poll_handler = res.async_poll_completion();

    // Sync with server the size of write data
    let resp_write_size: State = res
        .sock
        .exchange_data(&State::WriteSize(RDMAMSGW.len().cast()));
    if let State::WriteSize(write_size) = resp_write_size {
        println!("receive write size from server: {}", write_size);
    } else {
        panic!("failed to receive write size");
    }

    let poll_res = poll_handler.join();
    if let Err(err) = poll_res {
        panic!("async poll completion failed, the error is: {:?}", err);
    }

    println!("\ntest result is: {}", rc);
    rc
}

///
pub fn run_server(input_dev_name: &str, gid_idx: c_int, ib_port: u8, sock_port: u16) -> c_int {
    let mut rc: c_int;

    println!("waiting on port {} for TCP connection", sock_port);
    let listen_sock = TcpSock::bind(sock_port);
    let client_sock = listen_sock.accept();

    // Create resources
    let mut res = Resources::new(input_dev_name, gid_idx, ib_port, client_sock);
    // Only in the server side put the message in the memory buffer
    copy_to_buf_pad(res.buf_slice_mut(), MSG);
    // Sync with client before send
    let resp_recv_ready: State = res.sock.exchange_data(&State::ReceiveReady);
    if let State::ReceiveReady = resp_recv_ready {
        println!("receive ready: {:?}", resp_recv_ready);
    } else {
        panic!("failed to receive ready");
    }
    println!("going to send the message: \"{}\"", MSG);
    rc = res.post_send(ibv_wr_opcode::IBV_WR_SEND);
    debug_assert_eq!(rc, 0, "failed to post sr");

    // Both sides expect to get a completion
    rc = res.poll_completion();
    debug_assert_eq!(rc, 0, "poll completion failed");
    let resp_send_size: State = res.sock.exchange_data(&State::SendSize(MSG.len().cast()));
    if let State::SendSize(send_size) = resp_send_size {
        println!("receive send size from client: {}", send_size);
    } else {
        panic!("failed to receive send size");
    }

    // Setup server buffer with read message
    copy_to_buf_pad(res.buf_slice_mut(), RDMAMSGR);
    // Sync with client the size of read data from server
    let resp_read_size: State = res
        .sock
        .exchange_data(&State::ReadSize(RDMAMSGR.len().cast()));
    if let State::ReadSize(read_size) = resp_read_size {
        println!("receive read size from client: {}", read_size);
    } else {
        panic!("failed to receive read size");
    }

    // Sync with client the size of write data to server
    let resp_write_size: State = res.sock.exchange_data(&State::WriteSize(INVALID_SIZE));
    let write_size = if let State::WriteSize(write_size) = resp_write_size {
        println!("receive write size from client: {}", write_size);
        write_size
    } else {
        panic!("failed to receive write size")
    };
    let write_msg = std::str::from_utf8(
        res.buf_slice()
            .get(0..(write_size.cast()))
            .unwrap_or_else(|| panic!("failed to slice to write size {}", write_size)),
    )
    .unwrap_or_else(|err| panic!("failed to build str from bytes, the error is: {}", err));
    println!("client write data to server buffer: {:?}", write_msg);

    println!("\ntest result is: {}", rc);
    rc
}
