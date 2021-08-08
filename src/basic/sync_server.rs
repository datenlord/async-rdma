use rdma_sys::{
    ibv_access_flags, ibv_ack_cq_events, ibv_alloc_pd, ibv_comp_channel, ibv_cq,
    ibv_create_comp_channel, ibv_create_cq, ibv_get_cq_event, ibv_mr, ibv_pd, ibv_poll_cq,
    ibv_post_recv, ibv_post_send, ibv_qp_cap, ibv_qp_init_attr, ibv_qp_type, ibv_recv_wr,
    ibv_reg_mr, ibv_req_notify_cq, ibv_send_flags, ibv_send_wr, ibv_sge, ibv_srq, ibv_wc,
    ibv_wc_opcode, ibv_wc_status, ibv_wr_opcode, rdma_accept, rdma_ack_cm_event, rdma_bind_addr,
    rdma_cm_event, rdma_cm_event_type, rdma_cm_id, rdma_conn_param, rdma_connect,
    rdma_create_event_channel, rdma_create_id, rdma_create_qp, rdma_get_cm_event,
    rdma_get_src_port, rdma_listen, rdma_port_space, rdma_resolve_addr, rdma_resolve_route, rdma_t,
    wr_t,
};
use std::ffi::CString;
use std::os::raw::{c_int, c_void};
use utilities::Cast;
use utilities::OverflowArithmetic;

use super::util;

///
const TIMEOUT_IN_MS: c_int = 500;
///
const BUF_SIZE: usize = 1024;
///
struct PData {
    ///
    buf_va: u64,
    ///
    buf_rkey: u32,
}

///
#[allow(
    clippy::too_many_lines,
    clippy::indexing_slicing,
    clippy::cognitive_complexity
)]
pub fn client(server_addr: &str, server_port: u16) {
    // Set up RDMA CM structures

    let mut dst_addr = std::ptr::null_mut::<libc::addrinfo>();
    let hint = std::ptr::null::<libc::addrinfo>();

    let mut ret: c_int;
    let server_addr_cstr = CString::new(server_addr).unwrap_or_else(|err| {
        panic!(
            "failed to build server address CString, the error is: {}",
            err,
        )
    });
    let server_port_cstr = CString::new(server_port.to_string()).unwrap_or_else(|err| {
        panic!("failed to build server port CString, the error is: {}", err,)
    });
    ret = unsafe {
        libc::getaddrinfo(
            server_addr_cstr.as_ptr(),
            server_port_cstr.as_ptr(),
            hint,
            &mut dst_addr,
        )
    };
    debug_assert_eq!(ret, 0, "rdma_create_id failed");
    //util::check_errno(ret)?;

    let event_channel = unsafe { rdma_create_event_channel() };
    debug_assert_ne!(
        event_channel,
        std::ptr::null_mut(),
        "rdma_create_event_channel failed"
    );
    //util::check_errno(ret)?;

    let mut client_id = std::ptr::null_mut::<rdma_cm_id>();
    let context: *mut c_void = std::ptr::null_mut();
    ret = unsafe {
        rdma_create_id(
            event_channel,
            &mut client_id,
            context,
            rdma_port_space::RDMA_PS_TCP,
        )
    };
    debug_assert_eq!(ret, 0, "rdma_create_id failed");
    //util::check_errno(ret)?;

    let src_addr = std::ptr::null_mut::<libc::sockaddr>();
    ret = unsafe { rdma_resolve_addr(client_id, src_addr, (*dst_addr).ai_addr, TIMEOUT_IN_MS) };
    debug_assert_eq!(ret, 0, "rdma_resolve_addr failed");
    //util::check_errno(ret)?;
    unsafe { libc::freeaddrinfo(dst_addr) };

    let mut event = std::ptr::null_mut::<rdma_cm_event>();
    ret = unsafe { rdma_get_cm_event(util::const_ptr_cast_mut(event_channel), &mut event) };
    debug_assert_eq!(ret, 0, "rdma_get_cm_event failed");
    let mut event_ref = unsafe { &*event };
    debug_assert_eq!(
        util::const_ptr_cast_mut(client_id),
        event_ref.id,
        "client ID not match",
    );
    debug_assert_eq!(
        event_ref.event,
        rdma_cm_event_type::RDMA_CM_EVENT_ADDR_RESOLVED,
        "match RDMA_CM_EVENT_ADDR_RESOLVED failed",
    );
    ret = unsafe { rdma_ack_cm_event(event) };
    debug_assert_eq!(ret, 0, "rdma_ack_cm_event failed");

    ret = unsafe { rdma_resolve_route(client_id, TIMEOUT_IN_MS) };
    debug_assert_eq!(ret, 0, "rdma_resolve_route failed");

    ret = unsafe { rdma_get_cm_event(util::const_ptr_cast_mut(event_channel), &mut event) };
    debug_assert_eq!(ret, 0, "rdma_get_cm_event failed");
    event_ref = unsafe { &*event };
    debug_assert_eq!(
        event_ref.event,
        rdma_cm_event_type::RDMA_CM_EVENT_ROUTE_RESOLVED,
        "match RDMA_CM_EVENT_ROUTE_RESOLVED failed",
    );
    ret = unsafe { rdma_ack_cm_event(event) };
    debug_assert_eq!(ret, 0, "rdma_ack_cm_event failed");

    // Create verbs objects now that we know which device to use

    let ctx = unsafe { (*client_id).verbs };
    let pd = unsafe { ibv_alloc_pd(ctx) };
    debug_assert_ne!(pd, std::ptr::null_mut::<ibv_pd>(), "ibv_alloc_pd failed",);
    let comp_channel = unsafe { ibv_create_comp_channel(ctx) };
    debug_assert_ne!(
        comp_channel,
        std::ptr::null_mut::<ibv_comp_channel>(),
        "ibv_create_comp_channel failed",
    );
    let cqe = 10; // cqe=10 is arbitrary
    let mut cq_context = std::ptr::null_mut::<c_void>();
    let comp_vector = 0;
    let mut cq = unsafe { ibv_create_cq(ctx, cqe, cq_context, comp_channel, comp_vector) };
    debug_assert_ne!(cq, std::ptr::null_mut::<ibv_cq>(), "ibv_create_cq failed",);
    let solicited_only = 0;
    ret = unsafe { ibv_req_notify_cq(&mut *cq, solicited_only) };
    debug_assert_eq!(ret, 0, "ibv_req_notify_cq failed");

    let mut buf = Box::pin([0_u8; BUF_SIZE]);
    let mr = unsafe {
        ibv_reg_mr(
            (*client_id).pd,
            util::mut_ptr_cast(buf.as_mut_ptr()),
            BUF_SIZE,
            ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0.cast(),
        )
    };
    debug_assert_ne!(mr, std::ptr::null_mut::<ibv_mr>(), "ibv_reg_mr failed");

    let mut qp_attr = ibv_qp_init_attr {
        qp_context: std::ptr::null_mut::<c_void>(),
        send_cq: cq,
        recv_cq: cq,
        srq: std::ptr::null_mut::<ibv_srq>(),
        cap: ibv_qp_cap {
            max_send_wr: 10,
            max_recv_wr: 10,
            max_send_sge: 1,
            max_recv_sge: 1,
            max_inline_data: 0,
        },
        qp_type: ibv_qp_type::IBV_QPT_RC,
        sq_sig_all: 0,
    };
    ret = unsafe { rdma_create_qp(client_id, pd, &mut qp_attr) };
    debug_assert_eq!(ret, 0, "rdma_create_qp failed");

    // Connect to server

    let mut cm_params: rdma_conn_param = unsafe { std::mem::zeroed() };
    cm_params.initiator_depth = 1;
    cm_params.retry_count = 7;
    ret = unsafe { rdma_connect(client_id, &mut cm_params) };
    debug_assert_eq!(ret, 0, "rdma_connect failed");

    ret = unsafe { rdma_get_cm_event(util::const_ptr_cast_mut(event_channel), &mut event) };
    debug_assert_eq!(ret, 0, "rdma_get_cm_event failed");
    event_ref = unsafe { &*event };
    debug_assert_eq!(
        event_ref.event,
        rdma_cm_event_type::RDMA_CM_EVENT_ESTABLISHED,
        "match RDMA_CM_EVENT_ESTABLISHED failed",
    );

    let mut buf_va = 0_u64;
    let mut buf_rkey = 0_u32;
    let server_pdata: *const PData =
        util::const_ptr_cast_mut(unsafe { event_ref.param.conn.private_data });
    if !server_pdata.is_null() {
        debug_assert_eq!(
            unsafe { event_ref.param.conn.private_data_len },
            std::mem::size_of::<PData>().cast(),
            "private data size not match",
        );
        buf_va = unsafe { (*server_pdata).buf_va };
        buf_rkey = unsafe { (*server_pdata).buf_rkey };
    }

    ret = unsafe { rdma_ack_cm_event(event) };
    debug_assert_eq!(ret, 0, "rdma_ack_cm_event failed");

    // Prepost receive

    let nsge = 1;
    let mut sgl = ibv_sge {
        addr: util::ptr_to_usize(buf.as_ptr()).cast(),
        length: BUF_SIZE.cast(), //buf.capacity().cast(),
        lkey: unsafe { (*mr).lkey },
    };
    let mut recv_wr = ibv_recv_wr {
        wr_id: 0, //util::ptr_to_usize(cm_id).cast(),
        next: std::ptr::null_mut(),
        sg_list: &mut sgl,
        num_sge: nsge,
    };
    let mut bad_recv_wr = std::ptr::null_mut::<ibv_recv_wr>();

    ret = unsafe { ibv_post_recv((*client_id).qp, &mut recv_wr, &mut bad_recv_wr) };
    debug_assert_eq!(ret, 0, "ibv_post_recv failed");

    // Write/send two integers to be added

    let val1 = 7_u8;
    let val2 = 15_u8;
    util::copy_slice(&mut *buf, &[val1, val2]);
    // debug_assert_eq!(buf.len(), 2, "buf size not match");

    let send_msg = format!(
        "message from active/client side with pid={}",
        std::process::id(),
    );
    util::copy_slice(&mut *buf, send_msg.as_bytes());

    sgl = ibv_sge {
        addr: util::ptr_to_usize(buf.as_ptr()).cast(),
        length: buf.len().cast(),
        lkey: unsafe { (*mr).lkey },
    };

    let mut send_wr = unsafe { std::mem::zeroed::<ibv_send_wr>() };
    send_wr.wr_id = 1;
    send_wr.sg_list = &mut sgl;
    send_wr.num_sge = nsge;
    if server_pdata.is_null() {
        send_wr.opcode = ibv_wr_opcode::IBV_WR_SEND;
        send_wr.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
    } else {
        debug_assert!(false, "should not run here");
        send_wr.opcode = ibv_wr_opcode::IBV_WR_RDMA_WRITE;
        send_wr.wr = wr_t {
            rdma: rdma_t {
                remote_addr: buf_va.to_be(),
                rkey: buf_rkey.to_be(),
            },
        };
    }
    let mut bad_send_wr = std::ptr::null_mut::<ibv_send_wr>();

    ret = unsafe { ibv_post_send((*client_id).qp, &mut send_wr, &mut bad_send_wr) };
    debug_assert_eq!(ret, 0, "ibv_post_send failed");

    // Wait for receive completion
    let nevents = 1;
    let num_entries = 1;
    let mut wc: ibv_wc = unsafe { std::mem::zeroed() };
    loop {
        ret = unsafe { ibv_get_cq_event(comp_channel, &mut cq, &mut cq_context) };
        debug_assert_eq!(ret, 0, "ibv_get_cq_event failed");
        unsafe { ibv_ack_cq_events(cq, nevents) };
        ret = unsafe { ibv_req_notify_cq(&mut *cq, solicited_only) };
        debug_assert_eq!(ret, 0, "ibv_req_notify_cq failed");

        ret = unsafe { ibv_poll_cq(&mut *cq, num_entries, &mut wc) };
        debug_assert_eq!(ret, 1, "ibv_poll_cq failed");
        debug_assert_eq!(
            wc.status,
            ibv_wc_status::IBV_WC_SUCCESS,
            "wc status={} is not IBV_WC_SUCCESS for opcode={}",
            wc.status,
            wc.opcode,
        );

        if wc.opcode == ibv_wc_opcode::IBV_WC_RECV {
            let recv_str = unsafe { CString::from_vec_unchecked(buf.to_vec()) };
            println!("received: {:?}", recv_str);
            println!("{} + {} = {}", val1, val2, buf[0]);
        } else if wc.opcode == ibv_wc_opcode::IBV_WC_SEND {
            println!("send completed successfully");
        } else {
            println!("other op={} completed", wc.opcode);
        }

        // if wc.wr_id == 0 {
        //   println!("{} + {} = {}", val1, val2, buf[0]);
        //   return;
        // }
    }
}

///
#[allow(
    clippy::too_many_lines,
    clippy::indexing_slicing,
    clippy::cognitive_complexity
)]
pub fn server(server_port: u16) {
    // Set up RDMA CM structures

    let mut addr: nix::sys::socket::sockaddr_in = unsafe { std::mem::zeroed() };
    addr.sin_family = libc::AF_INET.cast();
    addr.sin_port = server_port.to_be();

    let mut ret: c_int;
    let event_channel = unsafe { rdma_create_event_channel() };
    debug_assert_ne!(
        event_channel,
        std::ptr::null_mut(),
        "rdma_create_event_channel failed"
    );
    //util::check_errno(ret)?;

    let mut server_id: *mut rdma_cm_id = std::ptr::null_mut();
    let context: *mut c_void = std::ptr::null_mut();
    ret = unsafe {
        rdma_create_id(
            event_channel,
            &mut server_id,
            context,
            rdma_port_space::RDMA_PS_TCP,
        )
    };
    debug_assert_eq!(ret, 0, "rdma_create_id failed");
    //util::check_errno(ret)?;
    ret = unsafe { rdma_bind_addr(server_id, util::const_ptr_cast_mut(&addr)) };
    debug_assert_eq!(ret, 0, "rdma_bind_addr failed");
    //util::check_errno(ret)?;
    let backlog = 10; // backlog=10 is arbitrary
    ret = unsafe { rdma_listen(server_id, backlog) };
    debug_assert_eq!(ret, 0, "rdma_listen failed");
    //util::check_errno(ret)?;
    let bind_port = u16::from_be(unsafe { rdma_get_src_port(server_id) });
    println!("listening on port={}", bind_port);

    let mut event = std::ptr::null_mut::<rdma_cm_event>();
    ret = unsafe { rdma_get_cm_event(util::const_ptr_cast_mut(event_channel), &mut event) };
    debug_assert_eq!(ret, 0, "rdma_get_cm_event failed");
    let mut event_ref = unsafe { &*event };
    let cm_id = event_ref.id;
    debug_assert_eq!(
        event_ref.event,
        rdma_cm_event_type::RDMA_CM_EVENT_CONNECT_REQUEST,
        "match RDMA_CM_EVENT_CONNECT_REQUEST failed",
    );
    ret = unsafe { rdma_ack_cm_event(event) };
    debug_assert_eq!(ret, 0, "rdma_ack_cm_event failed");

    // Create verbs objects now that we know which device to use

    let ctx = unsafe { (*cm_id).verbs };
    let pd = unsafe { ibv_alloc_pd(ctx) };
    debug_assert_ne!(pd, std::ptr::null_mut::<ibv_pd>(), "ibv_alloc_pd failed",);
    let comp_channel = unsafe { ibv_create_comp_channel(ctx) };
    debug_assert_ne!(
        comp_channel,
        std::ptr::null_mut::<ibv_comp_channel>(),
        "ibv_create_comp_channel failed",
    );
    let cqe = 10; // cqe=10 is arbitrary
    let mut cq_context = std::ptr::null_mut::<c_void>();
    let comp_vector = 0;
    let mut cq = unsafe { ibv_create_cq(ctx, cqe, cq_context, comp_channel, comp_vector) };
    debug_assert_ne!(cq, std::ptr::null_mut::<ibv_cq>(), "ibv_create_cq failed",);
    let solicited_only = 0;
    ret = unsafe { ibv_req_notify_cq(&mut *cq, solicited_only) };
    debug_assert_eq!(ret, 0, "ibv_req_notify_cq failed");

    let mut buf = Box::pin([0_u8; BUF_SIZE]);
    let mr = unsafe {
        ibv_reg_mr(
            pd,
            util::const_ptr_cast_mut(buf.as_ptr()),
            BUF_SIZE,
            (ibv_access_flags::IBV_ACCESS_LOCAL_WRITE
                | ibv_access_flags::IBV_ACCESS_REMOTE_READ
                | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE)
                .0
                .cast(),
        )
    };
    debug_assert_ne!(mr, std::ptr::null_mut::<ibv_mr>(), "ibv_reg_mr failed");

    let mut qp_attr = ibv_qp_init_attr {
        qp_context: std::ptr::null_mut::<c_void>(),
        send_cq: cq,
        recv_cq: cq,
        srq: std::ptr::null_mut::<ibv_srq>(),
        cap: ibv_qp_cap {
            max_send_wr: 10,
            max_recv_wr: 10,
            max_send_sge: 1,
            max_recv_sge: 1,
            max_inline_data: 0,
        },
        qp_type: ibv_qp_type::IBV_QPT_RC,
        sq_sig_all: 0,
    };
    ret = unsafe { rdma_create_qp(cm_id, pd, &mut qp_attr) };
    debug_assert_eq!(ret, 0, "rdma_create_qp failed");

    // Post receive before accepting connection

    let nsge = 1;
    let mut sgl = ibv_sge {
        addr: util::ptr_to_usize(buf.as_ptr()).cast(),
        length: BUF_SIZE.cast(),
        lkey: unsafe { (*mr).lkey },
    };
    let mut recv_wr = ibv_recv_wr {
        wr_id: util::ptr_to_usize(cm_id).cast(),
        next: std::ptr::null_mut(),
        sg_list: &mut sgl,
        num_sge: nsge,
    };
    let mut bad_recv_wr = std::ptr::null_mut::<ibv_recv_wr>();

    ret = unsafe { ibv_post_recv((*cm_id).qp, &mut recv_wr, &mut bad_recv_wr) };
    debug_assert_eq!(ret, 0, "ibv_post_recv failed");

    // Accept connection
    let pdata = PData {
        buf_va: u64::to_be(util::ptr_to_usize(buf.as_ptr()).cast()),
        buf_rkey: u32::to_be(unsafe { (*mr).rkey }),
    };
    let mut cm_params: rdma_conn_param = unsafe { std::mem::zeroed() };
    cm_params.responder_resources = 1;
    cm_params.private_data = util::const_ptr_cast(&pdata);
    cm_params.private_data_len = std::mem::size_of_val(&pdata).cast();
    ret = unsafe { rdma_accept(cm_id, &mut cm_params) };
    debug_assert_eq!(ret, 0, "rdma_accept failed");

    ret = unsafe { rdma_get_cm_event(util::const_ptr_cast_mut(event_channel), &mut event) };
    debug_assert_eq!(ret, 0, "rdma_get_cm_event failed");
    event_ref = unsafe { &*event };
    debug_assert_eq!(
        event_ref.event,
        rdma_cm_event_type::RDMA_CM_EVENT_ESTABLISHED,
        "match RDMA_CM_EVENT_ESTABLISHED failed",
    );
    ret = unsafe { rdma_ack_cm_event(event) };
    debug_assert_eq!(ret, 0, "rdma_ack_cm_event failed");

    // Wait for receive completion

    let nevents = 1;
    let num_entries = 1;
    let mut wc: ibv_wc = unsafe { std::mem::zeroed() };
    ret = unsafe { ibv_get_cq_event(comp_channel, &mut cq, &mut cq_context) };
    debug_assert_eq!(ret, 0, "ibv_get_cq_event failed");
    unsafe { ibv_ack_cq_events(cq, nevents) };
    ret = unsafe { ibv_req_notify_cq(&mut *cq, solicited_only) };
    debug_assert_eq!(ret, 0, "ibv_req_notify_cq failed");

    ret = unsafe { ibv_poll_cq(&mut *cq, num_entries, &mut wc) };
    debug_assert_eq!(ret, 1, "ibv_poll_cq failed");
    debug_assert_eq!(
        wc.status,
        ibv_wc_status::IBV_WC_SUCCESS,
        "wc status={} is not IBV_WC_SUCCESS",
        wc.status,
    );

    // Add two integers and send reply back

    let recv_str = unsafe { CString::from_vec_unchecked(buf.to_vec()) };
    println!("received: {:?}", recv_str);
    let send_msg = format!(
        "message from passive/server side with pid={}",
        std::process::id(),
    );
    let add_val = buf[0].overflow_add(buf[1]);
    buf[0] = add_val;
    util::copy_slice(&mut *buf, send_msg.as_bytes());

    sgl = ibv_sge {
        addr: util::ptr_to_usize(buf.as_ptr()).cast(),
        length: BUF_SIZE.cast(), //buf.len().cast(),
        lkey: unsafe { (*mr).lkey },
    };

    let mut send_wr = unsafe { std::mem::zeroed::<ibv_send_wr>() };
    send_wr.opcode = ibv_wr_opcode::IBV_WR_SEND;
    send_wr.wr_id = 0;
    send_wr.sg_list = &mut sgl;
    send_wr.num_sge = nsge;
    send_wr.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
    // send_wr.wr = wr_t {
    //   rdma: rdma_t {
    //     remote_addr: pdata.buf_va.to_be(),
    //     rkey: pdata.buf_rkey.to_be(),
    //   },
    // };
    let mut bad_send_wr = std::ptr::null_mut::<ibv_send_wr>();

    ret = unsafe { ibv_post_send((*cm_id).qp, &mut send_wr, &mut bad_send_wr) };
    debug_assert_eq!(ret, 0, "ibv_post_send failed");

    // Wait for send completion

    ret = unsafe { ibv_get_cq_event(comp_channel, &mut cq, &mut cq_context) };
    debug_assert_eq!(ret, 0, "ibv_get_cq_event failed");
    unsafe { ibv_ack_cq_events(cq, nevents) };
    ret = unsafe { ibv_req_notify_cq(&mut *cq, solicited_only) };
    debug_assert_eq!(ret, 0, "ibv_req_notify_cq failed");

    ret = unsafe { ibv_poll_cq(&mut *cq, num_entries, &mut wc) };
    debug_assert_eq!(ret, 1, "ibv_poll_cq failed");
    debug_assert_eq!(
        wc.status,
        ibv_wc_status::IBV_WC_SUCCESS,
        "wc status={} is not IBV_WC_SUCCESS",
        wc.status,
    );
}
