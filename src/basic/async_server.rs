use super::util::{self, ScopedCB};
use rdma_sys::{
    ibv_access_flags, ibv_ack_cq_events, ibv_alloc_pd, ibv_comp_channel, ibv_cq,
    ibv_create_comp_channel, ibv_create_cq, ibv_dealloc_pd, ibv_dereg_mr, ibv_destroy_comp_channel,
    ibv_destroy_cq, ibv_device, ibv_get_cq_event, ibv_get_device_list, ibv_get_device_name, ibv_mr,
    ibv_pd, ibv_poll_cq, ibv_post_recv, ibv_post_send, ibv_qp_cap, ibv_qp_init_attr, ibv_qp_type,
    ibv_recv_wr, ibv_reg_mr, ibv_req_notify_cq, ibv_send_flags, ibv_send_wr, ibv_sge, ibv_srq,
    ibv_wc, ibv_wc_opcode, ibv_wc_status, ibv_wr_opcode, rdma_accept, rdma_ack_cm_event,
    rdma_bind_addr, rdma_cm_event, rdma_cm_event_type, rdma_cm_id, rdma_conn_param, rdma_connect,
    rdma_create_event_channel, rdma_create_id, rdma_create_qp, rdma_destroy_event_channel,
    rdma_destroy_id, rdma_destroy_qp, rdma_disconnect, rdma_event_channel, rdma_get_cm_event,
    rdma_get_src_port, rdma_listen, rdma_port_space, rdma_resolve_addr, rdma_resolve_route,
};
use std::collections::HashMap;
use std::ffi::CString;
use std::os::raw::{c_char, c_int, c_void};
use std::sync::Arc;
use utilities::Cast;

///
const BUFFER_SIZE: usize = 1024;
///
const TIMEOUT_IN_MS: c_int = 500;

///
pub struct ClientLink {
    ///
    client_id: usize,
}

impl Drop for ClientLink {
    fn drop(&mut self) {
        unsafe {
            rdma_destroy_qp(self.rdma_cm_id());
            ibv_destroy_cq((*self.rdma_cm_id()).recv_cq);
            ibv_destroy_comp_channel((*self.rdma_cm_id()).recv_cq_channel);
            ibv_dealloc_pd((*self.rdma_cm_id()).pd);
            rdma_destroy_id(self.rdma_cm_id());
        }
    }
}

impl ClientLink {
    ///
    fn new(id: *mut rdma_cm_id) -> Self {
        let ctx = unsafe { (*id).verbs };
        let pd = unsafe { ibv_alloc_pd(ctx) };
        debug_assert_ne!(pd, std::ptr::null_mut::<ibv_pd>(), "ibv_alloc_pd failed",);
        let comp_channel = unsafe { ibv_create_comp_channel(ctx) };
        debug_assert_ne!(
            comp_channel,
            std::ptr::null_mut::<ibv_comp_channel>(),
            "ibv_create_comp_channel failed",
        );
        let cqe = 10; // cqe=10 is arbitrary
        let cq_context = std::ptr::null_mut::<c_void>();
        let comp_vector = 0;
        let cq = unsafe { ibv_create_cq(ctx, cqe, cq_context, comp_channel, comp_vector) };
        debug_assert_ne!(cq, std::ptr::null_mut::<ibv_cq>(), "ibv_create_cq failed",);
        let solicited_only = 0;
        let mut ret: c_int;
        ret = unsafe { ibv_req_notify_cq(&mut *cq, solicited_only) };
        debug_assert_eq!(ret, 0, "ibv_req_notify_cq failed");

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
        ret = unsafe { rdma_create_qp(id, pd, &mut qp_attr) };
        debug_assert_eq!(ret, 0, "rdma_create_qp failed");
        unsafe {
            (*id).pd = pd;
            (*id).send_cq = cq;
            (*id).recv_cq = cq;
            (*id).send_cq_channel = comp_channel;
            (*id).recv_cq_channel = comp_channel;
        }

        Self {
            client_id: util::mut_ptr_to_usize(id),
        }
    }

    ///
    const fn rdma_cm_id(&self) -> *mut rdma_cm_id {
        unsafe { util::usize_to_mut_ptr(self.client_id) }
    }

    /// TODO: move to client
    fn resolve_route(&self, region: &Region) {
        self.post_recv(region);

        let ret = unsafe { rdma_resolve_route(self.rdma_cm_id(), TIMEOUT_IN_MS) };
        debug_assert_eq!(ret, 0, "rdma_resolve_route failed");
    }

    /// TODO: move to client
    fn connect(&self) {
        let mut cm_params: rdma_conn_param = unsafe { std::mem::zeroed() };
        let ret = unsafe { rdma_connect(self.rdma_cm_id(), &mut cm_params) };
        debug_assert_eq!(ret, 0, "rdma_connect failed");
    }

    /// TODO: move to client
    fn disconnect(&self) {
        let ret = unsafe { rdma_disconnect(self.rdma_cm_id()) };
        debug_assert_eq!(ret, 0, "rdma_disconnect failed");
    }

    ///
    fn post_recv(&self, region: &Region) {
        let nsge = 1;
        let mut sgl = ibv_sge {
            addr: region.addr().cast(),
            length: region.len().cast(),
            lkey: unsafe { (*region.mr()).lkey },
        };
        let mut wr = ibv_recv_wr {
            wr_id: self.client_id.cast(),
            next: std::ptr::null_mut(),
            sg_list: &mut sgl,
            num_sge: nsge,
        };
        let mut bad = std::ptr::null_mut::<ibv_recv_wr>();

        let ret = unsafe { ibv_post_recv((*self.rdma_cm_id()).qp, &mut wr, &mut bad) };
        debug_assert_eq!(ret, 0, "ibv_post_recv failed")
    }

    ///
    fn post_send(&self, region: &Region) {
        let nsge = 1;
        let mut sgl = ibv_sge {
            addr: region.addr().cast(),
            length: region.len().cast(),
            lkey: unsafe { (*region.mr()).lkey },
        };
        let mut wr = unsafe { std::mem::zeroed::<ibv_send_wr>() };
        wr.opcode = ibv_wr_opcode::IBV_WR_SEND;
        wr.wr_id = self.client_id.cast();
        wr.sg_list = &mut sgl;
        wr.num_sge = nsge;
        wr.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
        let mut bad = std::ptr::null_mut::<ibv_send_wr>();

        let ret = unsafe { ibv_post_send((*self.rdma_cm_id()).qp, &mut wr, &mut bad) };
        debug_assert_eq!(ret, 0, "ibv_post_send failed")
    }

    ///
    fn poll_cq(link: &Self, region: &Region, poll_cb: impl Fn(&Self, &[u8], &ibv_wc)) {
        let mut cq_context = std::ptr::null_mut::<c_void>();
        let mut ret: c_int;
        let mut cq = std::ptr::null_mut::<ibv_cq>();
        let mut wc: ibv_wc = unsafe { std::mem::zeroed() };
        let solicited_only = 0;
        let num_entries = 1;

        loop {
            ret = unsafe {
                ibv_get_cq_event(
                    (*link.rdma_cm_id()).recv_cq_channel, // Same as send_cq_channel
                    &mut cq,
                    &mut cq_context,
                )
            };
            debug_assert_eq!(ret, 0, "ibv_get_cq_event failed");
            unsafe { ibv_ack_cq_events(cq, 1) };
            ret = unsafe { ibv_req_notify_cq(&mut *cq, solicited_only) };
            debug_assert_eq!(ret, 0, "ibv_req_notify_cq failed");

            loop {
                let ne = unsafe { ibv_poll_cq(&mut *cq, num_entries, &mut wc) };
                debug_assert!(ne >= 0, "ibv_poll_cq failed");
                if ne == 0 {
                    break;
                }
                poll_cb(link, region.data(), &wc);
            }
        }
    }
}

///
struct Region {
    ///
    mr: usize,
    ///
    buf: std::pin::Pin<Box<[u8; BUFFER_SIZE]>>,
}

impl Drop for Region {
    fn drop(&mut self) {
        unsafe {
            ibv_dereg_mr(self.mr());
        }
    }
}

impl Region {
    ///
    fn new(link: &ClientLink, access: ibv_access_flags, data: &[u8]) -> Self {
        debug_assert!(
            data.len() <= BUFFER_SIZE,
            "data size larger than buffer size"
        );
        let mut buf = Box::pin([0; BUFFER_SIZE]);
        if !data.is_empty() {
            util::copy_slice(&mut *buf, data);
        }
        let mr = unsafe {
            ibv_reg_mr(
                (*link.rdma_cm_id()).pd,
                util::const_ptr_cast_mut(buf.as_ptr()),
                BUFFER_SIZE,
                access.0.cast(),
            )
        };
        // let res = util::check_errno(-1);
        // println!("the error is: {:?}", res);
        debug_assert_ne!(mr, std::ptr::null_mut::<ibv_mr>(), "ibv_reg_mr failed");
        Self {
            mr: util::mut_ptr_to_usize(mr),
            buf,
        }
        // println!(
        //     "Region::new(): mr={:?}, usize={:x}, mr()={:?}",
        //     mr,
        //     r.mr,
        //     r.mr()
        // );
        // r
    }

    ///
    const fn mr(&self) -> *mut ibv_mr {
        unsafe { util::usize_to_mut_ptr(self.mr) }
    }

    ///
    fn addr(&self) -> usize {
        util::ptr_to_usize(self.buf.as_ptr())
    }

    ///
    fn len(&self) -> usize {
        self.buf.len()
    }

    ///
    fn data(&self) -> &[u8] {
        &*self.buf
    }
}

/*
///
struct Region {
    ///
    recv_mr: usize,
    ///
    send_mr: usize,
    ///
    recv_region: Vec<u8>,
    ///
    send_region: String,
}

impl Drop for Region {
    fn drop(&mut self) {
        unsafe {
            ibv_dereg_mr(self.send_mr());
            ibv_dereg_mr(self.recv_mr());
        }
    }
}

impl Region {
    ///
    fn new(link: &ClientLink, buf_size: usize) -> Self {
        let mut send_region = String::with_capacity(buf_size);
        send_region.push_str(&format!(
            "message from passive/server side with pid={}",
            std::process::id()
        ));
        let recv_region = Vec::with_capacity(buf_size);

        let send_mr = unsafe {
            ibv_reg_mr(
                (*link.rdma_cm_id()).pd,
                util::const_ptr_cast_mut(&send_region),
                send_region.len(),
                0,
            )
        };
        let recv_mr = unsafe {
            ibv_reg_mr(
                (*link.rdma_cm_id()).pd,
                util::const_ptr_cast_mut(&recv_region),
                recv_region.len(),
                ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0.cast(),
            )
        };

        Self {
            send_mr: util::mut_ptr_to_usize(send_mr),
            recv_mr: util::mut_ptr_to_usize(recv_mr),
            send_region,
            recv_region,
        }
    }

    ///
    fn send_mr(&self) -> *mut ibv_mr {
        unsafe { util::usize_to_mut_ptr(self.send_mr) }
    }

    ///
    fn recv_mr(&self) -> *mut ibv_mr {
        unsafe { util::usize_to_mut_ptr(self.recv_mr) }
    }
}
*/

///
fn server_poll_cb(_link: &ClientLink, recv_buf: &[u8], wc: &ibv_wc) {
    debug_assert_eq!(
        wc.status,
        ibv_wc_status::IBV_WC_SUCCESS,
        "server_poll_cb: status={} is not IBV_WC_SUCCESS",
        wc.status,
    );

    if wc.opcode == ibv_wc_opcode::IBV_WC_RECV {
        println!("received message: {}", String::from_utf8_lossy(recv_buf),);
    } else if wc.opcode == ibv_wc_opcode::IBV_WC_SEND {
        println!("send completed successfully");
    } else {
        println!("other op={} completed", wc.opcode);
    }
}

///
fn client_poll_cb(link: &ClientLink, recv_buf: &[u8], wc: &ibv_wc) {
    debug_assert_eq!(
        wc.status,
        ibv_wc_status::IBV_WC_SUCCESS,
        "client_poll_cb: status={} is not IBV_WC_SUCCESS",
        wc.status,
    );

    if wc.opcode == ibv_wc_opcode::IBV_WC_RECV {
        println!("received message: {}", String::from_utf8_lossy(recv_buf));
    } else if wc.opcode == ibv_wc_opcode::IBV_WC_SEND {
        println!("send completed successfully");
    } else {
        println!("other op={} completed", wc.opcode);
    }

    link.disconnect();
}

///
pub struct Client {
    ///
    dev_list: Vec<*mut ibv_device>,
    ///
    event_channel: &'static rdma_event_channel,
    ///
    client_id: &'static rdma_cm_id,
}

impl Drop for Client {
    fn drop(&mut self) {
        unsafe {
            // TODO: protential memory leak
            // rdma_destroy_id(util::const_ptr_cast_mut(self.client_id));
            rdma_destroy_event_channel(util::const_ptr_cast_mut(self.event_channel));
        }
    }
}

impl Client {
    ///
    pub fn new(server_addr: &str, server_port: u16) -> Self {
        let mut dst_addr = std::ptr::null_mut::<libc::addrinfo>();
        let hint = std::ptr::null::<libc::addrinfo>();

        let mut ret: c_int;
        let server_add_cstr = CString::new(server_addr).unwrap_or_else(|err| {
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
                server_add_cstr.as_ptr(),
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

        let mut conn = std::ptr::null_mut::<rdma_cm_id>();
        let context: *mut c_void = std::ptr::null_mut();
        ret = unsafe {
            rdma_create_id(
                event_channel,
                &mut conn,
                context,
                rdma_port_space::RDMA_PS_TCP,
            )
        };
        debug_assert_eq!(ret, 0, "rdma_create_id failed");
        //util::check_errno(ret)?;

        let src_addr = std::ptr::null_mut::<libc::sockaddr>();
        ret = unsafe { rdma_resolve_addr(conn, src_addr, (*dst_addr).ai_addr, TIMEOUT_IN_MS) };
        debug_assert_eq!(ret, 0, "rdma_resolve_addr failed");
        //util::check_errno(ret)?;
        unsafe { libc::freeaddrinfo(dst_addr) };

        let mut num_devices = 0;
        let dev_list_ptr = unsafe { ibv_get_device_list(&mut num_devices) };
        debug_assert!(
            !util::is_null_mut_ptr(dev_list_ptr),
            "ibv_get_device_list failed"
        );

        Self {
            dev_list: unsafe {
                std::slice::from_raw_parts(dev_list_ptr, num_devices.cast()).to_vec()
            },
            event_channel: unsafe { &*event_channel },
            client_id: unsafe { &*conn },
        }
    }

    /// Client side run
    pub fn run(&self) {
        for dev in &self.dev_list {
            let dev_name = unsafe { ibv_get_device_name(util::const_ptr_cast_mut(dev)) };
            debug_assert_ne!(
                dev_name,
                std::ptr::null_mut::<c_char>(),
                "ibv_get_device_name failed",
            );

            let dev_cstr = unsafe { CString::from_raw(libc::strdup(dev_name)) };
            println!("device name: {:?}", dev_cstr);
        }

        let mut event = std::ptr::null_mut::<rdma_cm_event>();
        let mut client_map = HashMap::new();
        let mut mr_map = HashMap::new();

        loop {
            let ret = unsafe {
                rdma_get_cm_event(util::const_ptr_cast_mut(self.event_channel), &mut event)
            };
            debug_assert_eq!(ret, 0, "rdma_get_cm_event failed");

            let _ack = ScopedCB::new(|| unsafe {
                let ret = rdma_ack_cm_event(event);
                debug_assert_eq!(ret, 0, "rdma_ack_cm_event failed");
            });

            let event_ref = unsafe { &*event };
            debug_assert_eq!(
                util::const_ptr_cast_mut(self.client_id),
                event_ref.id,
                "client ID not match",
            );
            match event_ref.event {
                rdma_cm_event_type::RDMA_CM_EVENT_ADDR_RESOLVED => {
                    let link = Arc::new(ClientLink::new(event_ref.id));
                    let send_msg = format!(
                        "message from active/client side with pid={}",
                        std::process::id(),
                    );
                    let send_region = Arc::new(Region::new(
                        &link,
                        ibv_access_flags::IBV_ACCESS_LOCAL_WRITE,
                        send_msg.as_bytes(),
                    ));
                    let recv_region = Arc::new(Region::new(
                        &link,
                        ibv_access_flags(0),
                        &[], // Empty init data
                    ));
                    link.resolve_route(&recv_region);

                    let link_arc = Arc::clone(&link);
                    let region_arc = Arc::clone(&recv_region);
                    std::thread::spawn(move || {
                        ClientLink::poll_cq(&link_arc, &region_arc, client_poll_cb);
                    });

                    let client_id = link.client_id;
                    client_map.insert(client_id, link);
                    mr_map.insert(client_id, (send_region, recv_region));
                }
                rdma_cm_event_type::RDMA_CM_EVENT_ROUTE_RESOLVED => {
                    let client_id = util::mut_ptr_to_usize(event_ref.id);
                    let link = client_map
                        .get(&client_id)
                        .unwrap_or_else(|| panic!("failed to find client ID={}", client_id));
                    link.connect();
                }
                rdma_cm_event_type::RDMA_CM_EVENT_ESTABLISHED => {
                    let client_id = util::mut_ptr_to_usize(event_ref.id);
                    let link = client_map
                        .get(&client_id)
                        .unwrap_or_else(|| panic!("failed to find client ID={}", client_id));
                    let (ref send_region, ref _recv_region) = *mr_map
                        .get(&client_id)
                        .unwrap_or_else(|| panic!("failed to find MR for client ID={}", client_id));
                    link.post_send(send_region);
                }
                rdma_cm_event_type::RDMA_CM_EVENT_DISCONNECTED => {
                    println!("client disconnected");
                    break;
                }
                _ => break,
            }
        }
    }
}

///
pub struct Server {
    ///
    dev_list: Vec<*mut ibv_device>,
    ///
    event_channel: &'static rdma_event_channel,
    ///
    server_id: &'static rdma_cm_id,
}

impl Drop for Server {
    fn drop(&mut self) {
        unsafe {
            rdma_destroy_id(util::const_ptr_cast_mut(self.server_id));
            rdma_destroy_event_channel(util::const_ptr_cast_mut(self.event_channel));
        }
    }
}

impl Server {
    ///
    pub fn new(srv_port: u16) -> Self {
        let mut addr: nix::sys::socket::sockaddr_in = unsafe { std::mem::zeroed() };
        addr.sin_family = libc::AF_INET.cast();
        addr.sin_port = srv_port.to_be();

        let mut ret: c_int;
        let event_channel = unsafe { rdma_create_event_channel() };
        debug_assert_ne!(
            event_channel,
            std::ptr::null_mut(),
            "rdma_create_event_channel failed"
        );
        //util::check_errno(ret)?;

        let mut listener: *mut rdma_cm_id = std::ptr::null_mut();
        let context: *mut c_void = std::ptr::null_mut();
        ret = unsafe {
            rdma_create_id(
                event_channel,
                &mut listener,
                context,
                rdma_port_space::RDMA_PS_TCP,
            )
        };
        debug_assert_eq!(ret, 0, "rdma_create_id failed");
        //util::check_errno(ret)?;
        ret = unsafe { rdma_bind_addr(listener, util::const_ptr_cast_mut(&addr)) };
        debug_assert_eq!(ret, 0, "rdma_bind_addr failed");
        //util::check_errno(ret)?;
        let backlog = 10; // backlog=10 is arbitrary
        ret = unsafe { rdma_listen(listener, backlog) };
        debug_assert_eq!(ret, 0, "rdma_listen failed");
        //util::check_errno(ret)?;
        let bind_port = u16::from_be(unsafe { rdma_get_src_port(listener) });
        println!("listening on port={}", bind_port);

        let mut num_devices = 0;
        let dev_list_ptr = unsafe { ibv_get_device_list(&mut num_devices) };
        debug_assert!(
            !util::is_null_mut_ptr(dev_list_ptr),
            "ibv_get_device_list failed"
        );
        Self {
            dev_list: unsafe {
                std::slice::from_raw_parts(dev_list_ptr, num_devices.cast()).to_vec()
            },
            event_channel: unsafe { &*event_channel },
            server_id: unsafe { &*listener },
        }
    }

    ///
    fn accept(link: &ClientLink, region: &Region) {
        println!("received connection request");

        link.post_recv(region);
        let mut cm_params: rdma_conn_param = unsafe { std::mem::zeroed() };
        let ret = unsafe { rdma_accept(link.rdma_cm_id(), &mut cm_params) };
        debug_assert_eq!(ret, 0, "rdma_accept failed");
    }

    /// Server side run
    pub fn run(&self) {
        for dev in &self.dev_list {
            let dev_name = unsafe { ibv_get_device_name(util::const_ptr_cast_mut(dev)) };
            debug_assert_ne!(
                dev_name,
                std::ptr::null_mut::<c_char>(),
                "ibv_get_device_name failed",
            );

            let dev_cstr = unsafe { CString::from_raw(libc::strdup(dev_name)) };
            println!("device name: {:?}", dev_cstr);
        }

        let mut event = std::ptr::null_mut::<rdma_cm_event>();
        let mut client_map = HashMap::new();
        let mut mr_map = HashMap::new();

        loop {
            let ret = unsafe {
                rdma_get_cm_event(util::const_ptr_cast_mut(self.event_channel), &mut event)
            };
            debug_assert_eq!(ret, 0, "rdma_get_cm_event failed");

            let _ack = ScopedCB::new(|| unsafe {
                let ret = rdma_ack_cm_event(event);
                debug_assert_eq!(ret, 0, "rdma_ack_cm_event failed");
            });

            let event_ref = unsafe { &*event };
            match event_ref.event {
                rdma_cm_event_type::RDMA_CM_EVENT_CONNECT_REQUEST => {
                    let link = Arc::new(ClientLink::new(event_ref.id));
                    let send_msg = format!(
                        "message from passive/server side with pid={}",
                        std::process::id(),
                    );
                    let send_region = Arc::new(Region::new(
                        &link,
                        ibv_access_flags::IBV_ACCESS_LOCAL_WRITE,
                        send_msg.as_bytes(),
                    ));
                    let recv_region = Arc::new(Region::new(
                        &link,
                        ibv_access_flags(0),
                        &[], // Empty init data
                    ));
                    Self::accept(&link, &recv_region);

                    let link_arc = Arc::clone(&link);
                    let region_arc = Arc::clone(&recv_region);
                    std::thread::spawn(move || {
                        ClientLink::poll_cq(&link_arc, &region_arc, server_poll_cb);
                    });

                    let client_id = link.client_id;
                    client_map.insert(client_id, link);
                    mr_map.insert(client_id, (send_region, recv_region));
                    //self.on_connect_request((*event).id);
                }
                rdma_cm_event_type::RDMA_CM_EVENT_ESTABLISHED => {
                    let client_id = util::mut_ptr_to_usize(event_ref.id);
                    let link = client_map
                        .get(&client_id)
                        .unwrap_or_else(|| panic!("failed to find client ID={}", client_id));
                    let (ref send_region, ref _recv_region) = *mr_map
                        .get(&client_id)
                        .unwrap_or_else(|| panic!("failed to find MR for client ID={}", client_id));

                    link.post_send(send_region);
                    //self.on_connection(event->id->context);
                }
                rdma_cm_event_type::RDMA_CM_EVENT_DISCONNECTED => {
                    //self.on_disconnect(event->id);
                    println!("peer disconnected");
                    break;
                }
                _ => break,
            }
        }
    }
}
