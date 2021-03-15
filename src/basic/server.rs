use nix::sys::socket::sockaddr_in;
use rdma_sys::*;
use std::boxed::Box;
use std::collections::HashMap;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use utilities::Cast;
//use std::sync::atomic::{AtomicU64, Ordering};

///
const BUFFER_SIZE: usize = 1024;

///
struct Context {
  ///
  ctx: *mut ibv_context,
  ///
  pd: *mut ibv_pd,
  ///
  cq: *mut ibv_cq,
  ///
  comp_channel: *mut ibv_comp_channel,
  ///
  cq_poller_thread: std::thread::JoinHandle<()>,
}

///
struct Connection {
  //qp: *mut ibv_qp,
  ///
  recv_mr: *mut ibv_mr,
  ///
  send_mr: *mut ibv_mr,
  ///
  recv_region: *mut [c_char; BUFFER_SIZE],
  ///
  send_region: *mut [c_char; BUFFER_SIZE],
}

///
pub struct Server {
  ///
  next_id: u64, //AtomicU64,
  ///
  ctx_map: HashMap<u64, Context>,
  ///
  event_channel: *mut rdma_event_channel,
  ///
  listener: *mut rdma_cm_id,
}

impl Drop for Server {
  fn drop(&mut self) {
    unsafe {
      rdma_destroy_id(self.listener);
      rdma_destroy_event_channel(self.event_channel);
    }
  }
}

impl Server {
  ///
  pub unsafe fn new(srv_port: u16) -> Self {
    let mut addr: sockaddr_in = std::mem::zeroed();
    addr.sin_port = srv_port;

    let mut ret: c_int;
    let event_channel = rdma_create_event_channel();

    let mut listener = utilities::const_ptr_to_mut(ptr::null::<rdma_cm_id>());
    let context = utilities::const_ptr_to_mut(ptr::null::<c_void>());
    ret = rdma_create_id(
      event_channel,
      &mut listener,
      context,
      rdma_port_space::RDMA_PS_TCP,
    );
    assert_eq!(ret, 0, "rdma_create_id failed");

    ret = rdma_bind_addr(listener, utilities::cast_to_mut_ptr(&mut addr));
    assert_eq!(ret, 0, "rdma_bind_addr failed");

    let backlog = 10; // backlog=10 is arbitrary
    ret = rdma_listen(listener, backlog);
    assert_eq!(ret, 0, "rdma_listen failed");

    let bind_port = rdma_get_src_port(listener);
    println!("listening on port={}", bind_port);

    Self {
      next_id: 1, //AtomicU64::new(1),
      ctx_map: HashMap::new(),
      event_channel,
      listener,
    }
  }

  ///
  pub unsafe fn run(&mut self) {
    let mut event = utilities::const_ptr_to_mut(ptr::null::<rdma_cm_event>());
    let mut ret: c_int;
    let mut stop = false;
    while !stop {
      ret = rdma_get_cm_event(self.event_channel, &mut event);
      assert_eq!(ret, 0, "rdma_get_cm_event failed");

      match (*event).event {
        rdma_cm_event_type::RDMA_CM_EVENT_CONNECT_REQUEST => {
          self.on_connect_request((*event).id);
        }
        rdma_cm_event_type::RDMA_CM_EVENT_ESTABLISHED => (),
        rdma_cm_event_type::RDMA_CM_EVENT_DISCONNECTED => (),
        _ => {
          stop = true;
        }
      }
      // if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST)
      //   r = on_connect_request(event->id);
      // else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
      //   r = on_connection(event->id->context);
      // else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
      //   r = on_disconnect(event->id);

      rdma_ack_cm_event(event);
    }
  }

  ///
  fn build_qp_attr(cq: &mut ibv_cq) -> ibv_qp_init_attr {
    ibv_qp_init_attr {
      qp_context: utilities::const_ptr_to_mut(ptr::null::<c_void>()),
      send_cq: cq,
      recv_cq: cq,
      srq: utilities::const_ptr_to_mut(ptr::null::<ibv_srq>()),
      cap: ibv_qp_cap {
        max_send_wr: 10,
        max_recv_wr: 10,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 0,
      },
      qp_type: ibv_qp_type::IBV_QPT_RC,
      sq_sig_all: 0,
    }
  }

  ///
  unsafe fn on_completion(wc: *const ibv_wc) {
    assert_eq!(
      (*wc).status,
      ibv_wc_status::IBV_WC_SUCCESS,
      "on_completion: status is not IBV_WC_SUCCESS",
    );
  
    if (*wc).opcode == ibv_wc_opcode::IBV_WC_RECV {
      let id: *const rdma_cm_id = utilities::usize_to_const_ptr((*wc).wr_id.cast());
      //struct connection *conn = (struct connection *)(uintptr_t)wc->wr_id;
      let conn: *const Connection = utilities::cast_to_ptr(&*(*id).context);
  
      println!("received message: {:?}", (*conn).recv_region);
  
    } else if (*wc).opcode == ibv_wc_opcode::IBV_WC_SEND {
      println!("send completed successfully");
    } else {
      println!("other op={} completed", (*wc).opcode);
    }
  }
  
  ///
  unsafe fn poll_cq(comp_channel_ref: *const c_void) {
    // struct ibv_cq *cq;
    // struct ibv_wc wc;
    // while (1) {
    //   TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
    //   ibv_ack_cq_events(cq, 1);
    //   TEST_NZ(ibv_req_notify_cq(cq, 0));
    //   while (ibv_poll_cq(cq, 1, &wc))
    //     on_completion(&wc);
    // }
    // return NULL;
    let mut cq_context = utilities::const_ptr_to_mut(ptr::null::<c_void>());
    let mut ret: c_int;
    let mut cq = utilities::const_ptr_to_mut(ptr::null::<ibv_cq>());
    let mut wc: ibv_wc = std::mem::zeroed();
    let solicited_only = 0;
    let num_entries = 1;
    loop {
      ret = ibv_get_cq_event(
        utilities::cast_to_mut_ptr(&mut *utilities::const_ptr_to_mut(comp_channel_ref)),
        &mut cq,
        &mut cq_context,
      );
      assert_eq!(ret, 0, "ibv_get_cq_event failed");
      ibv_ack_cq_events(cq, 1);
      ret = ibv_req_notify_cq(&mut *cq, solicited_only);
      assert_eq!(ret, 0, "ibv_req_notify_cq failed");
      while ibv_poll_cq(&mut *cq, num_entries, &mut wc) != 0 {
        Self::on_completion(&wc)
      }
    }
  }

  ///
  unsafe fn build_context(&mut self, verbs: *mut ibv_context) -> &Context {
    let pd = ibv_alloc_pd(verbs);
    assert_ne!(
      pd,
      utilities::const_ptr_to_mut(ptr::null::<ibv_pd>()),
      "ibv_alloc_pd failed",
    );
    let comp_channel = ibv_create_comp_channel(verbs);
    assert_ne!(
      comp_channel,
      utilities::const_ptr_to_mut(ptr::null::<ibv_comp_channel>()),
      "ibv_create_comp_channel failed",
    );

    let cqe = 10; // cqe=10 is arbitrary
    let cq_context = utilities::const_ptr_to_mut(ptr::null::<c_void>());
    let comp_vector = 0;
    let cq = ibv_create_cq(verbs, cqe, cq_context, comp_channel, comp_vector);
    assert_ne!(
      cq,
      utilities::const_ptr_to_mut(ptr::null::<ibv_cq>()),
      "ibv_create_cq failed",
    );
    let ret: c_int;
    let solicited_only = 0;
    ret = ibv_req_notify_cq(&mut *cq, solicited_only);
    assert_eq!(ret, 0, "ibv_req_notify_cq failed");

    let comp_channel_ref: &c_void = &*utilities::cast_to_ptr(&*comp_channel);
    let cq_poller_thread = std::thread::spawn(move || {
      Self::poll_cq(comp_channel_ref);
    });

    let cntx = Context {
      ctx: verbs,
      pd,
      cq,
      comp_channel,
      cq_poller_thread,
    };
    self.ctx_map.insert(self.next_id, cntx);
    let res_cntx = self.ctx_map.get(&self.next_id);
    self.next_id = self.next_id + 1;
    res_cntx.unwrap()
  }

  ///
  unsafe fn register_memory(pd: &mut ibv_pd) -> *mut Connection {
    let send_region = Box::into_raw(Box::new([0; BUFFER_SIZE]));
    let recv_region = Box::into_raw(Box::new([0; BUFFER_SIZE]));
    let send_mr = ibv_reg_mr(
      pd,
      utilities::cast_to_mut_ptr(&mut *send_region),
      BUFFER_SIZE,
      0,
    );
    let recv_mr = ibv_reg_mr(
      pd,
      utilities::cast_to_mut_ptr(&mut *recv_region),
      BUFFER_SIZE,
      ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0.cast(),
    );

    Box::into_raw(Box::new(Connection {
      //qp,
      recv_mr,
      send_mr,
      recv_region,
      send_region,
    }))
  }

  ///
  unsafe fn post_receives(id: *mut rdma_cm_id) {
    // struct ibv_recv_wr wr, *bad_wr = NULL;
    // struct ibv_sge sge;
    // wr.wr_id = (uintptr_t)conn;
    // wr.next = NULL;
    // wr.sg_list = &sge;
    // wr.num_sge = 1;
    let conn: *mut Connection = utilities::cast_to_mut_ptr(&mut *(*id).context);
    let mut sgl = ibv_sge {
      addr: utilities::ptr_to_usize((*conn).recv_region).cast(),
      length: BUFFER_SIZE.cast(),
      lkey: (*(*conn).recv_mr).lkey,
    };
    //TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
    let nsge = 1;
    let ret = rdma_post_recvv(id, utilities::cast_to_mut_ptr(&mut *id), &mut sgl, nsge);
    assert_eq!(ret, 0, "rdma_post_recvv failed")
  }

  ///
  unsafe fn on_connect_request(&mut self, id: *mut rdma_cm_id) -> c_int {
    let mut cm_params: rdma_conn_param = std::mem::zeroed();

    println!("received connection request");

    let cntx = self.build_context((*id).verbs);
    let mut qp_attr = Self::build_qp_attr(&mut *cntx.cq);

    let mut ret: c_int;
    ret = rdma_create_qp(id, cntx.pd, &mut qp_attr);
    assert_eq!(ret, 0, "rdma_create_qp failed");

    //id.context = conn = (struct connection *)malloc(sizeof(struct connection));

    let conn = Self::register_memory(&mut *cntx.pd);
    (*id).context = utilities::cast_to_mut_ptr(&mut *conn);
    Self::post_receives(id);

    //memset(&cm_params, 0, sizeof(cm_params));
    ret = rdma_accept(id, &mut cm_params);
    assert_eq!(ret, 0, "rdma_accept failed");

    0
  }
}
