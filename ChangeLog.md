# ChangeLog

## 0.4.0

  In this release, we add some APIs to make it easier to establish connections and 
  provide more configurable options.

  We provide support for sending/receiving raw data and allocating raw memory region
  to enable users to implement the upper layer protocol or memory pool by themselves.

  APIs for supporting multi-connection can be used to easily establish multiple connections 
  with more than one remote end and reuse resources.

### New features

* Adapt RDMA CM APIs. Add `cm_connect` to establish connection with CM Server.
  Add `{send/recv}_raw` APIs to send/recv raw data without the help of agent.
* Add APIs to set attributes of queue pair like `set_max_{send/recv}_{sge/wr}`.
* Add APIs for `RdmaBuilder` to establish connections conveniently.
* Add APIs to alloc raw mr. Sometimes users want to setup memory pool by themselves 
  instead of using `Jemalloc` to manage mrs. So we define two strategies.
* Add APIs to alloc mrs with different accesses and protection domains from `Jemalloc`.
 `MrAllocator` will create a new arena when we alloc mr with not the default access and pd.
* Add access API for mrs to query access.
* Support multi-connection APIs. Add APIs for `Rdma` to create a new `Rdma` that has the 
  same `mr_allocator` and `event_listener` as parent.
* Add `RemoteMr` access control. Add `set_max_rmr_access` API to set the maximum permission on 
  `RemoteMr` that the remote end can request.

### Optimizations and refactors

* Use submodule to setup CI environment.
* Reorganize some attributes to avoid too many arguments.
* Update examples. Replace unsafe blocks and add more comments. Show more APIs.

### Bug fixes

* Disable `tcache` of `Jemalloc` as default. `tcache` is a feature of `Jemalloc` to speed up 
  memory allocation. However `Jemalloc` may alloc `MR` with wrong `arena_index` from `tcache` 
  when we create more than one `Jemalloc` enabled `mr_allocator`s. So we disable `tcache` by default.

## 0.3.0

  In this release, we adapted `Jemalloc` to manage RDMA memory region to improve memory
  allocation efficiency.

  Some safety and performance issues have been fixed, thanks to @Nugine's comments.

### New features

* `Jemalloc` was adapted to manage memory region. We inject custom extent hooks into `Jemalloc`
  to empower it to manage `RawMr`s. That avoids the overhead of repeatedly reg/dereg memory region.
  Use `BTreeMap` to record the relationship between `addr` and `RawMr`. When we alloc memory from
  `Jemalloc`, `mr_allocator` will lookup the related `RawMr` by `addr`.
* Add timeout mechanism for `RemoteMr`. Request `RemoteMr` from remote without timeout may cause
  remote OOM. So we add `RemoteMrManager` to manage `RemoteMr` and free them after timeout.

### Optimizations and refactors

* Avoid unnecessary overflow checking operations to improve performance.
* Optimize cq poll work flow. Poll single `CQE` at a time is inefficient in high concurrency.
  Here we made the maximum number of `CQE` to poll at a time configurable. Accordingly,
  `event_listener` should be able to wake up multiple tasks at a time.
* Redesign `LocalMr` and `event_listener` to ensure cancel safety. Let `event_listener` holds
  the `Arc`s of the `LocalMrInner`s that are being used by RDMA ops to ensure cancel safety.
  `LocalMr` was replaced with new struct `LocalMrInner`. Because every struct that can use APIs
  should hold an `Arc` of the mr's metadata, but previous `LocalMr` can't hold itself. Add `RwLock`
  to avoid potential race condition.
* Add zeroed `LocalMr` API. Uninitialized memory region is fast but not safe, so we add zeroed API
  and mark uninitialized memory alloc API as unsafe.

### Bug fixes

* Fix `Gid` impl that used to have alignment mismatch bug.
* Fix the handling of return values of ibv APIs. Most ibv APIs return -1 or NULL on error
  and if the call fails, errno will be set to indicate the reason for the failure.But there
  are some places treat the return value as errno, we fixed these wrong handlings.
* Ensure cancel safety. Undefined behavior will happen if the future dropped(cancelled)
  during the execution of RDMA operations. So we redesign `LocalMr` and `event_listener` to
  hold `Arc` of memory regions until the operations are complete.
* Mark unsafe traits and functions. The safety of access traits and functions cannot be guaranteed
  by themselves. So we need to mark them as unsafe. And the unsafe traits are marked as `sealed` to
  avoid being unsafely impl externally.

## 0.2.0

  In this release, the code related to `memory region` has been reorganized.
  This change makes the abstraction more explicit and easy to maintain later.

  Some new APIs for RDMA operations with immediate data was added. And we also
  fixed some bugs, which made the lib more stable.

### New features

* Implement memory region slice. `LocalMrSlice` and `RemoteMrSlice` enable us to operate on
  part of memory region. And we can only get one mutable slice or many unmutable slices at the
  same time, just like any other type in rust.
* Add imm data APIs.
  * send_with_imm
  * receive_with_imm
  * write_with_imm
  * receive_write_imm

### Optimizations and refactors

* Redesign memory region. Use traits to describe three kinds of memory region abstraction.
  `RawMemoryRegion` records all information about the memory region Registered locally.
  `LocalMr` records information for local use. `RemoteMr` records information for remote use.
* Discard preapplication of memroy region strategy. The old preapplication strategy is not
  flexible enough, so that is currently abandoned. And we will transform jemalloc to manage
  memory region and reuse it's preapplication strategy in the next release.
* Change from dynamic generic to static generic.
* Implement multi-task receiver. Just one task work with one `ibv_recv_wr` can not
  handle high-concurrency SEND requests. So more receiver tasks with more `ibv_recv_wr` were
  spawned to handle highly concurrent requests in this release.
* Redesign memory region transfer APIs.
  * Refine the interface capability
    * Change from `send_mr` to `send_local_mr` and `send_remote_mr`.
    * Change from `receive_mr` to `receive_local_mr` and `receive_remote_mr`.
  * Take the ownership of the sent memory region. Prevent both ends from operating on the
    same memory region at the same time.

### Bug fixes

* Fix retry bug. When `rnr_retry`==7, the sender will keep retrying until the system freezes.
  We can't get any effective information to debug it because this part of the work is performed
  by the kernel module and there is no error message. But that will deplete CPU resources and the
  only thing we can observe is the system freezes. Solution is to make `rnr_retry` < 7.
* Add timeout mechanism for remote requet operations to prevent Infinite wait.
* Make `Agent` hold `AgentThread` to prevent the senders' release.

## 0.1.0

### What is it and why make it?

[**`Async-rdma`**](https://github.com/datenlord/async-rdma) is a framework for
writing RDMA applications with high-level abstraction and asynchronous APIs.

Remote Direct Memory Access(RDMA) is direct access of memory from memory of one machine to the
memory of another. It helps in boosting the performance of applications that need low latency
and high throughput as it supports **kernel bypass** and **zero copy** while **not involving CPU**.

However, writing RDMA applications with low-level c library is laborious and error-prone. We want
easy-to-use APIs that hide the complexity of the underlying RDMA operations, so we developed
`async-rdma`. With the help of `async-rdma`, most RDMA operations can be completed by writing
only **one line** of code.

### What does it provide?

* Tools for establishing connections with rdma endpoints.

* High-level async APIs for data transmission between endpoints.

* High-level APIs for rdma memory region management.

* A framework working behind APIs for memory region management and executing rdma requests asynchronously.

### Note

We develop `async-rdma` with the aim of becoming production-grade. But now it is too young, still in the
experimental stage. We are adding more features and improving stability. We welcome everyone to try, ask
questions and make suggestions.

### Links

* Github: <https://github.com/datenlord/async-rdma>

* Crate: <https://crates.io/crates/async-rdma>

* Docs: <https://docs.rs/async-rdma/0.1.0/async_rdma>

* RDMA introduction video: <https://www.youtube.com/watch?v=lu78_C-9jvA>

* RDMA introduction doc: <http://www.reports.ias.ac.in/report/12829/understanding-the-concepts-and-mechanisms-of-rdma>
