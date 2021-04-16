//! Sync data via socket

use serde::{de::DeserializeOwned, Serialize};
// use std::cmp::Ordering;
use std::ffi::{CStr, CString};
use std::net::ToSocketAddrs;
use std::net::UdpSocket;
use std::os::raw::{c_char, c_int};
use std::os::unix::io::RawFd;
use utilities::Cast; //, OverflowArithmetic};

use super::util;

/// UDP socket wrapper struct
pub struct Udp {
    /// UDP socket
    sock: UdpSocket,
}

impl Udp {
    /// Constructor
    pub fn bind(addr: impl ToSocketAddrs) -> Self {
        let sock = UdpSocket::bind(addr)
            .unwrap_or_else(|err| panic!("couldn't bind to address, the error is: {}", err));
        Self { sock }
    }

    /// Send data
    pub fn send_to<T: Serialize>(&self, data: &T, addr: impl ToSocketAddrs) {
        let encoded: Vec<u8> = bincode::serialize(data)
            .unwrap_or_else(|err| panic!("failed to encode, the error is: {}", err));
        self.sock
            .send_to(&encoded, addr)
            .unwrap_or_else(|err| panic!("couldn't send data, the error is: {}", err));
        // println!("sent {} bytes", encoded.len());
    }

    /// Receive data
    pub fn recv_from<T: DeserializeOwned>(&self, buf_size: usize) -> (T, std::net::SocketAddr) {
        // println!("socket buf size: {}", buf_size);
        let mut buf = Vec::with_capacity(buf_size);
        unsafe { buf.set_len(buf.capacity()) };
        let (number_of_bytes, src_addr) = self
            .sock
            .recv_from(&mut buf)
            .unwrap_or_else(|err| panic!("failed to receive data, the error is: {}", err));
        unsafe { buf.set_len(number_of_bytes) };

        // println!("received {} bytes", number_of_bytes);
        // dbg!(&buf);

        let decode = bincode::deserialize(&buf)
            .unwrap_or_else(|err| panic!("failed to decode, the error is: {}", err));
        (decode, src_addr)
    }
    /*
        ///
        fn recv(&self, mut buffer: &mut [u8]) -> usize {
            println!("listening...");
            let (number_of_bytes, src_addr) = self
                .sock
                .recv_from(&mut buffer)
                .unwrap_or_else(|err| panic!("no data received, the error is: {}", err));
            println!("{:?}", number_of_bytes);
            println!("{:?}", src_addr);
            number_of_bytes
        }

        ///
        fn send(&self, msg: &[u8], receiver: &str) -> usize {
            println!("sending data");
            self.sock
                .send_to(msg, receiver)
                .unwrap_or_else(|err| panic!("failed to send message, the error is: {}", err))
        }
    */
}

/// TCP socket wrapper struct
pub struct Tcp {
    /// TCP socket
    sockfd: RawFd,
}

impl Drop for Tcp {
    fn drop(&mut self) {
        unsafe { libc::close(self.sockfd) };
    }
}

impl Tcp {
    /// Build server TCP socket
    pub fn bind(port: u16) -> Self {
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

        let mut rc: c_int;
        let mut resolved_addr = std::ptr::null_mut::<libc::addrinfo>();

        // Resolve DNS address
        rc = unsafe {
            libc::getaddrinfo(
                std::ptr::null::<c_char>(),
                server_port_cstr.as_ptr(),
                &hints,
                &mut resolved_addr,
            )
        };
        if rc < 0 {
            let err_cstr = unsafe { CStr::from_ptr(libc::gai_strerror(rc)) };
            panic!(
                "failed to bind to port {}, the error is: {:?}",
                port, err_cstr
            );
        }
        // util::check_errno(rc)?;

        // Search through results and find the one we want
        let mut iterator = resolved_addr;
        let mut listenfd = -1;
        while !util::is_null_mut_ptr(iterator) {
            let addr = unsafe { &*iterator };
            listenfd = unsafe { libc::socket(addr.ai_family, addr.ai_socktype, addr.ai_protocol) };
            if listenfd >= 0 {
                // Server mode, set up listening socket an accept a connection
                rc = unsafe { libc::bind(listenfd, addr.ai_addr, addr.ai_addrlen) };
                if rc != 0 {
                    panic!("socket bind failed");
                }
            } else {
                panic!("faield to create socket")
            }
            iterator = addr.ai_next;
        }

        if !util::is_null_mut_ptr(resolved_addr) {
            unsafe { libc::freeaddrinfo(resolved_addr) };
        }
        Self { sockfd: listenfd }
    }

    /// Accept client TCP socket
    pub fn accept(&self) -> Self {
        let backlog = 1;
        let rc = unsafe { libc::listen(self.sockfd, backlog) };
        if rc != 0 {
            panic!("socket listen failed");
        }
        let client_addr = std::ptr::null_mut::<libc::sockaddr>();
        let mut addr_len = 0;
        let sockfd = unsafe { libc::accept(self.sockfd, client_addr, &mut addr_len) };
        if sockfd < 0 {
            panic!("socket accept failed");
        }
        Self { sockfd }
    }

    /// Build client TCP socket
    pub fn connect(server_name: &str, port: u16) -> Self {
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

        let mut rc: c_int;
        let mut resolved_addr = std::ptr::null_mut::<libc::addrinfo>();

        // Resolve DNS address
        rc = unsafe {
            libc::getaddrinfo(
                server_addr_cstr.as_ptr(),
                server_port_cstr.as_ptr(),
                &hints,
                &mut resolved_addr,
            )
        };
        if rc < 0 {
            let err_cstr = unsafe { CStr::from_ptr(libc::gai_strerror(rc)) };
            panic!("{:?} for {}:{}", err_cstr, server_name, port);
        }
        // util::check_errno(rc)?;

        // Search through results and find the one we want
        let mut iterator = resolved_addr;
        let mut sockfd = -1;
        while !util::is_null_mut_ptr(iterator) {
            let addr = unsafe { &*iterator };
            sockfd = unsafe { libc::socket(addr.ai_family, addr.ai_socktype, addr.ai_protocol) };
            if sockfd >= 0 {
                // Client mode. Initiate connection to remote
                rc = unsafe { libc::connect(sockfd, addr.ai_addr, addr.ai_addrlen) };
                if rc != 0 {
                    sockfd = -1;
                    unsafe { libc::close(sockfd) };
                    util::check_errno(rc).unwrap_or_else(|err| {
                        panic!("socket connect failed, the error is: {}", err)
                    });
                }
            }

            iterator = addr.ai_next;
        }
        if !util::is_null_mut_ptr(resolved_addr) {
            unsafe { libc::freeaddrinfo(resolved_addr) };
        }
        Self { sockfd }
    }

    /// Send data
    pub fn send<T: Serialize>(&self, data: &T) -> isize {
        let encoded: Vec<u8> = bincode::serialize(data)
            .unwrap_or_else(|err| panic!("failed to encode, the error is: {}", err));
        let rc = unsafe {
            libc::write(
                self.sockfd,
                util::const_ptr_cast(encoded.as_ptr()),
                encoded.len(),
            )
        };
        if rc < encoded.len().cast() {
            panic!("failed to send data via TCP");
        }
        println!("sent {} bytes", rc);
        rc
    }

    /// Recieve data
    pub fn recv<T: DeserializeOwned>(&self, buf_size: usize) -> T {
        println!("socket buf size: {}", buf_size);
        let mut buf = Vec::with_capacity(buf_size);
        unsafe { buf.set_len(buf.capacity()) };
        let rc = unsafe {
            libc::read(
                self.sockfd,
                util::const_ptr_cast_mut(buf.as_ptr()),
                buf.capacity(),
            )
        };
        println!("read {} bytes", rc);
        unsafe {
            buf.set_len(rc.cast());
        }
        bincode::deserialize(&buf)
            .unwrap_or_else(|err| panic!("failed to decode, the error is: {}", err))
    }
}

/// Unit test
mod test {
    use super::*;
    use serde::Deserialize;

    /// Data to be sent and received via socket
    #[derive(Deserialize, Serialize)]
    struct TestStruct {
        /// Number field
        num: u64,
        /// String field
        msg: String,
    }

    #[test]
    fn test_udp() {
        let num = 64;
        let msg = std::iter::repeat("Test Data String, ")
            .take(1000)
            .collect::<String>();
        let buf_size = std::mem::size_of::<TestStruct>() + msg.len();
        let srv_addr = "0.0.0.0:54321";
        let clt_addr = "0.0.0.0:23456";

        let msg_clone = msg.clone();
        let srv_handle = std::thread::spawn(move || {
            let srv_sock = Udp::bind(srv_addr);
            let wait_secs = std::time::Duration::new(5, 0);
            srv_sock
                .sock
                .set_read_timeout(Some(wait_secs))
                .unwrap_or_else(|err| panic!("failed to set read timeout, the error is: {}", err));
            let (recv_data, _) = srv_sock.recv_from::<TestStruct>(buf_size);
            assert_eq!(recv_data.num, num);
            assert_eq!(recv_data.msg, msg_clone);
        });

        let clt_handle = std::thread::spawn(move || {
            let clt_sock = Udp::bind(clt_addr);
            let send_data = TestStruct { num, msg };
            clt_sock.send_to(&send_data, "127.0.0.1:54321");
        });
        let srv_res = srv_handle.join();
        let clt_res = clt_handle.join();
        assert!(srv_res.is_ok(), "failed to join receive thread");
        assert!(clt_res.is_ok(), "failed to join send thread");
    }

    #[test]
    fn test_tcp() {
        let num = 64;
        let msg = std::iter::repeat("Test Data String, ")
            .take(1000)
            .collect::<String>();
        let buf_size = std::mem::size_of::<TestStruct>() + msg.len();
        let srv_port = 54321;
        let srv_addr = "127.0.0.1";

        let msg_clone = msg.clone();
        let srv_handle = std::thread::spawn(move || {
            let srv_sock = Tcp::bind(srv_port);
            let rmt_sock = srv_sock.accept();

            let recv_data: TestStruct = rmt_sock.recv(buf_size);
            assert_eq!(recv_data.num, num);
            assert_eq!(recv_data.msg, msg_clone);
        });

        let clt_handle = std::thread::spawn(move || {
            let clt_sock = Tcp::connect(srv_addr, srv_port);
            let send_data = TestStruct { num, msg };
            clt_sock.send(&send_data);
        });
        let srv_res = srv_handle.join();
        let clt_res = clt_handle.join();
        assert!(srv_res.is_ok(), "failed to join receive thread");
        assert!(clt_res.is_ok(), "failed to join send thread");
    }
}
