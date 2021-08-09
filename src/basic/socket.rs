//! Sync data via socket

use serde::{de::DeserializeOwned, Serialize};
// use std::cmp::Ordering;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use utilities::OverflowArithmetic; //, OverflowArithmetic};

/// UDP socket wrapper struct
pub struct Udp {
    /// UDP socket
    sock: UdpSocket,
    /// Peer socket address
    peer_addr: SocketAddr,
}

impl Udp {
    /// Constructor
    pub fn bind(port: u16) -> Self {
        let sock = UdpSocket::bind(format!("0.0.0.0:{}", port))
            .unwrap_or_else(|err| panic!("failed to bind to address, the error is: {}", err));
        let peer_addr = SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)),
            port,
        );
        Self { sock, peer_addr }
    }

    /// Accept
    pub fn accept(&self) -> Self {
        let (_, clt_addr) = self.recv_from::<u8>(1);
        println!("accept: {}", clt_addr);
        let clt_sock = UdpSocket::bind("0.0.0.0:0")
            .unwrap_or_else(|err| panic!("failed to bind to address, the error is: {}", err));

        clt_sock.send_to(&[2_u8], &clt_addr).unwrap_or_else(|err| {
            panic!(
                "failed to send connect data to client, the error is: {}",
                err
            )
        });
        Self {
            sock: clt_sock,
            peer_addr: clt_addr,
        }
    }

    /// Connect
    pub fn connect(addr: impl ToSocketAddrs) -> Self {
        let sock = UdpSocket::bind("0.0.0.0:0")
            .unwrap_or_else(|err| panic!("failed to bind to address, the error is: {}", err));
        let mut addr_iter = addr
            .to_socket_addrs()
            .unwrap_or_else(|err| panic!("failed to convert to SocketAddr, the error is:{}", err));
        let srv_addr = addr_iter
            .next()
            .unwrap_or_else(|| panic!("failed to get SocketAddr from iterator"));

        let mut buf = [1_u8];
        sock.send_to(&buf, &srv_addr).unwrap_or_else(|err| {
            panic!(
                "failed to send connect data to server, the error is: {}",
                err
            )
        });
        let (_, peer_addr) = sock.recv_from(&mut buf).unwrap_or_else(|err| {
            panic!(
                "failed to receive connect data from server, the error is: {}",
                err
            )
        });
        println!("srv addr: {}, peer addr: {}", srv_addr, peer_addr);
        Self { sock, peer_addr }
    }

    /// Send data
    pub fn send_to<T: Serialize>(&self, data: &T, addr: impl ToSocketAddrs) {
        let encoded: Vec<u8> = bincode::serialize(data)
            .unwrap_or_else(|err| panic!("failed to encode, the error is: {}", err));
        let send_size = self
            .sock
            .send_to(&encoded, addr)
            .unwrap_or_else(|err| panic!("couldn't send data, the error is: {}", err));
        // println!("sent {} bytes", encoded.len());

        if send_size != encoded.len() {
            panic!(
                "send data size not match, expect: {}, send: {}",
                encoded.len(),
                send_size
            );
        }
    }

    /// Receive data
    pub fn recv_from<T: DeserializeOwned>(&self, buf_size: usize) -> (T, SocketAddr) {
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

    /// Exchange data, send and receive the same type data with a peer
    pub fn exchange_data<T: Serialize, U: DeserializeOwned>(&self, data: &T) -> U {
        let xfer_size = std::mem::size_of::<T>().overflow_add(8); // where does the magic number 8 come from?
        self.send_to(data, self.peer_addr);
        // println!("send to: {}", self.peer_addr);
        let (decoded, _) = self.recv_from::<U>(xfer_size);
        decoded
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
        const SRV_MSG: &str = "HELLO FROM SERVER";
        const CLT_MSG: &str = "HELLO FROM CLIENT";

        let num = 64;
        let msg = std::iter::repeat("Test Data String, ")
            .take(1000)
            .collect::<String>();
        let buf_size = std::mem::size_of::<TestStruct>() + msg.len();
        let srv_port = 54321;

        let msg_clone = msg.clone();
        let srv_handle = std::thread::spawn(move || {
            let srv_sock = Udp::bind(srv_port);
            let wait_secs = std::time::Duration::new(5, 0);
            srv_sock
                .sock
                .set_read_timeout(Some(wait_secs))
                .unwrap_or_else(|err| panic!("failed to set read timeout, the error is: {}", err));

            let srv_data = TestStruct {
                num: 1,
                msg: SRV_MSG.into(),
            };
            let clt_sock = srv_sock.accept();
            let clt_data: TestStruct = clt_sock.exchange_data(&srv_data);
            assert_eq!(clt_data.msg, CLT_MSG);

            let (recv_data, _) = srv_sock.recv_from::<TestStruct>(buf_size);
            assert_eq!(recv_data.num, num);
            assert_eq!(recv_data.msg, msg_clone);
        });

        let srv_addr = format!("127.0.0.1:{}", srv_port);
        let clt_handle = std::thread::spawn(move || {
            let clt_sock = Udp::connect(&srv_addr);
            let wait_secs = std::time::Duration::new(5, 0);
            clt_sock
                .sock
                .set_read_timeout(Some(wait_secs))
                .unwrap_or_else(|err| panic!("failed to set read timeout, the error is: {}", err));

            let clt_data = TestStruct {
                num: 2,
                msg: CLT_MSG.into(),
            };
            let srv_data: TestStruct = clt_sock.exchange_data(&clt_data);
            assert_eq!(srv_data.msg, SRV_MSG);

            let send_data = TestStruct { num, msg };
            clt_sock.send_to(&send_data, &srv_addr);
        });
        let srv_res = srv_handle.join();
        let clt_res = clt_handle.join();
        assert!(srv_res.is_ok(), "failed to join receive thread");
        assert!(clt_res.is_ok(), "failed to join send thread");
    }
}
