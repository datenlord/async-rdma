//! RDMA Rust Async API

#![deny(
    // The following are allowed by default lints according to
    // https://doc.rust-lang.org/rustc/lints/listing/allowed-by-default.html
    anonymous_parameters,
    bare_trait_objects,
    // box_pointers, // use box pointer to allocate on heap
    // elided_lifetimes_in_paths, // allow anonymous lifetime
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs, // TODO: add documents
    single_use_lifetimes, // TODO: fix lifetime names only used once
    trivial_casts, // TODO: remove trivial casts in code
    trivial_numeric_casts,
    // unreachable_pub, allow clippy::redundant_pub_crate lint instead
    // unsafe_code,
    unstable_features,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications,
    // unused_results, // TODO: fix unused results
    variant_size_differences,

    warnings, // treat all wanings as errors

    clippy::all,
    clippy::restriction,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo
)]
#![allow(
    // Some explicitly allowed Clippy lints, must have clear reason to allow
    clippy::blanket_clippy_restriction_lints, // allow denying clippy::restriction directly
    clippy::implicit_return, // actually omitting the return keyword is idiomatic Rust code
    clippy::module_name_repetitions, // repeation of module name in a struct name is not big deal
    clippy::multiple_crate_versions, // multi-version dependency crates is not able to fix
    clippy::panic, // allow debug_assert, panic in production code
    clippy::panic_in_result_fn, // allow debug_assert
)]

// use std::env;
// use std::ffi::CString;
// use std::os::raw::{c_char, c_int};
// use std::os::unix::ffi::OsStringExt;
// use utilities::Cast;
// use std::os::raw::{c_char, c_int};

mod basic;

// #[cfg(target_os = "linux")]
// extern "C" {
//     fn server_main(argc: c_int, argv: *const *const c_char) -> c_int;
//     fn client_main(argc: c_int, argv: *const *const c_char) -> c_int;
// }

///
const SERVER_PORT_ARG_NAME: &str = "server_port";
///
const SERVER_ADDR_ARG_NAME: &str = "server_addr";

fn main() -> anyhow::Result<()> {
    env_logger::init();

    let matches = clap::App::new("async-rdma")
        .about("Async RDMA API")
        .arg(
            clap::Arg::with_name(SERVER_PORT_ARG_NAME)
                .short("p")
                .long(SERVER_PORT_ARG_NAME)
                .value_name("SERVER_PORT")
                .takes_value(true)
                .required(true)
                .help("Set the RDMA server port, no default value"),
        )
        .arg(
            clap::Arg::with_name(SERVER_ADDR_ARG_NAME)
                .short("s")
                .long(SERVER_ADDR_ARG_NAME)
                .value_name("SERVER_ADDR")
                .takes_value(true)
                .help("Set the RDMA server address, no default value"),
        )
        .get_matches();
    let server_port = match matches.value_of(SERVER_PORT_ARG_NAME) {
        Some(sp) => sp.parse::<u16>()?,
        None => panic!("No server port input"),
    };
    let server_addr = matches.value_of(SERVER_ADDR_ARG_NAME).unwrap_or("");

    // run server or client
    let dev_name = ""; //"rxe_eth0";
    let gid_idx = 1; // 0: IPv6, 1: IPv4
    let ib_port = 1;

    if server_addr.is_empty() {
        basic::ibv::run_server(dev_name, gid_idx, ib_port, server_port);
    } else {
        basic::ibv::run_client(server_addr, dev_name, gid_idx, ib_port, server_port);
        // basic::util::check_errno(-1)?;
    }
    if true {
        #[allow(clippy::exit)]
        std::process::exit(0);
    }

    if server_addr.is_empty() {
        basic::pure_ibv::run("", dev_name, gid_idx, ib_port, server_port);
    } else {
        basic::pure_ibv::run(server_addr, dev_name, gid_idx, ib_port, server_port);
        // basic::util::check_errno(-1)?;
    }
    if true {
        #[allow(clippy::exit)]
        std::process::exit(0);
    }

    // if server_addr.is_empty() {
    //     basic::pingpong::run_server("", dev_name, gid_idx, ib_port, server_port);
    // } else {
    //     basic::pingpong::run_client(server_addr, dev_name, gid_idx, ib_port, server_port);
    //     // basic::util::check_errno(-1)?;
    // }
    // if true {
    //     #[allow(clippy::exit)]
    //     std::process::exit(0);
    // }

    // if server_addr.is_empty() {
    //     basic::srq::run(true, "");
    // } else {
    //     basic::srq::run(false, server_addr);
    //     // basic::util::check_errno(-1)?;
    // }
    // if true {
    //     #[allow(clippy::exit)]
    //     std::process::exit(0);
    // }

    // if server_addr.is_empty() {
    //     basic::ud_pingpong::run("", dev_name, gid_idx, ib_port, server_port);
    // } else {
    //     basic::ud_pingpong::run(server_addr, dev_name, gid_idx, ib_port, server_port);
    //     // basic::util::check_errno(-1)?;
    // }
    // if true {
    //     #[allow(clippy::exit)]
    //     std::process::exit(0);
    // }

    if server_addr.is_empty() {
        let server = basic::async_server::Server::new(server_port);
        server.run();
    } else {
        let client = basic::async_server::Client::new(server_addr, server_port);
        client.run();
        // basic::util::check_errno(-1)?;
    }
    if true {
        #[allow(clippy::exit)]
        std::process::exit(0);
    }

    if server_addr.is_empty() {
        basic::sync_server::server(server_port);
    } else {
        basic::sync_server::client(server_addr, server_port);
        basic::util::check_errno(-1)?;
    }

    // let connections: c_int = 1;
    // let message_size: c_int = 100;
    // let message_count: c_int = 10;
    // let is_sender: bool = true;
    // let unmapped_addr: c_int = 1;
    // let dst_addr: *mut c_char = std::ptr::null_mut();
    // let src_addr: *mut c_char = std::ptr::null_mut();
    // basic::mckey::run_main(
    //     connections,
    //     message_size,
    //     message_count,
    //     is_sender,
    //     unmapped_addr,
    //     dst_addr,
    //     src_addr,
    // );

    Ok(())
}
