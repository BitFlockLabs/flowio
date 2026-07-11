//! Blocking standard-library peer helpers for TLS cancellation tests.

use std::io::{self, Read};
use std::net::TcpStream;
use std::os::fd::AsRawFd;
use std::time::Duration;

const CLIENT_HELLO_DRAIN_TIMEOUT: Duration = Duration::from_millis(100);

/// Configures `tcp` so closing it emits a reset instead of a graceful FIN.
pub fn force_reset_on_drop(tcp: &TcpStream) {
    let linger = libc::linger {
        l_onoff: 1,
        l_linger: 0,
    };
    // SAFETY: `linger` is a live `libc::linger` value with the exact size
    // required by SO_LINGER, and the borrowed stream keeps the fd open.
    let rc = unsafe {
        libc::setsockopt(
            tcp.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_LINGER,
            &linger as *const libc::linger as *const libc::c_void,
            std::mem::size_of::<libc::linger>() as libc::socklen_t,
        )
    };
    assert_eq!(rc, 0, "setsockopt SO_LINGER failed");
}

/// Drains available ClientHello bytes, then returns and closes the peer.
///
/// Panics when no bytes arrive within the bounded test timeout or when the
/// blocking test peer encounters an unexpected read error.
pub fn drain_available_client_hello(mut tcp: TcpStream) {
    tcp.set_read_timeout(Some(CLIENT_HELLO_DRAIN_TIMEOUT))
        .expect("set_read_timeout failed");

    let mut saw_bytes = false;
    let mut buf = [0u8; 4096];
    loop {
        match tcp.read(&mut buf) {
            Ok(0) => break,
            Ok(_) => saw_bytes = true,
            Err(err)
                if err.kind() == io::ErrorKind::WouldBlock
                    || err.kind() == io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(err) => panic!("server client-hello drain failed: {err}"),
        }
    }
    assert!(
        saw_bytes,
        "server did not receive a ClientHello before peer close/reset"
    );
}
