#![allow(dead_code)]

use crate::runtime_longevity_support as fd_observation;
use flowio::net::sctp::{SctpInitConfig, SctpListener, SctpNotificationMask, SctpSocketConfig};
use flowio::net::tcp::TcpListener;
use flowio::net::udp::UdpSocket;
use flowio::test_support::net::bind_setup::{BindSetupReport, BindStage, BindTransport, arm};
use flowio::test_support::net::sctp::test_sctp_socket_options;
use std::fs::File;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket as StdUdpSocket};
use std::os::fd::{AsRawFd, RawFd};
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
enum Api {
    Tcp,
    TcpReusePort,
    Udp,
    UdpReusePort,
    Sctp,
    SctpReusePort,
}

#[derive(Clone, Copy)]
struct Row {
    api: Api,
    failure: Option<(BindStage, i32)>,
    trace: &'static [BindStage],
}

use BindStage::{Bind, Configure, Listen, LocalAddress, ReuseAddress, ReusePort, Socket};

const TCP_ROWS: [Row; 8] = [
    Row {
        api: Api::Tcp,
        failure: Some((Socket, libc::EMFILE)),
        trace: &[Socket],
    },
    Row {
        api: Api::Tcp,
        failure: Some((ReuseAddress, libc::EACCES)),
        trace: &[Socket, ReuseAddress],
    },
    Row {
        api: Api::TcpReusePort,
        failure: Some((ReusePort, libc::ENOPROTOOPT)),
        trace: &[Socket, ReuseAddress, ReusePort],
    },
    Row {
        api: Api::Tcp,
        failure: Some((Bind, libc::EADDRINUSE)),
        trace: &[Socket, ReuseAddress, Bind],
    },
    Row {
        api: Api::Tcp,
        failure: Some((Listen, libc::EIO)),
        trace: &[Socket, ReuseAddress, Bind, Listen],
    },
    Row {
        api: Api::Tcp,
        failure: Some((LocalAddress, libc::EFAULT)),
        trace: &[Socket, ReuseAddress, Bind, Listen, LocalAddress],
    },
    Row {
        api: Api::Tcp,
        failure: None,
        trace: &[Socket, ReuseAddress, Bind, Listen, LocalAddress],
    },
    Row {
        api: Api::TcpReusePort,
        failure: None,
        trace: &[Socket, ReuseAddress, ReusePort, Bind, Listen, LocalAddress],
    },
];

const UDP_ROWS: [Row; 5] = [
    Row {
        api: Api::Udp,
        failure: Some((Socket, libc::EMFILE)),
        trace: &[Socket],
    },
    Row {
        api: Api::Udp,
        failure: Some((Bind, libc::EADDRINUSE)),
        trace: &[Socket, Bind],
    },
    Row {
        api: Api::Udp,
        failure: None,
        trace: &[Socket, Bind],
    },
    Row {
        api: Api::UdpReusePort,
        failure: Some((ReusePort, libc::ENOPROTOOPT)),
        trace: &[Socket, ReusePort],
    },
    Row {
        api: Api::UdpReusePort,
        failure: None,
        trace: &[Socket, ReusePort, Bind],
    },
];

const SCTP_ROWS: [Row; 9] = [
    Row {
        api: Api::Sctp,
        failure: Some((Socket, libc::EMFILE)),
        trace: &[Socket],
    },
    Row {
        api: Api::Sctp,
        failure: Some((Configure, libc::EINVAL)),
        trace: &[Socket, Configure],
    },
    Row {
        api: Api::Sctp,
        failure: Some((ReuseAddress, libc::EACCES)),
        trace: &[Socket, Configure, ReuseAddress],
    },
    Row {
        api: Api::Sctp,
        failure: Some((Bind, libc::EADDRINUSE)),
        trace: &[Socket, Configure, ReuseAddress, Bind],
    },
    Row {
        api: Api::Sctp,
        failure: Some((Listen, libc::EIO)),
        trace: &[Socket, Configure, ReuseAddress, Bind, Listen],
    },
    Row {
        api: Api::Sctp,
        failure: Some((LocalAddress, libc::EFAULT)),
        trace: &[Socket, Configure, ReuseAddress, Bind, Listen, LocalAddress],
    },
    Row {
        api: Api::Sctp,
        failure: None,
        trace: &[Socket, Configure, ReuseAddress, Bind, Listen, LocalAddress],
    },
    Row {
        api: Api::SctpReusePort,
        failure: Some((ReusePort, libc::ENOPROTOOPT)),
        trace: &[Socket, Configure, ReuseAddress, ReusePort],
    },
    Row {
        api: Api::SctpReusePort,
        failure: None,
        trace: &[
            Socket,
            Configure,
            ReuseAddress,
            ReusePort,
            Bind,
            Listen,
            LocalAddress,
        ],
    },
];

enum BoundSocket {
    Tcp(TcpListener),
    Udp(UdpSocket),
    Sctp(SctpListener),
}

impl Api {
    fn transport(self) -> BindTransport {
        match self {
            Self::Tcp | Self::TcpReusePort => BindTransport::Tcp,
            Self::Udp | Self::UdpReusePort => BindTransport::Udp,
            Self::Sctp | Self::SctpReusePort => BindTransport::Sctp,
        }
    }

    fn bind(self, addr: SocketAddr, config: SctpSocketConfig) -> io::Result<BoundSocket> {
        match self {
            Self::Tcp => TcpListener::bind(addr, 8).map(BoundSocket::Tcp),
            Self::TcpReusePort => TcpListener::bind_reuse_port(addr, 8).map(BoundSocket::Tcp),
            Self::Udp => UdpSocket::bind(addr).map(BoundSocket::Udp),
            Self::UdpReusePort => UdpSocket::bind_reuse_port(addr).map(BoundSocket::Udp),
            Self::Sctp => SctpListener::bind_with_config(addr, 8, config).map(BoundSocket::Sctp),
            Self::SctpReusePort => {
                SctpListener::bind_reuse_port_with_config(addr, 8, config).map(BoundSocket::Sctp)
            }
        }
    }
}

pub(super) fn reserve_udp_reuse_port() -> StdUdpSocket {
    // Claim an exclusive automatic port before enabling sharing on the held
    // reservation, so group construction does not leave the port unbound.
    let reservation = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("exclusive UDP port reservation failed");
    let enabled: libc::c_int = 1;
    // SAFETY: The descriptor is owned by the live reservation, and the option
    // points to one initialized integer for the duration of setsockopt.
    let rc = unsafe {
        libc::setsockopt(
            reservation.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_REUSEPORT,
            (&enabled as *const libc::c_int).cast(),
            std::mem::size_of_val(&enabled) as libc::socklen_t,
        )
    };
    assert_eq!(
        rc,
        0,
        "UDP port reservation could not permit group membership: {}",
        io::Error::last_os_error()
    );
    reservation
}

pub(super) fn socket_option(fd: RawFd, name: libc::c_int) -> libc::c_int {
    let mut value: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: `value` is initialized writable storage for the supplied length;
    // `len` remains live and writable throughout this non-owning query.
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            name,
            std::ptr::addr_of_mut!(value).cast(),
            &mut len,
        )
    };
    assert_eq!(
        rc,
        0,
        "getsockopt({name}) failed: {}",
        io::Error::last_os_error()
    );
    assert_eq!(len as usize, std::mem::size_of::<libc::c_int>());
    value
}

fn assert_report(report: BindSetupReport, row: Row) {
    assert!(!report.trace_overflow);
    assert!(!report.unexpected_transport);
    assert!(!report.invalid_socket_report);
    assert!(!report.failure_pending);
    assert_eq!(report.injections, u8::from(row.failure.is_some()));
    assert_eq!(report.trace_len, row.trace.len());
    for (index, stage) in row.trace.iter().copied().enumerate() {
        assert_eq!(report.trace[index], Some(stage));
    }
    assert!(report.trace[row.trace.len()..].iter().all(Option::is_none));
}

fn assert_live_socket(socket: &BoundSocket, api: Api, expected_fd: RawFd) {
    let (fd, local_addr) = match socket {
        BoundSocket::Tcp(listener) => (listener.as_raw_fd(), listener.local_addr()),
        BoundSocket::Udp(socket) => {
            assert_eq!(socket.peer_addr(), None);
            (
                socket.as_raw_fd(),
                socket.local_addr().expect("bound UDP local address"),
            )
        }
        BoundSocket::Sctp(listener) => (listener.as_raw_fd(), listener.local_addr()),
    };
    assert_eq!(fd, expected_fd);
    assert!(crate::common::raw_fd_is_open(fd));
    assert_eq!(local_addr.ip(), Ipv4Addr::LOCALHOST);
    assert_ne!(local_addr.port(), 0);
    // SAFETY: Both fcntl queries borrow a known-live descriptor and take no
    // ownership or pointer argument.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    assert!(flags >= 0);
    assert_ne!(flags & libc::O_NONBLOCK, 0);
    let fd_flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    assert!(fd_flags >= 0);
    assert_ne!(fd_flags & libc::FD_CLOEXEC, 0);
    assert_eq!(
        socket_option(fd, libc::SO_REUSEADDR),
        i32::from(!matches!(api, Api::Udp | Api::UdpReusePort))
    );
    match api {
        Api::Tcp => {
            assert_eq!(socket_option(fd, libc::SO_ACCEPTCONN), 1);
            assert_eq!(socket_option(fd, libc::SO_REUSEPORT), 0);
        }
        Api::TcpReusePort => {
            assert_eq!(socket_option(fd, libc::SO_ACCEPTCONN), 1);
            assert_eq!(socket_option(fd, libc::SO_REUSEPORT), 1);
        }
        Api::Udp | Api::UdpReusePort => assert_eq!(
            socket_option(fd, libc::SO_REUSEPORT),
            i32::from(matches!(api, Api::UdpReusePort))
        ),
        Api::Sctp | Api::SctpReusePort => {
            assert_eq!(
                socket_option(fd, libc::SO_REUSEPORT),
                i32::from(matches!(api, Api::SctpReusePort))
            );
            assert_eq!(socket_option(fd, libc::SO_ACCEPTCONN), 1);
            let options =
                test_sctp_socket_options(fd).expect("real configured SCTP listener options");
            assert_eq!(options.notifications, SctpNotificationMask::none());
            assert!(!options.recv_rcvinfo);
            assert!(options.nodelay);
        }
    }
}

fn run_rows(rows: &[Row], expected_rows: usize) {
    fd_observation::assert_fd_count_instrument_discriminates();
    let initial = fd_observation::process_fd_count();
    let sentinel = File::open("/dev/null").expect("open held descriptor sentinel");
    let sentinel_fd = sentinel.as_raw_fd();
    assert_eq!(fd_observation::process_fd_count(), initial + 1);
    let mut completed = 0;
    for &row in rows {
        assert_eq!(fd_observation::process_fd_count(), initial + 1);
        // Keep the failure row's reservation exclusive so an unintended bind
        // cannot join another process's group. The success row permits sharing
        // while the reservation holds its explicit address.
        let reservation = matches!(row.api, Api::UdpReusePort).then(|| {
            if row.failure.is_some() {
                StdUdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("reserve UDP failure address")
            } else {
                reserve_udp_reuse_port()
            }
        });
        let baseline = fd_observation::process_fd_count();
        assert_eq!(baseline, initial + 1 + usize::from(reservation.is_some()));
        let addr = reservation.as_ref().map_or_else(
            || SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            |socket| socket.local_addr().expect("reserved UDP local address"),
        );
        let config = SctpSocketConfig::data(SctpInitConfig::default());
        let probe = arm(row.api.transport(), row.failure);
        let expected_fd = crate::common::lowest_available_fd();
        // No descriptor may be opened or closed between this probe and bind.
        let result = row.api.bind(addr, config);
        let report = probe.finish();
        // Check the numeric descriptor before /proc can reuse a closed slot.
        match (row.failure, result) {
            (Some((stage, errno)), Err(error)) => {
                assert!(!crate::common::raw_fd_is_open(expected_fd));
                assert!(crate::common::raw_fd_is_open(sentinel_fd));
                assert_eq!(error.raw_os_error(), Some(errno));
                assert_eq!(
                    report.socket_fd,
                    if stage == Socket {
                        None
                    } else {
                        Some(expected_fd)
                    }
                );
                assert_report(report, row);
                assert_eq!(fd_observation::process_fd_count(), baseline);
            }
            (None, Ok(socket)) => {
                assert!(crate::common::raw_fd_is_open(expected_fd));
                assert!(crate::common::raw_fd_is_open(sentinel_fd));
                assert_eq!(report.socket_fd, Some(expected_fd));
                assert_report(report, row);
                assert_live_socket(&socket, row.api, expected_fd);
                assert_eq!(fd_observation::process_fd_count(), baseline + 1);
                drop(socket);
                assert!(!crate::common::raw_fd_is_open(expected_fd));
                assert!(crate::common::raw_fd_is_open(sentinel_fd));
                assert_eq!(fd_observation::process_fd_count(), baseline);
            }
            (Some(_), Ok(_socket)) => panic!("injected setup failure unexpectedly succeeded"),
            (None, Err(error)) => panic!("real {:?} setup must succeed: {error}", row.api),
        }
        drop(reservation);
        assert_eq!(fd_observation::process_fd_count(), initial + 1);
        completed += 1;
    }
    assert_eq!(completed, expected_rows);
    assert!(crate::common::raw_fd_is_open(sentinel_fd));
    drop(sentinel);
    assert!(!crate::common::raw_fd_is_open(sentinel_fd));
    assert_eq!(fd_observation::process_fd_count(), initial);
}

fn run(test_name: &str, child_env: &str, rows: &[Row], expected_rows: usize) {
    if std::env::var_os(child_env).is_none() {
        crate::common::run_exact_test_child_with_watchdog(
            test_name,
            child_env,
            Duration::from_secs(30),
        );
        return;
    }
    assert_eq!(std::env::var(child_env).as_deref(), Ok("1"));
    run_rows(rows, expected_rows);
}

pub fn tcp() {
    run(
        "runtime_tcp_bind_setup_failures_recover_exact_descriptors",
        "FLOWIO_TCP_BIND_SETUP_CHILD",
        &TCP_ROWS,
        8,
    );
}

pub fn udp() {
    if std::env::var_os("FLOWIO_UDP_BIND_SETUP_CHILD").is_some() {
        for addr in [
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            SocketAddr::from((Ipv6Addr::LOCALHOST, 0)),
        ] {
            let probe = arm(BindTransport::Udp, Some((Socket, libc::EMFILE)));
            let error = UdpSocket::bind_reuse_port(addr)
                .err()
                .expect("UDP reuse-port must reject automatic port selection");
            let report = probe.finish();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(report.trace_len, 0);
            assert!(report.trace.iter().all(Option::is_none));
            assert_eq!(report.socket_fd, None);
            assert_eq!(report.injections, 0);
            assert!(report.failure_pending);
            assert!(!report.trace_overflow);
            assert!(!report.unexpected_transport);
            assert!(!report.invalid_socket_report);
        }
    }
    run(
        "runtime_udp_bind_setup_failures_recover_exact_descriptors",
        "FLOWIO_UDP_BIND_SETUP_CHILD",
        &UDP_ROWS,
        5,
    );
}

pub fn sctp() {
    run(
        "runtime_sctp_bind_setup_failures_recover_exact_descriptors",
        "FLOWIO_SCTP_BIND_SETUP_CHILD",
        &SCTP_ROWS,
        9,
    );
}
