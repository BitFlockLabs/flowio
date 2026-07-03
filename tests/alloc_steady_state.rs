use flowio::net::sctp::{SctpConnector, SctpInitConfig, SctpListener, SctpSocketConfig};
use flowio::net::tcp::TcpStream;
use flowio::net::udp::UdpSocket;
use flowio::runtime::executor::Executor;
use flowio::runtime::timer::sleep;
use std::alloc::{GlobalAlloc, Layout, System};
use std::net::{Ipv4Addr, SocketAddr, UdpSocket as StdUdpSocket};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

struct CountingAllocator;

static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static DEALLOCS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        DEALLOCS.fetch_add(1, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !ptr.is_null() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            DEALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        ptr
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

#[derive(Clone, Copy)]
struct AllocationSnapshot {
    allocs: usize,
    deallocs: usize,
}

impl AllocationSnapshot {
    fn current() -> Self {
        Self {
            allocs: ALLOCS.load(Ordering::Relaxed),
            deallocs: DEALLOCS.load(Ordering::Relaxed),
        }
    }

    fn assert_unchanged_since(self, before: Self) {
        assert_eq!(
            self.allocs, before.allocs,
            "steady-state path performed heap allocations"
        );
        assert_eq!(
            self.deallocs, before.deallocs,
            "steady-state path performed heap deallocations"
        );
    }
}

fn sctp_unsupported(err: &std::io::Error) -> bool {
    matches!(
        err.raw_os_error(),
        Some(libc::EPROTONOSUPPORT)
            | Some(libc::ESOCKTNOSUPPORT)
            | Some(libc::EAFNOSUPPORT)
            | Some(libc::EPFNOSUPPORT)
    )
}

fn spawn_tcp_echo(rounds: usize) -> (SocketAddr, JoinHandle<()>) {
    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("tcp echo bind failed");
    let addr = listener.local_addr().expect("tcp echo local_addr failed");
    let handle = std::thread::spawn(move || {
        use std::io::{Read, Write};

        let (mut stream, _) = listener.accept().expect("tcp echo accept failed");
        let mut buf = [0u8; 4];
        for _ in 0..rounds {
            stream.read_exact(&mut buf).expect("tcp echo read failed");
            assert_eq!(&buf, b"ping");
            stream.write_all(b"pong").expect("tcp echo write failed");
        }
    });
    (addr, handle)
}

fn spawn_udp_echo(rounds: usize) -> (SocketAddr, JoinHandle<()>) {
    let socket = StdUdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("udp echo bind failed");
    let addr = socket.local_addr().expect("udp echo local_addr failed");
    let handle = std::thread::spawn(move || {
        let mut buf = [0u8; 4];
        for _ in 0..rounds {
            let (read, peer) = socket.recv_from(&mut buf).expect("udp echo recv failed");
            assert_eq!(read, 4);
            assert_eq!(&buf, b"ping");
            socket.send_to(b"pong", peer).expect("udp echo send failed");
        }
    });
    (addr, handle)
}

async fn tcp_echo_once(stream: &mut TcpStream, recv: Vec<u8>) -> Vec<u8> {
    let payload: &'static [u8] = b"ping";
    let (send_res, _payload) = stream.write_all(payload).await;
    assert_eq!(send_res.expect("tcp send failed"), 4);

    let (recv_res, recv) = stream.read(recv, 4).await;
    assert_eq!(recv_res.expect("tcp recv failed"), 4);
    assert_eq!(&recv[..], b"pong");
    recv
}

async fn udp_echo_once(socket: &mut UdpSocket, recv: Vec<u8>) -> Vec<u8> {
    let payload: &'static [u8] = b"ping";
    let (send_res, _payload) = socket.send(payload).await;
    assert_eq!(send_res.expect("udp send failed"), 4);

    let (recv_res, recv) = socket.recv(recv, 4).await;
    assert_eq!(recv_res.expect("udp recv failed"), 4);
    assert_eq!(&recv[..], b"pong");
    recv
}

#[test]
fn steady_state_runtime_and_transport_paths_do_not_allocate_after_warmup() {
    const STEADY_ROUNDS: usize = 4;
    const TOTAL_ROUNDS: usize = STEADY_ROUNDS + 2;

    // This test enforces the steady-state claim. DNS resolution and TLS
    // handshakes are setup/control-plane work and are intentionally outside
    // this measured data-path window.
    let (tcp_addr, tcp_thread) = spawn_tcp_echo(TOTAL_ROUNDS);
    let (udp_addr, udp_thread) = spawn_udp_echo(TOTAL_ROUNDS);

    let sctp_config = SctpSocketConfig::data(SctpInitConfig::diameter_default());
    let sctp_setup = match SctpListener::bind_with_config(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        128,
        sctp_config,
    ) {
        Ok(listener) => {
            let addr = listener.local_addr();
            Some((listener, SctpConnector::with_config(sctp_config), addr))
        }
        Err(err) if sctp_unsupported(&err) => {
            eprintln!("skipping SCTP allocation-counting segment: SCTP unsupported ({err})");
            None
        }
        Err(err) => panic!("failed to bind SCTP allocation-counting listener: {err}"),
    };

    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor
        .run(async move {
            let mut tcp = TcpStream::connect(tcp_addr)
                .expect("tcp connect init failed")
                .await
                .expect("tcp connect failed");
            let mut tcp_recv = Vec::with_capacity(4);

            let mut udp =
                UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("udp bind");
            udp.connect(udp_addr).expect("udp connect failed");
            let mut udp_recv = Vec::with_capacity(4);

            let mut sctp_client_and_server =
                if let Some((mut listener, mut connector, addr)) = sctp_setup {
                    let server = Executor::spawn(async move {
                        let (mut stream, _) = listener.accept().await.expect("sctp accept failed");
                        let mut recv = Vec::with_capacity(4);
                        for _ in 0..TOTAL_ROUNDS {
                            let (recv_res, returned) = stream.recv(recv, 4).await;
                            recv = returned;
                            assert_eq!(recv_res.expect("sctp server recv failed"), 4);
                            assert_eq!(&recv[..], b"ping");

                            let payload: &'static [u8] = b"pong";
                            let (send_res, _payload) = stream.send(payload).await;
                            assert_eq!(send_res.expect("sctp server send failed"), 4);
                        }
                    })
                    .expect("sctp server spawn failed");

                    let client = connector
                        .connect(addr)
                        .expect("sctp connect init failed")
                        .await
                        .expect("sctp connect failed");
                    Some((client, server, Vec::with_capacity(4)))
                } else {
                    None
                };

            tcp_recv = tcp_echo_once(&mut tcp, tcp_recv).await;
            udp_recv = udp_echo_once(&mut udp, udp_recv).await;
            if let Some((client, _server, recv)) = sctp_client_and_server.as_mut() {
                let payload: &'static [u8] = b"ping";
                let (send_res, _payload) = client.send(payload).await;
                assert_eq!(send_res.expect("sctp client send failed"), 4);
                let (recv_res, returned) = client.recv(std::mem::take(recv), 4).await;
                *recv = returned;
                assert_eq!(recv_res.expect("sctp client recv failed"), 4);
                assert_eq!(&recv[..], b"pong");
            }
            sleep(Duration::from_millis(1))
                .await
                .expect("timer warmup sleep failed");
            let warm = Executor::spawn(async { 7usize }).expect("spawn warmup failed");
            assert_eq!(warm.await, 7);

            let before = AllocationSnapshot::current();

            for _ in 0..STEADY_ROUNDS {
                tcp_recv = tcp_echo_once(&mut tcp, tcp_recv).await;
                udp_recv = udp_echo_once(&mut udp, udp_recv).await;

                if let Some((client, _server, recv)) = sctp_client_and_server.as_mut() {
                    let payload: &'static [u8] = b"ping";
                    let (send_res, _payload) = client.send(payload).await;
                    assert_eq!(send_res.expect("sctp client send failed"), 4);
                    let (recv_res, returned) = client.recv(std::mem::take(recv), 4).await;
                    *recv = returned;
                    assert_eq!(recv_res.expect("sctp client recv failed"), 4);
                    assert_eq!(&recv[..], b"pong");
                }

                sleep(Duration::from_millis(1))
                    .await
                    .expect("timer steady-state sleep failed");
                let handle = Executor::spawn(async { 11usize }).expect("spawn failed");
                assert_eq!(handle.await, 11);
            }

            let after = AllocationSnapshot::current();
            after.assert_unchanged_since(before);

            tcp_recv = tcp_echo_once(&mut tcp, tcp_recv).await;
            udp_recv = udp_echo_once(&mut udp, udp_recv).await;
            let _ = (tcp_recv, udp_recv);
            if let Some((client, _server, recv)) = sctp_client_and_server.as_mut() {
                let payload: &'static [u8] = b"ping";
                let (send_res, _payload) = client.send(payload).await;
                assert_eq!(send_res.expect("sctp client post send failed"), 4);
                let (recv_res, returned) = client.recv(std::mem::take(recv), 4).await;
                *recv = returned;
                assert_eq!(recv_res.expect("sctp client post recv failed"), 4);
                assert_eq!(&recv[..], b"pong");
            }
        })
        .expect("executor run failed");

    tcp_thread.join().expect("tcp echo thread panicked");
    udp_thread.join().expect("udp echo thread panicked");
}
