mod common;

use common::{
    EmptyProjected, TestIoBuffMut as IoBuffMut, TestProjected, TryCountMismatchedProjected,
    TryMismatchedProjected, TryOversizedProjected, fill_try_send_buffer, make_payload_chain,
    make_read_chain, make_read_only_chain, run_test,
};
use flowio::net::tcp::{TcpConnector, TcpListener, TcpStream};
use flowio::runtime::buffer::pool::{IoBuffPool, IoBuffPoolConfig};
use flowio::runtime::executor::Executor;
use flowio::runtime::timer::sleep;
use flowio::test_support::net::tcp::test_accept_slot_drop_cached_state_closes_completed_fd;
use flowio::test_support::runtime::test_hooks;
use std::cell::Cell;
use std::future::Future;
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, Shutdown, SocketAddr};
use std::os::fd::{AsRawFd, OwnedFd};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

/// Spawns a std TCP peer that connects, verifies the payload it receives, and
/// writes a fixed response.
fn spawn_std_tcp_peer(
    addr: SocketAddr,
    expected_recv: Vec<u8>,
    response: Vec<u8>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        use std::io::{Read, Write};

        let mut stream = std::net::TcpStream::connect(addr).expect("std connect failed");
        let mut buf = vec![0u8; expected_recv.len()];
        stream.read_exact(&mut buf).expect("std read failed");
        assert_eq!(buf, expected_recv, "std peer received unexpected payload");
        stream.write_all(&response).expect("std write failed");
    })
}

/// Returns a FlowIO TcpStream wrapping an accepted nonblocking std socket plus
/// its connected std peer for try_* tests that do not need a reactor.
fn connected_try_tcp_stream() -> (TcpStream, std::net::TcpStream) {
    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");
    let peer = std::net::TcpStream::connect(addr).expect("std connect failed");
    let (stream, _) = listener.accept().expect("std accept failed");
    stream
        .set_nonblocking(true)
        .expect("set_nonblocking failed");
    (TcpStream::from_owned_fd(stream.into()), peer)
}

#[test]
fn runtime_tcp_owned_fd_adoption_preserves_nonblocking_and_close_ownership() {
    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");
    let mut peer = std::net::TcpStream::connect(addr).expect("std connect failed");
    let (standard, _) = listener.accept().expect("std accept failed");
    standard
        .set_nonblocking(true)
        .expect("set_nonblocking failed");

    let raw = standard.as_raw_fd();
    let owned: OwnedFd = standard.into();
    let stream = TcpStream::from_owned_fd(owned);
    assert_eq!(stream.as_raw_fd(), raw);
    let status = unsafe { libc::fcntl(raw, libc::F_GETFL) };
    assert!(status >= 0, "F_GETFL failed for adopted TCP fd");
    assert_ne!(
        status & libc::O_NONBLOCK,
        0,
        "adopted TCP fd became blocking"
    );

    drop(stream);
    peer.set_read_timeout(Some(Duration::from_secs(1)))
        .expect("set_read_timeout failed");
    let mut byte = [0u8; 1];
    match peer.read(&mut byte) {
        Ok(0) => {}
        Err(err) if err.kind() == io::ErrorKind::ConnectionReset => {}
        result => panic!("adopted TCP descriptor was not closed exactly once: {result:?}"),
    }
}

/// Asserts that the runtime closed an orphaned accepted fd by observing EOF or
/// reset on the connected peer, with a bounded poll loop.
async fn wait_for_tcp_peer_close(mut stream: std::net::TcpStream) {
    stream
        .set_nonblocking(true)
        .expect("set_nonblocking failed");
    let mut byte = [0u8; 1];
    for _ in 0..100 {
        match stream.read(&mut byte) {
            Ok(0) => return,
            Ok(n) => panic!("orphaned accept peer unexpectedly read {n} bytes"),
            Err(err)
                if err.kind() == io::ErrorKind::WouldBlock
                    || err.kind() == io::ErrorKind::Interrupted =>
            {
                sleep(Duration::from_millis(5))
                    .await
                    .expect("close wait sleep failed");
            }
            Err(err) if err.kind() == io::ErrorKind::ConnectionReset => return,
            Err(err) => panic!("orphaned accept peer read failed unexpectedly: {err}"),
        }
    }
    panic!("orphaned accept result fd was not closed");
}

#[test]
fn runtime_tcp_try_read_immediate_success() {
    let (mut stream, mut peer) = connected_try_tcp_stream();
    peer.write_all(b"pong").expect("std write failed");

    let (res, buf) = stream.try_read(vec![0u8; 4], 4);
    assert_eq!(res.expect("try_read failed"), 4);
    assert_eq!(&buf[..], b"pong");
}

#[test]
fn runtime_tcp_try_read_would_block() {
    let (mut stream, _peer) = connected_try_tcp_stream();

    let (res, buf) = stream.try_read(vec![0u8; 4], 4);
    let err = res.expect_err("try_read should report WouldBlock");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(
        buf.len(),
        4,
        "buffer ownership and length should be preserved"
    );
}

#[test]
fn runtime_tcp_try_read_partial_success() {
    let (mut stream, mut peer) = connected_try_tcp_stream();
    peer.write_all(b"hi").expect("std write failed");

    let mut recv = IoBuffMut::new(0, 8, 0);
    recv.payload_append(b"HEAD").unwrap();
    let (res, buf) = stream.try_read(recv, 4);
    assert_eq!(res.expect("try_read failed"), 2);
    assert_eq!(buf.payload_bytes(), b"HEADhi");
}

#[test]
fn runtime_tcp_try_read_eof() {
    let (mut stream, peer) = connected_try_tcp_stream();
    peer.shutdown(Shutdown::Write)
        .expect("std shutdown write failed");

    let mut buf = b"HEAD".to_vec();
    buf.reserve(4);
    for _ in 0..100 {
        let (res, returned) = stream.try_read(buf, 4);
        buf = returned;
        match res {
            Ok(0) => {
                assert_eq!(buf, b"HEAD", "EOF must preserve existing contents");
                return;
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => std::thread::yield_now(),
            other => panic!("unexpected try_read EOF result: {other:?}"),
        }
    }

    panic!("try_read did not observe EOF");
}

#[test]
fn runtime_tcp_try_read_rejects_invalid_len() {
    let (mut stream, _peer) = connected_try_tcp_stream();
    let mut recv = IoBuffMut::new(0, 4, 0);
    recv.payload_append(b"ab").unwrap();

    let (res, recv) = stream.try_read(recv, 3);
    let err = res.expect_err("oversize try_read should fail");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(recv.payload_bytes(), b"ab");
}

#[test]
fn runtime_tcp_try_read_append_success_and_partial() {
    let (mut stream, mut peer) = connected_try_tcp_stream();
    peer.write_all(b"body").expect("std write failed");

    let mut recv = IoBuffMut::new(0, 12, 0);
    recv.payload_append(b"HEAD").unwrap();
    let (res, recv) = stream.try_read_append(recv, 4);
    assert_eq!(res.expect("try_read_append failed"), 4);
    assert_eq!(recv.payload_bytes(), b"HEADbody");

    peer.write_all(b"!!").expect("std write failed");
    let (res, recv) = stream.try_read_append(recv, 4);
    assert_eq!(res.expect("partial try_read_append failed"), 2);
    assert_eq!(recv.payload_bytes(), b"HEADbody!!");
}

#[test]
fn runtime_tcp_try_read_append_would_block_and_invalid_len() {
    let (mut stream, _peer) = connected_try_tcp_stream();
    let mut recv = IoBuffMut::new(0, 6, 0);
    recv.payload_append(b"HEAD").unwrap();

    let (res, recv) = stream.try_read_append(recv, 2);
    let err = res.expect_err("try_read_append should report WouldBlock");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(recv.payload_bytes(), b"HEAD");

    let (res, recv) = stream.try_read_append(recv, 3);
    let err = res.expect_err("oversize try_read_append should fail");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(recv.payload_bytes(), b"HEAD");
}

#[test]
fn runtime_tcp_try_read_append_eof_preserves_payload() {
    let (mut stream, peer) = connected_try_tcp_stream();
    peer.shutdown(Shutdown::Write)
        .expect("std shutdown write failed");

    let mut recv = IoBuffMut::new(0, 8, 0);
    recv.payload_append(b"HEAD").unwrap();

    for _ in 0..100 {
        let (res, returned) = stream.try_read_append(recv, 4);
        recv = returned;
        match res {
            Ok(0) => {
                assert_eq!(recv.payload_bytes(), b"HEAD");
                return;
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => std::thread::yield_now(),
            other => panic!("unexpected try_read_append EOF result: {other:?}"),
        }
    }

    panic!("try_read_append did not observe EOF");
}

#[test]
fn runtime_tcp_try_write_immediate_success() {
    let (mut stream, mut peer) = connected_try_tcp_stream();

    let (res, buf) = stream.try_write(b"ping".to_vec());
    assert_eq!(res.expect("try_write failed"), 4);
    assert_eq!(buf, b"ping".to_vec());

    let mut got = [0u8; 4];
    peer.read_exact(&mut got).expect("std read failed");
    assert_eq!(&got, b"ping");
}

#[test]
fn runtime_tcp_try_write_partial_and_would_block() {
    let (mut stream, _peer) = connected_try_tcp_stream();
    stream
        .set_send_buffer_size(4096)
        .expect("set send buffer size failed");

    let (saw_partial, payload) = fill_try_send_buffer(|payload| stream.try_write(payload));
    assert!(
        saw_partial,
        "bounded nonblocking fill should observe at least one partial write"
    );

    let source = TestProjected::new([&b"x"[..]]);
    let (res, source) = stream.try_writev_projected(source);
    let err = res.expect_err("full socket should reject try_writev_projected");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(source.expected(), b"x".to_vec());

    let (res, payload) = stream.try_write(payload);
    let err = res.expect_err("full socket should reject try_write");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(payload.len(), 1024 * 1024);
}

#[test]
fn runtime_tcp_try_writev_projected_immediate_success() {
    let (mut stream, mut peer) = connected_try_tcp_stream();

    let source = TestProjected::new([&b"hello"[..], &b""[..], &b" "[..], &b"world"[..]]);
    let expected = source.expected();
    let (res, source) = stream.try_writev_projected(source);
    assert_eq!(res.expect("try_writev_projected failed"), expected.len());
    assert_eq!(source.expected(), expected);

    let mut got = vec![0u8; expected.len()];
    peer.read_exact(&mut got).expect("std read failed");
    assert_eq!(got, expected);
}

#[test]
fn runtime_tcp_try_writev_projected_large_piece_count_immediate_success() {
    let (mut stream, mut peer) = connected_try_tcp_stream();

    let source = TestProjected::new([&b"x"[..]; 17]);
    let expected = source.expected();
    let (res, source) = stream.try_writev_projected(source);
    assert_eq!(
        res.expect("17-piece try_writev_projected failed"),
        expected.len()
    );
    assert_eq!(source.expected(), expected);

    let mut got = vec![0u8; expected.len()];
    peer.read_exact(&mut got).expect("std read failed");
    assert_eq!(got, expected);
}

#[test]
fn runtime_tcp_try_writev_projected_invalid_projection_returns_source() {
    let (mut stream, mut peer) = connected_try_tcp_stream();

    common::assert_empty_projected_try_cases!(stream);

    let (res, source) = stream.try_writev_projected(TryMismatchedProjected);
    let err = res.expect_err("mismatched projection should fail");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    let _source = source;

    let (res, source) = stream.try_writev_projected(TryCountMismatchedProjected);
    let err = res.expect_err("piece-count mismatch should fail");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    let _source = source;

    let (res, source) = stream.try_writev_projected(TryOversizedProjected);
    let err = res.expect_err("oversized projection should fail");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    let _source = source;

    peer.set_nonblocking(true)
        .expect("peer set_nonblocking failed");
    let mut byte = [0u8; 1];
    let err = peer
        .read(&mut byte)
        .expect_err("rejected projected writes should send no bytes");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
}

#[test]
fn runtime_tcp_async_empty_projected_validation_uses_no_submission_or_retained_scratch() {
    let (mut stream, mut peer) = connected_try_tcp_stream();
    let mut executor = Executor::new().expect("failed to construct runtime executor");
    let returned_stream = Rc::new(Cell::new(None));
    let return_slot = Rc::clone(&returned_stream);

    executor
        .run(async move {
            common::assert_empty_projected_async_cases!(stream, writev_projected);
            common::assert_empty_projected_async_cases!(stream, writev_all_projected);

            return_slot.set(Some(stream));
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.sqe_submits, 0);
        assert_eq!(stats.retained_pooled_allocs, 0);
        assert_eq!(stats.retained_heap_fallbacks, 0);
        assert_eq!(stats.writev_scratch_inline_allocs, 0);
        assert_eq!(stats.writev_scratch_pooled_allocs, 0);
    }

    let stream = returned_stream
        .take()
        .expect("empty projected test did not return the stream");

    peer.set_nonblocking(true)
        .expect("peer set_nonblocking failed");
    let mut byte = [0u8; 1];
    let err = peer
        .read(&mut byte)
        .expect_err("empty projected validation should send no bytes");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    drop(stream);
}

#[test]
fn runtime_tcp_async_projected_shape_mismatches_return_source() {
    let (mut stream, _peer) = connected_try_tcp_stream();

    run_test(async move {
        common::assert_projected_async_mismatches!(stream, writev_projected);
        common::assert_projected_async_mismatches!(stream, writev_all_projected);
    });
}

#[test]
fn runtime_tcp_empty_projected_rejects_outside_run_before_projection() {
    let (mut stream, _peer) = connected_try_tcp_stream();
    let mut cx = Context::from_waker(Waker::noop());

    let mut future = Box::pin(stream.writev_projected(EmptyProjected::valid()));
    match future.as_mut().poll(&mut cx) {
        Poll::Ready((Err(err), source)) => {
            assert_eq!(err.kind(), io::ErrorKind::NotConnected);
            assert_eq!(source.projection_calls(), 0);
        }
        _ => panic!("empty partial projection should reject an inactive poll context"),
    }
    drop(future);

    let mut future = Box::pin(stream.writev_all_projected(EmptyProjected::valid()));
    match future.as_mut().poll(&mut cx) {
        Poll::Ready((Err(err), source)) => {
            assert_eq!(err.kind(), io::ErrorKind::NotConnected);
            assert_eq!(source.projection_calls(), 0);
        }
        _ => panic!("empty all projection should reject an inactive poll context"),
    }
}

#[test]
fn runtime_tcp_try_clone_for_split_duplicates_connected_stream_descriptor() {
    let (mut read_owner, mut peer) = connected_try_tcp_stream();
    let mut write_owner = read_owner
        .try_clone_for_split()
        .expect("try_clone_for_split failed");

    let (res, sent) = write_owner.try_write(b"ping".to_vec());
    assert_eq!(res.expect("split write failed"), 4);
    assert_eq!(sent, b"ping".to_vec());

    let mut got = [0u8; 4];
    peer.read_exact(&mut got).expect("std peer read failed");
    assert_eq!(&got, b"ping");

    drop(write_owner);

    peer.write_all(b"pong").expect("std peer write failed");
    let (res, recv) = read_owner.try_read(vec![0u8; 4], 4);
    assert_eq!(res.expect("read after dropping split owner failed"), 4);
    assert_eq!(&recv[..], b"pong");
}

#[test]
fn runtime_tcp_split_clone_supports_concurrent_async_read_and_write() {
    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = std::thread::spawn(move || {
        let mut stream = std::net::TcpStream::connect(addr).expect("std connect failed");
        stream.write_all(b"pong").expect("std write failed");

        let mut got = [0u8; 4];
        stream.read_exact(&mut got).expect("std read failed");
        assert_eq!(&got, b"ping");
    });

    run_test(async move {
        let (mut read_owner, _addr) = listener.accept().await.expect("accept failed");
        let mut write_owner = read_owner
            .try_clone_for_split()
            .expect("try_clone_for_split failed");

        let writer = Executor::spawn(async move {
            let (res, sent) = write_owner.write_all(b"ping".to_vec()).await;
            assert_eq!(res.expect("split async write failed"), 4);
            sent
        })
        .expect("spawn split writer failed");

        let reader = Executor::spawn(async move {
            let (res, recv) = read_owner.read_exact(vec![0u8; 4], 4).await;
            assert_eq!(res.expect("split async read failed"), 4);
            recv
        })
        .expect("spawn split reader failed");

        assert_eq!(
            writer.await.expect("writer task cancelled"),
            b"ping".to_vec()
        );
        assert_eq!(&reader.await.expect("reader task cancelled")[..], b"pong");
    });

    peer.join().expect("peer panicked");
}

#[test]
fn runtime_tcp_ping_pong() {
    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();
    let peer = spawn_std_tcp_peer(addr, b"ping".to_vec(), b"pong".to_vec());

    run_test(async move {
        let (mut stream, _addr) = listener.accept().await.expect("accept failed");

        let (res, _buf) = stream.write(b"ping".to_vec()).await;
        assert_eq!(res.expect("write failed"), 4);

        let recv = vec![0u8; 4];
        let (res, buf) = stream.read(recv, 4).await;
        assert_eq!(res.expect("read failed"), 4);
        assert_eq!(&buf[..4], b"pong");
    });

    peer.join().expect("peer panicked");
}

/// Cancelling accept after a client connected can still leave a completed
/// kernel accept result; the reactor must close that orphaned fd and keep the
/// listener reusable.
#[test]
fn runtime_tcp_cancelled_accept_closes_orphan_fd_and_reaccepts() {
    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    run_test(async move {
        let mut accept = Box::pin(listener.accept());
        std::future::poll_fn(|cx| match Future::poll(accept.as_mut(), cx) {
            Poll::Pending => Poll::Ready(()),
            Poll::Ready(_) => panic!("accept completed before test client connected"),
        })
        .await;

        let orphan_client = std::net::TcpStream::connect(addr).expect("orphan client connect");
        drop(accept);
        wait_for_tcp_peer_close(orphan_client).await;

        let client = std::net::TcpStream::connect(addr).expect("second client connect");
        let client_addr = client.local_addr().expect("client local_addr failed");
        let (_stream, remote_addr) = listener.accept().await.expect("second accept failed");
        assert_eq!(remote_addr, client_addr);
    });
}

/// Teardown probe: a completed accept CQE held in AcceptSlot must close the
/// orphaned accepted fd when dropped without being polled.
#[test]
fn tcp_accept_slot_drop_cached_state_closes_completed_fd() {
    test_accept_slot_drop_cached_state_closes_completed_fd().unwrap();
}

#[test]
fn tcp_accept_forgotten_future_reports_busy_slot_would_block() {
    let mut listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128)
        .expect("listener bind failed");
    let first_accept = listener.accept();
    std::mem::forget(first_accept);

    run_test(async move {
        let err = match listener.accept().await {
            Ok(_) => panic!("forgotten accept future should keep listener slot busy"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    });
}

#[test]
fn tcp_accept_busy_error_outside_run_prefers_context_error() {
    let mut listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128)
        .expect("listener bind failed");
    let first_accept = listener.accept();
    std::mem::forget(first_accept);

    let mut second_accept = listener.accept();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(
        Pin::new(&mut second_accept).poll(&mut cx),
        Poll::Ready(Err(err)) if err.kind() == io::ErrorKind::NotConnected
    ));
}

#[test]
fn runtime_tcp_connect_ping_pong() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");

    let peer = std::thread::spawn(move || {
        use std::io::{Read, Write};

        let (mut stream, _) = listener.accept().expect("std accept failed");
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).expect("std read failed");
        assert_eq!(&buf, b"ping");
        stream.write_all(b"pong").expect("std write failed");
    });

    let mut connector = TcpConnector::new();

    executor
        .run(async move {
            let mut stream = connector
                .connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");

            let (res, _buf) = stream.write(b"ping".to_vec()).await;
            assert_eq!(res.expect("write failed"), 4);

            let recv = vec![0u8; 4];
            let (res, buf) = stream.read(recv, 4).await;
            assert_eq!(res.expect("read failed"), 4);
            assert_eq!(&buf[..4], b"pong");
        })
        .expect("executor run failed");

    peer.join().expect("peer panicked");
}

#[test]
fn runtime_tcp_write_all_read_exact() {
    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();
    let peer = spawn_std_tcp_peer(addr, b"ping".to_vec(), b"pong".to_vec());

    run_test(async move {
        let (mut stream, _addr) = listener.accept().await.expect("accept failed");

        let send = b"ping".to_vec();
        let (res, _buf) = stream.write_all(send).await;
        assert_eq!(res.expect("write_all failed"), 4);

        let recv = vec![0u8; 4];
        let (res, buf) = stream.read_exact(recv, 4).await;
        assert_eq!(res.expect("read_exact failed"), 4);
        assert_eq!(&buf[..], b"pong");
    });

    peer.join().expect("peer panicked");
}

#[test]
fn runtime_tcp_write_all_read_exact_large_payload() {
    let msg_size = 256 * 1024; // 256KB
    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();
    let peer = spawn_std_tcp_peer(addr, vec![0xABu8; msg_size], vec![0xABu8; msg_size]);

    run_test(async move {
        let (mut stream, _addr) = listener.accept().await.expect("accept failed");

        let send = vec![0xABu8; msg_size];
        let (res, _buf) = stream.write_all(send).await;
        assert_eq!(res.expect("write_all failed"), msg_size);

        let recv = vec![0u8; msg_size];
        let (res, buf) = stream.read_exact(recv, msg_size).await;
        assert_eq!(res.expect("read_exact failed"), msg_size);
        assert!(buf.iter().all(|&b| b == 0xAB), "data mismatch on read");
    });

    peer.join().expect("peer panicked");
}

#[test]
fn runtime_tcp_read_exact_eof() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = std::thread::spawn(move || {
        use std::io::Write;

        let mut stream = std::net::TcpStream::connect(addr).expect("std connect failed");
        stream.write_all(b"hi").expect("std write failed");
        drop(stream); // close before sending 4 bytes
    });

    executor
        .run(async move {
            let (mut stream, _addr) = listener.accept().await.expect("accept failed");

            let mut recv = IoBuffMut::new(0, 8, 0);
            recv.payload_append(b"HEAD").unwrap();
            let (res, buf) = stream.read_exact(recv, 4).await;
            let err = res.expect_err("should fail with UnexpectedEof");
            assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
            assert_eq!(buf.payload_bytes(), b"HEADhi");
        })
        .expect("executor run failed");

    peer.join().expect("peer panicked");
}

/// TcpStream::connect() convenience creates a connection without a TcpConnector.
#[test]
fn runtime_tcp_stream_connect() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");

    let peer = std::thread::spawn(move || {
        use std::io::{Read, Write};
        let (mut stream, _) = listener.accept().expect("std accept failed");
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).expect("std read failed");
        assert_eq!(&buf, b"ping");
        stream.write_all(b"pong").expect("std write failed");
    });

    executor
        .run(async move {
            let mut stream = TcpStream::connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");

            let send = b"ping".to_vec();
            let (res, _buf) = stream.write_all(send).await;
            assert_eq!(res.expect("write_all failed"), 4);

            let recv = vec![0u8; 4];
            let (res, buf) = stream.read_exact(recv, 4).await;
            assert_eq!(res.expect("read_exact failed"), 4);
            assert_eq!(&buf[..], b"pong");
        })
        .expect("executor run failed");

    peer.join().expect("peer panicked");
}

#[test]
fn runtime_tcp_stream_connect_timeout_success() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");

    let peer = std::thread::spawn(move || {
        let (_stream, _) = listener.accept().expect("std accept failed");
    });

    executor
        .run(async move {
            let stream = TcpStream::connect_timeout(addr, Duration::from_secs(1))
                .expect("connect_timeout init failed")
                .await
                .expect("connect_timeout failed");
            assert_eq!(stream.peer_addr().expect("peer_addr failed"), addr);
        })
        .expect("executor run failed");

    peer.join().expect("peer panicked");
}

#[test]
fn runtime_tcp_connector_connect_timeout_success() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");

    let peer = std::thread::spawn(move || {
        let (_stream, _) = listener.accept().expect("std accept failed");
    });

    let mut connector = TcpConnector::new();
    executor
        .run(async move {
            let stream = connector
                .connect_timeout(addr, Duration::from_secs(1))
                .expect("connect_timeout init failed")
                .await
                .expect("connect_timeout failed");
            assert_eq!(stream.peer_addr().expect("peer_addr failed"), addr);
        })
        .expect("executor run failed");

    peer.join().expect("peer panicked");
}

#[test]
fn runtime_tcp_stream_connect_timeout_propagates_connect_error() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");
    drop(listener);

    executor
        .run(async move {
            let result = TcpStream::connect_timeout(addr, Duration::from_secs(1))
                .expect("connect_timeout init failed")
                .await;
            let err = match result {
                Ok(_) => panic!("connect_timeout should propagate connect failure"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), std::io::ErrorKind::ConnectionRefused);
        })
        .expect("executor run failed");
}

#[test]
fn runtime_tcp_connector_connect_timeout_propagates_connect_error() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");
    drop(listener);

    let mut connector = TcpConnector::new();
    executor
        .run(async move {
            let result = connector
                .connect_timeout(addr, Duration::from_secs(1))
                .expect("connect_timeout init failed")
                .await;
            let err = match result {
                Ok(_) => panic!("connect_timeout should propagate connect failure"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), std::io::ErrorKind::ConnectionRefused);
        })
        .expect("executor run failed");
}

#[test]
fn runtime_tcp_connect_timeout_preserves_timer_runtime_error() {
    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            test_hooks::fail_next_timer_alloc();
            let result = TcpStream::connect_timeout(addr, Duration::from_secs(1))
                .expect("connect_timeout init failed")
                .await;
            let err = match result {
                Ok(_) => panic!("timer allocation failure should abort connect_timeout"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), io::ErrorKind::OutOfMemory);
        })
        .expect("executor run failed");
}

/// TcpStream address queries and socket options.
#[test]
fn runtime_tcp_socket_options() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = std::thread::spawn(move || {
        let mut stream = std::net::TcpStream::connect(addr).expect("std connect failed");
        let mut buf = [0u8; 1];
        use std::io::Read;
        let _ = stream.read(&mut buf);
    });

    executor
        .run(async move {
            let (stream, peer_addr) = listener.accept().await.expect("accept failed");

            let local = stream.local_addr().expect("local_addr failed");
            assert_eq!(local, addr);
            let remote = stream.peer_addr().expect("peer_addr failed");
            assert_eq!(remote, peer_addr);

            stream.set_nodelay(true).expect("set_nodelay failed");
            assert!(stream.nodelay().expect("nodelay failed"));
            stream.set_nodelay(false).expect("set_nodelay false failed");
            assert!(!stream.nodelay().expect("nodelay false failed"));

            stream.set_keepalive(true).expect("set_keepalive failed");

            stream
                .set_send_buffer_size(65536)
                .expect("set_send_buffer_size failed");
            assert!(stream.send_buffer_size().expect("send_buffer_size failed") > 0);

            stream
                .set_recv_buffer_size(65536)
                .expect("set_recv_buffer_size failed");
            assert!(stream.recv_buffer_size().expect("recv_buffer_size failed") > 0);

            stream
                .shutdown(std::net::Shutdown::Write)
                .expect("shutdown failed");
        })
        .expect("executor run failed");

    peer.join().expect("peer panicked");
}

/// TcpListener::bind_reuse_port sets SO_REUSEPORT.
#[test]
fn runtime_tcp_listener_reuse_port() {
    let listener = TcpListener::bind_reuse_port(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128)
        .expect("bind_reuse_port failed");

    let addr = listener.local_addr();
    assert_ne!(addr.port(), 0);
}

/// `TcpConnector::default()` produces a working connector identical to `TcpConnector::new()`.
#[test]
fn runtime_tcp_connector_default_trait() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("std bind failed");
    let addr = listener.local_addr().expect("local_addr failed");

    let peer = std::thread::spawn(move || {
        let (_stream, _) = listener.accept().expect("std accept failed");
    });

    let mut connector = TcpConnector::default();
    executor
        .run(async move {
            let stream = connector
                .connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");
            assert_eq!(stream.peer_addr().expect("peer_addr failed"), addr);
        })
        .expect("executor run failed");

    peer.join().expect("peer panicked");
}

// ============================================================================
// IoBuffMut / IoBuff transport integration tests
// ============================================================================

/// Ping-pong using IoBuffMut for receive and IoBuff (frozen) for send.
#[test]
fn runtime_tcp_ping_pong_iobuff() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = std::thread::spawn(move || {
        use std::io::{Read, Write};

        let mut stream = std::net::TcpStream::connect(addr).expect("std connect failed");
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).expect("std read failed");
        assert_eq!(&buf, b"ping");
        stream.write_all(b"pong").expect("std write failed");
    });

    executor
        .run(async move {
            let (mut stream, _addr) = listener.accept().await.expect("accept failed");

            let mut send_buf = IoBuffMut::new(0, 4, 0);
            send_buf.payload_append(b"ping").unwrap();
            let (res, _buf) = stream.write(send_buf).await;
            assert_eq!(res.expect("write failed"), 4);

            let mut recv_buf = IoBuffMut::new(0, 8, 0);
            recv_buf.payload_append(b"HEAD").unwrap();
            let (res, buf) = stream.read(recv_buf, 4).await;
            assert_eq!(res.expect("read failed"), 4);
            assert_eq!(buf.payload_bytes(), b"HEADpong");
        })
        .expect("executor run failed");

    peer.join().expect("peer panicked");
}

/// write_all with frozen IoBuff, read_exact with IoBuffMut.
#[test]
fn runtime_tcp_write_all_read_exact_iobuff() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = std::thread::spawn(move || {
        use std::io::{Read, Write};

        let mut stream = std::net::TcpStream::connect(addr).expect("std connect failed");
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).expect("std read failed");
        assert_eq!(&buf, b"ping");
        stream.write_all(b"pong").expect("std write failed");
    });

    executor
        .run(async move {
            let (mut stream, _addr) = listener.accept().await.expect("accept failed");

            let mut send_buf = IoBuffMut::new(0, 4, 0);
            send_buf.payload_append(b"ping").unwrap();
            let frozen = send_buf.freeze();
            let (res, _buf) = stream.write_all(frozen).await;
            assert_eq!(res.expect("write_all failed"), 4);

            let mut recv_buf = IoBuffMut::new(0, 8, 0);
            recv_buf.payload_append(b"HEAD").unwrap();
            let (res, buf) = stream.read_exact(recv_buf, 4).await;
            assert_eq!(res.expect("read_exact failed"), 4);
            assert_eq!(buf.payload_bytes(), b"HEADpong");
        })
        .expect("executor run failed");

    peer.join().expect("peer panicked");
}

/// Staged IoBuffMut append reads preserve previously-read payload bytes.
#[test]
fn runtime_tcp_read_exact_append_iobuff_staged() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = std::thread::spawn(move || {
        use std::io::Write;

        let mut stream = std::net::TcpStream::connect(addr).expect("std connect failed");
        stream.write_all(b"HEADbody").expect("std write failed");
    });

    executor
        .run(async move {
            let (mut stream, _addr) = listener.accept().await.expect("accept failed");

            let recv_buf = IoBuffMut::new(0, 8, 0);
            let (res, buf) = stream.read_exact_append(recv_buf, 4).await;
            assert_eq!(res.expect("header append read failed"), 4);
            assert_eq!(buf.payload_bytes(), b"HEAD");

            let (res, buf) = stream.read_exact_append(buf, 4).await;
            assert_eq!(res.expect("body append read failed"), 4);
            assert_eq!(buf.payload_len(), 8);
            assert_eq!(buf.payload_bytes(), b"HEADbody");
        })
        .expect("executor run failed");

    peer.join().expect("peer panicked");
}

/// IoBuffMut with headroom — prepend a protocol header before sending.
#[test]
fn runtime_tcp_iobuff_headroom() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = std::thread::spawn(move || {
        use std::io::Read;

        let mut stream = std::net::TcpStream::connect(addr).expect("std connect failed");
        let mut buf = [0u8; 9];
        stream.read_exact(&mut buf).expect("std read failed");
        assert_eq!(&buf, b"HDR:world");
    });

    executor
        .run(async move {
            let (mut stream, _addr) = listener.accept().await.expect("accept failed");

            let mut buf = IoBuffMut::new(4, 16, 0);
            buf.payload_append(b"world").unwrap();
            buf.headroom_prepend(b"HDR:").unwrap();
            assert_eq!(buf.bytes(), b"HDR:world");

            let (res, _buf) = stream.write_all(buf).await;
            assert_eq!(res.expect("write_all failed"), 9);
        })
        .expect("executor run failed");

    peer.join().expect("peer panicked");
}

/// Pool-allocated buffers through TCP transport.
#[test]
fn runtime_tcp_pool_buffers() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = std::thread::spawn(move || {
        use std::io::{Read, Write};

        let mut stream = std::net::TcpStream::connect(addr).expect("std connect failed");
        let mut buf = [0u8; 5];
        stream.read_exact(&mut buf).expect("std read failed");
        assert_eq!(&buf, b"hello");
        stream.write_all(b"world").expect("std write failed");
    });

    executor
        .run(async move {
            let (mut stream, _addr) = listener.accept().await.expect("accept failed");

            let mut pool = IoBuffPool::new(IoBuffPoolConfig {
                headroom: 0,
                payload: 64,
                tailroom: 0,
                objs_per_slab: 16,
            })
            .expect("pool config invalid");
            pool.init();

            let mut send_buf = pool.alloc().expect("pool alloc failed");
            send_buf.payload_append(b"hello").unwrap();
            let (res, _buf) = stream.write_all(send_buf).await;
            assert_eq!(res.expect("write_all failed"), 5);

            let recv_buf = pool.alloc().expect("pool alloc failed");
            let (res, buf) = stream.read_exact(recv_buf, 5).await;
            assert_eq!(res.expect("read_exact failed"), 5);
            assert_eq!(buf.payload_bytes(), b"world");
        })
        .expect("executor run failed");

    peer.join().expect("peer panicked");
}

/// Large payload with IoBuffMut — forces partial kernel transfers.
#[test]
fn runtime_tcp_write_all_read_exact_large_iobuff() {
    let msg_size = 256 * 1024;
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = std::thread::spawn(move || {
        use std::io::{Read, Write};

        let mut stream = std::net::TcpStream::connect(addr).expect("std connect failed");
        let mut buf = vec![0u8; msg_size];
        stream.read_exact(&mut buf).expect("std read failed");
        assert!(buf.iter().all(|&b| b == 0xAB), "data mismatch");
        stream.write_all(&buf).expect("std write failed");
    });

    executor
        .run(async move {
            let (mut stream, _addr) = listener.accept().await.expect("accept failed");

            let mut send_buf = IoBuffMut::new(0, msg_size, 0);
            send_buf.payload_append(&vec![0xABu8; msg_size]).unwrap();
            let (res, _buf) = stream.write_all(send_buf).await;
            assert_eq!(res.expect("write_all failed"), msg_size);

            let recv_buf = IoBuffMut::new(0, msg_size, 0);
            let (res, buf) = stream.read_exact(recv_buf, msg_size).await;
            assert_eq!(res.expect("read_exact failed"), msg_size);
            assert!(
                buf.payload_bytes().iter().all(|&b| b == 0xAB),
                "data mismatch on read"
            );
        })
        .expect("executor run failed");

    peer.join().expect("peer panicked");
}

// ============================================================================
// Vectored I/O (readv / writev) tests
// ============================================================================

/// writev 3 segments to a std peer, readv the echo back.
#[test]
fn runtime_tcp_writev_readv() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();

    let peer = std::thread::spawn(move || {
        use std::io::{Read, Write};

        let mut stream = std::net::TcpStream::connect(addr).expect("std connect failed");
        let mut buf = [0u8; 11];
        stream.read_exact(&mut buf).expect("std read failed");
        assert_eq!(&buf, b"hello world");
        stream.write_all(&buf).expect("std write failed");
    });

    executor
        .run(async move {
            let (mut stream, _addr) = listener.accept().await.expect("accept failed");

            let frozen = make_payload_chain([&b"hello"[..], &b" "[..], &b"world"[..]]);

            let (res, _chain) = stream.writev(frozen).await;
            assert_eq!(res.expect("writev failed"), 11);

            let read_chain = make_read_chain([6, 5]);

            let (res, chain) = stream.readv(read_chain).await;
            assert_eq!(res.expect("readv failed"), 11);
            assert_eq!(chain.get(0).expect("seg0").payload_bytes(), b"hello ");
            assert_eq!(chain.get(1).expect("seg1").payload_bytes(), b"world");
        })
        .expect("executor run failed");

    peer.join().expect("peer panicked");
}

#[test]
fn runtime_tcp_vectored_empty_chain_semantics() {
    let (mut stream, _peer) = connected_try_tcp_stream();

    run_test(async move {
        let (res, chain) = stream.writev(make_payload_chain::<0>([])).await;
        assert_eq!(res.expect("writev empty failed"), 0);
        assert!(chain.is_empty());

        let (res, chain) = stream.writev_all(make_payload_chain::<0>([])).await;
        assert_eq!(res.expect("writev_all empty failed"), 0);
        assert!(chain.is_empty());

        let (res, chain) = stream.writev(make_read_only_chain::<0>([])).await;
        assert_eq!(res.expect("empty read-only chain writev failed"), 0);
        assert!(chain.is_empty());

        let (res, source) = stream.writev_projected(TestProjected::<0>::new([])).await;
        assert_eq!(res.expect("writev_projected empty failed"), 0);
        assert!(source.expected().is_empty());

        let (res, source) = stream
            .writev_all_projected(TestProjected::<0>::new([]))
            .await;
        assert_eq!(res.expect("writev_all_projected empty failed"), 0);
        assert!(source.expected().is_empty());

        let (res, chain) = stream.readv(make_read_chain::<0>([])).await;
        let err = res.expect_err("readv empty should reject ambiguous EOF result");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(chain.is_empty());

        let (res, chain) = stream.readv_exact(make_read_chain::<0>([]), 0).await;
        assert_eq!(res.expect("readv_exact zero failed"), 0);
        assert!(chain.is_empty());
    });
}

#[test]
fn runtime_tcp_writev_all_readonly_chain_to_std_peer() {
    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();
    let peer = spawn_std_tcp_peer(addr, b"hello world".to_vec(), b"ack".to_vec());

    run_test(async move {
        let (mut stream, _addr) = listener.accept().await.expect("accept failed");

        let chain = make_read_only_chain([&b"hello"[..], &b""[..], &b" "[..], &b"world"[..]]);
        let (res, chain) = stream.writev_all(chain).await;
        assert_eq!(res.expect("read-only chain writev_all failed"), 11);
        assert_eq!(chain.segments(), 4);

        let (res, buf) = stream.read_exact(vec![0u8; 3], 3).await;
        assert_eq!(res.expect("read_exact failed"), 3);
        assert_eq!(&buf[..], b"ack");
    });

    peer.join().expect("peer panicked");
}

#[test]
fn runtime_tcp_writev_all_projected_to_std_peer() {
    let mut listener =
        TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128).expect("bind failed");
    let addr = listener.local_addr();
    let peer = spawn_std_tcp_peer(addr, b"hello world".to_vec(), b"ack".to_vec());

    run_test(async move {
        let (mut stream, _addr) = listener.accept().await.expect("accept failed");

        let source = TestProjected::new([&b"hello"[..], &b""[..], &b" "[..], &b"world"[..]]);
        let expected = source.expected();
        let (res, source) = stream.writev_all_projected(source).await;
        assert_eq!(res.expect("writev_all_projected failed"), expected.len());
        assert_eq!(source.expected(), expected);

        let (res, buf) = stream.read_exact(vec![0u8; 3], 3).await;
        assert_eq!(res.expect("read_exact failed"), 3);
        assert_eq!(&buf[..], b"ack");
    });

    peer.join().expect("peer panicked");
}
