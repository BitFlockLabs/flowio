mod common;

use common::{
    InitializationTrackedReadWrite, TestIoBuffMut as IoBuffMut, TestProjected,
    TryCountMismatchedProjected, TryMismatchedProjected, TryOversizedProjected,
    assert_poll_after_ready_parks, fill_try_send_buffer, make_payload_chain, make_read_chain,
    make_read_only_chain, poll_once_pending, run_test, set_positive_linger,
};
#[cfg(all(target_os = "linux", target_pointer_width = "64", not(miri)))]
use common::{SparseOversizedReadOnly, assert_oversized_send_rejected, run_test_output};
use flowio::net::unix::UnixStream;
use flowio::runtime::buffer::iobuffvec::IoBuffVecMut;
use flowio::runtime::buffer::pool::{IoBuffPool, IoBuffPoolConfig};
use flowio::runtime::executor::Executor;
use std::cell::Cell;
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, OwnedFd};
use std::rc::Rc;
use std::task::Poll;

/// Returns a FlowIO UnixStream wrapping one nonblocking socketpair endpoint
/// plus its connected std peer for one-shot tests that do not need a reactor.
fn connected_try_unix_stream() -> (UnixStream, std::os::unix::net::UnixStream) {
    let (stream, peer) = std::os::unix::net::UnixStream::pair().expect("socketpair failed");
    stream
        .set_nonblocking(true)
        .expect("set_nonblocking failed");
    (UnixStream::from_owned_fd(stream.into()), peer)
}

#[test]
fn runtime_retry_initial_submissions_extract_poll_context_once() {
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            {
                let (mut stream, _peer) = UnixStream::pair().expect("socketpair failed");
                poll_once_pending(stream.write_all(b"w".to_vec())).await;
            }
            {
                let (_peer, mut stream) = UnixStream::pair().expect("socketpair failed");
                poll_once_pending(stream.read_exact(vec![0u8; 1], 1)).await;
            }
            {
                let (_peer, mut stream) = UnixStream::pair().expect("socketpair failed");
                poll_once_pending(stream.read_exact_append(IoBuffMut::new(0, 1, 0), 1)).await;
            }
            {
                let (mut stream, _peer) = UnixStream::pair().expect("socketpair failed");
                poll_once_pending(stream.writev_all(make_payload_chain([&b"v"[..]]))).await;
            }
            {
                let (mut stream, _peer) = UnixStream::pair().expect("socketpair failed");
                poll_once_pending(stream.writev_all_projected(TestProjected::new([&b"p"[..]])))
                    .await;
            }
            {
                let (_peer, mut stream) = UnixStream::pair().expect("socketpair failed");
                poll_once_pending(stream.readv_exact(make_read_chain([1]), 1)).await;
            }
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    assert_eq!(
        executor.last_stats().poll_context_extractions,
        6,
        "each initial retry submission should derive the validated context once"
    );
}

#[test]
fn runtime_unix_one_shot_initial_submissions_extract_poll_context_once() {
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            {
                let (_peer, mut stream) = UnixStream::pair().expect("socketpair failed");
                poll_once_pending(stream.read(vec![0u8; 1], 1)).await;
            }
            {
                let (mut stream, _peer) = UnixStream::pair().expect("socketpair failed");
                poll_once_pending(stream.write(b"w".to_vec())).await;
            }
            {
                let (_peer, mut stream) = UnixStream::pair().expect("socketpair failed");
                poll_once_pending(stream.readv(make_read_chain([1]))).await;
            }
            {
                let (mut stream, _peer) = UnixStream::pair().expect("socketpair failed");
                poll_once_pending(stream.writev(make_payload_chain([&b"v"[..]]))).await;
            }
            {
                let (mut stream, _peer) = UnixStream::pair().expect("socketpair failed");
                poll_once_pending(stream.writev_projected(TestProjected::new([&b"p"[..]]))).await;
            }
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    assert_eq!(
        executor.last_stats().poll_context_extractions,
        5,
        "each one-shot stream submission should derive the validated context once"
    );
}

#[test]
fn runtime_retry_partial_resubmit_refreshes_waiter_without_reextracting_context() {
    let (mut stream, mut peer) = connected_try_unix_stream();
    peer.write_all(b"a").expect("initial peer write failed");
    peer.set_nonblocking(true)
        .expect("set peer nonblocking failed");
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            let mut future = std::pin::pin!(stream.read_exact(vec![0u8; 2], 2));
            let mut outer_polls = 0;
            let (result, buffer) = std::future::poll_fn(|cx| {
                outer_polls += 1;
                match outer_polls {
                    1 => {
                        assert!(future.as_mut().poll(cx).is_pending());
                        assert!(
                            future.as_mut().poll(cx).is_pending(),
                            "in-flight repoll should remain pending"
                        );
                        Poll::Pending
                    }
                    2 => {
                        assert!(
                            future.as_mut().poll(cx).is_pending(),
                            "partial completion should resubmit the remaining read"
                        );
                        assert_eq!(peer.write(b"b").expect("second peer write failed"), 1);
                        Poll::Pending
                    }
                    _ => future.as_mut().poll(cx),
                }
            })
            .await;

            assert_eq!(result.expect("read_exact failed"), 2);
            assert_eq!(&buffer[..], b"ab");
            assert_eq!(outer_polls, 3);
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    assert_eq!(
        executor.last_stats().poll_context_extractions,
        4,
        "initial submit, pending repoll, partial retry, and final completion should each derive once"
    );
}

#[test]
fn runtime_unix_fresh_pair_drop_skips_linger_query() {
    let (left, right) = UnixStream::pair().expect("runtime socketpair failed");
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            drop(left);
            drop(right);
        })
        .expect("fresh Unix pair close failed");
    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.close_linger_queries, 0);
        assert_eq!(stats.close_ring_submissions, 2);
        assert_eq!(stats.close_direct_closes, 0);
        assert_eq!(stats.close_worker_admissions, 0);
    }
}

#[test]
fn runtime_unix_saved_public_fd_positive_linger_routes_to_worker() {
    let (left, right) = UnixStream::pair().expect("runtime socketpair failed");
    let saved_raw = left.as_raw_fd();
    set_positive_linger(saved_raw);
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            drop(left);
        })
        .expect("positive-linger Unix close failed");
    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.close_linger_queries, 1);
        assert_eq!(stats.close_worker_admissions, 1);
        assert_eq!(stats.close_ring_submissions, 0);
        assert_eq!(stats.close_direct_closes, 0);
    }
    drop(executor);
    drop(right);
}

#[test]
fn runtime_unix_owned_fd_adoption_preserves_nonblocking_and_close_ownership() {
    let (standard, mut peer) = std::os::unix::net::UnixStream::pair().expect("socketpair failed");
    standard
        .set_nonblocking(true)
        .expect("set_nonblocking failed");

    let raw = standard.as_raw_fd();
    let owned: OwnedFd = standard.into();
    let stream = UnixStream::from_owned_fd(owned);
    let status = unsafe { libc::fcntl(raw, libc::F_GETFL) };
    assert!(status >= 0, "F_GETFL failed for adopted Unix fd");
    assert_ne!(
        status & libc::O_NONBLOCK,
        0,
        "adopted Unix fd became blocking"
    );

    let mut executor = Executor::new().expect("failed to construct executor");
    executor
        .run(async move {
            drop(stream);
        })
        .expect("runtime-owned Unix close run failed");
    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.close_linger_queries, 1);
        assert_eq!(stats.close_ring_submissions, 1);
        assert_eq!(stats.close_direct_closes, 0);
        assert_eq!(stats.close_worker_admissions, 0);
    }
    drop(executor);

    peer.set_read_timeout(Some(std::time::Duration::from_secs(1)))
        .expect("set_read_timeout failed");
    let mut byte = [0u8; 1];
    assert_eq!(
        peer.read(&mut byte)
            .expect("peer read after owner drop failed"),
        0,
        "adopted Unix descriptor stayed open after owner drop"
    );
}

#[test]
fn runtime_unix_try_read_immediate_partial_and_would_block() {
    let (mut stream, mut peer) = connected_try_unix_stream();
    peer.write_all(b"hi").expect("std write failed");

    let (res, buf) = stream.try_read(vec![0u8; 5], 5);
    assert_eq!(res.expect("try_read failed"), 2);
    assert_eq!(&buf[..], b"hi");

    let (res, buf) = stream.try_read(vec![0u8; 4], 4);
    let err = res.expect_err("try_read should report WouldBlock");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(
        buf.len(),
        4,
        "buffer ownership and length should be preserved"
    );

    peer.write_all(b"pong").expect("std write failed");
    let (res, buf) = stream.try_read(buf, 4);
    assert_eq!(res.expect("second try_read failed"), 4);
    assert_eq!(&buf[..], b"pong");
}

#[test]
fn runtime_unix_kernel_read_skips_userspace_initialization() {
    run_test(async move {
        let (mut stream, mut peer) = connected_try_unix_stream();
        peer.write_all(b"ping").expect("std write failed");

        let initialization_calls = Rc::new(Cell::new(0));
        let initialized_bytes = Rc::new(Cell::new(0));
        let buffer =
            InitializationTrackedReadWrite::new(8, &initialization_calls, &initialized_bytes);
        let (result, buffer) = stream.read(buffer, 8).await;

        assert_eq!(result.expect("kernel read failed"), 4);
        assert_eq!(buffer.bytes(), b"ping");
        assert_eq!(initialization_calls.get(), 0);
        assert_eq!(initialized_bytes.get(), 0);
    });
}

#[test]
fn runtime_unix_try_read_rejects_invalid_len() {
    let (mut stream, _peer) = connected_try_unix_stream();
    let mut recv = IoBuffMut::new(0, 4, 0);
    recv.payload_append(b"ab").unwrap();

    let (res, recv) = stream.try_read(recv, 3);
    let err = res.expect_err("oversize try_read should fail");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(recv.payload_bytes(), b"ab");
}

#[test]
fn runtime_unix_try_read_prefilled_and_sealed_zero_preserve_payload() {
    let (mut stream, mut peer) = connected_try_unix_stream();
    peer.write_all(b"ok").expect("std write failed");

    let mut recv = IoBuffMut::new(0, 8, 0);
    recv.payload_append(b"HEAD").unwrap();
    let (res, recv) = stream.try_read(recv, 2);
    assert_eq!(res.expect("prefilled try_read failed"), 2);
    assert_eq!(recv.payload_bytes(), b"HEADok");

    let mut sealed = IoBuffMut::new(0, 8, 2);
    sealed.payload_append(b"HEAD").unwrap();
    sealed.tailroom_append(b":T").unwrap();
    let (res, sealed) = stream.try_read(sealed, 0);
    assert_eq!(res.expect("sealed zero read failed"), 0);
    assert_eq!(sealed.bytes(), b"HEAD:T");

    let (res, sealed) = stream.try_read(sealed, 1);
    let err = res.expect_err("sealed positive read should fail");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(sealed.bytes(), b"HEAD:T");
}

#[test]
fn runtime_unix_try_read_append_success_partial_and_would_block() {
    let (mut stream, mut peer) = connected_try_unix_stream();
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

    let (res, recv) = stream.try_read_append(recv, 2);
    let err = res.expect_err("try_read_append should report WouldBlock");
    assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(recv.payload_bytes(), b"HEADbody!!");

    let (res, recv) = stream.try_read_append(recv, 3);
    let err = res.expect_err("oversize try_read_append should fail");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(recv.payload_bytes(), b"HEADbody!!");
}

#[test]
fn runtime_unix_try_write_immediate_success() {
    let (mut stream, mut peer) = connected_try_unix_stream();

    let (res, buf) = stream.try_write(b"ping".to_vec());
    assert_eq!(res.expect("try_write failed"), 4);
    assert_eq!(buf, b"ping".to_vec());

    let mut got = [0u8; 4];
    peer.read_exact(&mut got).expect("std read failed");
    assert_eq!(&got, b"ping");
}

#[test]
fn runtime_unix_try_write_partial_and_would_block() {
    let (mut stream, _peer) = connected_try_unix_stream();
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
fn runtime_unix_try_writev_projected_immediate_success() {
    let (mut stream, mut peer) = connected_try_unix_stream();

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
fn runtime_unix_try_writev_projected_large_piece_count_immediate_success() {
    let (mut stream, mut peer) = connected_try_unix_stream();

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
fn runtime_unix_try_writev_projected_invalid_projection_returns_source() {
    let (mut stream, mut peer) = connected_try_unix_stream();

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
fn runtime_unix_async_empty_projected_validation_uses_no_submission_or_retained_scratch() {
    let (mut stream, mut peer) = connected_try_unix_stream();
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
fn runtime_unix_async_projected_shape_mismatches_return_source() {
    let (mut stream, _peer) = connected_try_unix_stream();

    run_test(async move {
        common::assert_projected_async_mismatches!(stream, writev_projected);
        common::assert_projected_async_mismatches!(stream, writev_all_projected);
    });
}

/// Basic ping-pong with Vec<u8> buffers and a spawned async peer.
#[test]
fn runtime_unix_stream_ping_pong() {
    run_test(async move {
        let (mut client, mut server) = UnixStream::pair().expect("socketpair failed");

        Executor::spawn(async move {
            let buf = vec![0u8; 4];
            let (res, buf) = server.read(buf, 4).await;
            assert_eq!(res.expect("server read failed"), 4);
            assert_eq!(&buf[..4], b"ping");

            let (res, _buf) = server.write(b"pong".to_vec()).await;
            assert_eq!(res.expect("server write failed"), 4);
        })
        .expect("spawn server failed");

        let (res, _buf) = client.write(b"ping".to_vec()).await;
        assert_eq!(res.expect("write failed"), 4);

        let mut recv = IoBuffMut::new(0, 8, 0);
        recv.payload_append(b"HEAD").unwrap();
        let (res, buf) = client.read(recv, 4).await;
        assert_eq!(res.expect("read failed"), 4);
        assert_eq!(buf.payload_bytes(), b"HEADpong");
    });
}

/// write_all / read_exact with Vec<u8> buffers and a spawned async peer.
#[test]
fn runtime_unix_write_all_read_exact() {
    run_test(async move {
        let (mut client, mut server) = UnixStream::pair().expect("socketpair failed");

        Executor::spawn(async move {
            let buf = vec![0u8; 4];
            let (res, buf) = server.read_exact(buf, 4).await;
            res.expect("server read_exact failed");
            assert_eq!(&buf[..], b"ping");

            let (res, _buf) = server.write_all(b"pong".to_vec()).await;
            res.expect("server write_all failed");
        })
        .expect("spawn server failed");

        let send = b"ping".to_vec();
        let (res, _buf) = client.write_all(send).await;
        assert_eq!(res.expect("write_all failed"), 4);

        let mut recv = IoBuffMut::new(0, 8, 0);
        recv.payload_append(b"HEAD").unwrap();
        let (res, buf) = client.read_exact(recv, 4).await;
        assert_eq!(res.expect("read_exact failed"), 4);
        assert_eq!(buf.payload_bytes(), b"HEADpong");
    });
}

/// 256KB payload forcing partial kernel transfers via write_all / read_exact.
#[test]
fn runtime_unix_write_all_read_exact_large_payload() {
    let msg_size = 256 * 1024;
    run_test(async move {
        let (mut client, mut server) = UnixStream::pair().expect("socketpair failed");

        Executor::spawn(async move {
            let buf = vec![0u8; msg_size];
            let (res, buf) = server.read_exact(buf, msg_size).await;
            res.expect("server read_exact failed");
            assert!(buf.iter().all(|&b| b == 0xAB), "server data mismatch");

            let (res, _buf) = server.write_all(buf).await;
            res.expect("server write_all failed");
        })
        .expect("spawn server failed");

        let send = vec![0xABu8; msg_size];
        let (res, _buf) = client.write_all(send).await;
        assert_eq!(res.expect("write_all failed"), msg_size);

        let recv = vec![0u8; msg_size];
        let (res, buf) = client.read_exact(recv, msg_size).await;
        assert_eq!(res.expect("read_exact failed"), msg_size);
        assert!(buf.iter().all(|&b| b == 0xAB), "data mismatch on read");
    });
}

/// Peer closes before target bytes are delivered — read_exact returns
/// UnexpectedEof and the buffer contains the partial data received so far.
#[test]
fn runtime_unix_read_exact_eof() {
    run_test(async move {
        let (mut client, mut server) = UnixStream::pair().expect("socketpair failed");

        Executor::spawn(async move {
            let (res, _buf) = server.write_all(b"hi".to_vec()).await;
            res.expect("server write_all failed");
            drop(server); // close before client reads 4 bytes
        })
        .expect("spawn server failed");

        let mut recv = IoBuffMut::new(0, 8, 0);
        recv.payload_append(b"HEAD").unwrap();
        let (res, buf) = client.read_exact(recv, 4).await;
        let err = res.expect_err("should fail with UnexpectedEof");
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        assert_eq!(buf.payload_bytes(), b"HEADhi");

        let (mut client, mut server) = UnixStream::pair().expect("second socketpair failed");
        Executor::spawn(async move {
            let (res, _buf) = server.write_all(b"hi".to_vec()).await;
            res.expect("second server write_all failed");
            drop(server);
        })
        .expect("second server spawn failed");

        let (res, buf) = client.read_exact(b"HEAD".to_vec(), 4).await;
        let err = res.expect_err("base-zero exact read should report partial EOF");
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        assert_eq!(buf, b"hi", "positive base-zero progress should overwrite");
    });
}

/// Writing an empty buffer completes immediately with Ok(0).
#[test]
fn runtime_unix_write_all_empty() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");
    let (mut left, _right) = UnixStream::pair().expect("socketpair failed");

    executor
        .run(async move {
            let empty: Vec<u8> = Vec::new();
            let (res, _buf) = left.write_all(empty).await;
            assert_eq!(res.expect("write_all empty failed"), 0);
        })
        .expect("executor run failed");
}

#[test]
fn runtime_unix_pair_sets_nonblocking_and_cloexec() {
    let (left, right) = UnixStream::pair().expect("socketpair failed");

    for fd in [left.as_raw_fd(), right.as_raw_fd()] {
        let fd_flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        assert!(fd_flags >= 0, "F_GETFD failed for fd {fd}");
        assert_ne!(
            fd_flags & libc::FD_CLOEXEC,
            0,
            "socketpair fd {fd} missing FD_CLOEXEC"
        );

        let status_flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        assert!(status_flags >= 0, "F_GETFL failed for fd {fd}");
        assert_ne!(
            status_flags & libc::O_NONBLOCK,
            0,
            "socketpair fd {fd} missing O_NONBLOCK"
        );
    }
}

/// Socket buffer options on UnixStream.
#[test]
fn runtime_unix_socket_options() {
    let (left, _right) = UnixStream::pair().expect("socketpair failed");

    left.set_send_buffer_size(65536)
        .expect("set_send_buffer_size failed");
    assert!(left.send_buffer_size().expect("send_buffer_size failed") > 0);

    left.set_recv_buffer_size(65536)
        .expect("set_recv_buffer_size failed");
    assert!(left.recv_buffer_size().expect("recv_buffer_size failed") > 0);

    let oversized = libc::c_int::MAX as usize + 1;
    let send_err = left
        .set_send_buffer_size(oversized)
        .expect_err("oversize send buffer should fail");
    assert_eq!(send_err.kind(), std::io::ErrorKind::InvalidInput);

    let recv_err = left
        .set_recv_buffer_size(oversized)
        .expect_err("oversize recv buffer should fail");
    assert_eq!(recv_err.kind(), std::io::ErrorKind::InvalidInput);
}

/// Shutdown write half — peer sees EOF on read.
#[test]
fn runtime_unix_shutdown_write() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");

            let (res, _buf) = writer.write_all(b"hello".to_vec()).await;
            res.expect("write failed");
            writer
                .shutdown(std::net::Shutdown::Write)
                .expect("shutdown failed");

            let buf = vec![0u8; 5];
            let (res, buf) = reader.read_exact(buf, 5).await;
            res.expect("read_exact failed");
            assert_eq!(&buf[..], b"hello");

            // Next read should see EOF
            let buf2 = b"X".to_vec();
            let (res, buf2) = reader.read(buf2, 1).await;
            assert_eq!(res.expect("read after shutdown failed"), 0);
            assert_eq!(buf2, b"X", "EOF must preserve existing contents");
        })
        .expect("executor run failed");
}

/// In-process ponger using Executor::spawn — the production pattern for
/// single-threaded async echo.  Validates write_all / read_exact with both
/// ends on the same executor, exercising waker-derived task identity across
/// interleaved polls in the executor's thread-local (TLS) context.
#[test]
fn runtime_unix_spawn_ponger_in_process() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            let (mut pinger, mut ponger) = UnixStream::pair().expect("socketpair failed");
            let msg_size = 64;
            let rounds = 100;

            Executor::spawn(async move {
                for _ in 0..rounds {
                    let buf = vec![0u8; msg_size];
                    let (res, buf) = ponger.read_exact(buf, msg_size).await;
                    res.expect("ponger read_exact failed");
                    let (res, _buf) = ponger.write_all(buf).await;
                    res.expect("ponger write_all failed");
                }
            })
            .expect("spawn ponger failed");

            let mut send_data = vec![0xCDu8; msg_size];
            for _ in 0..rounds {
                let (res, buf) = pinger.write_all(send_data).await;
                res.expect("pinger write_all failed");
                send_data = buf;

                let recv_buf = vec![0u8; msg_size];
                let (res, buf) = pinger.read_exact(recv_buf, msg_size).await;
                res.expect("pinger read_exact failed");
                assert!(buf.iter().all(|&b| b == 0xCD), "echo data mismatch");
            }
        })
        .expect("executor run failed");
}

#[test]
fn runtime_unix_read_exact_rejects_oversize_iobuff() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            let (_writer, mut reader) = UnixStream::pair().expect("socketpair failed");

            let recv = IoBuffMut::new(0, 4, 0);
            let (res, buf) = reader.read_exact(recv, 5).await;
            let err = res.expect_err("oversize read_exact should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(buf.payload_len(), 0);
            assert_eq!(buf.payload_remaining(), 4);
        })
        .expect("executor run failed");
}

#[test]
fn runtime_unix_readv_exact_rejects_oversize_chain() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            let (_writer, mut reader) = UnixStream::pair().expect("socketpair failed");

            let recv = make_read_chain([4]);

            let (res, chain) = reader.readv_exact(recv, 5).await;
            let err = res.expect_err("oversize readv_exact should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(
                chain
                    .get(0)
                    .expect("valid readv_exact segment in test")
                    .payload_len(),
                0
            );
            assert_eq!(
                chain
                    .get(0)
                    .expect("valid readv_exact segment in test")
                    .payload_remaining(),
                4
            );
        })
        .expect("executor run failed");
}

#[test]
#[cfg(all(target_os = "linux", target_pointer_width = "64", not(miri)))]
fn runtime_unix_write_rejects_oversize_iobuff() {
    let oversized =
        SparseOversizedReadOnly::new().expect("failed to reserve sparse oversized mapping");
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    let (_oversized, _writer, _reader) = run_test_output(&mut executor, async move {
        let (mut writer, reader) = UnixStream::pair().expect("socketpair failed");

        let (res, oversized) = writer.write(oversized).await;
        assert_oversized_send_rejected(res, &oversized);

        let (res, oversized) = writer.write_all(oversized).await;
        assert_oversized_send_rejected(res, &oversized);

        (oversized, writer, reader)
    });

    #[cfg(debug_assertions)]
    assert_eq!(
        executor.last_stats().sqe_submits,
        0,
        "oversized Unix writes should submit no SQE"
    );
}

// ============================================================================
// IoBuffMut / IoBuff transport integration tests
// ============================================================================

/// Ping-pong using IoBuffMut for receive and IoBuff (frozen) for send.
#[test]
fn runtime_unix_ping_pong_iobuff() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            let (mut client, mut server) = UnixStream::pair().expect("socketpair failed");

            Executor::spawn(async move {
                let recv_buf = IoBuffMut::new(0, 4, 0);
                let (res, buf) = server.read(recv_buf, 4).await;
                assert_eq!(res.expect("server read failed"), 4);
                assert_eq!(buf.payload_bytes(), b"ping");

                let mut send_buf = IoBuffMut::new(0, 4, 0);
                send_buf.payload_append(b"pong").unwrap();
                let frozen = send_buf.freeze();
                let (res, _buf) = server.write(frozen).await;
                assert_eq!(res.expect("server write failed"), 4);
            })
            .expect("spawn server failed");

            let mut send_buf = IoBuffMut::new(0, 4, 0);
            send_buf.payload_append(b"ping").unwrap();
            let (res, _buf) = client.write(send_buf).await;
            assert_eq!(res.expect("write failed"), 4);

            let recv_buf = IoBuffMut::new(0, 4, 0);
            let (res, buf) = client.read(recv_buf, 4).await;
            assert_eq!(res.expect("read failed"), 4);
            assert_eq!(buf.payload_bytes(), b"pong");
        })
        .expect("executor run failed");
}

/// write_all / read_exact with IoBuffMut and IoBuff.
#[test]
fn runtime_unix_write_all_read_exact_iobuff() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            let (mut client, mut server) = UnixStream::pair().expect("socketpair failed");

            Executor::spawn(async move {
                let recv_buf = IoBuffMut::new(0, 4, 0);
                let (res, buf) = server.read_exact(recv_buf, 4).await;
                res.expect("server read_exact failed");
                assert_eq!(buf.payload_bytes(), b"ping");

                let mut reply = IoBuffMut::new(0, 4, 0);
                reply.payload_append(b"pong").unwrap();
                let (res, _buf) = server.write_all(reply.freeze()).await;
                res.expect("server write_all failed");
            })
            .expect("spawn server failed");

            let mut send_buf = IoBuffMut::new(0, 4, 0);
            send_buf.payload_append(b"ping").unwrap();
            let (res, _buf) = client.write_all(send_buf).await;
            assert_eq!(res.expect("write_all failed"), 4);

            let recv_buf = IoBuffMut::new(0, 4, 0);
            let (res, buf) = client.read_exact(recv_buf, 4).await;
            assert_eq!(res.expect("read_exact failed"), 4);
            assert_eq!(buf.payload_bytes(), b"pong");
        })
        .expect("executor run failed");
}

/// Staged IoBuffMut append reads preserve previously-read payload bytes.
#[test]
fn runtime_unix_read_exact_append_iobuff_staged() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");

            Executor::spawn(async move {
                let (res, _buf) = writer.write_all(b"HEADbody".to_vec()).await;
                res.expect("writer write_all failed");
            })
            .expect("spawn writer failed");

            let recv_buf = IoBuffMut::new(0, 8, 0);
            let (res, buf) = reader.read_exact_append(recv_buf, 4).await;
            assert_eq!(res.expect("header append read failed"), 4);
            assert_eq!(buf.payload_bytes(), b"HEAD");

            let (res, buf) = reader.read_exact_append(buf, 4).await;
            assert_eq!(res.expect("body append read failed"), 4);
            assert_eq!(buf.payload_len(), 8);
            assert_eq!(buf.payload_bytes(), b"HEADbody");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_unix_read_exact_append_rejects_oversize_iobuff() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            let (_writer, mut reader) = UnixStream::pair().expect("socketpair failed");

            let mut recv = IoBuffMut::new(0, 6, 0);
            recv.payload_append(b"seed").unwrap();

            let (res, buf) = reader.read_exact_append(recv, 3).await;
            let err = res.expect_err("oversize read_exact_append should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(buf.payload_len(), 4);
            assert_eq!(buf.payload_remaining(), 2);
            assert_eq!(buf.payload_bytes(), b"seed");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_unix_read_exact_append_eof_preserves_partial_iobuff() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");

            Executor::spawn(async move {
                let (res, _buf) = writer.write_all(b"tail".to_vec()).await;
                res.expect("writer write_all failed");
                drop(writer);
            })
            .expect("spawn writer failed");

            let mut recv = IoBuffMut::new(0, 12, 0);
            recv.payload_append(b"head").unwrap();

            let (res, buf) = reader.read_exact_append(recv, 8).await;
            let err = res.expect_err("should fail with UnexpectedEof");
            assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
            assert_eq!(buf.payload_len(), 8);
            assert_eq!(buf.payload_bytes(), b"headtail");
        })
        .expect("executor run failed");
}

/// IoBuffMut with headroom — prepend a protocol header after filling payload.
#[test]
fn runtime_unix_iobuff_headroom() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");

            // Build a framed message: 4-byte header + 5-byte payload.
            let mut buf = IoBuffMut::new(4, 16, 0);
            buf.payload_append(b"world").unwrap();
            buf.headroom_prepend(b"HDR:").unwrap();
            assert_eq!(buf.bytes(), b"HDR:world");

            let (res, _buf) = writer.write_all(buf).await;
            assert_eq!(res.expect("write_all failed"), 9);

            let recv_buf = IoBuffMut::new(0, 16, 0);
            let (res, buf) = reader.read_exact(recv_buf, 9).await;
            assert_eq!(res.expect("read_exact failed"), 9);
            assert_eq!(buf.payload_bytes(), b"HDR:world");
        })
        .expect("executor run failed");
}

/// IoBuff clone — send the same frozen buffer to two readers.
#[test]
fn runtime_unix_iobuff_clone_send() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            let (mut w1, mut r1) = UnixStream::pair().expect("socketpair 1 failed");
            let (mut w2, mut r2) = UnixStream::pair().expect("socketpair 2 failed");

            let mut send_buf = IoBuffMut::new(0, 4, 0);
            send_buf.payload_append(b"echo").unwrap();
            let frozen = send_buf.freeze();
            let frozen2 = frozen.clone();

            Executor::spawn(async move {
                let recv = IoBuffMut::new(0, 4, 0);
                let (res, buf) = r1.read_exact(recv, 4).await;
                res.expect("r1 read_exact failed");
                assert_eq!(buf.payload_bytes(), b"echo");
            })
            .expect("spawn r1 failed");

            Executor::spawn(async move {
                let recv = IoBuffMut::new(0, 4, 0);
                let (res, buf) = r2.read_exact(recv, 4).await;
                res.expect("r2 read_exact failed");
                assert_eq!(buf.payload_bytes(), b"echo");
            })
            .expect("spawn r2 failed");

            let (res, _buf) = w1.write_all(frozen).await;
            res.expect("w1 write_all failed");
            let (res, _buf) = w2.write_all(frozen2).await;
            res.expect("w2 write_all failed");
        })
        .expect("executor run failed");
}

/// Pool-allocated buffers through the transport layer.
#[test]
fn runtime_unix_pool_buffers() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            let (mut client, mut server) = UnixStream::pair().expect("socketpair failed");

            let mut pool = IoBuffPool::new(IoBuffPoolConfig {
                headroom: 0,
                payload: 64,
                tailroom: 0,
                objs_per_slab: 16,
            })
            .expect("pool config invalid");
            pool.init();

            Executor::spawn(async move {
                let recv_buf = IoBuffMut::new(0, 64, 0);
                let (res, buf) = server.read_exact(recv_buf, 5).await;
                res.expect("server read_exact failed");
                assert_eq!(buf.payload_bytes(), b"hello");

                let mut reply = IoBuffMut::new(0, 64, 0);
                reply.payload_append(b"world").unwrap();
                let (res, _buf) = server.write_all(reply).await;
                res.expect("server write_all failed");
            })
            .expect("spawn server failed");

            let mut send_buf = pool.alloc().expect("pool alloc failed");
            send_buf.payload_append(b"hello").unwrap();
            let (res, _buf) = client.write_all(send_buf).await;
            assert_eq!(res.expect("write_all failed"), 5);

            let recv_buf = pool.alloc().expect("pool alloc failed");
            let (res, buf) = client.read_exact(recv_buf, 5).await;
            assert_eq!(res.expect("read_exact failed"), 5);
            assert_eq!(buf.payload_bytes(), b"world");
        })
        .expect("executor run failed");
}

/// Large payload with IoBuffMut — forces partial kernel transfers.
#[test]
fn runtime_unix_write_all_read_exact_large_iobuff() {
    let msg_size = 256 * 1024;
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            let (mut client, mut server) = UnixStream::pair().expect("socketpair failed");

            Executor::spawn(async move {
                let recv_buf = IoBuffMut::new(0, msg_size, 0);
                let (res, buf) = server.read_exact(recv_buf, msg_size).await;
                res.expect("server read_exact failed");
                assert!(
                    buf.payload_bytes().iter().all(|&b| b == 0xAB),
                    "server data mismatch"
                );

                let (res, _buf) = server.write_all(buf).await;
                res.expect("server write_all failed");
            })
            .expect("spawn server failed");

            let mut send_buf = IoBuffMut::new(0, msg_size, 0);
            send_buf.payload_append(&vec![0xABu8; msg_size]).unwrap();
            let (res, _buf) = client.write_all(send_buf).await;
            assert_eq!(res.expect("write_all failed"), msg_size);

            let recv_buf = IoBuffMut::new(0, msg_size, 0);
            let (res, buf) = client.read_exact(recv_buf, msg_size).await;
            assert_eq!(res.expect("read_exact failed"), msg_size);
            assert!(
                buf.payload_bytes().iter().all(|&b| b == 0xAB),
                "data mismatch on read"
            );
        })
        .expect("executor run failed");
}

// ============================================================================
// Vectored I/O (readv / writev) tests
// ============================================================================

/// writev with a 3-segment chain, readv into a 3-segment chain.
#[test]
fn runtime_unix_writev_readv() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");

            let frozen = make_payload_chain([&b"hello"[..], &b" "[..], &b"world"[..]]);

            let (res, _chain) = writer.writev(frozen).await;
            assert_eq!(res.expect("writev failed"), 11);

            let read_chain = make_read_chain([5, 1, 5]);

            let (res, chain) = reader.readv(read_chain).await;
            assert_eq!(res.expect("readv failed"), 11);
            assert_eq!(chain.get(0).expect("seg0").payload_bytes(), b"hello");
            assert_eq!(chain.get(1).expect("seg1").payload_bytes(), b" ");
            assert_eq!(chain.get(2).expect("seg2").payload_bytes(), b"world");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_unix_readv_skips_nonwritable_segments() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");

            let (res, _payload) = writer.write_all(b"abcde".to_vec()).await;
            assert_eq!(res.expect("first write_all failed"), 5);

            let mut full_prefix = IoBuffMut::new(0, 4, 0);
            full_prefix
                .payload_append(b"KEEP")
                .expect("full prefix initialization failed");
            let zero = IoBuffMut::new(0, 0, 0);
            let first_writable = IoBuffMut::new(0, 2, 0);
            let mut full_middle = IoBuffMut::new(0, 1, 0);
            full_middle
                .payload_append(b"X")
                .expect("full middle initialization failed");
            let second_writable = IoBuffMut::new(0, 3, 0);
            let read_chain = IoBuffVecMut::from_array([
                full_prefix,
                zero,
                first_writable,
                full_middle,
                second_writable,
            ]);

            let (res, chain) = reader.readv(read_chain).await;
            assert_eq!(res.expect("readv failed"), 5);
            assert_eq!(
                chain.get(0).expect("full prefix missing").payload_bytes(),
                b"KEEP"
            );
            assert_eq!(chain.get(1).expect("zero segment missing").payload_len(), 0);
            assert_eq!(
                chain
                    .get(2)
                    .expect("first writable missing")
                    .payload_bytes(),
                b"ab"
            );
            assert_eq!(
                chain.get(3).expect("full middle missing").payload_bytes(),
                b"X"
            );
            assert_eq!(
                chain
                    .get(4)
                    .expect("second writable missing")
                    .payload_bytes(),
                b"cde"
            );

            let (res, _payload) = writer.write_all(b"uvwxyz".to_vec()).await;
            assert_eq!(res.expect("second write_all failed"), 6);

            let zero = IoBuffMut::new(0, 0, 0);
            let first_writable = IoBuffMut::new(0, 3, 0);
            let mut full_middle = IoBuffMut::new(0, 2, 0);
            full_middle
                .payload_append(b"OK")
                .expect("exact full segment initialization failed");
            let second_writable = IoBuffMut::new(0, 3, 0);
            let exact_chain =
                IoBuffVecMut::from_array([zero, first_writable, full_middle, second_writable]);

            let (res, chain) = reader.readv_exact(exact_chain, 6).await;
            assert_eq!(res.expect("readv_exact failed"), 6);
            assert_eq!(chain.get(0).expect("exact zero missing").payload_len(), 0);
            assert_eq!(
                chain.get(1).expect("exact first missing").payload_bytes(),
                b"uvw"
            );
            assert_eq!(
                chain.get(2).expect("exact full missing").payload_bytes(),
                b"OK"
            );
            assert_eq!(
                chain.get(3).expect("exact second missing").payload_bytes(),
                b"xyz"
            );

            let mut full = IoBuffMut::new(0, 2, 0);
            full.payload_append(b"zz")
                .expect("all-full segment initialization failed");
            let all_full = IoBuffVecMut::from_array([full, IoBuffMut::new(0, 0, 0)]);
            let (res, chain) = reader.readv(all_full).await;
            let err = res.expect_err("all-full readv should reject an ambiguous EOF result");
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(
                chain
                    .get(0)
                    .expect("all-full segment missing")
                    .payload_bytes(),
                b"zz"
            );
        })
        .expect("executor run failed");
}

/// writev_all + readv_exact with a 3-segment chain.
#[test]
fn runtime_unix_writev_all_readv_exact() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");

            let frozen = make_payload_chain([&b"hello"[..], &b" "[..], &b"world"[..]]);

            let (res, _chain) = writer.writev_all(frozen).await;
            assert_eq!(res.expect("writev_all failed"), 11);

            let read_chain = make_read_chain([6, 5]);

            let (res, chain) = reader.readv_exact(read_chain, 11).await;
            assert_eq!(res.expect("readv_exact failed"), 11);
            assert_eq!(chain.get(0).expect("seg0").payload_bytes(), b"hello ");
            assert_eq!(chain.get(1).expect("seg1").payload_bytes(), b"world");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_unix_readv_exact_clamps_target_below_chain_capacity() {
    run_test(async move {
        let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");

        Executor::spawn(async move {
            let payload = b"abcdefghijklmnopqrstuvwx".to_vec();
            let (res, payload) = writer.write_all(payload).await;
            assert_eq!(res.expect("write_all failed"), payload.len());
        })
        .expect("spawn writer failed");

        let read_chain = make_read_chain([8, 8, 8]);
        let (res, chain) = reader.readv_exact(read_chain, 10).await;
        assert_eq!(res.expect("readv_exact failed"), 10);
        assert_eq!(chain.get(0).expect("seg0").payload_bytes(), b"abcdefgh");
        assert_eq!(chain.get(1).expect("seg1").payload_bytes(), b"ij");
        assert_eq!(chain.get(2).expect("seg2").payload_len(), 0);

        let (res, tail) = reader.read_exact(vec![0u8; 14], 14).await;
        assert_eq!(res.expect("tail read_exact failed"), 14);
        assert_eq!(&tail[..], b"klmnopqrstuvwx");
    });
}

/// Large writev_all + readv_exact forcing partial kernel transfers.
#[test]
fn runtime_unix_writev_all_readv_exact_large() {
    let seg_size = 512 * 1024; // 512KB per segment
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");
            writer
                .set_send_buffer_size(4096)
                .expect("set writer send buffer failed");
            reader
                .set_recv_buffer_size(4096)
                .expect("set reader recv buffer failed");

            Executor::spawn(async move {
                // Reader: readv_exact 2 segments of 512KB each = 1MB total.
                let read_chain = make_read_chain([seg_size, seg_size]);

                let total = seg_size * 2;
                let (res, chain) = reader.readv_exact(read_chain, total).await;
                assert_eq!(res.expect("readv_exact failed"), total);
                assert!(
                    chain
                        .get(0)
                        .expect("seg0")
                        .payload_bytes()
                        .iter()
                        .all(|&b| b == 0xAB),
                    "segment 0 data mismatch"
                );
                assert!(
                    chain
                        .get(1)
                        .expect("seg1")
                        .payload_bytes()
                        .iter()
                        .all(|&b| b == 0xAB),
                    "segment 1 data mismatch"
                );
            })
            .expect("spawn reader failed");

            // Writer: writev_all 2 segments of 512KB each.
            let data = vec![0xABu8; seg_size];
            let mut seg1 = IoBuffMut::new(0, seg_size, 0);
            seg1.payload_append(&data).unwrap();
            let mut seg2 = IoBuffMut::new(0, seg_size, 0);
            seg2.payload_append(&data).unwrap();

            let mut write_chain = IoBuffVecMut::<2>::new();
            write_chain.push(seg1).unwrap();
            write_chain.push(seg2).unwrap();
            let frozen = write_chain.freeze();

            let (res, _chain) = writer.writev_all(frozen).await;
            assert_eq!(res.expect("writev_all failed"), seg_size * 2);
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert!(
            stats.writev_partial_continuations > 0,
            "large writev_all/readv_exact should force a partial write continuation"
        );
    }
}

/// writev + readv round-trip with spawned ponger.
#[test]
fn runtime_unix_writev_readv_echo() {
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            let (mut client, mut server) = UnixStream::pair().expect("socketpair failed");

            Executor::spawn(async move {
                // Server: read 11 bytes, echo them back.
                let recv_buf = IoBuffMut::new(0, 64, 0);
                let (res, buf) = server.read_exact(recv_buf, 11).await;
                res.expect("server read_exact failed");
                assert_eq!(buf.payload_bytes(), b"hello world");

                let (res, _buf) = server.write_all(buf).await;
                res.expect("server write_all failed");
            })
            .expect("spawn server failed");

            let frozen = make_payload_chain([&b"hello"[..], &b" "[..], &b"world"[..]]);

            let (res, _chain) = client.writev(frozen).await;
            assert_eq!(res.expect("writev failed"), 11);

            let read_chain = make_read_chain([6, 5]);

            let (res, chain) = client.readv(read_chain).await;
            assert_eq!(res.expect("readv failed"), 11);
            assert_eq!(chain.get(0).expect("seg0").payload_bytes(), b"hello ");
            assert_eq!(chain.get(1).expect("seg1").payload_bytes(), b"world");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_unix_vectored_zero_length_operations() {
    run_test(async move {
        let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");

        let (res, write_chain) = writer.writev(make_payload_chain::<0>([])).await;
        assert_eq!(res.expect("writev empty failed"), 0);
        assert!(write_chain.is_empty());

        let (res, write_chain) = writer.writev_all(make_payload_chain::<0>([])).await;
        assert_eq!(res.expect("writev_all empty failed"), 0);
        assert!(write_chain.is_empty());

        let (res, read_chain) = reader.readv(make_read_chain::<0>([])).await;
        let err = res.expect_err("readv empty should reject ambiguous EOF result");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(read_chain.is_empty());

        let (res, read_chain) = reader.readv_exact(make_read_chain::<0>([]), 0).await;
        assert_eq!(res.expect("readv_exact zero failed"), 0);
        assert!(read_chain.is_empty());
    });
}

#[test]
fn runtime_unix_rental_futures_poll_after_ready_parks() {
    run_test(async move {
        let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");

        assert_poll_after_ready_parks(writer.write(Vec::<u8>::new())).await;
        assert_poll_after_ready_parks(reader.read(Vec::<u8>::new(), 0)).await;
        assert_poll_after_ready_parks(writer.write_all(Vec::<u8>::new())).await;
        assert_poll_after_ready_parks(reader.read_exact(Vec::<u8>::new(), 0)).await;
        assert_poll_after_ready_parks(reader.read_exact_append(IoBuffMut::new(0, 16, 0), 0)).await;
        assert_poll_after_ready_parks(writer.writev(make_payload_chain::<0>([]))).await;
        assert_poll_after_ready_parks(writer.writev_all(make_payload_chain::<0>([]))).await;
        assert_poll_after_ready_parks(reader.readv(make_read_chain::<0>([]))).await;
        assert_poll_after_ready_parks(reader.readv_exact(make_read_chain::<0>([]), 0)).await;
        assert_poll_after_ready_parks(writer.writev(make_read_only_chain::<0>([]))).await;
        assert_poll_after_ready_parks(writer.writev_all(make_read_only_chain::<0>([]))).await;
        assert_poll_after_ready_parks(writer.writev_projected(TestProjected::<0>::new([]))).await;
        assert_poll_after_ready_parks(writer.writev_all_projected(TestProjected::<0>::new([])))
            .await;
    });
}

#[test]
fn runtime_unix_writev_readonly_chain_empty_and_single_segment() {
    run_test(async move {
        let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");

        let (res, chain) = writer.writev(make_read_only_chain::<0>([])).await;
        assert_eq!(res.expect("empty read-only chain writev failed"), 0);
        assert!(chain.is_empty());

        let (res, chain) = writer
            .writev_all(make_read_only_chain([&b"single"[..]]))
            .await;
        assert_eq!(res.expect("read-only chain writev_all failed"), 6);
        let recovered: Vec<Vec<u8>> = chain.into_iter().collect();
        assert_eq!(recovered, vec![b"single".to_vec()]);

        let (res, buf) = reader.read_exact(vec![0u8; 6], 6).await;
        assert_eq!(res.expect("read_exact failed"), 6);
        assert_eq!(&buf[..], b"single");
    });
}

#[test]
fn runtime_unix_writev_all_readonly_chain_writes_segments_in_order() {
    run_test(async move {
        let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");

        let chain = make_read_only_chain([&b"hello"[..], &b""[..], &b" "[..], &b"world"[..]]);
        let (res, chain) = writer.writev_all(chain).await;
        assert_eq!(res.expect("read-only chain writev_all failed"), 11);
        assert_eq!(chain.segments(), 4);

        let (res, buf) = reader.read_exact(vec![0u8; 11], 11).await;
        assert_eq!(res.expect("read_exact failed"), 11);
        assert_eq!(&buf[..], b"hello world");
    });
}

#[test]
fn runtime_unix_writev_projected_empty_and_single_segment() {
    run_test(async move {
        let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");

        let (res, source) = writer.writev_projected(TestProjected::<0>::new([])).await;
        assert_eq!(res.expect("writev_projected empty failed"), 0);
        assert!(source.expected().is_empty());

        let source = TestProjected::new([&b"single"[..]]);
        let expected = source.expected();
        let (res, source) = writer.writev_all_projected(source).await;
        assert_eq!(res.expect("writev_all_projected failed"), expected.len());
        assert_eq!(source.expected(), expected);

        let (res, buf) = reader
            .read_exact(vec![0u8; expected.len()], expected.len())
            .await;
        assert_eq!(res.expect("read_exact failed"), expected.len());
        assert_eq!(&buf[..], &expected[..]);
    });
}

#[test]
fn runtime_unix_writev_all_projected_writes_segments_in_order() {
    run_test(async move {
        let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");

        let source = TestProjected::new([&b"hello"[..], &b""[..], &b" "[..], &b"world"[..]]);
        let expected = source.expected();
        let (res, source) = writer.writev_all_projected(source).await;
        assert_eq!(res.expect("writev_all_projected failed"), expected.len());
        assert_eq!(source.expected(), expected);

        let (res, buf) = reader
            .read_exact(vec![0u8; expected.len()], expected.len())
            .await;
        assert_eq!(res.expect("read_exact failed"), expected.len());
        assert_eq!(&buf[..], &expected[..]);
    });
}

#[test]
fn runtime_unix_writev_all_readonly_chain_large() {
    let seg_size = 128 * 1024;
    run_test(async move {
        let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");

        Executor::spawn(async move {
            let total = seg_size * 2;
            let (res, buf) = reader.read_exact(vec![0u8; total], total).await;
            assert_eq!(res.expect("read_exact failed"), total);
            assert!(buf[..seg_size].iter().all(|&b| b == 0xAB));
            assert!(buf[seg_size..].iter().all(|&b| b == 0xCD));
        })
        .expect("spawn reader failed");

        let first = vec![0xABu8; seg_size];
        let second = vec![0xCDu8; seg_size];
        let chain =
            flowio::runtime::buffer::iobuffvec::IoBuffReadOnlyVec::<Vec<u8>, 2>::from_array([
                first, second,
            ]);

        let (res, chain) = writer.writev_all(chain).await;
        assert_eq!(
            res.expect("read-only chain writev_all failed"),
            seg_size * 2
        );
        assert_eq!(chain.segments(), 2);
    });
}

#[test]
fn runtime_unix_readv_exact_eof_distributes_partial_data() {
    run_test(async move {
        let (mut client, mut server) = UnixStream::pair().expect("socketpair failed");

        Executor::spawn(async move {
            let (res, _buf) = server.write_all(b"hello".to_vec()).await;
            res.expect("server write_all failed");
            drop(server);
        })
        .expect("spawn server failed");

        let (res, chain) = client.readv_exact(make_read_chain([3, 3]), 6).await;
        let err = res.expect_err("readv_exact should fail with UnexpectedEof");
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        assert_eq!(chain.get(0).expect("seg0").payload_bytes(), b"hel");
        assert_eq!(chain.get(1).expect("seg1").payload_bytes(), b"lo");
    });
}
