mod common;

use common::TestIoBuffMut as IoBuffMut;
use flowio::net::unix::UnixStream;
use flowio::runtime::buffer::iobuffvec::IoBuffVecMut;
use flowio::runtime::buffer::pool::{IoBuffPool, IoBuffPoolConfig};
use flowio::runtime::executor::Executor;

/// Basic ping-pong with Vec<u8> buffers and a spawned async peer.
#[test]
fn runtime_unix_stream_ping_pong()
{
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
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

            let recv = vec![0u8; 4];
            let (res, buf) = client.read(recv, 4).await;
            assert_eq!(res.expect("read failed"), 4);
            assert_eq!(&buf[..4], b"pong");
        })
        .expect("executor run failed");
}

/// write_all / read_exact with Vec<u8> buffers and a spawned async peer.
#[test]
fn runtime_unix_write_all_read_exact()
{
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
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

            let recv = vec![0u8; 4];
            let (res, buf) = client.read_exact(recv, 4).await;
            assert_eq!(res.expect("read_exact failed"), 4);
            assert_eq!(&buf[..], b"pong");
        })
        .expect("executor run failed");
}

/// 256KB payload forcing partial kernel transfers via write_all / read_exact.
#[test]
fn runtime_unix_write_all_read_exact_large_payload()
{
    let msg_size = 256 * 1024;
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
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
        })
        .expect("executor run failed");
}

/// Peer closes before target bytes are delivered — read_exact returns
/// UnexpectedEof and the buffer contains the partial data received so far.
#[test]
fn runtime_unix_read_exact_eof()
{
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            let (mut client, mut server) = UnixStream::pair().expect("socketpair failed");

            Executor::spawn(async move {
                let (res, _buf) = server.write_all(b"hi".to_vec()).await;
                res.expect("server write_all failed");
                drop(server); // close before client reads 4 bytes
            })
            .expect("spawn server failed");

            let recv = vec![0u8; 4];
            let (res, buf) = client.read_exact(recv, 4).await;
            let err = res.expect_err("should fail with UnexpectedEof");
            assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
            assert_eq!(&buf[..2], b"hi");
        })
        .expect("executor run failed");
}

/// Writing an empty buffer completes immediately with Ok(0).
#[test]
fn runtime_unix_write_all_empty()
{
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

/// Socket buffer options on UnixStream.
#[test]
fn runtime_unix_socket_options()
{
    let (left, _right) = UnixStream::pair().expect("socketpair failed");

    left.set_send_buffer_size(65536)
        .expect("set_send_buffer_size failed");
    assert!(left.send_buffer_size().expect("send_buffer_size failed") > 0);

    left.set_recv_buffer_size(65536)
        .expect("set_recv_buffer_size failed");
    assert!(left.recv_buffer_size().expect("recv_buffer_size failed") > 0);
}

/// Shutdown write half — peer sees EOF on read.
#[test]
fn runtime_unix_shutdown_write()
{
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
            let buf2 = vec![0u8; 1];
            let (res, _buf) = reader.read(buf2, 1).await;
            assert_eq!(res.expect("read after shutdown failed"), 0);
        })
        .expect("executor run failed");
}

/// In-process ponger using Executor::spawn — the production pattern for
/// single-threaded async echo.  Validates write_all / read_exact with both
/// ends on the same executor, exercising owner_task cycling in the TLS context.
#[test]
fn runtime_unix_spawn_ponger_in_process()
{
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            let (mut pinger, mut ponger) = UnixStream::pair().expect("socketpair failed");
            let msg_size = 64;
            let rounds = 100;

            Executor::spawn(async move {
                for _ in 0..rounds
                {
                    let buf = vec![0u8; msg_size];
                    let (res, buf) = ponger.read_exact(buf, msg_size).await;
                    res.expect("ponger read_exact failed");
                    let (res, _buf) = ponger.write_all(buf).await;
                    res.expect("ponger write_all failed");
                }
            })
            .expect("spawn ponger failed");

            let mut send_data = vec![0xCDu8; msg_size];
            for _ in 0..rounds
            {
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
fn runtime_unix_read_exact_rejects_oversize_iobuff()
{
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
fn runtime_unix_readv_exact_rejects_oversize_chain()
{
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            let (_writer, mut reader) = UnixStream::pair().expect("socketpair failed");

            let mut recv = IoBuffVecMut::<1>::new();
            recv.push(IoBuffMut::new(0, 4, 0)).unwrap();

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

// ============================================================================
// IoBuffMut / IoBuff transport integration tests
// ============================================================================

/// Ping-pong using IoBuffMut for receive and IoBuff (frozen) for send.
#[test]
fn runtime_unix_ping_pong_iobuff()
{
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
fn runtime_unix_write_all_read_exact_iobuff()
{
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

/// IoBuffMut with headroom — prepend a protocol header after filling payload.
#[test]
fn runtime_unix_iobuff_headroom()
{
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
fn runtime_unix_iobuff_clone_send()
{
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
fn runtime_unix_pool_buffers()
{
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
fn runtime_unix_write_all_read_exact_large_iobuff()
{
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
fn runtime_unix_writev_readv()
{
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");

            // Build 3-segment write chain: "hello" + " " + "world"
            let mut seg1 = IoBuffMut::new(0, 16, 0);
            seg1.payload_append(b"hello").unwrap();
            let mut seg2 = IoBuffMut::new(0, 16, 0);
            seg2.payload_append(b" ").unwrap();
            let mut seg3 = IoBuffMut::new(0, 16, 0);
            seg3.payload_append(b"world").unwrap();

            let mut write_chain = IoBuffVecMut::<3>::new();
            write_chain.push(seg1).unwrap();
            write_chain.push(seg2).unwrap();
            write_chain.push(seg3).unwrap();
            let frozen = write_chain.freeze();

            let (res, _chain) = writer.writev(frozen).await;
            assert_eq!(res.expect("writev failed"), 11);

            // Read into 3-segment chain.
            let mut read_chain = IoBuffVecMut::<3>::new();
            read_chain.push(IoBuffMut::new(0, 5, 0)).unwrap();
            read_chain.push(IoBuffMut::new(0, 1, 0)).unwrap();
            read_chain.push(IoBuffMut::new(0, 5, 0)).unwrap();

            let (res, chain) = reader.readv(read_chain).await;
            assert_eq!(res.expect("readv failed"), 11);
            assert_eq!(chain.get(0).expect("seg0").payload_bytes(), b"hello");
            assert_eq!(chain.get(1).expect("seg1").payload_bytes(), b" ");
            assert_eq!(chain.get(2).expect("seg2").payload_bytes(), b"world");
        })
        .expect("executor run failed");
}

/// writev_all + readv_exact with a 3-segment chain.
#[test]
fn runtime_unix_writev_all_readv_exact()
{
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");

            // Build 3-segment write chain.
            let mut seg1 = IoBuffMut::new(0, 16, 0);
            seg1.payload_append(b"hello").unwrap();
            let mut seg2 = IoBuffMut::new(0, 16, 0);
            seg2.payload_append(b" ").unwrap();
            let mut seg3 = IoBuffMut::new(0, 16, 0);
            seg3.payload_append(b"world").unwrap();

            let mut write_chain = IoBuffVecMut::<3>::new();
            write_chain.push(seg1).unwrap();
            write_chain.push(seg2).unwrap();
            write_chain.push(seg3).unwrap();
            let frozen = write_chain.freeze();

            let (res, _chain) = writer.writev_all(frozen).await;
            assert_eq!(res.expect("writev_all failed"), 11);

            // readv_exact into 2 segments: exactly 11 bytes.
            let mut read_chain = IoBuffVecMut::<2>::new();
            read_chain.push(IoBuffMut::new(0, 6, 0)).unwrap();
            read_chain.push(IoBuffMut::new(0, 5, 0)).unwrap();

            let (res, chain) = reader.readv_exact(read_chain, 11).await;
            assert_eq!(res.expect("readv_exact failed"), 11);
            assert_eq!(chain.get(0).expect("seg0").payload_bytes(), b"hello ");
            assert_eq!(chain.get(1).expect("seg1").payload_bytes(), b"world");
        })
        .expect("executor run failed");
}

/// Large writev_all + readv_exact forcing partial kernel transfers.
#[test]
fn runtime_unix_writev_all_readv_exact_large()
{
    let seg_size = 128 * 1024; // 128KB per segment
    let mut executor = Executor::new().expect("failed to construct runtime executor");

    executor
        .run(async move {
            let (mut writer, mut reader) = UnixStream::pair().expect("socketpair failed");

            Executor::spawn(async move {
                // Reader: readv_exact 2 segments of 128KB each = 256KB total.
                let mut read_chain = IoBuffVecMut::<2>::new();
                read_chain.push(IoBuffMut::new(0, seg_size, 0)).unwrap();
                read_chain.push(IoBuffMut::new(0, seg_size, 0)).unwrap();

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

            // Writer: writev_all 2 segments of 128KB each.
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
}

/// writev + readv round-trip with spawned ponger.
#[test]
fn runtime_unix_writev_readv_echo()
{
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

            // Client: writev 3 segments.
            let mut seg1 = IoBuffMut::new(0, 16, 0);
            seg1.payload_append(b"hello").unwrap();
            let mut seg2 = IoBuffMut::new(0, 16, 0);
            seg2.payload_append(b" ").unwrap();
            let mut seg3 = IoBuffMut::new(0, 16, 0);
            seg3.payload_append(b"world").unwrap();

            let mut write_chain = IoBuffVecMut::<3>::new();
            write_chain.push(seg1).unwrap();
            write_chain.push(seg2).unwrap();
            write_chain.push(seg3).unwrap();
            let frozen = write_chain.freeze();

            let (res, _chain) = client.writev(frozen).await;
            assert_eq!(res.expect("writev failed"), 11);

            // Read echo back into 2-segment chain.
            let mut read_chain = IoBuffVecMut::<2>::new();
            read_chain.push(IoBuffMut::new(0, 6, 0)).unwrap();
            read_chain.push(IoBuffMut::new(0, 5, 0)).unwrap();

            let (res, chain) = client.readv(read_chain).await;
            assert_eq!(res.expect("readv failed"), 11);
            assert_eq!(chain.get(0).expect("seg0").payload_bytes(), b"hello ");
            assert_eq!(chain.get(1).expect("seg1").payload_bytes(), b"world");
        })
        .expect("executor run failed");
}
