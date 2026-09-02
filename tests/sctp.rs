mod common;

use common::{
    DropTrackedReadOnly, DropTrackedReadWrite, TestIoBuffMut as IoBuffMut,
    enable_socket_timestampns, lowest_available_fd, make_payload_chain, make_read_chain,
    poll_once_pending, raw_fd_is_open, run_test_output, set_positive_linger, wait_for_drop_count,
    wait_for_live_slots,
};
#[cfg(all(target_os = "linux", target_pointer_width = "64", not(miri)))]
use common::{SparseOversizedReadOnly, assert_oversized_send_rejected};
use flowio::net::sctp::{
    SctpAddStreams, SctpAssocConfig, SctpAssocStatus, SctpConnector, SctpInitConfig, SctpListener,
    SctpNotification, SctpNotificationKind, SctpNotificationMask, SctpPeerAddrInfo,
    SctpPeerAddrParams, SctpReconfigFlags, SctpRecvInfo, SctpRecvMeta, SctpResetStreams,
    SctpSendInfo, SctpSocketConfig, SctpStream,
};
#[cfg(any(debug_assertions, feature = "test-support"))]
use flowio::runtime::buffer::IoBuffReadWrite;
use flowio::runtime::buffer::bytes::{ByteWriteAt, read_u32_at};
use flowio::runtime::buffer::iobuffvec::{IoBuffVec, IoBuffVecMut};
use flowio::runtime::buffer::pool::{IoBuffPool, IoBuffPoolConfig};
use flowio::runtime::executor::{Executor, ExecutorConfig};
use flowio::runtime::reactor::ReactorConfig;
use flowio::runtime::timer::{TimeoutError, sleep, timeout};
use flowio::test_support::net::sctp::{
    SctpRecordRecoverySnapshot, SctpSocketOptionSnapshot, SctpStashedRecvStateSnapshot,
    append_initialized_test_cmsg, capability_unavailable,
    test_accept_slot_drop_cached_state_preserves_unrelated_fd,
    test_accept_slot_drop_future_preserves_unrelated_fd, test_accept_with_established_config_error,
    test_adaptation_indication_type, test_apply_sctp_socket_options, test_assoc_change_type,
    test_assoc_reset_event_type, test_authentication_event_type,
    test_connect_slot_drop_cached_state_closes_socket_fd,
    test_connect_slot_drop_future_closes_socket_fd, test_fail_notification_mask_after_query,
    test_parse_notification, test_parse_recv_meta, test_parse_recv_meta_bare_with_policy,
    test_parse_recv_meta_with_policy, test_parse_stream_recv_meta,
    test_partial_delivery_event_type, test_peer_addr_change_type,
    test_peer_addr_params_rejects_optlen, test_remote_error_type, test_sctp_socket_options,
    test_sctp_socket_receive_options, test_sctp_stream_apply_unpublished_completion,
    test_sctp_stream_begin_data_tail, test_sctp_stream_receive_policy,
    test_sctp_stream_stashed_recv_state, test_send_failed_error_offset,
    test_send_failed_event_type, test_send_failed_info_offset, test_send_failed_type,
    test_sender_dry_event_type, test_shutdown_event_type, test_stream_change_event_type,
    test_stream_reset_event_type,
};
use flowio::test_support::runtime::test_hooks;
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::net::{Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

const SCTP_SHUTDOWN_FALLBACK_CHILD_ENV: &str = "FLOWIO_SCTP_SHUTDOWN_FALLBACK_CHILD";
const SCTP_SHUTDOWN_FALLBACK_TEST: &str =
    "runtime_sctp_shutdown_fallback_abandons_unretired_readiness_state_with_watchdog";
#[cfg(any(debug_assertions, feature = "test-support"))]
const SCTP_ABANDONED_STASH_CHILD_ENV: &str = "FLOWIO_SCTP_ABANDONED_STASH_CHILD";
#[cfg(any(debug_assertions, feature = "test-support"))]
const SCTP_ABANDONED_STASH_TEST: &str =
    "runtime_sctp_abandoned_stash_remains_terminal_through_stream_drop_with_watchdog";
const SCTP_EXTERNAL_ADOPTION_CLOSE_CHILD_ENV: &str = "FLOWIO_SCTP_EXTERNAL_ADOPTION_CLOSE_CHILD";
const SCTP_EXTERNAL_ADOPTION_CLOSE_TEST: &str =
    "runtime_sctp_external_adoption_classifies_then_uses_ring_close";
const SCTP_ACTIVE_IOVEC_REJECTION_CHILD_ENV: &str = "FLOWIO_SCTP_ACTIVE_IOVEC_REJECTION_CHILD";
const SCTP_ACTIVE_IOVEC_REJECTION_TEST: &str =
    "runtime_sctp_active_iovec_limit_rejects_before_submission_and_returns_owners";
const SCTP_ACTIVE_IOVEC_BOUNDARY_CHILD_ENV: &str = "FLOWIO_SCTP_ACTIVE_IOVEC_BOUNDARY_CHILD";
const SCTP_ACTIVE_IOVEC_BOUNDARY_TEST: &str =
    "runtime_sctp_active_iovec_boundary_accepts_sparse_and_excess_capacity_chains";
const SCTP_CONNECT_REUSE_CHILD_ENV: &str = "FLOWIO_SCTP_CONNECT_REUSE_CHILD";
const SCTP_CONNECT_REUSE_TEST: &str =
    "runtime_sctp_reusable_dropped_connect_retains_socket_until_connect_cqe";
const SCTP_ACTIVE_IOVEC_TEST_STACK_BYTES: &str = "33554432";

#[cfg(any(debug_assertions, feature = "test-support"))]
struct PointerTrackedReadWrite {
    bytes: Vec<u8>,
    identity: usize,
    pointer_calls: Rc<Cell<usize>>,
    drops: Rc<Cell<usize>>,
    panic_on_pointer: bool,
}

#[cfg(any(debug_assertions, feature = "test-support"))]
impl PointerTrackedReadWrite {
    fn new(
        len: usize,
        identity: usize,
        pointer_calls: &Rc<Cell<usize>>,
        drops: &Rc<Cell<usize>>,
    ) -> Self {
        Self {
            bytes: vec![0; len],
            identity,
            pointer_calls: Rc::clone(pointer_calls),
            drops: Rc::clone(drops),
            panic_on_pointer: false,
        }
    }

    fn panicking(
        len: usize,
        identity: usize,
        pointer_calls: &Rc<Cell<usize>>,
        drops: &Rc<Cell<usize>>,
    ) -> Self {
        let mut buffer = Self::new(len, identity, pointer_calls, drops);
        buffer.panic_on_pointer = true;
        buffer
    }
}

#[cfg(any(debug_assertions, feature = "test-support"))]
impl Drop for PointerTrackedReadWrite {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

// SAFETY: the Vec allocation remains pointer-stable across moves, and every
// exposed writable byte is initialized. Tests never resize it while a pointer
// is exposed, and `set_written_len` clamps to the allocation capacity.
#[cfg(any(debug_assertions, feature = "test-support"))]
unsafe impl IoBuffReadWrite for PointerTrackedReadWrite {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.pointer_calls.set(self.pointer_calls.get() + 1);
        if self.panic_on_pointer {
            panic!("intentional SCTP retained-receive pointer panic");
        }
        self.bytes.as_mut_ptr()
    }

    fn writable_len(&self) -> usize {
        self.bytes.capacity()
    }

    unsafe fn set_written_len(&mut self, len: usize) {
        unsafe { self.bytes.set_len(len.min(self.bytes.capacity())) };
    }
}

fn bind_sctp_listener_or_skip(test_name: &str, config: SctpSocketConfig) -> Option<SctpListener> {
    match SctpListener::bind_with_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128, config) {
        Ok(listener) => Some(listener),
        Err(err) if capability_unavailable(&err) => {
            eprintln!("skipping {test_name}: SCTP unsupported ({err})");
            None
        }
        Err(err) => panic!("failed to bind sctp listener for {test_name}: {err}"),
    }
}

fn raw_sctp_socket_or_skip(test_name: &str, domain: libc::c_int) -> Option<OwnedFd> {
    let fd = unsafe {
        libc::socket(
            domain,
            libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            libc::IPPROTO_SCTP,
        )
    };
    if fd >= 0 {
        // SAFETY: a successful socket call returns one descriptor owned only
        // by this helper.
        return Some(unsafe { OwnedFd::from_raw_fd(fd) });
    }

    let err = std::io::Error::last_os_error();
    if capability_unavailable(&err) {
        eprintln!("skipping {test_name}: SCTP unsupported ({err})");
        return None;
    }
    panic!("failed to create sctp socket for {test_name}: {err}");
}

fn raw_sctp_stream_or_skip(test_name: &str) -> Option<(SctpStream, std::os::fd::RawFd)> {
    let fd = raw_sctp_socket_or_skip(test_name, libc::AF_INET)?;
    let raw = fd.as_raw_fd();
    Some((
        SctpStream::from_owned_fd(fd, SocketAddr::from((Ipv4Addr::LOCALHOST, 0))),
        raw,
    ))
}

#[cfg(any(debug_assertions, feature = "test-support"))]
#[test]
fn runtime_sctp_recv_msg_alloc_failure_precedes_buffer_pointer_callback() {
    let Some((mut stream, _raw)) = raw_sctp_stream_or_skip(
        "runtime_sctp_recv_msg_alloc_failure_precedes_buffer_pointer_callback",
    ) else {
        return;
    };
    let pointer_calls = Rc::new(Cell::new(0));
    let drops = Rc::new(Cell::new(0));
    let mut executor = Executor::new_with_config(ExecutorConfig {
        reactor: ReactorConfig { ring_entries: 1 },
        ..ExecutorConfig::default()
    })
    .expect("failed to construct one-slot executor");

    let test_pointer_calls = Rc::clone(&pointer_calls);
    let test_drops = Rc::clone(&drops);
    executor
        .run(async move {
            let buffer = PointerTrackedReadWrite::new(32, 68, &test_pointer_calls, &test_drops);
            test_hooks::fail_next_op_alloc();
            let (result, returned) = stream.recv_msg(buffer, 16).await;
            let err = result.expect_err("forced op allocation should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
            assert_eq!(returned.identity, 68);
            assert_eq!(test_pointer_calls.get(), 0);
            assert_eq!(test_drops.get(), 0);
            drop(returned);
            assert_eq!(test_drops.get(), 1);
        })
        .expect("SCTP op-allocation failure run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.sqe_submits, stats.close_ring_submissions);
        assert_eq!(stats.retained_pooled_allocs, 0);
        assert_eq!(stats.retained_pooled_frees, 0);
    }
    assert_eq!(pointer_calls.get(), 0);
    assert_eq!(drops.get(), 1);
}

#[cfg(any(debug_assertions, feature = "test-support"))]
#[test]
fn runtime_sctp_recv_msg_submit_failure_recycles_local_state_and_returns_buffer() {
    let Some((mut stream, _raw)) = raw_sctp_stream_or_skip(
        "runtime_sctp_recv_msg_submit_failure_recycles_local_state_and_returns_buffer",
    ) else {
        return;
    };
    let pointer_calls = Rc::new(Cell::new(0));
    let drops = Rc::new(Cell::new(0));
    let mut executor = Executor::new_with_config(ExecutorConfig {
        reactor: ReactorConfig { ring_entries: 1 },
        ..ExecutorConfig::default()
    })
    .expect("failed to construct one-slot executor");

    let test_pointer_calls = Rc::clone(&pointer_calls);
    let test_drops = Rc::clone(&drops);
    executor
        .run(async move {
            for identity in 1..=2 {
                let buffer =
                    PointerTrackedReadWrite::new(32, identity, &test_pointer_calls, &test_drops);
                test_hooks::fail_next_sqe_submit();
                let (result, returned) = stream.recv_msg(buffer, 16).await;
                let err = result.expect_err("forced SQE submission should fail");
                assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
                assert_eq!(returned.identity, identity);
                assert_eq!(test_pointer_calls.get(), identity);
                assert_eq!(test_drops.get(), identity - 1);
                drop(returned);
                assert_eq!(test_drops.get(), identity);
            }
        })
        .expect("SCTP submit-failure run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.sqe_submits, stats.close_ring_submissions);
        assert_eq!(stats.retained_pooled_allocs, 2);
        assert_eq!(stats.retained_pooled_frees, 2);
        assert_eq!(stats.retained_pooled_reuses, 1);
    }
    assert_eq!(pointer_calls.get(), 2);
    assert_eq!(drops.get(), 2);
}

#[cfg(any(debug_assertions, feature = "test-support"))]
#[test]
fn runtime_sctp_recv_msg_vectored_submit_failure_returns_exact_chain_and_reuses_state() {
    let Some((mut stream, _raw)) = raw_sctp_stream_or_skip(
        "runtime_sctp_recv_msg_vectored_submit_failure_returns_exact_chain_and_reuses_state",
    ) else {
        return;
    };
    let mut executor = Executor::new_with_config(ExecutorConfig {
        reactor: ReactorConfig { ring_entries: 1 },
        ..ExecutorConfig::default()
    })
    .expect("failed to construct one-slot executor");

    executor
        .run(async move {
            let mut pool = IoBuffPool::new(IoBuffPoolConfig {
                headroom: 0,
                payload: 32,
                tailroom: 0,
                objs_per_slab: 2,
            })
            .expect("pool config invalid");
            pool.init();

            for _ in 0..2 {
                let mut chain = IoBuffVecMut::<2>::new();
                chain
                    .push(pool.alloc().expect("first recv alloc failed"))
                    .unwrap();
                chain
                    .push(pool.alloc().expect("second recv alloc failed"))
                    .unwrap();
                let first_ptr = chain
                    .get_mut(0)
                    .expect("first recv segment missing")
                    .as_mut_ptr();
                let second_ptr = chain
                    .get_mut(1)
                    .expect("second recv segment missing")
                    .as_mut_ptr();

                test_hooks::fail_next_sqe_submit();
                let (result, mut returned) = stream.recv_msg_vectored(chain).await;
                let err = result.expect_err("forced SQE submission should fail");
                assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
                assert_eq!(pool.live_slots_for_test(), 2);
                assert_eq!(
                    returned
                        .get_mut(0)
                        .expect("returned first recv segment missing")
                        .as_mut_ptr(),
                    first_ptr
                );
                assert_eq!(
                    returned
                        .get_mut(1)
                        .expect("returned second recv segment missing")
                        .as_mut_ptr(),
                    second_ptr
                );
                drop(returned);
                assert_eq!(pool.live_slots_for_test(), 0);
            }
        })
        .expect("SCTP vectored submit-failure run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.sqe_submits, stats.close_ring_submissions);
        assert_eq!(stats.retained_pooled_allocs, 2);
        assert_eq!(stats.retained_pooled_frees, 2);
        assert_eq!(stats.retained_pooled_reuses, 1);
    }
}

#[cfg(any(debug_assertions, feature = "test-support"))]
#[test]
fn runtime_sctp_send_msg_submit_failure_returns_exact_buffer_and_reuses_state() {
    let Some((mut stream, _raw)) = raw_sctp_stream_or_skip(
        "runtime_sctp_send_msg_submit_failure_returns_exact_buffer_and_reuses_state",
    ) else {
        return;
    };
    let drops = Rc::new(Cell::new(0));
    let mut executor = Executor::new_with_config(ExecutorConfig {
        reactor: ReactorConfig { ring_entries: 1 },
        ..ExecutorConfig::default()
    })
    .expect("failed to construct one-slot executor");

    let test_drops = Rc::clone(&drops);
    executor
        .run(async move {
            for (index, len) in [8, 16].into_iter().enumerate() {
                let identity = index + 1;
                let bytes = vec![identity as u8; len];
                let expected_ptr = bytes.as_ptr();
                let buffer = DropTrackedReadOnly::new(bytes, &test_drops);

                test_hooks::fail_next_sqe_submit();
                let (result, returned) = stream.send_msg(buffer, SctpSendInfo::default()).await;
                let err = result.expect_err("forced SQE submission should fail");
                assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
                assert_eq!(returned.bytes().as_ptr(), expected_ptr);
                assert_eq!(returned.bytes().len(), len);
                assert!(
                    returned.bytes().iter().all(|byte| *byte == identity as u8),
                    "returned scalar send bytes changed during rollback"
                );
                assert_eq!(test_drops.get(), identity - 1);
                drop(returned);
                assert_eq!(test_drops.get(), identity);
            }
        })
        .expect("SCTP send_msg submit-failure run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.sqe_submits, stats.close_ring_submissions);
        assert_eq!(stats.retained_pooled_allocs, 2);
        assert_eq!(stats.retained_pooled_frees, 2);
        assert_eq!(stats.retained_pooled_reuses, 1);
    }
    assert_eq!(drops.get(), 2);
}

#[cfg(any(debug_assertions, feature = "test-support"))]
#[test]
fn runtime_sctp_send_msg_vectored_submit_failure_returns_exact_chain_and_reuses_state() {
    let Some((mut stream, _raw)) = raw_sctp_stream_or_skip(
        "runtime_sctp_send_msg_vectored_submit_failure_returns_exact_chain_and_reuses_state",
    ) else {
        return;
    };
    let mut executor = Executor::new_with_config(ExecutorConfig {
        reactor: ReactorConfig { ring_entries: 1 },
        ..ExecutorConfig::default()
    })
    .expect("failed to construct one-slot executor");

    executor
        .run(async move {
            let mut pool = IoBuffPool::new(IoBuffPoolConfig {
                headroom: 0,
                payload: 8,
                tailroom: 0,
                objs_per_slab: 2,
            })
            .expect("pool config invalid");
            pool.init();

            for _ in 0..2 {
                let chain = pooled_sctp_payload_chain(&mut pool);
                let first_ptr = chain.get(0).expect("first send segment missing").as_ptr();
                let second_ptr = chain.get(1).expect("second send segment missing").as_ptr();

                test_hooks::fail_next_sqe_submit();
                let (result, returned) = stream
                    .send_msg_vectored(chain, SctpSendInfo::default())
                    .await;
                let err = result.expect_err("forced SQE submission should fail");
                assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
                assert_eq!(pool.live_slots_for_test(), 2);
                assert_eq!(
                    returned
                        .get(0)
                        .expect("returned first send segment missing")
                        .as_ptr(),
                    first_ptr
                );
                assert_eq!(
                    returned
                        .get(1)
                        .expect("returned second send segment missing")
                        .as_ptr(),
                    second_ptr
                );
                drop(returned);
                assert_eq!(pool.live_slots_for_test(), 0);
            }
        })
        .expect("SCTP vectored send submit-failure run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.sqe_submits, stats.close_ring_submissions);
        assert_eq!(stats.retained_pooled_allocs, 2);
        assert_eq!(stats.retained_pooled_frees, 2);
        assert_eq!(stats.retained_pooled_reuses, 1);
    }
}

#[cfg(any(debug_assertions, feature = "test-support"))]
#[test]
fn runtime_sctp_recv_msg_pointer_panic_keeps_state_unpublished_and_reusable() {
    let Some((mut stream, _raw)) = raw_sctp_stream_or_skip(
        "runtime_sctp_recv_msg_pointer_panic_keeps_state_unpublished_and_reusable",
    ) else {
        return;
    };
    let pointer_calls = Rc::new(Cell::new(0));
    let drops = Rc::new(Cell::new(0));
    let mut executor = Executor::new_with_config(ExecutorConfig {
        reactor: ReactorConfig { ring_entries: 1 },
        ..ExecutorConfig::default()
    })
    .expect("failed to construct one-slot executor");

    let test_pointer_calls = Rc::clone(&pointer_calls);
    let test_drops = Rc::clone(&drops);
    executor
        .run(async move {
            let buffer =
                PointerTrackedReadWrite::panicking(32, 1, &test_pointer_calls, &test_drops);
            let mut recv = Box::pin(stream.recv_msg(buffer, 16));
            let panicked = std::future::poll_fn(|cx| {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    recv.as_mut().poll(cx)
                }));
                Poll::Ready(result.is_err())
            })
            .await;
            assert!(panicked, "buffer pointer callback should unwind");
            assert_eq!(test_pointer_calls.get(), 1);
            assert_eq!(test_drops.get(), 0, "future must still own its buffer");
            drop(recv);
            assert_eq!(test_drops.get(), 1, "future buffer dropped exactly once");

            // With a one-slot reactor, reaching the second callback proves the
            // panicking path freed its local state/waiter and did not publish a
            // cancellable operation into the first future.
            let buffer = PointerTrackedReadWrite::new(32, 2, &test_pointer_calls, &test_drops);
            test_hooks::fail_next_sqe_submit();
            let (result, returned) = stream.recv_msg(buffer, 16).await;
            let err = result.expect_err("forced SQE submission should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
            assert_eq!(returned.identity, 2);
            assert_eq!(test_pointer_calls.get(), 2);
            assert_eq!(test_drops.get(), 1);
            drop(returned);
            assert_eq!(test_drops.get(), 2);
        })
        .expect("SCTP pointer-panic cleanup run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.sqe_submits, stats.close_ring_submissions);
        assert_eq!(stats.retained_pooled_allocs, 2);
        assert_eq!(stats.retained_pooled_frees, 2);
        assert_eq!(stats.retained_pooled_reuses, 1);
    }
    assert_eq!(pointer_calls.get(), 2);
    assert_eq!(drops.get(), 2);
}

fn bound_non_listening_sctp_endpoint_or_skip(test_name: &str) -> Option<(OwnedFd, SocketAddr)> {
    let fd = raw_sctp_socket_or_skip(test_name, libc::AF_INET)?;
    let bind_addr = libc::sockaddr_in {
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: 0,
        sin_addr: libc::in_addr {
            s_addr: u32::from_ne_bytes(Ipv4Addr::LOCALHOST.octets()),
        },
        sin_zero: [0; 8],
    };

    let rc = unsafe {
        libc::bind(
            fd.as_raw_fd(),
            (&bind_addr as *const libc::sockaddr_in).cast::<libc::sockaddr>(),
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
        )
    };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        if capability_unavailable(&err) {
            eprintln!("skipping {test_name}: SCTP unsupported ({err})");
            return None;
        }
        panic!("failed to bind non-listening SCTP endpoint for {test_name}: {err}");
    }

    let mut bound_addr: libc::sockaddr_in = unsafe { std::mem::zeroed() };
    let mut bound_len = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockname(
            fd.as_raw_fd(),
            (&mut bound_addr as *mut libc::sockaddr_in).cast::<libc::sockaddr>(),
            &mut bound_len,
        )
    };
    assert_eq!(
        rc,
        0,
        "failed to read non-listening SCTP endpoint for {test_name}: {}",
        std::io::Error::last_os_error()
    );
    assert_eq!(
        bound_len as usize,
        std::mem::size_of::<libc::sockaddr_in>(),
        "unexpected non-listening SCTP endpoint length for {test_name}",
    );
    assert_eq!(
        bound_addr.sin_family,
        libc::AF_INET as libc::sa_family_t,
        "unexpected non-listening SCTP endpoint family for {test_name}",
    );

    let addr = SocketAddr::from((
        Ipv4Addr::from(bound_addr.sin_addr.s_addr.to_ne_bytes()),
        u16::from_be(bound_addr.sin_port),
    ));
    assert_eq!(
        addr.ip(),
        Ipv4Addr::LOCALHOST,
        "unexpected non-listening SCTP endpoint address for {test_name}",
    );
    assert_ne!(
        addr.port(),
        0,
        "kernel did not assign a non-listening SCTP endpoint port for {test_name}",
    );
    Some((fd, addr))
}

fn sctp_ipv6_bind_capability_unavailable(err: &std::io::Error) -> bool {
    capability_unavailable(err) || err.raw_os_error() == Some(libc::EADDRNOTAVAIL)
}

fn raw_sctp_ipv6_loopback_or_skip(test_name: &str) -> bool {
    let Some(fd) = raw_sctp_socket_or_skip(test_name, libc::AF_INET6) else {
        return false;
    };
    let addr = libc::sockaddr_in6 {
        sin6_family: libc::AF_INET6 as libc::sa_family_t,
        sin6_port: 0,
        sin6_flowinfo: 0,
        sin6_addr: libc::in6_addr {
            s6_addr: Ipv6Addr::LOCALHOST.octets(),
        },
        sin6_scope_id: 0,
    };
    let rc = unsafe {
        libc::bind(
            fd.as_raw_fd(),
            &addr as *const libc::sockaddr_in6 as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
        )
    };
    if rc == 0 {
        return true;
    }

    let err = std::io::Error::last_os_error();
    if sctp_ipv6_bind_capability_unavailable(&err) {
        eprintln!("skipping {test_name}: IPv6 SCTP loopback unavailable ({err})");
        return false;
    }
    panic!("failed to bind IPv6 SCTP capability socket for {test_name}: {err}");
}

fn socket_addr_matches_ip_and_port(actual: SocketAddr, expected: SocketAddr) -> bool {
    // SCTP enumeration may attach interface scope metadata that differs from
    // getpeername/getsockname. The deterministic conversion regression covers
    // exact flowinfo/scope preservation; live association identity is IP+port.
    actual.ip() == expected.ip() && actual.port() == expected.port()
}

fn assert_assoc_addr_contains(label: &str, addrs: &[SocketAddr], expected: SocketAddr) {
    assert!(
        addrs
            .iter()
            .copied()
            .any(|addr| socket_addr_matches_ip_and_port(addr, expected)),
        "{label} did not contain {expected}: {addrs:?}"
    );
}

fn assert_live_sctp_assoc_addrs(
    mut listener: SctpListener,
    client_bind_addr: SocketAddr,
    config: SctpSocketConfig,
) {
    const DEADLINE: Duration = Duration::from_secs(2);

    let listener_addr = listener.local_addr();
    let mut connector = SctpConnector::with_config(config).with_local_addr(client_bind_addr);
    let mut executor = Executor::new().expect("failed to construct SCTP address-test executor");

    executor
        .run(async move {
            let server = Executor::spawn(async move {
                timeout(DEADLINE, listener.accept())
                    .await
                    .expect("SCTP association-address accept timed out")
                    .expect("SCTP association-address accept failed")
            })
            .expect("SCTP association-address accept spawn failed");

            let client = connector
                .connect_timeout(listener_addr, DEADLINE)
                .expect("SCTP association-address connect init failed")
                .await
                .expect("SCTP association-address connect failed");
            let (server, accepted_remote_addr) = timeout(DEADLINE, server)
                .await
                .expect("SCTP association-address accept join timed out")
                .expect("SCTP association-address accept task was cancelled");

            let client_local_addr = client.local_addr().expect("client local_addr failed");
            let client_peer_addr = client.peer_addr();
            let server_local_addr = server.local_addr().expect("server local_addr failed");
            let server_peer_addr = server.peer_addr();

            assert!(socket_addr_matches_ip_and_port(
                client_peer_addr,
                listener_addr
            ));
            assert!(socket_addr_matches_ip_and_port(
                server_local_addr,
                listener_addr
            ));
            assert!(socket_addr_matches_ip_and_port(
                accepted_remote_addr,
                client_local_addr
            ));
            assert!(socket_addr_matches_ip_and_port(
                server_peer_addr,
                client_local_addr
            ));

            assert_assoc_addr_contains(
                "client local_addrs",
                &client.local_addrs().expect("client local_addrs failed"),
                client_local_addr,
            );
            assert_assoc_addr_contains(
                "client peer_addrs",
                &client.peer_addrs().expect("client peer_addrs failed"),
                client_peer_addr,
            );
            assert_assoc_addr_contains(
                "server local_addrs",
                &server.local_addrs().expect("server local_addrs failed"),
                server_local_addr,
            );
            assert_assoc_addr_contains(
                "server peer_addrs",
                &server.peer_addrs().expect("server peer_addrs failed"),
                server_peer_addr,
            );
        })
        .expect("SCTP association-address executor run failed");
}

#[test]
fn runtime_sctp_fresh_listener_drop_skips_linger_query() {
    let Some(listener) = bind_sctp_listener_or_skip(
        "runtime_sctp_fresh_listener_drop_skips_linger_query",
        SctpSocketConfig::data(SctpInitConfig::diameter_default()),
    ) else {
        return;
    };
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            drop(listener);
        })
        .expect("fresh SCTP listener close failed");
    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.close_linger_queries, 0);
        assert_eq!(stats.close_ring_submissions, 1);
        assert_eq!(stats.close_direct_closes, 0);
        assert_eq!(stats.close_worker_admissions, 0);
    }
}

#[test]
fn runtime_sctp_saved_public_fd_positive_linger_routes_to_worker() {
    let Some(listener) = bind_sctp_listener_or_skip(
        "runtime_sctp_saved_public_fd_positive_linger_routes_to_worker",
        SctpSocketConfig::data(SctpInitConfig::diameter_default()),
    ) else {
        return;
    };
    let saved_raw = listener.as_raw_fd();
    set_positive_linger(saved_raw);
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            drop(listener);
        })
        .expect("positive-linger SCTP listener close failed");
    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.close_linger_queries, 1);
        assert_eq!(stats.close_worker_admissions, 1);
        assert_eq!(stats.close_ring_submissions, 0);
        assert_eq!(stats.close_direct_closes, 0);
    }
    drop(executor);
}

#[test]
fn runtime_sctp_external_adoption_classifies_then_uses_ring_close() {
    if std::env::var_os(SCTP_EXTERNAL_ADOPTION_CLOSE_CHILD_ENV).is_none() {
        common::run_exact_test_child_with_watchdog(
            SCTP_EXTERNAL_ADOPTION_CLOSE_TEST,
            SCTP_EXTERNAL_ADOPTION_CLOSE_CHILD_ENV,
            Duration::from_secs(8),
        );
        return;
    }

    let Some((stream, raw)) =
        raw_sctp_stream_or_skip("runtime_sctp_external_adoption_classifies_then_uses_ring_close")
    else {
        return;
    };
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            drop(stream);
        })
        .expect("runtime-owned SCTP close run failed");
    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.close_linger_queries, 1);
        assert_eq!(stats.close_ring_submissions, 1);
        assert_eq!(stats.close_direct_closes, 0);
        assert_eq!(stats.close_worker_admissions, 0);
    }

    // SAFETY: F_GETFD accepts any integer descriptor; EBADF proves closure.
    let rc = unsafe { libc::fcntl(raw, libc::F_GETFD) };
    assert_eq!(rc, -1, "externally adopted SCTP fd was not closed");
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::EBADF)
    );
    drop(executor);
}

async fn accepted_sctp_pair(
    mut listener: SctpListener,
    mut connector: SctpConnector,
    addr: SocketAddr,
) -> (SctpStream, SctpStream) {
    let server =
        Executor::spawn(async move { listener.accept().await.expect("sctp accept failed").0 })
            .expect("sctp accept spawn failed");

    let client = connector
        .connect(addr)
        .expect("sctp connect init failed")
        .await
        .expect("sctp connect failed");
    let server = server.await.expect("SCTP accept task cancelled");

    (client, server)
}

async fn assert_repeated_lean_rejection(stream: &mut SctpStream, lineage: &str) {
    for attempt in 1..=2 {
        let lean = vec![0u8; 16];
        let lean_ptr = lean.as_ptr();
        let (lean_result, returned) = timeout(Duration::from_millis(100), stream.recv(lean, 16))
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "lean receive behind {lineage} attempt {attempt} did not reject promptly: {err}"
                )
            });
        assert_eq!(
            lean_result
                .expect_err("lean receive bypassed dropped rich receive")
                .kind(),
            std::io::ErrorKind::InvalidInput
        );
        assert_eq!(
            returned.as_ptr(),
            lean_ptr,
            "lean rejection returned a different buffer allocation"
        );
    }
}

fn established_sctp_pair(
    listener: SctpListener,
    config: SctpSocketConfig,
) -> (SctpStream, SctpStream) {
    let addr = listener.local_addr();
    let mut executor = Executor::new().expect("failed to construct setup executor");
    run_test_output(
        &mut executor,
        accepted_sctp_pair(listener, SctpConnector::with_config(config), addr),
    )
}

#[test]
fn runtime_sctp_initial_submissions_extract_poll_context_once() {
    const TEST_NAME: &str = "runtime_sctp_initial_submissions_extract_poll_context_once";
    let config = SctpSocketConfig::signaling(SctpInitConfig::diameter_default());
    let Some(data_listener) = bind_sctp_listener_or_skip(TEST_NAME, config) else {
        return;
    };
    let Some(metadata_listener) = bind_sctp_listener_or_skip(TEST_NAME, config) else {
        return;
    };
    let Some(vectored_listener) = bind_sctp_listener_or_skip(TEST_NAME, config) else {
        return;
    };

    let (mut data_client, mut data_server) = established_sctp_pair(data_listener, config);
    let (mut metadata_client, mut metadata_server) =
        established_sctp_pair(metadata_listener, config);
    let (mut vectored_client, mut vectored_server) =
        established_sctp_pair(vectored_listener, config);
    let Some(mut listener) = bind_sctp_listener_or_skip(TEST_NAME, config) else {
        return;
    };
    let addr = listener.local_addr();
    let mut connector = SctpConnector::with_config(config);
    let mut executor = Executor::new().expect("failed to construct executor");

    #[cfg(debug_assertions)]
    let mut poll_context_extractions = 0;

    executor
        .run(async move {
            poll_once_pending(data_client.send(b"d".to_vec())).await;
            poll_once_pending(data_server.recv(vec![0u8; 1], 1)).await;
        })
        .expect("SCTP data submission run failed");
    #[cfg(debug_assertions)]
    {
        poll_context_extractions += executor.last_stats().poll_context_extractions;
    }

    executor
        .run(async move {
            poll_once_pending(metadata_client.send_msg(b"m".to_vec(), SctpSendInfo::default()))
                .await;
            poll_once_pending(metadata_server.recv_msg(vec![0u8; 1], 1)).await;
        })
        .expect("SCTP metadata submission run failed");
    #[cfg(debug_assertions)]
    {
        poll_context_extractions += executor.last_stats().poll_context_extractions;
    }

    executor
        .run(async move {
            poll_once_pending(
                vectored_client
                    .send_msg_vectored(make_payload_chain([&b"v"[..]]), SctpSendInfo::default()),
            )
            .await;
            poll_once_pending(vectored_server.recv_msg_vectored(make_read_chain([1]))).await;
        })
        .expect("SCTP vectored submission run failed");
    #[cfg(debug_assertions)]
    {
        poll_context_extractions += executor.last_stats().poll_context_extractions;
    }

    executor
        .run(async move {
            poll_once_pending(listener.accept()).await;
            poll_once_pending(connector.connect(addr).expect("connect init failed")).await;
        })
        .expect("SCTP accept/connect submission run failed");
    #[cfg(debug_assertions)]
    {
        poll_context_extractions += executor.last_stats().poll_context_extractions;
    }

    #[cfg(debug_assertions)]
    assert_eq!(
        poll_context_extractions, 8,
        "each SCTP data, metadata, vectored, accept, and connect submission should derive the validated context once"
    );
}

#[test]
fn runtime_sctp_accept_inherits_known_listener_provenance() {
    let config = SctpSocketConfig::data(SctpInitConfig::diameter_default());
    let Some(listener) = bind_sctp_listener_or_skip(
        "runtime_sctp_accept_inherits_known_listener_provenance",
        config,
    ) else {
        return;
    };
    let addr = listener.local_addr();
    let connector = SctpConnector::with_config(config);
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            let (client, server) = accepted_sctp_pair(listener, connector, addr).await;
            drop(client);
            drop(server);
        })
        .expect("known SCTP accept close run failed");
    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.close_linger_queries, 0);
        assert_eq!(stats.close_ring_submissions, 3);
        assert_eq!(stats.close_worker_admissions, 0);
    }
}

#[test]
fn runtime_sctp_accept_inherits_exposed_positive_listener_provenance() {
    let config = SctpSocketConfig::data(SctpInitConfig::diameter_default());
    let Some(listener) = bind_sctp_listener_or_skip(
        "runtime_sctp_accept_inherits_exposed_positive_listener_provenance",
        config,
    ) else {
        return;
    };
    let addr = listener.local_addr();
    set_positive_linger(listener.as_raw_fd());
    let connector = SctpConnector::with_config(config);
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            let (client, server) = accepted_sctp_pair(listener, connector, addr).await;
            drop(client);
            drop(server);
        })
        .expect("uncertain SCTP accept close run failed");
    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.close_linger_queries, 2);
        assert_eq!(stats.close_worker_admissions, 2);
        assert_eq!(stats.close_ring_submissions, 1);
    }
    drop(executor);
}

#[test]
fn runtime_sctp_shutdown_fallback_abandons_unretired_readiness_state_with_watchdog() {
    if std::env::var_os(SCTP_SHUTDOWN_FALLBACK_CHILD_ENV).is_none() {
        common::run_exact_test_child_with_watchdog(
            SCTP_SHUTDOWN_FALLBACK_TEST,
            SCTP_SHUTDOWN_FALLBACK_CHILD_ENV,
            Duration::from_secs(15),
        );
        return;
    }

    let config = SctpSocketConfig::data(SctpInitConfig::diameter_default());
    let Some(mut listener) = bind_sctp_listener_or_skip(
        "runtime_sctp_shutdown_fallback_abandons_unretired_readiness_state_with_watchdog",
        config,
    ) else {
        return;
    };
    let addr = listener.local_addr();
    let listener_fd = listener.as_raw_fd();
    set_positive_linger(listener.as_raw_fd());
    let mut server_executor = Executor::new_with_config(ExecutorConfig {
        reactor: ReactorConfig { ring_entries: 1 },
        ..ExecutorConfig::default()
    })
    .expect("failed to construct one-slot server executor");
    let staged = Rc::new(RefCell::new(Some(Box::pin(async move {
        listener.accept().await
    }))));
    let staged_for_run = Rc::clone(&staged);

    let err = server_executor
        .run(async move {
            std::future::poll_fn(|cx| {
                let mut slot = staged_for_run.borrow_mut();
                let accept = slot.as_mut().expect("staged SCTP accept missing");
                match Future::poll(accept.as_mut(), cx) {
                    Poll::Pending => Poll::Ready(()),
                    Poll::Ready(_) => panic!("SCTP accept completed before shutdown peer"),
                }
            })
            .await;
            test_hooks::fail_next_ring_wait_errno(libc::EIO);
            std::future::pending::<()>().await;
        })
        .expect_err("injected wait failure should stop the SCTP executor");
    assert_eq!(err.raw_os_error(), Some(libc::EIO));

    let mut connector = SctpConnector::with_config(config);
    let mut client_executor = Executor::new().expect("failed to construct client executor");
    let client_slot = Rc::new(RefCell::new(None));
    let client_output = Rc::clone(&client_slot);
    client_executor
        .run(async move {
            let client = connector
                .connect(addr)
                .expect("SCTP connect init failed")
                .await
                .expect("SCTP shutdown peer connect failed");
            *client_output.borrow_mut() = Some(client);
        })
        .expect("SCTP client executor failed");
    let client = client_slot
        .borrow_mut()
        .take()
        .expect("SCTP client output missing");
    drop(client_executor);

    test_hooks::force_next_reactor_shutdown_fallback();
    drop(server_executor);
    assert_eq!(
        test_hooks::reactor_shutdown_fallbacks_remaining(),
        0,
        "forced SCTP shutdown fallback was not consumed"
    );
    let mut accept = staged
        .borrow_mut()
        .take()
        .expect("staged SCTP accept disappeared");
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(
        Future::poll(accept.as_mut(), &mut cx),
        Poll::Ready(Err(err)) if err.kind() == std::io::ErrorKind::NotConnected
    ));
    drop(accept);
    let flags = unsafe { libc::fcntl(listener_fd, libc::F_GETFD) };
    assert!(
        flags >= 0,
        "ring-abandoned SCTP readiness owner was released without its target CQE: {}",
        std::io::Error::last_os_error()
    );
    drop(client);
}

#[cfg(any(debug_assertions, feature = "test-support"))]
#[test]
fn runtime_sctp_abandoned_stash_remains_terminal_through_stream_drop_with_watchdog() {
    if std::env::var_os(SCTP_ABANDONED_STASH_CHILD_ENV).is_none() {
        common::run_exact_test_child_with_watchdog(
            SCTP_ABANDONED_STASH_TEST,
            SCTP_ABANDONED_STASH_CHILD_ENV,
            Duration::from_secs(15),
        );
        return;
    }

    let config = SctpSocketConfig::data(SctpInitConfig::diameter_default());
    let Some(listener) = bind_sctp_listener_or_skip(SCTP_ABANDONED_STASH_TEST, config) else {
        return;
    };
    let addr = listener.local_addr();
    let connector = SctpConnector::with_config(config);
    let stream_slot = Rc::new(RefCell::new(None));
    let client_slot = Rc::new(RefCell::new(None));
    let stream_output = Rc::clone(&stream_slot);
    let client_output = Rc::clone(&client_slot);
    let stashed_pointer_calls = Rc::new(Cell::new(0));
    let stashed_drops = Rc::new(Cell::new(0));
    let staged_pointer_calls = Rc::clone(&stashed_pointer_calls);
    let staged_drops = Rc::clone(&stashed_drops);
    let mut executor = Executor::new().expect("failed to construct SCTP stash executor");

    let err = executor
        .run(async move {
            let (client, mut server) = accepted_sctp_pair(listener, connector, addr).await;
            let buffer = PointerTrackedReadWrite::new(32, 1, &staged_pointer_calls, &staged_drops);
            let mut recv = Box::pin(server.recv_msg(buffer, 16));
            std::future::poll_fn(|cx| {
                assert!(
                    recv.as_mut().poll(cx).is_pending(),
                    "staged SCTP metadata receive completed before teardown"
                );
                Poll::Ready(())
            })
            .await;
            drop(recv);
            assert_eq!(staged_pointer_calls.get(), 1);
            assert_eq!(staged_drops.get(), 0);
            *stream_output.borrow_mut() = Some(server);
            *client_output.borrow_mut() = Some(client);
            test_hooks::fail_next_ring_wait_errno(libc::EIO);
            std::future::pending::<()>().await;
        })
        .expect_err("injected wait failure should stop the SCTP stash executor");
    assert_eq!(err.raw_os_error(), Some(libc::EIO));

    let mut server = stream_slot
        .borrow_mut()
        .take()
        .expect("stashed SCTP stream did not escape the failed run");
    let client = client_slot
        .borrow_mut()
        .take()
        .expect("SCTP stash peer did not escape the failed run");
    test_hooks::force_next_reactor_shutdown_fallback();
    drop(executor);
    assert_eq!(
        test_hooks::reactor_shutdown_fallbacks_remaining(),
        0,
        "forced SCTP stash shutdown fallback was not consumed"
    );
    assert_eq!(stashed_drops.get(), 0);
    assert_eq!(
        test_sctp_stream_stashed_recv_state(&server),
        SctpStashedRecvStateSnapshot::Live,
        "executor teardown must leave the dropped receive live until recovery observes abandonment"
    );

    let returned_pointer_calls = Rc::new(Cell::new(0));
    let returned_drops = Rc::new(Cell::new(0));
    let mut cx = Context::from_waker(Waker::noop());
    for attempt in 1..=2 {
        let buffer =
            PointerTrackedReadWrite::new(16, attempt + 1, &returned_pointer_calls, &returned_drops);
        let original_ptr = buffer.bytes.as_ptr();
        let mut recv = Box::pin(server.recv_msg(buffer, 16));
        let Poll::Ready((Err(err), returned)) = recv.as_mut().poll(&mut cx) else {
            panic!("post-abandonment rich receive attempt {attempt} remained pending");
        };
        assert_eq!(err.kind(), std::io::ErrorKind::NotConnected);
        assert_eq!(returned.identity, attempt + 1);
        assert_eq!(returned.bytes.as_ptr(), original_ptr);
        assert_eq!(
            returned_pointer_calls.get(),
            0,
            "terminal stash recovery exposed a new caller buffer"
        );
        assert_eq!(returned_drops.get(), attempt - 1);
        drop(recv);
        assert_eq!(
            test_sctp_stream_stashed_recv_state(&server),
            SctpStashedRecvStateSnapshot::Abandoned,
            "terminal rich receive did not publish the explicit abandoned marker"
        );
        drop(returned);
        assert_eq!(returned_drops.get(), attempt);
    }

    assert_eq!(
        test_sctp_stream_stashed_recv_state(&server),
        SctpStashedRecvStateSnapshot::Abandoned,
        "later terminal behavior did not retain the marker before stream teardown"
    );
    drop(server);
    assert_eq!(stashed_drops.get(), 0);
    drop(client);
}

#[test]
fn runtime_sctp_forgotten_accept_observes_late_listener_exposure() {
    let config = SctpSocketConfig::data(SctpInitConfig::diameter_default());
    let Some(mut listener) = bind_sctp_listener_or_skip(
        "runtime_sctp_forgotten_accept_observes_late_listener_exposure",
        config,
    ) else {
        return;
    };
    let addr = listener.local_addr();
    let mut connector = SctpConnector::with_config(config);
    let mut executor = Executor::new().expect("failed to construct executor");
    let client_slot = Rc::new(RefCell::new(None));
    let client_output = Rc::clone(&client_slot);

    executor
        .run(async move {
            let mut accept = Box::pin(listener.accept());
            std::future::poll_fn(|cx| match Future::poll(accept.as_mut(), cx) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(_) => panic!("SCTP accept completed before connect"),
            })
            .await;
            std::mem::forget(accept);

            set_positive_linger(listener.as_raw_fd());
            let client = connector
                .connect(addr)
                .expect("SCTP connect init failed")
                .await
                .expect("SCTP connect failed");
            sleep(Duration::from_millis(10))
                .await
                .expect("SCTP readiness completion wait failed");
            drop(listener);
            *client_output.borrow_mut() = Some(client);
        })
        .expect("late-exposure SCTP accept run failed");
    let client = client_slot
        .borrow_mut()
        .take()
        .expect("late-exposure SCTP client output missing");
    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.close_linger_queries, 1);
        assert_eq!(stats.close_worker_admissions, 1);
        assert_eq!(stats.close_ring_submissions, 0);
    }
    drop(executor);
    drop(client);
}

fn pooled_sctp_payload_chain(pool: &mut IoBuffPool) -> IoBuffVec<2> {
    let mut chain = IoBuffVecMut::<2>::new();

    let mut first = pool.alloc().expect("first pool send buffer alloc failed");
    first
        .payload_append(b"ab")
        .expect("first payload append failed");
    chain.push(first).expect("first chain push failed");

    let mut second = pool.alloc().expect("second pool send buffer alloc failed");
    second
        .payload_append(b"cd")
        .expect("second payload append failed");
    chain.push(second).expect("second chain push failed");

    chain.freeze()
}

fn pooled_sctp_recv_chain<const N: usize>(pool: &mut IoBuffPool) -> IoBuffVecMut<N> {
    let mut chain = IoBuffVecMut::<N>::new();
    for _ in 0..N {
        chain
            .push(pool.alloc().expect("pool recv buffer alloc failed"))
            .unwrap();
    }
    chain
}

fn assert_sctp_send_kernel_error(err: &std::io::Error) {
    assert!(
        matches!(
            err.raw_os_error(),
            Some(libc::EPIPE | libc::ECONNRESET | libc::ESHUTDOWN | libc::ENOTCONN)
        ) || matches!(
            err.kind(),
            std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::NotConnected
        ),
        "unexpected SCTP send kernel error: {err}"
    );
}

fn test_send_info(stream_id: u16, ppid: u32) -> SctpSendInfo {
    SctpSendInfo {
        stream_id,
        flags: 0,
        ppid,
        context: 0,
        assoc_id: 0,
    }
}

fn assert_data_rcvinfo(meta: SctpRecvMeta, stream_id: u16, ppid: u32) {
    match meta {
        SctpRecvMeta::Data(info) => {
            assert_eq!(info.stream_id, stream_id);
            assert_eq!(info.ppid, ppid);
            assert!(info.end_of_record);
        }
        SctpRecvMeta::Notification(notification) => {
            panic!("expected SCTP data metadata, got notification {notification:?}");
        }
    }
}

const LINUX_SCTP_COMM_UP: u16 = 0;
const LINUX_SCTP_COMM_LOST: u16 = 1;
const LINUX_SCTP_RESTART: u16 = 2;
const LINUX_SCTP_SHUTDOWN_COMP: u16 = 3;
const LINUX_SCTP_CANT_STR_ASSOC: u16 = 4;

/// Returns whether a Linux association-change state proves teardown.
fn sctp_assoc_change_is_terminal(state: u16) -> bool {
    matches!(
        state,
        LINUX_SCTP_COMM_LOST | LINUX_SCTP_SHUTDOWN_COMP | LINUX_SCTP_CANT_STR_ASSOC
    )
}

#[test]
fn sctp_peer_teardown_assoc_change_classifier_is_exact() {
    for (state, expected_terminal) in [
        (LINUX_SCTP_COMM_UP, false),
        (LINUX_SCTP_COMM_LOST, true),
        (LINUX_SCTP_RESTART, false),
        (LINUX_SCTP_SHUTDOWN_COMP, true),
        (LINUX_SCTP_CANT_STR_ASSOC, true),
        (u16::MAX, false),
    ] {
        assert_eq!(
            sctp_assoc_change_is_terminal(state),
            expected_terminal,
            "unexpected teardown classification for SCTP association state {state}"
        );
    }
}

#[test]
fn sctp_capability_policy_accepts_only_kernel_absence_and_permission_denial() {
    for errno in [
        libc::EPROTONOSUPPORT,
        libc::ESOCKTNOSUPPORT,
        libc::EAFNOSUPPORT,
        libc::EPFNOSUPPORT,
        libc::EPERM,
        libc::EACCES,
    ] {
        let err = std::io::Error::from_raw_os_error(errno);
        assert!(
            capability_unavailable(&err),
            "accepted SCTP capability errno {errno} was not classified unavailable"
        );
    }

    for errno in [libc::EINVAL, libc::ENOPROTOOPT, libc::EOPNOTSUPP, libc::EIO] {
        let err = std::io::Error::from_raw_os_error(errno);
        assert!(
            !capability_unavailable(&err),
            "SCTP capability errno {errno} should remain a failure"
        );
    }

    assert!(
        !capability_unavailable(&std::io::Error::other("probe failed without an errno")),
        "an SCTP capability failure without an errno should remain visible"
    );
}

#[test]
fn sctp_ipv6_bind_capability_policy_is_narrow() {
    for errno in [
        libc::EPROTONOSUPPORT,
        libc::ESOCKTNOSUPPORT,
        libc::EAFNOSUPPORT,
        libc::EPFNOSUPPORT,
        libc::EPERM,
        libc::EACCES,
        libc::EADDRNOTAVAIL,
    ] {
        let err = std::io::Error::from_raw_os_error(errno);
        assert!(
            sctp_ipv6_bind_capability_unavailable(&err),
            "accepted IPv6 SCTP bind errno {errno} was not classified unavailable"
        );
    }

    for errno in [libc::EINVAL, libc::ENOPROTOOPT, libc::EOPNOTSUPP, libc::EIO] {
        let err = std::io::Error::from_raw_os_error(errno);
        assert!(
            !sctp_ipv6_bind_capability_unavailable(&err),
            "IPv6 SCTP bind errno {errno} should remain a failure"
        );
    }

    assert!(
        !sctp_ipv6_bind_capability_unavailable(&std::io::Error::other(
            "probe failed without an errno"
        )),
        "an IPv6 SCTP bind failure without an errno should remain visible"
    );
}

#[test]
fn runtime_sctp_assoc_addrs_preserve_ipv4_addresses_and_ports() {
    const TEST_NAME: &str = "runtime_sctp_assoc_addrs_preserve_ipv4_addresses_and_ports";

    let Some(capability_socket) = raw_sctp_socket_or_skip(TEST_NAME, libc::AF_INET) else {
        return;
    };
    drop(capability_socket);

    let config = SctpSocketConfig::data(SctpInitConfig::diameter_default());
    let listener =
        SctpListener::bind_with_config(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128, config)
            .expect("FlowIO IPv4 SCTP bind failed after the raw capability probe succeeded");

    assert_live_sctp_assoc_addrs(listener, SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), config);
}

#[test]
fn runtime_sctp_assoc_addrs_preserve_ipv6_addresses_and_ports() {
    const TEST_NAME: &str = "runtime_sctp_assoc_addrs_preserve_ipv6_addresses_and_ports";

    if !raw_sctp_ipv6_loopback_or_skip(TEST_NAME) {
        return;
    }

    let config = SctpSocketConfig::data(SctpInitConfig::diameter_default());
    let listener =
        SctpListener::bind_with_config(SocketAddr::from((Ipv6Addr::LOCALHOST, 0)), 128, config)
            .expect("FlowIO IPv6 SCTP bind failed after the raw capability probe succeeded");

    assert_live_sctp_assoc_addrs(listener, SocketAddr::from((Ipv6Addr::LOCALHOST, 0)), config);
}

#[test]
fn sctp_reset_streams_constructors_preserve_direction_and_explicit_all_intent() {
    let streams = [0, u16::MAX, u16::MAX];
    let incoming = SctpResetStreams::incoming(&streams);
    let outgoing = SctpResetStreams::outgoing(&streams);
    let bidirectional = SctpResetStreams::bidirectional(&streams);

    assert_eq!(incoming.assoc_id, 0);
    assert_eq!(outgoing.assoc_id, 0);
    assert_eq!(bidirectional.assoc_id, 0);
    assert_eq!(incoming.streams, streams);
    assert_eq!(outgoing.streams, streams);
    assert_eq!(bidirectional.streams, streams);
    assert_ne!(incoming.flags, 0);
    assert_ne!(outgoing.flags, 0);
    assert_eq!(incoming.flags | outgoing.flags, bidirectional.flags);

    let all_incoming = SctpResetStreams::all_incoming();
    let all_outgoing = SctpResetStreams::all_outgoing();
    let all_bidirectional = SctpResetStreams::all_bidirectional();

    assert_eq!(all_incoming.assoc_id, 0);
    assert_eq!(all_outgoing.assoc_id, 0);
    assert_eq!(all_bidirectional.assoc_id, 0);
    assert!(all_incoming.streams.is_empty());
    assert!(all_outgoing.streams.is_empty());
    assert!(all_bidirectional.streams.is_empty());
    assert_eq!(all_incoming.flags, incoming.flags);
    assert_eq!(all_outgoing.flags, outgoing.flags);
    assert_eq!(all_bidirectional.flags, bidirectional.flags);

    // The public fields deliberately have the same Linux wire shape, while
    // the private constructor tag keeps explicit all-stream intent distinct.
    assert_ne!(all_incoming, SctpResetStreams::incoming(&[]));
    assert_ne!(all_outgoing, SctpResetStreams::outgoing(&[]));
    assert_ne!(all_bidirectional, SctpResetStreams::bidirectional(&[]));
}

#[test]
fn sctp_reset_streams_rejects_invalid_shapes_before_the_socket_option() {
    let (socket, _peer) =
        std::os::unix::net::UnixStream::pair().expect("Unix socket pair creation failed");
    socket
        .set_nonblocking(true)
        .expect("Unix test socket nonblocking setup failed");
    let stream =
        SctpStream::from_owned_fd(socket.into(), SocketAddr::from((Ipv4Addr::LOCALHOST, 3868)));

    let mut invalid_all = SctpResetStreams::all_incoming();
    invalid_all.streams.push(1);

    for request in [
        SctpResetStreams::incoming(&[]),
        SctpResetStreams::outgoing(&[]),
        SctpResetStreams::bidirectional(&[]),
        invalid_all,
    ] {
        let err = stream
            .reset_streams(&request)
            .expect_err("invalid reset shape should be rejected");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(err.raw_os_error(), None);
    }
}

/// Builds a zeroed SCTP notification buffer with sn_type/sn_flags/sn_length
/// header fields prefilled.
fn notification_buffer(notification_type: libc::c_int, flags: u16, len: usize) -> Vec<u8> {
    let mut buf = vec![0u8; len];
    buf.write_u16_at(0, notification_type as u16)
        .expect("test notification type write should fit");
    buf.write_u16_at(2, flags)
        .expect("test notification flags write should fit");
    buf.write_u32_at(4, len as u32)
        .expect("test notification length write should fit");
    buf
}

/// Appends an SCTP_RCVINFO fixture after any existing prefix.
fn append_rcvinfo_cmsg(control: &mut Vec<u8>, info: libc::sctp_rcvinfo) -> usize {
    let offset = control.len();
    let data_len = std::mem::size_of::<libc::sctp_rcvinfo>();
    let data_offset =
        append_initialized_test_cmsg(control, libc::IPPROTO_SCTP, libc::SCTP_RCVINFO, data_len);
    let cmsg_len = data_offset - offset + data_len;
    control.truncate(data_offset + data_len);
    unsafe {
        std::ptr::write_unaligned(
            control.as_mut_ptr().add(data_offset) as *mut libc::sctp_rcvinfo,
            info,
        );
    }

    cmsg_len
}

/// Builds raw 127.0.0.1:port sockaddr_storage for notification fixtures.
fn localhost_sockaddr_storage(port: u16) -> libc::sockaddr_storage {
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let addr = libc::sockaddr_in {
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: port.to_be(),
        sin_addr: libc::in_addr {
            s_addr: read_u32_at(&[127, 0, 0, 1], 0).expect("IPv4 octets should fit"),
        },
        sin_zero: [0; 8],
    };
    unsafe {
        std::ptr::write_unaligned(
            &mut storage as *mut libc::sockaddr_storage as *mut libc::sockaddr_in,
            addr,
        );
    }
    storage
}

/// Dropping a future with completed readiness must release its reusable slot
/// without treating the readiness mask as a descriptor.
#[test]
fn sctp_accept_slot_drop_future_preserves_unrelated_fd() {
    test_accept_slot_drop_future_preserves_unrelated_fd().unwrap();
}

/// Forgotten-future listener teardown has the same readiness-only ownership
/// guarantee.
#[test]
fn sctp_accept_slot_drop_cached_state_preserves_unrelated_fd() {
    test_accept_slot_drop_cached_state_preserves_unrelated_fd().unwrap();
}

/// Dropping an in-flight connect future closes the connecting socket fd and
/// resets the reusable slot.
#[test]
fn sctp_connect_slot_drop_future_closes_socket_fd() {
    test_connect_slot_drop_future_closes_socket_fd().unwrap();
}

/// Forgotten-future connector teardown closes the cached connect socket and
/// releases the reusable slot.
#[test]
fn sctp_connect_slot_drop_cached_state_closes_socket_fd() {
    test_connect_slot_drop_cached_state_closes_socket_fd().unwrap();
}

#[test]
fn sctp_peer_addr_params_rejects_exact_length_neighbors() {
    for optlen in [149, 150, 151, 153, 154, 155, 157] {
        test_peer_addr_params_rejects_optlen(optlen).unwrap();
    }
}

#[test]
fn parse_assoc_change_notification() {
    let mut buf = vec![0u8; 20];
    buf.write_u16_at(0, test_assoc_change_type() as u16)
        .expect("assoc change type write should fit");
    buf.write_u16_at(2, 0)
        .expect("assoc change flags write should fit");
    buf.write_u32_at(4, 20)
        .expect("assoc change length write should fit");
    buf.write_u16_at(8, 1)
        .expect("assoc state write should fit");
    buf.write_u16_at(10, 2)
        .expect("assoc error write should fit");
    buf.write_u16_at(12, 3)
        .expect("assoc outbound streams write should fit");
    buf.write_u16_at(14, 4)
        .expect("assoc inbound streams write should fit");
    buf.write_i32_at(16, 5).expect("assoc id write should fit");

    let parsed = test_parse_notification(&buf).expect("assoc change parse failed");
    assert_eq!(
        parsed,
        SctpRecvMeta::Notification(SctpNotification::AssocChange {
            state: 1,
            error: 2,
            outbound_streams: 3,
            inbound_streams: 4,
            assoc_id: 5,
        })
    );
}

#[test]
fn parse_adaptation_notification() {
    let mut buf = vec![0u8; 16];
    buf.write_u16_at(0, test_adaptation_indication_type() as u16)
        .expect("adaptation type write should fit");
    buf.write_u16_at(2, 0)
        .expect("adaptation flags write should fit");
    buf.write_u32_at(4, 16)
        .expect("adaptation length write should fit");
    buf.write_u32_at(8, 0x0102_0304)
        .expect("adaptation indication write should fit");
    buf.write_i32_at(12, 7)
        .expect("adaptation assoc id write should fit");

    let parsed = test_parse_notification(&buf).expect("adaptation parse failed");
    assert_eq!(
        parsed,
        SctpRecvMeta::Notification(SctpNotification::Adaptation {
            indication: 0x0102_0304,
            assoc_id: 7,
        })
    );
}

#[test]
fn parse_authentication_notification() {
    let mut buf = notification_buffer(test_authentication_event_type(), 0x1234, 20);
    buf.write_u16_at(8, 0x1122)
        .expect("authentication key number write should fit");
    buf.write_u16_at(10, 0x3344)
        .expect("authentication alternate key number write should fit");
    buf.write_u32_at(12, 0x5566_7788)
        .expect("authentication indication write should fit");
    buf.write_i32_at(16, 0x1020_3040)
        .expect("authentication association id write should fit");

    assert_eq!(
        test_parse_notification(&buf).expect("authentication notification should parse"),
        SctpRecvMeta::Notification(SctpNotification::Authentication {
            flags: 0x1234,
            key_number: 0x1122,
            alternate_key_number: 0x3344,
            indication: 0x5566_7788,
            assoc_id: 0x1020_3040,
        })
    );
}

/// Parses the legacy SCTP_SEND_FAILED layout that carries sctp_sndrcvinfo.
#[test]
fn parse_legacy_send_failed_notification() {
    let sndrcvinfo_len = std::mem::size_of::<libc::sctp_sndrcvinfo>();
    let error_offset = test_send_failed_error_offset();
    let info_offset = test_send_failed_info_offset();
    assert_eq!(error_offset, 8);
    assert_eq!(info_offset, 12);
    let mut buf = vec![0u8; info_offset + sndrcvinfo_len + 4];
    buf.write_u16_at(0, test_send_failed_type() as u16)
        .expect("legacy send failed type write should fit");
    buf.write_u16_at(2, 1)
        .expect("legacy send failed flags write should fit");
    buf.write_u32_at(4, (info_offset + sndrcvinfo_len + 4) as u32)
        .expect("legacy send failed length write should fit");
    buf.write_u32_at(error_offset, 9)
        .expect("legacy send failed error write should fit");

    let sndrcvinfo = libc::sctp_sndrcvinfo {
        sinfo_stream: 3,
        sinfo_ssn: 0,
        sinfo_flags: 4,
        sinfo_ppid: (0x0506_0708u32).to_be(),
        sinfo_context: 10,
        sinfo_timetolive: 0,
        sinfo_tsn: 0,
        sinfo_cumtsn: 0,
        sinfo_assoc_id: 11,
    };
    unsafe {
        std::ptr::write_unaligned(
            buf.as_mut_ptr().add(info_offset) as *mut libc::sctp_sndrcvinfo,
            sndrcvinfo,
        );
    }
    let assoc_base = info_offset + sndrcvinfo_len;
    buf.write_i32_at(assoc_base, 12)
        .expect("legacy send failed assoc id write should fit");

    let parsed = test_parse_notification(&buf).expect("legacy send failed parse failed");
    assert_eq!(
        parsed,
        SctpRecvMeta::Notification(SctpNotification::SendFailed {
            flags: 1,
            error: 9,
            info: SctpSendInfo {
                stream_id: 3,
                flags: 4,
                ppid: 0x0506_0708,
                context: 10,
                assoc_id: 11,
            },
            assoc_id: 12,
        })
    );
}

#[test]
fn parse_recv_meta_rejects_truncated_payload_and_control() {
    let payload_err = test_parse_recv_meta(&[], 0, libc::MSG_TRUNC, &[])
        .expect_err("MSG_TRUNC should reject truncated SCTP payloads");
    assert_eq!(payload_err.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        payload_err.to_string().contains("payload"),
        "payload truncation error should name payload truncation: {payload_err}"
    );

    let control_err = test_parse_recv_meta(&[], 0, libc::MSG_CTRUNC | libc::MSG_EOR, &[])
        .expect_err("MSG_CTRUNC should reject missing SCTP control data");
    assert_eq!(control_err.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(
        control_err.to_string(),
        "SCTP recvmsg fixed control buffer capacity was exhausted",
        "control truncation error should identify fixed-capacity exhaustion"
    );
}

#[test]
fn sctp_diagnostic_error_modes_cover_every_static_receive_diagnostic() {
    let short_header = vec![0u8; std::mem::size_of::<libc::cmsghdr>() - 1];
    let malformed_length = vec![0u8; std::mem::size_of::<libc::cmsghdr>()];
    let mut truncated_rcvinfo = Vec::new();
    append_initialized_test_cmsg(
        &mut truncated_rcvinfo,
        libc::IPPROTO_SCTP,
        libc::SCTP_RCVINFO,
        0,
    );

    let mut covered = 0_usize;
    let mut assert_modes = |control: &[u8],
                            controllen: usize,
                            msg_flags: libc::c_int,
                            data_slice: &[u8],
                            recv_rcvinfo_requested: bool,
                            expected_message: &str| {
        let production_error = test_parse_recv_meta_with_policy(
            control,
            controllen,
            msg_flags,
            data_slice,
            recv_rcvinfo_requested,
        )
        .expect_err("production receive classification should fail");
        assert_eq!(production_error.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(production_error.to_string(), expected_message);
        assert!(production_error.get_ref().is_some());

        let bare_error = test_parse_recv_meta_bare_with_policy(
            control,
            controllen,
            msg_flags,
            data_slice,
            recv_rcvinfo_requested,
        )
        .expect_err("bare receive classification should fail");
        assert_eq!(bare_error.kind(), std::io::ErrorKind::InvalidData);
        assert!(bare_error.get_ref().is_none());
        covered += 1;
    };

    assert_modes(
        &short_header,
        short_header.len(),
        libc::MSG_EOR,
        b"payload",
        true,
        "SCTP recvmsg control message header was malformed",
    );
    assert_modes(
        &malformed_length,
        malformed_length.len(),
        libc::MSG_EOR,
        b"payload",
        true,
        "SCTP recvmsg control message length was malformed",
    );
    assert_modes(
        &truncated_rcvinfo,
        truncated_rcvinfo.len(),
        libc::MSG_EOR,
        b"payload",
        true,
        "SCTP_RCVINFO control message was truncated",
    );
    assert_modes(
        &[],
        0,
        libc::MSG_TRUNC,
        b"payload",
        true,
        "SCTP recvmsg payload was truncated",
    );
    assert_modes(
        &[],
        0,
        0,
        b"payload",
        true,
        "SCTP recvmsg payload was partial before end-of-record",
    );
    assert_modes(
        &[],
        0,
        libc::MSG_EOR | libc::MSG_CTRUNC,
        b"payload",
        true,
        "SCTP recvmsg fixed control buffer capacity was exhausted",
    );
    assert_modes(
        &[],
        0,
        libc::MSG_EOR,
        b"payload",
        true,
        "SCTP recvmsg omitted requested SCTP_RCVINFO",
    );
    assert_eq!(covered, 7, "every static SCTP receive diagnostic is paired");

    let mut control = Vec::new();
    let controllen = append_rcvinfo_cmsg(
        &mut control,
        libc::sctp_rcvinfo {
            rcv_sid: 3,
            rcv_ssn: 4,
            rcv_flags: 5,
            rcv_ppid: 0x0607_0809_u32.to_be(),
            rcv_tsn: 10,
            rcv_cumtsn: 11,
            rcv_context: 12,
            rcv_assoc_id: 13,
        },
    );
    let production =
        test_parse_recv_meta_with_policy(&control, controllen, libc::MSG_EOR, b"payload", true)
            .expect("production success classification failed");
    let bare = test_parse_recv_meta_bare_with_policy(
        &control,
        controllen,
        libc::MSG_EOR,
        b"payload",
        true,
    )
    .expect("bare success classification failed");
    assert_eq!(production, bare);
}

#[test]
fn parse_recv_meta_accepts_data_without_rcvinfo_control() {
    let parsed = test_parse_recv_meta(&[], 0, libc::MSG_EOR, b"payload")
        .expect("data without SCTP_RCVINFO should parse");
    assert_eq!(
        parsed,
        SctpRecvMeta::Data(SctpRecvInfo {
            end_of_record: true,
            ..SctpRecvInfo::default()
        })
    );

    let malformed = [0u8; std::mem::size_of::<libc::cmsghdr>()];
    let err = test_parse_recv_meta(&malformed, malformed.len(), libc::MSG_EOR, b"payload")
        .expect_err("present malformed control data should still be rejected");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}

#[test]
fn parse_recv_meta_rejects_missing_eor_for_non_empty_messages() {
    let mut control = Vec::new();
    let controllen = append_rcvinfo_cmsg(
        &mut control,
        libc::sctp_rcvinfo {
            rcv_sid: 3,
            rcv_ssn: 4,
            rcv_flags: 5,
            rcv_ppid: (0x0607_0809u32).to_be(),
            rcv_tsn: 10,
            rcv_cumtsn: 11,
            rcv_context: 12,
            rcv_assoc_id: 13,
        },
    );

    let data_err = test_parse_recv_meta(&control, controllen, 0, b"ping")
        .expect_err("missing MSG_EOR should reject partial SCTP data");
    assert_eq!(data_err.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        data_err.to_string().contains("end-of-record"),
        "missing-EOR error should name record completeness: {data_err}"
    );

    let notification_err = test_parse_recv_meta(&[], 0, libc::MSG_NOTIFICATION, &[1, 2, 3, 4])
        .expect_err("missing MSG_EOR should reject partial SCTP notification tail");
    assert_eq!(notification_err.kind(), std::io::ErrorKind::InvalidData);
    assert!(
        notification_err.to_string().contains("end-of-record"),
        "missing-EOR notification error should name record completeness: {notification_err}"
    );
}

#[test]
fn parse_recv_meta_accepts_unaligned_rcvinfo_cmsg() {
    let mut storage = vec![0u8];
    let info = libc::sctp_rcvinfo {
        rcv_sid: 3,
        rcv_ssn: 4,
        rcv_flags: 5,
        rcv_ppid: (0x0607_0809u32).to_be(),
        rcv_tsn: 10,
        rcv_cumtsn: 11,
        rcv_context: 12,
        rcv_assoc_id: 13,
    };
    let controllen = append_rcvinfo_cmsg(&mut storage, info);
    let control = &storage[1..];

    let parsed = test_parse_recv_meta(control, controllen, libc::MSG_EOR, b"ping")
        .expect("rcvinfo parse failed");
    assert_eq!(
        parsed,
        SctpRecvMeta::Data(flowio::net::sctp::SctpRecvInfo {
            stream_id: 3,
            ssn: 4,
            flags: 5,
            ppid: 0x0607_0809,
            tsn: 10,
            cumtsn: 11,
            context: 12,
            assoc_id: 13,
            end_of_record: true,
        })
    );
}

/// Parses the newer SCTP_SEND_FAILED_EVENT layout; it must produce the same
/// SendFailed result shape as the legacy layout above.
#[test]
fn parse_send_failed_event_notification() {
    let sndinfo_len = std::mem::size_of::<libc::sctp_sndinfo>();
    let error_offset = test_send_failed_error_offset();
    let info_offset = test_send_failed_info_offset();
    assert_eq!(error_offset, 8);
    assert_eq!(info_offset, 12);
    let mut buf = vec![0u8; info_offset + sndinfo_len + 4];
    buf.write_u16_at(0, test_send_failed_event_type() as u16)
        .expect("send failed type write should fit");
    buf.write_u16_at(2, 0x7a5c)
        .expect("send failed flags write should fit");
    buf.write_u32_at(4, (info_offset + sndinfo_len + 4) as u32)
        .expect("send failed length write should fit");
    buf.write_u32_at(error_offset, 9)
        .expect("send failed error write should fit");

    let sndinfo = libc::sctp_sndinfo {
        snd_sid: 3,
        snd_flags: 4,
        snd_ppid: (0x0506_0708u32).to_be(),
        snd_context: 10,
        snd_assoc_id: 11,
    };
    unsafe {
        std::ptr::write_unaligned(
            buf.as_mut_ptr().add(info_offset) as *mut libc::sctp_sndinfo,
            sndinfo,
        );
    }
    let assoc_base = info_offset + sndinfo_len;
    buf.write_i32_at(assoc_base, 12)
        .expect("send failed assoc id write should fit");

    let parsed = test_parse_notification(&buf).expect("send failed parse failed");
    assert_eq!(
        parsed,
        SctpRecvMeta::Notification(SctpNotification::SendFailed {
            flags: 0x7a5c,
            error: 9,
            info: SctpSendInfo {
                stream_id: 3,
                flags: 4,
                ppid: 0x0506_0708,
                context: 10,
                assoc_id: 11,
            },
            assoc_id: 12,
        })
    );
}

#[test]
fn parse_peer_addr_change_notification() {
    use std::net::{Ipv4Addr, SocketAddr};

    let storage_len = std::mem::size_of::<libc::sockaddr_storage>();
    let mut buf = notification_buffer(test_peer_addr_change_type(), 0, 8 + storage_len + 12);
    let storage = localhost_sockaddr_storage(3868);
    unsafe {
        std::ptr::copy_nonoverlapping(
            &storage as *const libc::sockaddr_storage as *const u8,
            buf.as_mut_ptr().add(8),
            storage_len,
        );
    }
    let base = 8 + storage_len;
    buf.write_i32_at(base, SctpPeerAddrInfo::ACTIVE)
        .expect("peer addr state write should fit");
    buf.write_i32_at(base + 4, 9)
        .expect("peer addr error write should fit");
    buf.write_i32_at(base + 8, 10)
        .expect("peer addr assoc id write should fit");

    let parsed = test_parse_notification(&buf).expect("peer addr change parse failed");
    assert_eq!(
        parsed,
        SctpRecvMeta::Notification(SctpNotification::PeerAddrChange {
            addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 3868)),
            state: SctpPeerAddrInfo::ACTIVE,
            error: 9,
            assoc_id: 10,
        })
    );
}

#[test]
fn parse_remote_error_and_shutdown_notifications() {
    let mut too_short = notification_buffer(test_remote_error_type(), 0, 14);
    too_short
        .write_u16_be_at(8, 0x1122)
        .expect("remote error code write should fit");
    assert!(
        test_parse_notification(&too_short).is_err(),
        "14-byte remote error should be rejected"
    );

    let mut remote_error = notification_buffer(test_remote_error_type(), 0, 16);
    remote_error
        .write_u16_be_at(8, 0x1122)
        .expect("remote error code write should fit");
    remote_error
        .write_i32_at(12, 12)
        .expect("remote error assoc id write should fit");
    let parsed = test_parse_notification(&remote_error).expect("remote error parse failed");
    assert_eq!(
        parsed,
        SctpRecvMeta::Notification(SctpNotification::RemoteError {
            error: 0x1122,
            assoc_id: 12,
        })
    );

    let mut shutdown = notification_buffer(test_shutdown_event_type(), 0, 12);
    shutdown
        .write_i32_at(8, 13)
        .expect("shutdown assoc id write should fit");
    let parsed = test_parse_notification(&shutdown).expect("shutdown parse failed");
    assert_eq!(
        parsed,
        SctpRecvMeta::Notification(SctpNotification::Shutdown { assoc_id: 13 })
    );

    let mut sender_dry = notification_buffer(test_sender_dry_event_type(), 0, 12);
    sender_dry
        .write_i32_at(8, 14)
        .expect("sender dry assoc id write should fit");
    let parsed = test_parse_notification(&sender_dry).expect("sender dry parse failed");
    assert_eq!(
        parsed,
        SctpRecvMeta::Notification(SctpNotification::SenderDry { assoc_id: 14 })
    );
}

#[test]
fn parse_partial_delivery_and_reset_notifications() {
    let mut partial_delivery = notification_buffer(test_partial_delivery_event_type(), 0, 24);
    partial_delivery
        .write_u32_at(8, 7)
        .expect("partial delivery indication write should fit");
    partial_delivery
        .write_i32_at(12, 15)
        .expect("partial delivery assoc id write should fit");
    partial_delivery
        .write_u32_at(16, 16)
        .expect("partial delivery stream write should fit");
    partial_delivery
        .write_u32_at(20, 17)
        .expect("partial delivery sequence write should fit");
    let parsed = test_parse_notification(&partial_delivery).expect("partial delivery parse failed");
    assert_eq!(
        parsed,
        SctpRecvMeta::Notification(SctpNotification::PartialDelivery {
            indication: 7,
            assoc_id: 15,
            stream: 16,
            sequence: 17,
        })
    );

    let mut stream_reset = notification_buffer(test_stream_reset_event_type(), 0x0123, 12);
    stream_reset
        .write_i32_at(8, 18)
        .expect("stream reset assoc id write should fit");
    let parsed = test_parse_notification(&stream_reset).expect("stream reset parse failed");
    assert_eq!(
        parsed,
        SctpRecvMeta::Notification(SctpNotification::StreamReset {
            flags: 0x0123,
            assoc_id: 18,
        })
    );

    let mut assoc_reset = notification_buffer(test_assoc_reset_event_type(), 0x0456, 20);
    assoc_reset
        .write_i32_at(8, 19)
        .expect("assoc reset assoc id write should fit");
    assoc_reset
        .write_u32_at(12, 20)
        .expect("assoc reset local tsn write should fit");
    assoc_reset
        .write_u32_at(16, 21)
        .expect("assoc reset remote tsn write should fit");
    let parsed = test_parse_notification(&assoc_reset).expect("assoc reset parse failed");
    assert_eq!(
        parsed,
        SctpRecvMeta::Notification(SctpNotification::AssocReset {
            flags: 0x0456,
            assoc_id: 19,
            local_tsn: 20,
            remote_tsn: 21,
        })
    );

    let mut stream_change = notification_buffer(test_stream_change_event_type(), 0x0789, 16);
    stream_change
        .write_i32_at(8, 22)
        .expect("stream change assoc id write should fit");
    stream_change
        .write_u16_at(12, 23)
        .expect("stream change inbound write should fit");
    stream_change
        .write_u16_at(14, 24)
        .expect("stream change outbound write should fit");
    let parsed = test_parse_notification(&stream_change).expect("stream change parse failed");
    assert_eq!(
        parsed,
        SctpRecvMeta::Notification(SctpNotification::StreamChange {
            flags: 0x0789,
            assoc_id: 22,
            inbound_streams: 23,
            outbound_streams: 24,
        })
    );
}

#[test]
fn sctp_nested_notification_recovery_retains_only_the_bounded_prefix() {
    let owned: OwnedFd = std::fs::File::open("/dev/null")
        .expect("test descriptor open failed")
        .into();
    let peer = SocketAddr::from((Ipv4Addr::LOCALHOST, 3868));
    let mut stream = SctpStream::from_owned_fd(owned, peer);
    let abort = notification_buffer(test_partial_delivery_event_type(), 0, 24);

    test_sctp_stream_begin_data_tail(&mut stream);
    assert_eq!(
        test_sctp_stream_apply_unpublished_completion(
            &mut stream,
            &abort[..7],
            libc::MSG_NOTIFICATION,
        ),
        SctpRecordRecoverySnapshot::DataNotificationTail {
            prefix_len: 7,
            classified: false,
        }
    );
    assert_eq!(
        test_sctp_stream_apply_unpublished_completion(
            &mut stream,
            &abort[7..],
            libc::MSG_NOTIFICATION,
        ),
        SctpRecordRecoverySnapshot::Synced,
        "a split complete PDAPI abort did not retire the underlying data tail"
    );

    test_sctp_stream_begin_data_tail(&mut stream);
    assert_eq!(
        test_sctp_stream_apply_unpublished_completion(
            &mut stream,
            &abort[..7],
            libc::MSG_NOTIFICATION,
        ),
        SctpRecordRecoverySnapshot::DataNotificationTail {
            prefix_len: 7,
            classified: false,
        }
    );
    assert_eq!(
        test_sctp_stream_apply_unpublished_completion(
            &mut stream,
            &abort[7..],
            libc::MSG_NOTIFICATION | libc::MSG_EOR,
        ),
        SctpRecordRecoverySnapshot::Synced,
        "a reachable terminal split PDAPI abort did not retire the data tail"
    );

    let stream_reset = notification_buffer(test_stream_reset_event_type(), 0, 44);
    test_sctp_stream_begin_data_tail(&mut stream);
    assert_eq!(
        test_sctp_stream_apply_unpublished_completion(
            &mut stream,
            &stream_reset[..8],
            libc::MSG_NOTIFICATION,
        ),
        SctpRecordRecoverySnapshot::DataNotificationTail {
            prefix_len: 8,
            classified: false,
        }
    );
    assert_eq!(
        test_sctp_stream_apply_unpublished_completion(&mut stream, &abort, libc::MSG_NOTIFICATION,),
        SctpRecordRecoverySnapshot::DataNotificationTail {
            prefix_len: 24,
            classified: true,
        },
        "header-shaped continuation bytes were reparsed as a fresh abort"
    );
    assert_eq!(
        test_sctp_stream_apply_unpublished_completion(
            &mut stream,
            &[0],
            libc::MSG_NOTIFICATION | libc::MSG_EOR,
        ),
        SctpRecordRecoverySnapshot::DataTail,
        "notification EOR retired the underlying data tail"
    );

    test_sctp_stream_begin_data_tail(&mut stream);
    assert_eq!(
        test_sctp_stream_apply_unpublished_completion(
            &mut stream,
            &stream_reset[..8],
            libc::MSG_NOTIFICATION,
        ),
        SctpRecordRecoverySnapshot::DataNotificationTail {
            prefix_len: 8,
            classified: false,
        }
    );
    assert_eq!(
        test_sctp_stream_apply_unpublished_completion(&mut stream, &abort, 0),
        SctpRecordRecoverySnapshot::DataNotificationTail {
            prefix_len: 8,
            classified: false,
        },
        "PDAPI-shaped peer data was appended to the notification classifier"
    );
    assert_eq!(
        test_sctp_stream_apply_unpublished_completion(&mut stream, &abort, libc::MSG_EOR),
        SctpRecordRecoverySnapshot::NotificationTail,
        "terminal peer data did not leave the unfinished notification fail-closed"
    );
}

#[test]
fn parse_unknown_notification_falls_back_to_other() {
    let parsed =
        test_parse_notification(&notification_buffer(0x800e, 0x0007, 8)).expect("other parse");
    assert_eq!(
        parsed,
        SctpRecvMeta::Notification(SctpNotification::Other {
            kind: 0x800e,
            flags: 0x0007,
            length: 8,
        })
    );
}

#[test]
fn known_notification_fields_never_extend_past_declared_length() {
    let storage_len = std::mem::size_of::<libc::sockaddr_storage>();
    let mut peer_addr = notification_buffer(test_peer_addr_change_type(), 0, 8 + storage_len + 12);
    let storage = localhost_sockaddr_storage(3868);
    unsafe {
        std::ptr::copy_nonoverlapping(
            &storage as *const libc::sockaddr_storage as *const u8,
            peer_addr.as_mut_ptr().add(8),
            storage_len,
        );
    }

    let info_offset = test_send_failed_info_offset();
    let cases = [
        (
            "assoc change",
            notification_buffer(test_assoc_change_type(), 0, 20),
        ),
        ("peer address change", peer_addr),
        (
            "legacy send failed",
            notification_buffer(
                test_send_failed_type(),
                0,
                info_offset + std::mem::size_of::<libc::sctp_sndrcvinfo>() + 4,
            ),
        ),
        (
            "remote error",
            notification_buffer(test_remote_error_type(), 0, 16),
        ),
        (
            "shutdown",
            notification_buffer(test_shutdown_event_type(), 0, 12),
        ),
        (
            "adaptation",
            notification_buffer(test_adaptation_indication_type(), 0, 16),
        ),
        (
            "authentication",
            notification_buffer(test_authentication_event_type(), 0, 20),
        ),
        (
            "partial delivery",
            notification_buffer(test_partial_delivery_event_type(), 0, 24),
        ),
        (
            "sender dry",
            notification_buffer(test_sender_dry_event_type(), 0, 12),
        ),
        (
            "stream reset",
            notification_buffer(test_stream_reset_event_type(), 0, 12),
        ),
        (
            "association reset",
            notification_buffer(test_assoc_reset_event_type(), 0, 20),
        ),
        (
            "stream change",
            notification_buffer(test_stream_change_event_type(), 0, 16),
        ),
        (
            "send failed event",
            notification_buffer(
                test_send_failed_event_type(),
                0,
                info_offset + std::mem::size_of::<libc::sctp_sndinfo>() + 4,
            ),
        ),
    ];

    for (name, mut buffer) in cases {
        test_parse_notification(&buffer)
            .unwrap_or_else(|err| panic!("{name} minimum fixture failed: {err}"));
        let declared_len = buffer.len() - 1;
        buffer
            .write_u32_at(4, declared_len as u32)
            .expect("declared notification length write should fit");
        let err = match test_parse_notification(&buffer) {
            Ok(parsed) => panic!("{name} borrowed fields beyond sn_length: {parsed:?}"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "{name}");
    }

    let mut unknown = notification_buffer(0x800e, 0x0007, 12);
    unknown
        .write_u32_at(4, 8)
        .expect("unknown declared length write should fit");
    assert_eq!(
        test_parse_notification(&unknown).expect("unknown notification should remain extensible"),
        SctpRecvMeta::Notification(SctpNotification::Other {
            kind: 0x800e,
            flags: 0x0007,
            length: 8,
        })
    );
}

#[test]
fn notification_helpers() {
    let notification = SctpNotification::Shutdown { assoc_id: 7 };
    assert_eq!(notification.kind(), SctpNotificationKind::Shutdown);
    let authentication = SctpNotification::Authentication {
        flags: 1,
        key_number: 2,
        alternate_key_number: 3,
        indication: 4,
        assoc_id: 5,
    };
    assert_eq!(authentication.kind(), SctpNotificationKind::Authentication);

    let recv_data = SctpRecvMeta::Data(Default::default());
    assert!(recv_data.is_data());
    assert!(!recv_data.is_notification());
    assert!(recv_data.data().is_some());
    assert!(recv_data.notification().is_none());
    assert!(recv_data.into_data().is_some());

    let recv_notification = SctpRecvMeta::Notification(notification);
    assert!(!recv_notification.is_data());
    assert!(recv_notification.is_notification());
    assert!(recv_notification.data().is_none());
    assert_eq!(
        recv_notification.notification().map(|value| value.kind()),
        Some(SctpNotificationKind::Shutdown)
    );
    assert_eq!(
        recv_notification
            .into_notification()
            .map(|value| value.kind()),
        Some(SctpNotificationKind::Shutdown)
    );
}

#[test]
fn notification_mask_all_and_none_round_trip() {
    let all = SctpNotificationMask::all();
    assert!(all.association);
    assert!(all.address);
    assert!(all.send_failure);
    assert!(all.peer_error);
    assert!(all.shutdown);
    assert!(all.partial_delivery);
    assert!(all.adaptation);
    assert!(all.authentication);
    assert!(all.sender_dry);
    assert!(all.stream_reset);
    assert!(all.assoc_reset);
    assert!(all.stream_change);

    let none = SctpNotificationMask::none();
    assert!(!none.association);
    assert!(!none.address);
    assert!(!none.send_failure);
    assert!(!none.peer_error);
    assert!(!none.shutdown);
    assert!(!none.partial_delivery);
    assert!(!none.adaptation);
    assert!(!none.authentication);
    assert!(!none.sender_dry);
    assert!(!none.stream_reset);
    assert!(!none.assoc_reset);
    assert!(!none.stream_change);
}

#[test]
fn notification_mask_defaults() {
    assert_eq!(
        SctpNotificationMask::default(),
        SctpNotificationMask::signaling_default()
    );
    assert!(!SctpNotificationMask::none().association);
    assert!(SctpNotificationMask::all().authentication);
}

#[test]
fn runtime_sctp_adopted_stream_refreshes_receive_info_policy() {
    const TEST_NAME: &str = "runtime_sctp_adopted_stream_refreshes_receive_info_policy";
    let Some(fd) = raw_sctp_socket_or_skip(TEST_NAME, libc::AF_INET) else {
        return;
    };

    let mut enabled = SctpSocketConfig::data(SctpInitConfig::default());
    enabled.recv_rcvinfo = true;
    test_apply_sctp_socket_options(fd.as_raw_fd(), enabled)
        .expect("failed to enable SCTP receive-info on adopted socket");

    let stream = SctpStream::from_owned_fd(fd, SocketAddr::from((Ipv4Addr::LOCALHOST, 0)));
    let expected_default = SctpRecvMeta::Data(SctpRecvInfo {
        end_of_record: true,
        ..SctpRecvInfo::default()
    });
    assert_eq!(
        test_parse_stream_recv_meta(&stream, &[], 0, libc::MSG_EOR, b"payload")
            .expect("adoption should initially retain the no-query receive policy"),
        expected_default
    );

    stream
        .set_notification_mask(SctpNotificationMask::none())
        .expect("failed to refresh enabled receive-info policy");
    let forced_pdapi = SctpNotificationMask {
        partial_delivery: true,
        ..SctpNotificationMask::none()
    };
    assert_sctp_receive_options(
        stream.as_raw_fd(),
        forced_pdapi,
        true,
        "enabled adopted stream",
    );
    let missing = test_parse_stream_recv_meta(&stream, &[], 0, libc::MSG_EOR, b"payload")
        .expect_err("refreshed enabled receive-info policy must reject absent metadata");
    assert_eq!(missing.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(
        missing.to_string(),
        "SCTP recvmsg omitted requested SCTP_RCVINFO"
    );

    let disabled = SctpSocketConfig::data(SctpInitConfig::default());
    test_apply_sctp_socket_options(stream.as_raw_fd(), disabled)
        .expect("failed to disable SCTP receive-info on adopted socket");
    test_parse_stream_recv_meta(&stream, &[], 0, libc::MSG_EOR, b"payload")
        .expect_err("external option mutation must not bypass an explicit policy refresh");

    stream
        .set_notification_mask(SctpNotificationMask::none())
        .expect("failed to refresh disabled receive-info policy");
    assert_sctp_receive_options(
        stream.as_raw_fd(),
        SctpNotificationMask::none(),
        false,
        "disabled adopted stream",
    );
    assert_eq!(
        test_parse_stream_recv_meta(&stream, &[], 0, libc::MSG_EOR, b"payload")
            .expect("refreshed disabled receive-info policy should default absent metadata"),
        expected_default
    );
}

#[test]
fn runtime_sctp_notification_mask_failure_retains_kernel_and_stream_policy() {
    const TEST_NAME: &str =
        "runtime_sctp_notification_mask_failure_retains_kernel_and_stream_policy";
    let Some(fd) = raw_sctp_socket_or_skip(TEST_NAME, libc::AF_INET) else {
        return;
    };

    let config = SctpSocketConfig::rich(SctpInitConfig::default());
    test_apply_sctp_socket_options(fd.as_raw_fd(), config)
        .expect("failed to establish the adopted socket's initial receive options");
    let initial_kernel = test_sctp_socket_receive_options(fd.as_raw_fd())
        .expect("failed to read the adopted socket's initial receive options");

    let stream = SctpStream::from_owned_fd(fd, SocketAddr::from((Ipv4Addr::LOCALHOST, 0)));
    assert_eq!(
        test_sctp_stream_receive_policy(&stream),
        (false, true, true)
    );

    let requested = SctpNotificationMask::none();
    let forced_pdapi = SctpNotificationMask {
        partial_delivery: true,
        ..SctpNotificationMask::none()
    };
    let (result, observed) = test_fail_notification_mask_after_query(&stream, requested, libc::EIO);
    let err = result.expect_err("request-scoped notification-mask failure unexpectedly succeeded");
    assert_eq!(err.raw_os_error(), Some(libc::EIO));
    assert_eq!(observed, Some(forced_pdapi));
    assert_eq!(
        test_sctp_socket_receive_options(stream.as_raw_fd())
            .expect("failed to read receive options after the injected failure"),
        initial_kernel,
        "failed notification-mask update changed kernel state"
    );
    assert_eq!(
        test_sctp_stream_receive_policy(&stream),
        (false, true, true),
        "failed notification-mask update changed stored receive policy"
    );

    stream
        .set_notification_mask(requested)
        .expect("notification-mask retry failed");
    assert_sctp_receive_options(stream.as_raw_fd(), forced_pdapi, true, "retried stream");
    assert_eq!(
        test_sctp_stream_receive_policy(&stream),
        (true, false, false)
    );
}

fn assert_sctp_receive_options(
    fd: std::os::fd::RawFd,
    expected_mask: SctpNotificationMask,
    expected_rcvinfo: bool,
    label: &str,
) {
    let (mask, recv_rcvinfo) = test_sctp_socket_receive_options(fd)
        .unwrap_or_else(|err| panic!("failed to read {label} SCTP receive options: {err}"));
    assert_eq!(mask, expected_mask, "unexpected {label} event mask");
    assert_eq!(
        recv_rcvinfo, expected_rcvinfo,
        "unexpected {label} SCTP_RECVRCVINFO state"
    );
}

fn assert_inherited_sctp_socket_options(
    actual: SctpSocketOptionSnapshot,
    listener: SctpSocketOptionSnapshot,
    label: &str,
) {
    assert_eq!(
        actual.notifications, listener.notifications,
        "unexpected {label} SCTP_EVENTS state"
    );
    assert_eq!(
        actual.recv_rcvinfo, listener.recv_rcvinfo,
        "unexpected {label} SCTP_RECVRCVINFO state"
    );
    assert_eq!(
        actual.nodelay, listener.nodelay,
        "unexpected {label} SCTP_NODELAY state"
    );
    assert_eq!(
        actual.send_buffer_size, listener.send_buffer_size,
        "unexpected {label} SO_SNDBUF value"
    );
    assert_eq!(
        actual.recv_buffer_size, listener.recv_buffer_size,
        "unexpected {label} SO_RCVBUF value"
    );
    assert_eq!(
        actual.default_send_info, listener.default_send_info,
        "unexpected {label} SCTP_DEFAULT_SNDINFO state"
    );
}

#[test]
fn runtime_sctp_accept_inherits_listener_socket_options_and_preserves_buffer_locks() {
    const TEST_NAME: &str =
        "runtime_sctp_accept_inherits_listener_socket_options_and_preserves_buffer_locks";
    let Some(default_socket) = raw_sctp_socket_or_skip(TEST_NAME, libc::AF_INET) else {
        return;
    };
    let default_options = test_sctp_socket_options(default_socket.as_raw_fd())
        .expect("failed to read fresh SCTP socket options");
    drop(default_socket);

    let requested_notifications = SctpNotificationMask {
        association: true,
        shutdown: true,
        ..SctpNotificationMask::none()
    };
    let expected_notifications = SctpNotificationMask {
        partial_delivery: true,
        ..requested_notifications
    };
    let expected_send_info = SctpSendInfo {
        stream_id: 3,
        flags: 0,
        ppid: 0x0102_0304,
        context: 0x5566_7788,
        assoc_id: 0,
    };

    let mut config = SctpSocketConfig::data(SctpInitConfig::diameter_default());
    config.notifications = requested_notifications;
    config.recv_rcvinfo = true;
    config.nodelay = true;
    config.send_buffer_size = Some(64 * 1024);
    config.recv_buffer_size = Some(96 * 1024);
    config.default_send_info = Some(expected_send_info);

    let Some(exposed_listener) = bind_sctp_listener_or_skip(TEST_NAME, config) else {
        return;
    };
    let exposed_listener_options = test_sctp_socket_options(exposed_listener.as_raw_fd())
        .expect("failed to read configured SCTP listener options");
    let Some(managed_listener) = bind_sctp_listener_or_skip(TEST_NAME, config) else {
        return;
    };

    assert_eq!(
        exposed_listener_options.notifications,
        expected_notifications
    );
    assert!(exposed_listener_options.recv_rcvinfo);
    assert!(exposed_listener_options.nodelay);
    assert_eq!(
        exposed_listener_options.default_send_info,
        expected_send_info
    );
    if let Some(buffer_locks) = exposed_listener_options.buffer_locks {
        assert_eq!(buffer_locks & 0b11, 0b11);
        assert_ne!(
            exposed_listener_options.buffer_locks,
            default_options.buffer_locks
        );
    } else {
        assert_eq!(default_options.buffer_locks, None);
    }
    assert_ne!(
        exposed_listener_options.notifications,
        default_options.notifications
    );
    assert_ne!(
        exposed_listener_options.recv_rcvinfo,
        default_options.recv_rcvinfo
    );
    assert_ne!(exposed_listener_options.nodelay, default_options.nodelay);
    assert_ne!(
        exposed_listener_options.default_send_info,
        default_options.default_send_info
    );

    let exposed_listener_fd = exposed_listener.as_raw_fd();
    let exposed_listener_addr = exposed_listener.local_addr();
    let managed_listener_addr = managed_listener.local_addr();
    let client_config = SctpSocketConfig::data(SctpInitConfig::diameter_default());
    let mut executor = Executor::new().expect("failed to construct executor");
    executor
        .run(async move {
            let mut raw_connector = SctpConnector::with_config(client_config);
            let raw_client = raw_connector
                .connect_timeout(exposed_listener_addr, Duration::from_secs(1))
                .expect("raw inheritance connect_timeout init failed")
                .await
                .expect("raw inheritance connect failed");

            let mut accepted = -1;
            for _ in 0..100 {
                accepted = unsafe {
                    libc::accept4(
                        exposed_listener_fd,
                        std::ptr::null_mut(),
                        std::ptr::null_mut(),
                        libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                    )
                };
                if accepted >= 0 {
                    break;
                }
                let err = std::io::Error::last_os_error();
                assert_eq!(
                    err.kind(),
                    std::io::ErrorKind::WouldBlock,
                    "raw SCTP accept failed: {err}"
                );
                sleep(Duration::from_millis(10))
                    .await
                    .expect("raw SCTP accept retry sleep failed");
            }
            assert!(accepted >= 0, "raw SCTP accept remained unavailable");
            // SAFETY: the successful accept4 returned one sole-owned fd.
            let raw_accepted = unsafe { OwnedFd::from_raw_fd(accepted) };
            let raw_accepted_options = test_sctp_socket_options(raw_accepted.as_raw_fd())
                .expect("failed to read raw accepted SCTP socket options");
            assert_inherited_sctp_socket_options(
                raw_accepted_options,
                exposed_listener_options,
                "raw accepted socket",
            );
            drop(raw_accepted);
            drop(raw_client);

            let mut mutated_config = client_config;
            mutated_config.nodelay = false;
            mutated_config.default_send_info = Some(SctpSendInfo::default());
            test_apply_sctp_socket_options(exposed_listener_fd, mutated_config)
                .expect("failed to mutate exposed SCTP listener options");
            let mutated_options = test_sctp_socket_options(exposed_listener_fd)
                .expect("failed to read mutated SCTP listener options");
            assert_ne!(
                mutated_options.notifications,
                exposed_listener_options.notifications
            );
            assert_ne!(
                mutated_options.recv_rcvinfo,
                exposed_listener_options.recv_rcvinfo
            );
            assert_ne!(mutated_options.nodelay, exposed_listener_options.nodelay);
            assert_ne!(
                mutated_options.default_send_info,
                exposed_listener_options.default_send_info
            );

            let (exposed_client, exposed_server) = accepted_sctp_pair(
                exposed_listener,
                SctpConnector::with_config(client_config),
                exposed_listener_addr,
            )
            .await;
            let exposed_accepted_options = test_sctp_socket_options(exposed_server.as_raw_fd())
                .expect("failed to read exposed-listener accepted SCTP socket options");
            assert_inherited_sctp_socket_options(
                exposed_accepted_options,
                exposed_listener_options,
                "exposed-listener accepted socket",
            );
            assert_eq!(
                exposed_accepted_options.buffer_locks, exposed_listener_options.buffer_locks,
                "exposed-listener accept did not restore explicit socket-buffer locks"
            );
            drop(exposed_client);
            drop(exposed_server);

            let (managed_client, managed_server) = accepted_sctp_pair(
                managed_listener,
                SctpConnector::with_config(client_config),
                managed_listener_addr,
            )
            .await;
            let managed_accepted_options = test_sctp_socket_options(managed_server.as_raw_fd())
                .expect("failed to read managed-listener accepted SCTP socket options");
            assert_inherited_sctp_socket_options(
                managed_accepted_options,
                exposed_listener_options,
                "managed-listener accepted socket",
            );
            assert_eq!(
                managed_accepted_options.buffer_locks, exposed_listener_options.buffer_locks,
                "managed-listener accept did not preserve explicit socket-buffer locks"
            );
            drop(managed_client);
            drop(managed_server);
        })
        .expect("SCTP option-inheritance run failed");
}

#[test]
fn runtime_sctp_metadata_only_receive_forces_pdapi_and_preserves_it() {
    let mut config = SctpSocketConfig::data(SctpInitConfig::diameter_default());
    config.recv_rcvinfo = true;
    let expected_mask = SctpNotificationMask {
        partial_delivery: true,
        ..SctpNotificationMask::none()
    };

    let Some(listener) = bind_sctp_listener_or_skip(
        "runtime_sctp_metadata_only_receive_forces_pdapi_and_preserves_it",
        config,
    ) else {
        return;
    };
    assert_sctp_receive_options(listener.as_raw_fd(), expected_mask, true, "listener");

    let addr = listener.local_addr();
    let connector = SctpConnector::with_config(config);
    let mut executor = Executor::new().expect("failed to construct executor");
    executor
        .run(async move {
            let (client, server) = accepted_sctp_pair(listener, connector, addr).await;

            assert_sctp_receive_options(client.as_raw_fd(), expected_mask, true, "client");
            assert_sctp_receive_options(server.as_raw_fd(), expected_mask, true, "accepted");

            client
                .set_notification_mask(SctpNotificationMask::none())
                .expect("client notification-mask update failed");
            server
                .set_notification_mask(SctpNotificationMask::none())
                .expect("accepted notification-mask update failed");
            assert_sctp_receive_options(client.as_raw_fd(), expected_mask, true, "updated client");
            assert_sctp_receive_options(
                server.as_raw_fd(),
                expected_mask,
                true,
                "updated accepted",
            );
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_data_receive_keeps_pdapi_and_rcvinfo_disabled() {
    let config = SctpSocketConfig::data(SctpInitConfig::diameter_default());
    let expected_mask = SctpNotificationMask::none();
    let Some(listener) = bind_sctp_listener_or_skip(
        "runtime_sctp_data_receive_keeps_pdapi_and_rcvinfo_disabled",
        config,
    ) else {
        return;
    };
    assert_sctp_receive_options(listener.as_raw_fd(), expected_mask, false, "listener");

    let addr = listener.local_addr();
    let connector = SctpConnector::with_config(config);
    let mut executor = Executor::new().expect("failed to construct executor");
    executor
        .run(async move {
            let (client, server) = accepted_sctp_pair(listener, connector, addr).await;
            assert_sctp_receive_options(client.as_raw_fd(), expected_mask, false, "client");
            assert_sctp_receive_options(server.as_raw_fd(), expected_mask, false, "accepted");

            client
                .set_notification_mask(SctpNotificationMask::none())
                .expect("client notification-mask update failed");
            assert_sctp_receive_options(client.as_raw_fd(), expected_mask, false, "updated client");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_data_config_recv_msg_defaults_missing_rcvinfo() {
    let mut config = SctpSocketConfig::data(SctpInitConfig::diameter_default());
    config.default_send_info = Some(SctpSendInfo {
        stream_id: 7,
        flags: 0,
        ppid: 0x0102_0304,
        context: 9,
        assoc_id: 0,
    });
    let expected_mask = SctpNotificationMask::none();
    let Some(listener) = bind_sctp_listener_or_skip(
        "runtime_sctp_data_config_recv_msg_defaults_missing_rcvinfo",
        config,
    ) else {
        return;
    };
    assert_sctp_receive_options(listener.as_raw_fd(), expected_mask, false, "listener");

    let addr = listener.local_addr();
    let connector = SctpConnector::with_config(config);
    let mut executor = Executor::new().expect("failed to construct executor");
    executor
        .run(async move {
            let (mut client, mut server) = accepted_sctp_pair(listener, connector, addr).await;
            assert_sctp_receive_options(client.as_raw_fd(), expected_mask, false, "client");
            assert_sctp_receive_options(server.as_raw_fd(), expected_mask, false, "accepted");

            let (send_result, _payload) = client.send(b"scalar".to_vec()).await;
            assert_eq!(send_result.expect("scalar data send failed"), 6);

            let recv = IoBuffMut::new(0, 64, 0);
            let (recv_result, recv) = timeout(Duration::from_secs(1), server.recv_msg(recv, 64))
                .await
                .expect("scalar data-config recv_msg timed out");
            let (recv_len, meta) = recv_result.expect("scalar data-config recv_msg should succeed");
            assert_eq!(recv_len, 6);
            assert_eq!(recv.payload_bytes(), b"scalar");
            assert_eq!(
                meta,
                SctpRecvMeta::Data(SctpRecvInfo {
                    end_of_record: true,
                    ..SctpRecvInfo::default()
                })
            );

            let (send_result, _payload) = client.send(b"vector".to_vec()).await;
            assert_eq!(send_result.expect("vectored data send failed"), 6);

            let mut chain = IoBuffVecMut::<2>::new();
            chain.push(IoBuffMut::new(0, 3, 0)).unwrap();
            chain.push(IoBuffMut::new(0, 3, 0)).unwrap();
            let (recv_result, chain) =
                timeout(Duration::from_secs(1), server.recv_msg_vectored(chain))
                    .await
                    .expect("data-config recv_msg_vectored timed out");
            let (recv_len, meta) =
                recv_result.expect("data-config recv_msg_vectored should succeed");
            assert_eq!(recv_len, 6);
            assert_eq!(chain.get(0).unwrap().payload_bytes(), b"vec");
            assert_eq!(chain.get(1).unwrap().payload_bytes(), b"tor");
            assert_eq!(
                meta,
                SctpRecvMeta::Data(SctpRecvInfo {
                    end_of_record: true,
                    ..SctpRecvInfo::default()
                })
            );
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_recv_msg_finds_rcvinfo_after_timestampns() {
    const TEST_NAME: &str = "runtime_sctp_recv_msg_finds_rcvinfo_after_timestampns";
    let mut config = SctpSocketConfig::data(SctpInitConfig::diameter_default());
    config.recv_rcvinfo = true;
    let Some(listener) = bind_sctp_listener_or_skip(TEST_NAME, config) else {
        return;
    };

    let addr = listener.local_addr();
    let connector = SctpConnector::with_config(config);
    let mut executor = Executor::new().expect("failed to construct executor");
    executor
        .run(async move {
            let (mut client, mut server) = accepted_sctp_pair(listener, connector, addr).await;
            enable_socket_timestampns(server.as_raw_fd());

            let (send_result, _payload) = client
                .send_msg(b"scalar".to_vec(), test_send_info(3, 0x0102_0304))
                .await;
            assert_eq!(send_result.expect("timestamped scalar send failed"), 6);

            let recv = IoBuffMut::new(0, 64, 0);
            let (recv_result, recv) = timeout(Duration::from_secs(1), server.recv_msg(recv, 64))
                .await
                .expect("timestamped scalar SCTP receive timed out");
            let (recv_len, meta) = recv_result.expect("timestamped scalar SCTP receive failed");
            assert_eq!(recv_len, 6);
            assert_eq!(recv.payload_bytes(), b"scalar");
            assert_data_rcvinfo(meta, 3, 0x0102_0304);
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_recv_msg_vectored_finds_rcvinfo_after_timestampns() {
    const TEST_NAME: &str = "runtime_sctp_recv_msg_vectored_finds_rcvinfo_after_timestampns";
    let mut config = SctpSocketConfig::data(SctpInitConfig::diameter_default());
    config.recv_rcvinfo = true;
    let Some(listener) = bind_sctp_listener_or_skip(TEST_NAME, config) else {
        return;
    };

    let addr = listener.local_addr();
    let connector = SctpConnector::with_config(config);
    let mut executor = Executor::new().expect("failed to construct executor");
    executor
        .run(async move {
            let (mut client, mut server) = accepted_sctp_pair(listener, connector, addr).await;
            enable_socket_timestampns(server.as_raw_fd());

            let (send_result, _payload) = client
                .send_msg(b"vector".to_vec(), test_send_info(4, 0x0506_0708))
                .await;
            assert_eq!(send_result.expect("timestamped vectored send failed"), 6);

            let mut chain = IoBuffVecMut::<2>::new();
            chain.push(IoBuffMut::new(0, 3, 0)).unwrap();
            chain.push(IoBuffMut::new(0, 3, 0)).unwrap();
            let (recv_result, chain) =
                timeout(Duration::from_secs(1), server.recv_msg_vectored(chain))
                    .await
                    .expect("timestamped vectored SCTP receive timed out");
            let (recv_len, meta) = recv_result.expect("timestamped vectored SCTP receive failed");
            assert_eq!(recv_len, 6);
            assert_eq!(chain.get(0).unwrap().payload_bytes(), b"vec");
            assert_eq!(chain.get(1).unwrap().payload_bytes(), b"tor");
            assert_data_rcvinfo(meta, 4, 0x0506_0708);
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_connect_delivers_comm_up_notification_when_subscribed() {
    let mut config = SctpSocketConfig::signaling(SctpInitConfig::diameter_default());
    config.notifications = SctpNotificationMask {
        association: true,
        ..SctpNotificationMask::none()
    };

    let mut listener = match bind_sctp_listener_or_skip(
        "runtime_sctp_connect_delivers_comm_up_notification_when_subscribed",
        config,
    ) {
        Some(listener) => listener,
        None => return,
    };

    let addr = listener.local_addr();
    let mut connector = SctpConnector::with_config(config);
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            let server =
                Executor::spawn(async move { listener.accept().await.expect("accept failed").0 })
                    .expect("accept spawn failed");

            let mut client = connector
                .connect(addr)
                .expect("connect setup failed")
                .await
                .expect("connect failed");
            let _server = server.await;

            let recv_res = timeout(Duration::from_secs(1), client.recv_msg(vec![0u8; 256], 256))
                .await
                .expect("COMM_UP notification was not delivered after connect");
            let (result, _buf) = recv_res;
            let (recv_len, meta) = result.expect("client recv_msg failed");
            assert!(recv_len > 0, "COMM_UP notification should carry bytes");
            match meta {
                SctpRecvMeta::Notification(SctpNotification::AssocChange { state, .. }) => {
                    assert_eq!(state, LINUX_SCTP_COMM_UP);
                }
                other => panic!("expected COMM_UP assoc-change notification, got {other:?}"),
            }
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_ping_pong() {
    use std::net::{Ipv4Addr, SocketAddr};

    let init = SctpInitConfig::diameter_default();
    assert_eq!(init, SctpInitConfig::default());

    let mut listener =
        match SctpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128, init) {
            Ok(listener) => listener,
            Err(err) => {
                if capability_unavailable(&err) {
                    eprintln!("skipping runtime_sctp_ping_pong: SCTP unsupported ({err})");
                    return;
                }
                panic!("failed to bind sctp listener: {err}");
            }
        };

    let mut executor = Executor::new().expect("failed to construct executor");
    let addr = listener.local_addr();
    let mut connector = SctpConnector::new(SctpInitConfig::diameter_default());
    let msg_size = 256;

    executor
        .run(async move {
            let srv_buf = vec![0u8; msg_size];
            let cli_send = b"ping".to_vec();
            let cli_recv = vec![0u8; msg_size];

            Executor::spawn(async move {
                let (mut stream, _remote) = listener.accept().await.expect("accept failed");
                assert_eq!(stream.local_addr().expect("local_addr failed"), addr);

                // Skip notifications until we get a data message.
                let mut current_buf = srv_buf;
                let (recv_len, meta, recv_buf) = loop {
                    let recv_res = stream.recv_msg(current_buf, msg_size).await;
                    let (recv_len, meta) = recv_res.0.expect("server recv failed");
                    match meta {
                        SctpRecvMeta::Notification(_) => current_buf = recv_res.1,
                        SctpRecvMeta::Data(info) => {
                            break (recv_len, SctpRecvMeta::Data(info), recv_res.1);
                        }
                    }
                };
                assert_eq!(recv_len, 4);
                match meta {
                    SctpRecvMeta::Data(info) => {
                        assert_eq!(info.stream_id, 1);
                        assert_eq!(info.ppid, 0x0102_0304);
                    }
                    _ => panic!("expected data, got notification"),
                }

                let (send_res, _buf) = stream
                    .send_msg(
                        recv_buf,
                        SctpSendInfo {
                            stream_id: 1,
                            flags: 0,
                            ppid: 0x0102_0304,
                            context: 0,
                            assoc_id: 0,
                        },
                    )
                    .await;
                send_res.expect("server send failed");
            })
            .expect("server spawn failed");

            let mut stream = connector
                .connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");
            let cached_peer_addr = stream.peer_addr();
            assert_eq!(cached_peer_addr, addr);

            let client_local_addr = stream.local_addr().expect("client local_addr failed");
            assert_ne!(client_local_addr, addr);

            let client_local_addrs = stream.local_addrs().expect("client local_addrs failed");
            assert!(client_local_addrs.contains(&client_local_addr));

            let client_peer_addrs = stream.peer_addrs().expect("client peer_addrs failed");
            assert!(client_peer_addrs.contains(&cached_peer_addr));

            let status = stream.status().expect("client status failed");
            assert_eq!(status.state, SctpAssocStatus::ESTABLISHED);
            assert!(status.inbound_streams > 0);
            assert!(status.outbound_streams > 0);
            assert_eq!(status.primary_path.address, addr);

            let primary_info = stream
                .primary_path_info()
                .expect("primary_path_info failed");
            assert_eq!(primary_info.address, addr);

            let send_buffer_size = stream.send_buffer_size().expect("send_buffer_size failed");
            assert!(send_buffer_size > 0);
            stream
                .set_send_buffer_size(send_buffer_size)
                .expect("set_send_buffer_size failed");

            let recv_buffer_size = stream.recv_buffer_size().expect("recv_buffer_size failed");
            assert!(recv_buffer_size > 0);
            stream
                .set_recv_buffer_size(recv_buffer_size)
                .expect("set_recv_buffer_size failed");

            stream
                .set_notification_mask(SctpNotificationMask::signaling_default())
                .expect("set_notification_mask failed");

            stream
                .apply_assoc_config(&SctpAssocConfig {
                    assoc_max_retrans: Some(4),
                    rto_initial_ms: Some(1000),
                    rto_min_ms: Some(500),
                    rto_max_ms: Some(4000),
                })
                .expect("apply_assoc_config failed");

            let peer_info = stream.peer_addr_info(addr).expect("peer_addr_info failed");
            assert_eq!(peer_info.address, addr);

            let peer_params = stream
                .peer_addr_params(Some(addr))
                .expect("peer_addr_params failed");
            assert_eq!(peer_params.address, Some(addr));
            stream
                .set_peer_addr_params(peer_params)
                .expect("set_peer_addr_params failed");
            stream
                .set_default_peer_addr_params(SctpPeerAddrParams::association_default())
                .expect("set_default_peer_addr_params failed");
            stream
                .set_primary_dest_addr(addr)
                .expect("set_primary_dest_addr failed");

            // The peer request may fail with EPERM/EACCES/EOPNOTSUPP depending on kernel policy.
            if let Err(err) = stream.request_peer_use_local_addr(client_local_addr) {
                let raw = err.raw_os_error();
                assert!(
                    matches!(
                        raw,
                        Some(libc::EPERM) | Some(libc::EACCES) | Some(libc::EOPNOTSUPP)
                    ),
                    "request_peer_use_local_addr failed unexpectedly: {err}"
                );
            }

            let reconfig = stream
                .reconfig_supported()
                .expect("reconfig_supported failed");
            let enable_res = stream.enable_stream_reset(SctpReconfigFlags {
                assoc_id: reconfig.assoc_id,
                flags: SctpReconfigFlags::RESET_STREAMS | SctpReconfigFlags::CHANGE_ASSOC,
            });
            // Stream reconfiguration may not be supported on all kernels.
            if let Err(err) = enable_res {
                let raw = err.raw_os_error();
                assert!(
                    matches!(
                        raw,
                        Some(libc::EOPNOTSUPP)
                            | Some(libc::ENOPROTOOPT)
                            | Some(libc::EINVAL)
                            | Some(libc::EPERM)
                            | Some(libc::EACCES)
                    ),
                    "enable_stream_reset failed unexpectedly: {err}"
                );
            } else {
                if let Err(err) = stream.reset_streams(&SctpResetStreams::outgoing(&[1])) {
                    let raw = err.raw_os_error();
                    assert!(
                        matches!(
                            raw,
                            Some(libc::EOPNOTSUPP)
                                | Some(libc::ENOPROTOOPT)
                                | Some(libc::EINVAL)
                                | Some(libc::EPERM)
                                | Some(libc::EACCES)
                        ),
                        "reset_streams failed unexpectedly: {err}"
                    );
                }

                if let Err(err) = stream.add_streams(SctpAddStreams::new(1, 1)) {
                    let raw = err.raw_os_error();
                    assert!(
                        matches!(
                            raw,
                            Some(libc::EOPNOTSUPP)
                                | Some(libc::ENOPROTOOPT)
                                | Some(libc::EINVAL)
                                | Some(libc::EPERM)
                                | Some(libc::EACCES)
                        ),
                        "add_streams failed unexpectedly: {err}"
                    );
                }
            }

            let (send_res, _) = stream
                .send_msg(
                    cli_send,
                    SctpSendInfo {
                        stream_id: 1,
                        flags: 0,
                        ppid: 0x0102_0304,
                        context: 0,
                        assoc_id: 0,
                    },
                )
                .await;
            send_res.expect("client send failed");

            // Skip notifications until we get data back.
            let mut current_buf = cli_recv;
            let (recv_len, meta, recv_buf) = loop {
                let recv_res = stream.recv_msg(current_buf, msg_size).await;
                let (recv_len, meta) = recv_res.0.expect("client recv failed");
                match meta {
                    SctpRecvMeta::Notification(_) => current_buf = recv_res.1,
                    SctpRecvMeta::Data(info) => {
                        break (recv_len, SctpRecvMeta::Data(info), recv_res.1);
                    }
                }
            };
            assert_eq!(recv_len, 4);
            assert_eq!(&recv_buf[..recv_len], b"ping");
            match meta {
                SctpRecvMeta::Data(info) => {
                    assert_eq!(info.stream_id, 1);
                    assert_eq!(info.ppid, 0x0102_0304);
                }
                _ => panic!("expected data, got notification"),
            }

            stream
                .shutdown(std::net::Shutdown::Write)
                .expect("shutdown failed");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_recv_msg_rejects_undersized_buffer_without_eor() {
    let init = SctpInitConfig::diameter_default();
    let mut socket_config = SctpSocketConfig::data(init);
    socket_config.recv_rcvinfo = true;

    let Some(mut listener) = bind_sctp_listener_or_skip(
        "runtime_sctp_recv_msg_rejects_undersized_buffer_without_eor",
        socket_config,
    ) else {
        return;
    };

    let mut executor = Executor::new().expect("failed to construct executor");
    let addr = listener.local_addr();
    let mut connector = SctpConnector::with_config(socket_config);

    executor
        .run(async move {
            let server = Executor::spawn(async move {
                let (mut stream, _remote) = listener.accept().await.expect("accept failed");
                let payload = b"0123456789abcdef".to_vec();
                let (send_res, _payload) = stream
                    .send_msg(
                        payload,
                        SctpSendInfo {
                            stream_id: 1,
                            flags: 0,
                            ppid: 0x0102_0304,
                            context: 0,
                            assoc_id: 0,
                        },
                    )
                    .await;
                send_res.expect("server send_msg failed");
            })
            .expect("server spawn failed");

            let mut stream = connector
                .connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");

            let mut recv_buf = IoBuffMut::new(0, 8, 0);
            recv_buf.payload_append(b"HEAD").unwrap();
            let (recv_res, recv_buf) = stream.recv_msg(recv_buf, 4).await;
            let err = recv_res.expect_err("undersized SCTP recv_msg should reject partial record");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
            assert!(
                err.to_string().contains("end-of-record") || err.to_string().contains("truncated"),
                "undersized SCTP receive should report record truncation: {err}"
            );
            assert_eq!(recv_buf.payload_bytes(), b"HEAD0123");

            server.await.expect("server task cancelled");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_recv_msg_resynchronizes_after_oversized_record() {
    let init = SctpInitConfig::diameter_default();
    let mut socket_config = SctpSocketConfig::data(init);
    socket_config.recv_rcvinfo = true;

    let Some(listener) = bind_sctp_listener_or_skip(
        "runtime_sctp_recv_msg_resynchronizes_after_oversized_record",
        socket_config,
    ) else {
        return;
    };
    let addr = listener.local_addr();
    let connector = SctpConnector::with_config(socket_config);
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            let (mut client, mut server) = accepted_sctp_pair(listener, connector, addr).await;

            let (send_res, _buf) = client
                .send_msg(b"0123456789abcdef".to_vec(), test_send_info(1, 0x0102_0304))
                .await;
            assert_eq!(send_res.expect("first send_msg failed"), 16);

            let (recv_res, recv_buf) = server.recv_msg(vec![0u8; 4], 4).await;
            let err = recv_res.expect_err("oversized SCTP record should fail once");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
            assert_eq!(&recv_buf[..], b"0123");

            let lean = vec![0u8; 8];
            let lean_ptr = lean.as_ptr();
            let (lean_result, returned_lean) =
                timeout(Duration::from_millis(100), server.recv(lean, 8))
                    .await
                    .expect(
                        "lean receive behind active rich-record recovery did not return locally",
                    );
            let err = lean_result.expect_err("lean receive bypassed active rich-record recovery");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(err.raw_os_error(), None);
            assert_eq!(returned_lean.as_ptr(), lean_ptr);
            assert_eq!(returned_lean, vec![0u8; 8]);

            let drops = Rc::new(Cell::new(0));
            let (zero_res, zero_buf) = server
                .recv_msg(DropTrackedReadWrite::zeroed(4, &drops), 0)
                .await;
            let err = zero_res.expect_err("zero-length recv_msg should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(
                drops.get(),
                0,
                "zero-length recv_msg dropped the returned buffer before the caller"
            );
            drop(zero_buf);
            assert_eq!(
                drops.get(),
                1,
                "zero-length recv_msg should return ownership exactly once"
            );

            let (send_res, _buf) = client
                .send_msg(b"second".to_vec(), test_send_info(2, 0x0506_0708))
                .await;
            assert_eq!(send_res.expect("second send_msg failed"), 6);

            let mut recv_buf = IoBuffMut::new(0, 68, 0);
            recv_buf.payload_append(b"HEAD").unwrap();
            let recv_res = timeout(Duration::from_secs(1), server.recv_msg(recv_buf, 64))
                .await
                .expect("resynchronized SCTP recv timed out");
            let (recv_result, recv_buf) = recv_res;
            let (recv_len, meta) = recv_result.expect("second recv_msg failed");
            assert_eq!(recv_len, 6);
            assert_eq!(recv_buf.payload_bytes(), b"HEADsecond");
            match meta {
                SctpRecvMeta::Data(info) => {
                    assert_eq!(info.stream_id, 2);
                    assert_eq!(info.ppid, 0x0506_0708);
                    assert!(info.end_of_record);
                }
                SctpRecvMeta::Notification(notification) => {
                    panic!("expected second data record, got {notification:?}");
                }
            }

            client
                .shutdown(Shutdown::Write)
                .expect("client shutdown write failed");
            let eof = timeout(Duration::from_secs(1), server.recv_msg(recv_buf, 32))
                .await
                .expect("post-resynchronization clean EOF timed out");
            let (eof_result, recv_buf) = eof;
            let (eof_len, eof_meta) = eof_result.expect("clean EOF should not error");
            assert_eq!(eof_len, 0);
            assert_eq!(recv_buf.payload_bytes(), b"HEADsecond");
            match eof_meta {
                SctpRecvMeta::Data(info) => assert_eq!(info, Default::default()),
                SctpRecvMeta::Notification(notification) => {
                    panic!("expected clean EOF data shape, got {notification:?}");
                }
            }
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_fragmented_notification_tail_recovers_only_at_eor() {
    const TEST_NAME: &str = "runtime_sctp_fragmented_notification_tail_recovers_only_at_eor";
    let init = SctpInitConfig::diameter_default();
    let mut socket_config = SctpSocketConfig::data(init);
    socket_config.recv_rcvinfo = true;
    socket_config.notifications.stream_reset = true;

    let Some(listener) = bind_sctp_listener_or_skip(TEST_NAME, socket_config) else {
        return;
    };
    let addr = listener.local_addr();
    let connector = SctpConnector::with_config(socket_config);
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            let (mut client, mut server) = accepted_sctp_pair(listener, connector, addr).await;
            let client_reconfig = match client.reconfig_supported() {
                Ok(reconfig) => reconfig,
                Err(err) if capability_unavailable(&err) => {
                    eprintln!("skipping {TEST_NAME}: SCTP reconfiguration unavailable ({err})");
                    return;
                }
                Err(err) => panic!("failed to query SCTP reconfiguration for {TEST_NAME}: {err}"),
            };
            if let Err(err) = client.enable_stream_reset(SctpReconfigFlags {
                assoc_id: client_reconfig.assoc_id,
                flags: SctpReconfigFlags::RESET_STREAMS,
            }) {
                if matches!(
                    err.raw_os_error(),
                    Some(libc::EOPNOTSUPP)
                        | Some(libc::ENOPROTOOPT)
                        | Some(libc::EINVAL)
                        | Some(libc::EPERM)
                        | Some(libc::EACCES)
                ) {
                    eprintln!("skipping {TEST_NAME}: SCTP stream reset unavailable ({err})");
                    return;
                }
                panic!("failed to enable SCTP stream reset for {TEST_NAME}: {err}");
            }

            let server_reconfig = match server.reconfig_supported() {
                Ok(reconfig) => reconfig,
                Err(err) if capability_unavailable(&err) => {
                    eprintln!(
                        "skipping {TEST_NAME}: peer SCTP reconfiguration unavailable ({err})"
                    );
                    return;
                }
                Err(err) => {
                    panic!("failed to query peer SCTP reconfiguration for {TEST_NAME}: {err}")
                }
            };
            if let Err(err) = server.enable_stream_reset(SctpReconfigFlags {
                assoc_id: server_reconfig.assoc_id,
                flags: SctpReconfigFlags::RESET_STREAMS,
            }) {
                if matches!(
                    err.raw_os_error(),
                    Some(libc::EOPNOTSUPP)
                        | Some(libc::ENOPROTOOPT)
                        | Some(libc::EINVAL)
                        | Some(libc::EPERM)
                        | Some(libc::EACCES)
                ) {
                    eprintln!("skipping {TEST_NAME}: peer SCTP stream reset unavailable ({err})");
                    return;
                }
                panic!("failed to enable peer SCTP stream reset for {TEST_NAME}: {err}");
            }

            let streams = (0_u16..16).collect::<Vec<_>>();
            if let Err(err) = client.reset_streams(&SctpResetStreams::outgoing(&streams)) {
                if matches!(
                    err.raw_os_error(),
                    Some(libc::EOPNOTSUPP)
                        | Some(libc::ENOPROTOOPT)
                        | Some(libc::EINVAL)
                        | Some(libc::EPERM)
                        | Some(libc::EACCES)
                ) {
                    eprintln!("skipping {TEST_NAME}: SCTP reset request unavailable ({err})");
                    return;
                }
                panic!("failed to request SCTP stream reset for {TEST_NAME}: {err}");
            }

            let first = timeout(Duration::from_secs(1), server.recv_msg(vec![0u8; 8], 8))
                .await
                .expect("fragmented stream-reset head timed out");
            let (first_result, first_buffer) = first;
            let err = first_result.expect_err("fragmented notification head parsed completely");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
            assert_eq!(
                err.to_string(),
                "SCTP recvmsg payload was partial before end-of-record"
            );
            assert_eq!(
                u16::from_ne_bytes([first_buffer[0], first_buffer[1]]),
                test_stream_reset_event_type() as u16
            );
            assert_eq!(
                u32::from_ne_bytes(first_buffer[4..8].try_into().expect("header length slice")),
                44
            );

            let lean = timeout(Duration::from_millis(100), server.recv(vec![0u8; 8], 8))
                .await
                .expect("lean receive behind notification tail did not return locally");
            let err = lean
                .0
                .expect_err("lean receive bypassed notification-tail recovery");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

            let (send_result, _payload) = client
                .send_msg(b"after".to_vec(), test_send_info(0, 0x0102_0304))
                .await;
            assert_eq!(send_result.expect("post-reset send failed"), 5);

            let recovered = timeout(Duration::from_secs(1), server.recv_msg(vec![0u8; 64], 64))
                .await
                .expect("notification-tail recovery timed out");
            let (recovered_result, recovered_buffer) = recovered;
            let (received, meta) = recovered_result.expect("post-notification receive failed");
            assert_eq!(received, 5);
            assert_eq!(&recovered_buffer[..received], b"after");
            match meta {
                SctpRecvMeta::Data(info) => {
                    assert_eq!(info.stream_id, 0);
                    assert_eq!(info.ppid, 0x0102_0304);
                    assert!(info.end_of_record);
                }
                SctpRecvMeta::Notification(notification) => {
                    panic!("expected post-recovery data, got {notification:?}");
                }
            }
            eprintln!("{TEST_NAME}: exercised=1 declared_len=44 capacity=8 recovered=1");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_recv_msg_vectored_resynchronizes_after_multi_completion_discard() {
    let init = SctpInitConfig::diameter_default();
    let mut socket_config = SctpSocketConfig::data(init);
    socket_config.recv_rcvinfo = true;

    let Some(listener) = bind_sctp_listener_or_skip(
        "runtime_sctp_recv_msg_vectored_resynchronizes_after_multi_completion_discard",
        socket_config,
    ) else {
        return;
    };
    let addr = listener.local_addr();
    let connector = SctpConnector::with_config(socket_config);
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            let (mut client, mut server) = accepted_sctp_pair(listener, connector, addr).await;
            let mut pool = IoBuffPool::new(IoBuffPoolConfig {
                headroom: 0,
                payload: 4,
                tailroom: 0,
                objs_per_slab: 2,
            })
            .expect("pool config invalid");
            pool.init();

            let oversized = vec![b'x'; 40];
            let (send_res, _buf) = client
                .send_msg(oversized, test_send_info(1, 0x0102_0304))
                .await;
            assert_eq!(send_res.expect("oversized send_msg failed"), 40);

            let first_chain = pooled_sctp_recv_chain::<2>(&mut pool);
            assert_eq!(pool.live_slots_for_test(), 2);
            let (recv_res, first_chain) = server.recv_msg_vectored(first_chain).await;
            let err = recv_res.expect_err("oversized SCTP record should fail once");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
            assert_eq!(first_chain.get(0).unwrap().payload_bytes(), b"xxxx");
            assert_eq!(first_chain.get(1).unwrap().payload_bytes(), b"xxxx");
            assert_eq!(
                pool.live_slots_for_test(),
                2,
                "vectored SCTP recv chain was not returned to the caller"
            );
            drop(first_chain);
            wait_for_live_slots(&pool, 0).await;

            let (send_res, _buf) = client
                .send_msg(b"ABCDEFGH".to_vec(), test_send_info(2, 0x0506_0708))
                .await;
            assert_eq!(send_res.expect("second send_msg failed"), 8);

            let second_chain = pooled_sctp_recv_chain::<2>(&mut pool);
            assert_eq!(pool.live_slots_for_test(), 2);
            let recv_res = timeout(
                Duration::from_secs(1),
                server.recv_msg_vectored(second_chain),
            )
            .await
            .expect("resynchronized SCTP vectored recv timed out");
            let (recv_result, second_chain) = recv_res;
            let (recv_len, meta) = recv_result.expect("second recv_msg_vectored failed");
            assert_eq!(recv_len, 8);
            assert_eq!(second_chain.get(0).unwrap().payload_bytes(), b"ABCD");
            assert_eq!(second_chain.get(1).unwrap().payload_bytes(), b"EFGH");
            match meta {
                SctpRecvMeta::Data(info) => {
                    assert_eq!(info.stream_id, 2);
                    assert_eq!(info.ppid, 0x0506_0708);
                    assert!(info.end_of_record);
                }
                SctpRecvMeta::Notification(notification) => {
                    panic!("expected second data record, got {notification:?}");
                }
            }
            assert_eq!(
                pool.live_slots_for_test(),
                2,
                "vectored SCTP discard path dropped the receive chain before returning it"
            );
            drop(second_chain);
            wait_for_live_slots(&pool, 0).await;
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_recv_msg_shutdown_without_control_is_clean_eof() {
    let init = SctpInitConfig::diameter_default();
    let mut socket_config = SctpSocketConfig::data(init);
    socket_config.recv_rcvinfo = true;

    let Some(listener) = bind_sctp_listener_or_skip(
        "runtime_sctp_recv_msg_shutdown_without_control_is_clean_eof",
        socket_config,
    ) else {
        return;
    };
    let addr = listener.local_addr();
    let connector = SctpConnector::with_config(socket_config);
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            let (client, mut server) = accepted_sctp_pair(listener, connector, addr).await;
            client
                .shutdown(Shutdown::Write)
                .expect("client shutdown write failed");

            let mut recv_buf = IoBuffMut::new(0, 36, 0);
            recv_buf.payload_append(b"HEAD").unwrap();
            let recv_res = timeout(Duration::from_secs(1), server.recv_msg(recv_buf, 32))
                .await
                .expect("SCTP clean EOF recv timed out");
            let (recv_result, recv_buf) = recv_res;
            let (recv_len, meta) = recv_result.expect("clean EOF should not error");
            assert_eq!(recv_len, 0);
            assert_eq!(recv_buf.payload_bytes(), b"HEAD");
            match meta {
                SctpRecvMeta::Data(info) => assert_eq!(info, Default::default()),
                SctpRecvMeta::Notification(notification) => {
                    panic!("expected clean EOF data shape, got {notification:?}");
                }
            }
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_recv_msg_discard_state_drop_returns_buffer_once() {
    let init = SctpInitConfig::diameter_default();
    let mut socket_config = SctpSocketConfig::data(init);
    socket_config.recv_rcvinfo = true;

    let Some(listener) = bind_sctp_listener_or_skip(
        "runtime_sctp_recv_msg_discard_state_drop_returns_buffer_once",
        socket_config,
    ) else {
        return;
    };
    let addr = listener.local_addr();
    let connector = SctpConnector::with_config(socket_config);
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            let (mut client, mut server) = accepted_sctp_pair(listener, connector, addr).await;

            // Three 8-byte receive chunks: first enters discard state, the
            // dropped second chunk is still non-EOR, and the next receive must
            // discard the final EOR chunk before returning the following
            // record.
            let oversized = vec![b'x'; 24];
            let (send_res, _buf) = client
                .send_msg(oversized, test_send_info(1, 0x0102_0304))
                .await;
            assert_eq!(send_res.expect("oversized send_msg failed"), 24);

            let (recv_res, recv_buf) = server.recv_msg(vec![0u8; 8], 8).await;
            let err = recv_res.expect_err("oversized SCTP record should enter discard state");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
            assert_eq!(&recv_buf[..], b"xxxxxxxx");

            let drops = Rc::new(Cell::new(0));
            let recv = DropTrackedReadWrite::zeroed(8, &drops);
            let mut discard = Box::pin(server.recv_msg(recv, 8));
            let first_poll =
                std::future::poll_fn(|cx| Poll::Ready(Future::poll(discard.as_mut(), cx))).await;
            assert!(
                matches!(first_poll, Poll::Pending),
                "discard-state recv should submit before being dropped"
            );
            drop(discard);
            assert_eq!(drops.get(), 0, "stashed discard buffer dropped early");

            let (invalid, invalid_buffer) = server.recv_msg(vec![0u8; 8], 0).await;
            let err = invalid.expect_err("zero-length metadata receive unexpectedly succeeded");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(invalid_buffer, vec![0u8; 8]);
            assert_eq!(
                drops.get(),
                0,
                "invalid metadata receive adopted the prior stash"
            );

            let (send_res, _buf) = client
                .send_msg(b"after".to_vec(), test_send_info(2, 0x0506_0708))
                .await;
            assert_eq!(send_res.expect("post-cancel send_msg failed"), 5);

            let recv_res = timeout(Duration::from_secs(1), server.recv_msg(vec![0u8; 64], 64))
                .await
                .expect("post-discard SCTP recv timed out");
            let (recv_result, recv_buf) = recv_res;
            let (recv_len, meta) = recv_result.expect("post-discard recv failed");
            assert_eq!(recv_len, 5);
            assert_eq!(&recv_buf[..recv_len], b"after");
            match meta {
                SctpRecvMeta::Data(info) => {
                    assert_eq!(info.stream_id, 2);
                    assert_eq!(info.ppid, 0x0506_0708);
                    assert!(info.end_of_record);
                }
                SctpRecvMeta::Notification(notification) => {
                    panic!("expected post-discard data record, got {notification:?}");
                }
            }
            wait_for_drop_count(&drops, 1).await;
            assert_eq!(
                drops.get(),
                1,
                "discard-state recv buffer dropped more than once"
            );
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_dropped_recv_msg_partial_head_discards_tail_before_next_record() {
    let init = SctpInitConfig::diameter_default();
    let mut socket_config = SctpSocketConfig::data(init);
    socket_config.recv_rcvinfo = true;

    let Some(listener) = bind_sctp_listener_or_skip(
        "runtime_sctp_dropped_recv_msg_partial_head_discards_tail_before_next_record",
        socket_config,
    ) else {
        return;
    };
    let addr = listener.local_addr();
    let connector = SctpConnector::with_config(socket_config);
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            let (mut client, mut server) = accepted_sctp_pair(listener, connector, addr).await;

            let (send_res, _buf) = client
                .send_msg(vec![b't'; 16], test_send_info(1, 0x0102_0304))
                .await;
            assert_eq!(send_res.expect("oversized send_msg failed"), 16);

            let drops = Rc::new(Cell::new(0));
            let recv = DropTrackedReadWrite::zeroed(8, &drops);
            let mut dropped_head = Box::pin(server.recv_msg(recv, 8));
            let first_poll =
                std::future::poll_fn(|cx| Poll::Ready(Future::poll(dropped_head.as_mut(), cx)))
                    .await;
            assert!(
                matches!(first_poll, Poll::Pending),
                "partial-head recv should submit before being dropped"
            );
            drop(dropped_head);
            assert_eq!(drops.get(), 0, "stashed partial-head buffer dropped early");

            let (send_res, _buf) = client
                .send_msg(b"after".to_vec(), test_send_info(2, 0x0506_0708))
                .await;
            assert_eq!(send_res.expect("second send_msg failed"), 5);

            let recv_res = timeout(Duration::from_secs(1), server.recv_msg(vec![0u8; 64], 64))
                .await
                .expect("resynchronized SCTP recv timed out");
            let (recv_result, recv_buf) = recv_res;
            let (recv_len, meta) = recv_result.expect("resynchronized recv failed");
            assert_eq!(recv_len, 5);
            assert_eq!(&recv_buf[..recv_len], b"after");
            match meta {
                SctpRecvMeta::Data(info) => {
                    assert_eq!(info.stream_id, 2);
                    assert_eq!(info.ppid, 0x0506_0708);
                    assert!(info.end_of_record);
                }
                SctpRecvMeta::Notification(notification) => {
                    panic!("expected resynchronized data record, got {notification:?}");
                }
            }
            wait_for_drop_count(&drops, 1).await;
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_dropped_recv_msg_vectored_eor_retires_discard_state() {
    let init = SctpInitConfig::diameter_default();
    let mut socket_config = SctpSocketConfig::data(init);
    socket_config.recv_rcvinfo = true;

    let Some(listener) = bind_sctp_listener_or_skip(
        "runtime_sctp_dropped_recv_msg_vectored_eor_retires_discard_state",
        socket_config,
    ) else {
        return;
    };
    let addr = listener.local_addr();
    let connector = SctpConnector::with_config(socket_config);
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            let (mut client, mut server) = accepted_sctp_pair(listener, connector, addr).await;
            let mut pool = IoBuffPool::new(IoBuffPoolConfig {
                headroom: 0,
                payload: 4,
                tailroom: 0,
                objs_per_slab: 2,
            })
            .expect("pool config invalid");
            pool.init();

            let (send_res, _buf) = client
                .send_msg(vec![b'x'; 16], test_send_info(1, 0x0102_0304))
                .await;
            assert_eq!(send_res.expect("oversized send_msg failed"), 16);

            let (recv_res, recv_buf) = server.recv_msg(vec![0u8; 8], 8).await;
            let err = recv_res.expect_err("oversized SCTP record should enter discard state");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
            assert_eq!(&recv_buf[..], b"xxxxxxxx");

            let chain = pooled_sctp_recv_chain::<2>(&mut pool);
            assert_eq!(pool.live_slots_for_test(), 2);
            let mut discard = Box::pin(server.recv_msg_vectored(chain));
            let first_poll =
                std::future::poll_fn(|cx| Poll::Ready(Future::poll(discard.as_mut(), cx))).await;
            assert!(
                matches!(first_poll, Poll::Pending),
                "vectored discard recv should submit before being dropped"
            );
            drop(discard);
            assert_eq!(
                pool.live_slots_for_test(),
                2,
                "stashed vectored discard chain dropped early"
            );

            let (invalid, invalid_chain) = server.recv_msg_vectored(IoBuffVecMut::<1>::new()).await;
            let err = invalid.expect_err("empty vectored metadata receive unexpectedly succeeded");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(invalid_chain.segments(), 0);
            assert_eq!(
                pool.live_slots_for_test(),
                2,
                "invalid vectored metadata receive adopted the prior stash"
            );

            let (send_res, _buf) = client
                .send_msg(b"after".to_vec(), test_send_info(2, 0x0506_0708))
                .await;
            assert_eq!(send_res.expect("second send_msg failed"), 5);

            let recv_res = timeout(Duration::from_secs(1), server.recv_msg(vec![0u8; 64], 64))
                .await
                .expect("post-vectored-discard SCTP recv timed out");
            let (recv_result, recv_buf) = recv_res;
            let (recv_len, meta) = recv_result.expect("post-vectored-discard recv failed");
            assert_eq!(recv_len, 5);
            assert_eq!(&recv_buf[..recv_len], b"after");
            match meta {
                SctpRecvMeta::Data(info) => {
                    assert_eq!(info.stream_id, 2);
                    assert_eq!(info.ppid, 0x0506_0708);
                    assert!(info.end_of_record);
                }
                SctpRecvMeta::Notification(notification) => {
                    panic!("expected post-vectored-discard data record, got {notification:?}");
                }
            }
            wait_for_live_slots(&pool, 0).await;
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_vectored_empty_chain_semantics() {
    let Some((mut stream, _)) =
        raw_sctp_stream_or_skip("runtime_sctp_vectored_empty_chain_semantics")
    else {
        return;
    };

    let mut executor = Executor::new().expect("failed to construct executor");
    executor
        .run(async move {
            let empty_send = IoBuffVecMut::<0>::new().freeze();
            let (send_res, send_chain) = stream
                .send_msg_vectored(empty_send, SctpSendInfo::default())
                .await;
            let err = send_res.expect_err("send_msg_vectored empty should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(err.raw_os_error(), None);
            assert_eq!(err.to_string(), "zero-length SCTP send request");
            assert!(send_chain.is_empty());

            let mut zero_readable = IoBuffVecMut::<2>::new();
            zero_readable
                .push(IoBuffMut::new(0, 0, 0))
                .expect("first zero-readable send chain push failed");
            zero_readable
                .push(IoBuffMut::new(0, 0, 0))
                .expect("second zero-readable send chain push failed");
            let (send_res, send_chain) = stream
                .send_msg_vectored(zero_readable.freeze(), SctpSendInfo::default())
                .await;
            let err = send_res.expect_err("send_msg_vectored zero-readable should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(err.raw_os_error(), None);
            assert_eq!(err.to_string(), "zero-length SCTP send request");
            assert_eq!(send_chain.segments(), 2);
            assert!(send_chain.is_empty());

            let (recv_res, recv_chain) = stream.recv_msg_vectored(IoBuffVecMut::<0>::new()).await;
            let err = recv_res.expect_err("recv_msg_vectored empty should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            assert!(recv_chain.is_empty());

            let mut zero_writable = IoBuffVecMut::<1>::new();
            zero_writable
                .push(IoBuffMut::new(0, 0, 0))
                .expect("zero-capacity recv chain push failed");
            let (recv_res, recv_chain) = stream.recv_msg_vectored(zero_writable).await;
            let err = recv_res.expect_err("recv_msg_vectored zero-writable should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            assert!(recv_chain.is_empty());
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.sqe_submits, stats.close_ring_submissions);
        assert_eq!(stats.retained_pooled_allocs, 0);
        assert_eq!(stats.retained_heap_fallbacks, 0);
    }
}

#[test]
fn runtime_sctp_default_peer_addr_params_rejects_specific_address() {
    use std::net::{Ipv4Addr, SocketAddr};

    let init = SctpInitConfig::diameter_default();
    let mut listener = match SctpListener::bind(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        128,
        init,
    ) {
        Ok(listener) => listener,
        Err(err) => {
            if capability_unavailable(&err) {
                eprintln!(
                    "skipping runtime_sctp_default_peer_addr_params_rejects_specific_address: SCTP unsupported ({err})"
                );
                return;
            }
            panic!("failed to bind sctp listener: {err}");
        }
    };

    let mut executor = Executor::new().expect("failed to construct executor");
    let addr = listener.local_addr();
    let mut connector = SctpConnector::new(init);

    executor
        .run(async move {
            Executor::spawn(async move {
                let _ = listener.accept().await.expect("accept failed");
            })
            .expect("server spawn failed");

            let stream = connector
                .connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");

            let err = stream
                .set_default_peer_addr_params(SctpPeerAddrParams::for_address(addr))
                .expect_err("specific-address default peer params should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_zero_length_sends_outside_run_prefer_context_error() {
    let (socket, _peer) =
        std::os::unix::net::UnixStream::pair().expect("Unix socket pair creation failed");
    socket
        .set_nonblocking(true)
        .expect("Unix test socket nonblocking setup failed");
    let mut stream =
        SctpStream::from_owned_fd(socket.into(), SocketAddr::from((Ipv4Addr::LOCALHOST, 3868)));
    let drops = Rc::new(Cell::new(0));
    let mut cx = Context::from_waker(Waker::noop());

    let bytes = Vec::with_capacity(1);
    let expected_ptr = bytes.as_ptr();
    let buffer = DropTrackedReadOnly::new(bytes, &drops);
    let mut future = Box::pin(stream.send(buffer));
    match Future::poll(future.as_mut(), &mut cx) {
        Poll::Ready((Err(err), returned)) => {
            assert_eq!(err.kind(), std::io::ErrorKind::NotConnected);
            assert_eq!(returned.bytes().as_ptr(), expected_ptr);
            assert!(returned.bytes().is_empty());
            drop(returned);
        }
        Poll::Ready((Ok(_), _)) => panic!("inactive zero-length SCTP data send succeeded"),
        Poll::Pending => panic!("inactive zero-length SCTP data send remained pending"),
    }
    drop(future);

    let bytes = Vec::with_capacity(1);
    let expected_ptr = bytes.as_ptr();
    let buffer = DropTrackedReadOnly::new(bytes, &drops);
    let mut future = Box::pin(stream.send_msg(buffer, SctpSendInfo::default()));
    match Future::poll(future.as_mut(), &mut cx) {
        Poll::Ready((Err(err), returned)) => {
            assert_eq!(err.kind(), std::io::ErrorKind::NotConnected);
            assert_eq!(returned.bytes().as_ptr(), expected_ptr);
            assert!(returned.bytes().is_empty());
            drop(returned);
        }
        Poll::Ready((Ok(_), _)) => panic!("inactive zero-length SCTP metadata send succeeded"),
        Poll::Pending => panic!("inactive zero-length SCTP metadata send remained pending"),
    }
    drop(future);

    let mut zero_readable = IoBuffVecMut::<1>::new();
    zero_readable
        .push(IoBuffMut::new(4, 0, 0))
        .expect("zero-readable send chain push failed");
    let zero_readable = zero_readable.freeze();
    let expected_ptr = zero_readable
        .get(0)
        .expect("zero-readable segment missing")
        .as_ptr();
    let mut future = Box::pin(stream.send_msg_vectored(zero_readable, SctpSendInfo::default()));
    match Future::poll(future.as_mut(), &mut cx) {
        Poll::Ready((Err(err), returned)) => {
            assert_eq!(err.kind(), std::io::ErrorKind::NotConnected);
            assert_eq!(returned.segments(), 1);
            assert_eq!(
                returned
                    .get(0)
                    .expect("returned zero-readable segment missing")
                    .as_ptr(),
                expected_ptr
            );
        }
        Poll::Ready((Ok(_), _)) => panic!("inactive zero-readable SCTP send succeeded"),
        Poll::Pending => panic!("inactive zero-readable SCTP send remained pending"),
    }
    drop(future);

    assert_eq!(drops.get(), 2);
}

#[cfg(any(debug_assertions, feature = "test-support"))]
#[test]
fn runtime_sctp_zero_length_sends_return_owners_without_submission() {
    let (socket, _peer) =
        std::os::unix::net::UnixStream::pair().expect("Unix socket pair creation failed");
    socket
        .set_nonblocking(true)
        .expect("Unix test socket nonblocking setup failed");
    let mut stream =
        SctpStream::from_owned_fd(socket.into(), SocketAddr::from((Ipv4Addr::LOCALHOST, 3868)));

    let drops = Rc::new(Cell::new(0));
    let returned_stream = Rc::new(Cell::new(None));
    let test_drops = Rc::clone(&drops);
    let stream_slot = Rc::clone(&returned_stream);
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            let bytes = Vec::with_capacity(1);
            let expected_ptr = bytes.as_ptr();
            let buffer = DropTrackedReadOnly::new(bytes, &test_drops);
            let (result, returned) = stream.send(buffer).await;
            let err = result.expect_err("zero-length SCTP data send should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(err.raw_os_error(), None);
            assert_eq!(err.to_string(), "zero-length SCTP send request");
            assert_eq!(returned.bytes().as_ptr(), expected_ptr);
            assert!(returned.bytes().is_empty());
            assert_eq!(test_drops.get(), 0);
            drop(returned);
            assert_eq!(test_drops.get(), 1);

            let bytes = Vec::with_capacity(1);
            let expected_ptr = bytes.as_ptr();
            let buffer = DropTrackedReadOnly::new(bytes, &test_drops);
            let (result, returned) = stream.send_msg(buffer, SctpSendInfo::default()).await;
            let err = result.expect_err("zero-length SCTP metadata send should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(err.raw_os_error(), None);
            assert_eq!(err.to_string(), "zero-length SCTP send request");
            assert_eq!(returned.bytes().as_ptr(), expected_ptr);
            assert!(returned.bytes().is_empty());
            assert_eq!(test_drops.get(), 1);
            drop(returned);
            assert_eq!(test_drops.get(), 2);

            let empty = IoBuffVecMut::<0>::new().freeze();
            let (result, returned) = stream
                .send_msg_vectored(empty, SctpSendInfo::default())
                .await;
            let err = result.expect_err("empty SCTP vectored send should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(err.raw_os_error(), None);
            assert_eq!(err.to_string(), "zero-length SCTP send request");
            assert_eq!(returned.segments(), 0);

            let mut zero_readable = IoBuffVecMut::<2>::new();
            zero_readable
                .push(IoBuffMut::new(4, 0, 0))
                .expect("first zero-readable send chain push failed");
            zero_readable
                .push(IoBuffMut::new(4, 0, 0))
                .expect("second zero-readable send chain push failed");
            let zero_readable = zero_readable.freeze();
            let first_ptr = zero_readable
                .get(0)
                .expect("first zero-readable segment missing")
                .as_ptr();
            let second_ptr = zero_readable
                .get(1)
                .expect("second zero-readable segment missing")
                .as_ptr();
            let (result, returned) = stream
                .send_msg_vectored(zero_readable, SctpSendInfo::default())
                .await;
            let err = result.expect_err("zero-readable SCTP vectored send should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(err.raw_os_error(), None);
            assert_eq!(err.to_string(), "zero-length SCTP send request");
            assert_eq!(returned.segments(), 2);
            assert!(returned.is_empty());
            assert_eq!(
                returned
                    .get(0)
                    .expect("returned first zero-readable segment missing")
                    .as_ptr(),
                first_ptr
            );
            assert_eq!(
                returned
                    .get(1)
                    .expect("returned second zero-readable segment missing")
                    .as_ptr(),
                second_ptr
            );

            stream_slot.set(Some(stream));
        })
        .expect("zero-length SCTP send validation run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.sqe_submits, 0);
        assert_eq!(stats.retained_pooled_allocs, 0);
        assert_eq!(stats.retained_heap_fallbacks, 0);
    }
    assert_eq!(drops.get(), 2);
    drop(
        returned_stream
            .take()
            .expect("zero-length send validation did not return its stream"),
    );
}

#[cfg(any(debug_assertions, feature = "test-support"))]
#[test]
fn runtime_sctp_zero_length_data_recv_returns_buffer_without_submission() {
    let (socket, _peer) =
        std::os::unix::net::UnixStream::pair().expect("Unix socket pair creation failed");
    socket
        .set_nonblocking(true)
        .expect("Unix test socket nonblocking setup failed");
    let mut stream =
        SctpStream::from_owned_fd(socket.into(), SocketAddr::from((Ipv4Addr::LOCALHOST, 3868)));

    let pointer_calls = Rc::new(Cell::new(0));
    let drops = Rc::new(Cell::new(0));
    let returned_stream = Rc::new(Cell::new(None));
    let returned_buffer = Rc::new(Cell::new(None));
    let test_pointer_calls = Rc::clone(&pointer_calls);
    let test_drops = Rc::clone(&drops);
    let stream_slot = Rc::clone(&returned_stream);
    let buffer_slot = Rc::clone(&returned_buffer);
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            let buffer = PointerTrackedReadWrite::new(8, 84, &test_pointer_calls, &test_drops);
            let (result, buffer) = stream.recv(buffer, 0).await;
            let err = result.expect_err("zero-length data receive should be rejected");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(err.raw_os_error(), None);
            assert_eq!(buffer.identity, 84);
            assert_eq!(test_pointer_calls.get(), 0);
            assert_eq!(test_drops.get(), 0);
            buffer_slot.set(Some(buffer));
            stream_slot.set(Some(stream));
        })
        .expect("zero-length SCTP receive validation run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.sqe_submits, 0);
        assert_eq!(stats.retained_pooled_allocs, 0);
        assert_eq!(stats.retained_heap_fallbacks, 0);
    }
    assert_eq!(pointer_calls.get(), 0);
    assert_eq!(drops.get(), 0);

    let returned_buffer = returned_buffer
        .take()
        .expect("zero-length receive did not return its buffer");
    assert_eq!(returned_buffer.identity, 84);
    drop(returned_buffer);
    assert_eq!(drops.get(), 1);
    drop(
        returned_stream
            .take()
            .expect("zero-length receive did not return its stream"),
    );
}

#[test]
fn runtime_sctp_fast_send_recv() {
    use std::net::{Ipv4Addr, SocketAddr};

    let init = SctpInitConfig::diameter_default();
    let mut socket_config = SctpSocketConfig::data(init);
    socket_config.default_send_info = Some(SctpSendInfo {
        stream_id: 1,
        flags: 0,
        ppid: 0x0102_0304,
        context: 0,
        assoc_id: 0,
    });

    let mut listener = match SctpListener::bind_with_config(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        128,
        socket_config,
    ) {
        Ok(listener) => listener,
        Err(err) => {
            if capability_unavailable(&err) {
                eprintln!("skipping runtime_sctp_fast_send_recv: SCTP unsupported ({err})");
                return;
            }
            panic!("failed to bind sctp listener: {err}");
        }
    };

    let mut executor = Executor::new().expect("failed to construct executor");
    let addr = listener.local_addr();
    let mut connector = SctpConnector::with_config(socket_config);

    executor
        .run(async move {
            Executor::spawn(async move {
                let (mut stream, _remote) = listener.accept().await.expect("accept failed");
                let recv = IoBuffMut::new(0, 64, 0);
                let (recv_res, recv_buf) = stream.recv(recv, 4).await;
                let recv_len = recv_res.expect("server recv failed");
                assert_eq!(recv_len, 4);
                assert_eq!(recv_buf.payload_bytes(), b"ping");

                let (send_res, _buf) = stream.send(recv_buf).await;
                assert_eq!(send_res.expect("server send failed"), 4);
            })
            .expect("server spawn failed");

            let mut stream = connector
                .connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");

            let mut send = IoBuffMut::new(0, 64, 0);
            send.payload_append(b"ping").unwrap();
            let (send_res, _buf) = stream.send(send).await;
            assert_eq!(send_res.expect("client send failed"), 4);

            let mut recv = IoBuffMut::new(0, 64, 0);
            recv.payload_append(b"HEAD").unwrap();
            let (recv_res, recv_buf) = stream.recv(recv, 4).await;
            let recv_len = recv_res.expect("client recv failed");
            assert_eq!(recv_len, 4);
            assert_eq!(recv_buf.payload_bytes(), b"HEADping");

            let mut zero = IoBuffMut::new(0, 4, 0);
            zero.payload_append(b"HEAD").unwrap();
            let (zero_res, zero) = stream.recv(zero, 0).await;
            let err = zero_res.expect_err("zero-length data recv should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(zero.payload_bytes(), b"HEAD");

            let mut invalid = IoBuffMut::new(0, 6, 0);
            invalid.payload_append(b"HEAD").unwrap();
            let (invalid_res, invalid) = stream.recv(invalid, 3).await;
            let err = invalid_res.expect_err("oversize data recv should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(invalid.payload_bytes(), b"HEAD");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_cancelled_data_recv_retains_buffer_until_cqe() {
    let config = SctpSocketConfig::data(SctpInitConfig::diameter_default());
    let Some(listener) = bind_sctp_listener_or_skip(
        "runtime_sctp_cancelled_data_recv_retains_buffer_until_cqe",
        config,
    ) else {
        return;
    };
    let addr = listener.local_addr();
    let connector = SctpConnector::with_config(config);
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            let (mut client, mut server) = accepted_sctp_pair(listener, connector, addr).await;

            let drops = Rc::new(Cell::new(0));
            let recv = DropTrackedReadWrite::zeroed(64, &drops);
            let result = timeout(Duration::from_millis(10), async {
                let (res, _buf) = client.recv(recv, 64).await;
                res
            })
            .await;
            assert!(
                matches!(result, Err(TimeoutError::Elapsed)),
                "sctp data recv should time out: {result:?}"
            );
            assert_eq!(
                drops.get(),
                0,
                "sctp data recv buffer dropped while original SQE was live"
            );

            let (send_res, _buf) = server.send(b"x".to_vec()).await;
            send_res.expect("server data send failed");
            wait_for_drop_count(&drops, 1).await;
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_cancelled_recv_msg_retains_buffer_until_cqe() {
    let mut config = SctpSocketConfig::data(SctpInitConfig::diameter_default());
    config.recv_rcvinfo = true;
    let Some(listener) = bind_sctp_listener_or_skip(
        "runtime_sctp_cancelled_recv_msg_retains_buffer_until_cqe",
        config,
    ) else {
        return;
    };
    let addr = listener.local_addr();
    let connector = SctpConnector::with_config(config);
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            let (mut client, mut server) = accepted_sctp_pair(listener, connector, addr).await;

            let drops = Rc::new(Cell::new(0));
            let recv = DropTrackedReadWrite::zeroed(64, &drops);
            let result = timeout(Duration::from_millis(10), async {
                let (res, _buf) = client.recv_msg(recv, 64).await;
                res
            })
            .await;
            assert!(
                matches!(result, Err(TimeoutError::Elapsed)),
                "sctp recv_msg should time out: {result:?}"
            );
            assert_eq!(
                drops.get(),
                0,
                "sctp recv_msg buffer dropped while original SQE was live"
            );

            assert_repeated_lean_rejection(&mut client, "scalar rich receive").await;
            assert_eq!(
                drops.get(),
                0,
                "lean rejection retired the dropped rich receive early"
            );

            let (send_res, _buf) = server
                .send_msg(b"x".to_vec(), test_send_info(1, 0x0102_0304))
                .await;
            send_res.expect("server first metadata send failed");
            let (send_res, _buf) = server
                .send_msg(b"y".to_vec(), test_send_info(2, 0x0506_0708))
                .await;
            send_res.expect("server second metadata send failed");
            let recv_res = timeout(Duration::from_secs(1), client.recv_msg(vec![0u8; 16], 16))
                .await
                .expect("adopting recv_msg timed out");
            let (recv_result, recv_buf) = recv_res;
            let (recv_len, meta) = recv_result.expect("adopting recv_msg failed");
            assert_eq!(recv_len, 1);
            assert_eq!(&recv_buf[..recv_len], b"y");
            match meta {
                SctpRecvMeta::Data(info) => {
                    assert_eq!(info.stream_id, 2);
                    assert_eq!(info.ppid, 0x0506_0708);
                }
                SctpRecvMeta::Notification(notification) => {
                    panic!("expected data after adopting dropped recv_msg, got {notification:?}");
                }
            }
            wait_for_drop_count(&drops, 1).await;
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_cancelled_recv_msg_vectored_retains_chain_until_cqe() {
    let mut config = SctpSocketConfig::data(SctpInitConfig::diameter_default());
    config.recv_rcvinfo = true;
    let Some(listener) = bind_sctp_listener_or_skip(
        "runtime_sctp_cancelled_recv_msg_vectored_retains_chain_until_cqe",
        config,
    ) else {
        return;
    };
    let addr = listener.local_addr();
    let connector = SctpConnector::with_config(config);
    let mut executor = Executor::new().expect("failed to construct executor");
    let (mut client, mut server) =
        run_test_output(&mut executor, accepted_sctp_pair(listener, connector, addr));
    executor
        .run(async move {
            let mut pool = IoBuffPool::new(IoBuffPoolConfig {
                headroom: 0,
                payload: 64,
                tailroom: 0,
                objs_per_slab: 2,
            })
            .expect("pool config invalid");
            pool.init();

            let mut chain = IoBuffVecMut::<2>::new();
            chain
                .push(pool.alloc().expect("first recv alloc failed"))
                .unwrap();
            chain
                .push(pool.alloc().expect("second recv alloc failed"))
                .unwrap();
            assert_eq!(pool.live_slots_for_test(), 2);

            let result = timeout(Duration::from_millis(10), async {
                let (res, _chain) = client.recv_msg_vectored(chain).await;
                res
            })
            .await;
            assert!(
                matches!(result, Err(TimeoutError::Elapsed)),
                "sctp recv_msg_vectored should time out: {result:?}"
            );
            assert_eq!(
                pool.live_slots_for_test(),
                2,
                "sctp recv_msg_vectored chain released before original CQE retired"
            );

            assert_repeated_lean_rejection(&mut client, "vectored rich receive").await;
            assert_eq!(
                pool.live_slots_for_test(),
                2,
                "lean rejection retired the dropped vectored receive early"
            );

            let (send_res, _buf) = server
                .send_msg(b"xy".to_vec(), test_send_info(1, 0x0102_0304))
                .await;
            send_res.expect("server first metadata send failed");
            let (send_res, _buf) = server
                .send_msg(b"z".to_vec(), test_send_info(2, 0x0506_0708))
                .await;
            send_res.expect("server second metadata send failed");
            let recv_res = timeout(Duration::from_secs(1), client.recv_msg(vec![0u8; 16], 16))
                .await
                .expect("adopting recv_msg after vectored drop timed out");
            let (recv_result, recv_buf) = recv_res;
            let (recv_len, meta) =
                recv_result.expect("adopting recv_msg after vectored drop failed");
            assert_eq!(recv_len, 1);
            assert_eq!(&recv_buf[..recv_len], b"z");
            match meta {
                SctpRecvMeta::Data(info) => {
                    assert_eq!(info.stream_id, 2);
                    assert_eq!(info.ppid, 0x0506_0708);
                }
                SctpRecvMeta::Notification(notification) => {
                    panic!(
                        "expected data after adopting dropped vectored recv, got {notification:?}"
                    );
                }
            }
            wait_for_live_slots(&pool, 0).await;
            assert_eq!(
                pool.live_slots_for_test(),
                0,
                "vectored receive chain did not retire exactly after its target CQE"
            );
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(
            stats.retained_pooled_allocs, 4,
            "expected one vectored receive, two sends, and one adopting receive"
        );
        assert_eq!(
            stats.retained_pooled_frees, 4,
            "all four retained payloads must retire after their target CQEs"
        );
        assert_eq!(
            stats.retained_heap_fallbacks, 0,
            "known-size SCTP payloads should remain pooled"
        );
        assert_eq!(
            stats.retained_heap_frees, 0,
            "no heap-backed SCTP payload should require release"
        );
    }
}

#[test]
fn runtime_sctp_cancelled_data_send_retains_buffer_until_cqe() {
    let config = SctpSocketConfig::data(SctpInitConfig::diameter_default());
    let Some(listener) = bind_sctp_listener_or_skip(
        "runtime_sctp_cancelled_data_send_retains_buffer_until_cqe",
        config,
    ) else {
        return;
    };
    let addr = listener.local_addr();
    let connector = SctpConnector::with_config(config);
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            let (mut client, mut server) = accepted_sctp_pair(listener, connector, addr).await;

            let drops = Rc::new(Cell::new(0));
            let payload = DropTrackedReadOnly::new(b"send".to_vec(), &drops);
            let mut send = Box::pin(client.send(payload));
            let first_poll =
                std::future::poll_fn(|cx| Poll::Ready(Future::poll(send.as_mut(), cx))).await;

            match first_poll {
                Poll::Pending => {
                    drop(send);
                    assert_eq!(
                        drops.get(),
                        0,
                        "sctp data send buffer dropped while original SQE was live"
                    );
                    let recv = vec![0u8; 16];
                    let _ = timeout(Duration::from_millis(100), server.recv(recv, 16)).await;
                    wait_for_drop_count(&drops, 1).await;
                }
                Poll::Ready((_res, returned)) => {
                    assert_eq!(drops.get(), 0, "sctp data send returned buffer");
                    drop(returned);
                    assert_eq!(drops.get(), 1, "sctp data send buffer dropped once");
                }
            }
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_cancelled_send_msg_retains_buffer_until_cqe() {
    let config = SctpSocketConfig::rich(SctpInitConfig::diameter_default());
    let Some(listener) = bind_sctp_listener_or_skip(
        "runtime_sctp_cancelled_send_msg_retains_buffer_until_cqe",
        config,
    ) else {
        return;
    };
    let addr = listener.local_addr();
    let connector = SctpConnector::with_config(config);
    let mut executor = Executor::new().expect("failed to construct executor");
    let (mut client, mut server) =
        run_test_output(&mut executor, accepted_sctp_pair(listener, connector, addr));
    executor
        .run(async move {
            let drops = Rc::new(Cell::new(0));
            let payload = DropTrackedReadOnly::new(b"send-msg".to_vec(), &drops);
            let mut send = Box::pin(client.send_msg(payload, SctpSendInfo::default()));
            let first_poll =
                std::future::poll_fn(|cx| Poll::Ready(Future::poll(send.as_mut(), cx))).await;

            assert!(
                matches!(first_poll, Poll::Pending),
                "fresh nonempty send_msg must submit before it can complete"
            );
            drop(send);
            assert_eq!(
                drops.get(),
                0,
                "sctp send_msg buffer dropped while original SQE was live"
            );
            let recv = vec![0u8; 16];
            let _ = timeout(Duration::from_millis(100), server.recv_msg(recv, 16)).await;
            wait_for_drop_count(&drops, 1).await;
            assert_eq!(
                drops.get(),
                1,
                "send_msg buffer did not retire exactly with its target CQE"
            );
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.retained_pooled_allocs, 2);
        assert_eq!(stats.retained_pooled_frees, 2);
        assert_eq!(stats.retained_heap_fallbacks, 0);
        assert_eq!(stats.retained_heap_frees, 0);
    }
}

#[test]
fn runtime_sctp_cancelled_send_msg_vectored_retains_chain_until_cqe() {
    let config = SctpSocketConfig::rich(SctpInitConfig::diameter_default());
    let Some(listener) = bind_sctp_listener_or_skip(
        "runtime_sctp_cancelled_send_msg_vectored_retains_chain_until_cqe",
        config,
    ) else {
        return;
    };
    let addr = listener.local_addr();
    let connector = SctpConnector::with_config(config);
    let mut executor = Executor::new().expect("failed to construct executor");
    let (mut client, mut server) =
        run_test_output(&mut executor, accepted_sctp_pair(listener, connector, addr));
    executor
        .run(async move {
            let mut pool = IoBuffPool::new(IoBuffPoolConfig {
                headroom: 0,
                payload: 8,
                tailroom: 0,
                objs_per_slab: 2,
            })
            .expect("pool config invalid");
            pool.init();

            let chain = pooled_sctp_payload_chain(&mut pool);
            assert_eq!(pool.live_slots_for_test(), 2);

            let mut send = Box::pin(client.send_msg_vectored(chain, SctpSendInfo::default()));
            let first_poll =
                std::future::poll_fn(|cx| Poll::Ready(Future::poll(send.as_mut(), cx))).await;

            assert!(
                matches!(first_poll, Poll::Pending),
                "fresh nonempty send_msg_vectored must submit before it can complete"
            );
            drop(send);
            assert_eq!(
                pool.live_slots_for_test(),
                2,
                "sctp send_msg_vectored chain released before original CQE retired"
            );
            let recv = vec![0u8; 16];
            let _ = timeout(Duration::from_millis(100), server.recv_msg(recv, 16)).await;
            wait_for_live_slots(&pool, 0).await;
            assert_eq!(
                pool.live_slots_for_test(),
                0,
                "vectored send chain did not retire exactly with its target CQE"
            );
        })
        .expect("executor run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.retained_pooled_allocs, 2);
        assert_eq!(stats.retained_pooled_frees, 2);
        assert_eq!(stats.retained_heap_fallbacks, 0);
        assert_eq!(stats.retained_heap_frees, 0);
    }
}

#[test]
fn runtime_sctp_kernel_error_send_returns_payload_once() {
    let config = SctpSocketConfig::data(SctpInitConfig::diameter_default());
    let Some(listener) = bind_sctp_listener_or_skip(
        "runtime_sctp_kernel_error_send_returns_payload_once",
        config,
    ) else {
        return;
    };
    let addr = listener.local_addr();
    let connector = SctpConnector::with_config(config);
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            let (mut client, _server) = accepted_sctp_pair(listener, connector, addr).await;
            client
                .shutdown(Shutdown::Write)
                .expect("client shutdown write failed");

            let drops = Rc::new(Cell::new(0));
            let payload = DropTrackedReadOnly::new(b"sctp-error".to_vec(), &drops);
            let (res, returned) = client.send(payload).await;
            let err = res.expect_err("sctp send after write shutdown should fail");
            assert_sctp_send_kernel_error(&err);
            assert_eq!(drops.get(), 0, "sctp send payload dropped before return");
            drop(returned);
            assert_eq!(drops.get(), 1, "sctp send payload dropped exactly once");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_multistream_long_lived() {
    use std::net::{Ipv4Addr, SocketAddr};

    let init = SctpInitConfig {
        outbound_streams: 8,
        inbound_streams: 8,
        ..SctpInitConfig::diameter_default()
    };
    let mut listener =
        match SctpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128, init) {
            Ok(listener) => listener,
            Err(err) => {
                if capability_unavailable(&err) {
                    eprintln!(
                        "skipping runtime_sctp_multistream_long_lived: SCTP unsupported ({err})"
                    );
                    return;
                }
                panic!("failed to bind sctp listener: {err}");
            }
        };

    let mut executor = Executor::new().expect("failed to construct executor");
    let addr = listener.local_addr();
    let mut connector = SctpConnector::new(init);
    let rounds = 32usize;
    let stream_count = 4usize;
    let msg_size = 256usize;

    executor
        .run(async move {
            Executor::spawn(async move {
                let (mut stream, _remote) = listener.accept().await.expect("accept failed");

                let mut current_buf = vec![0u8; msg_size];
                for round in 0..rounds {
                    let expected_stream = (round % stream_count) as u16;
                    let expected_ppid = 0x0102_0304u32 + round as u32;
                    let expected_payload = format!("ping-{round:02}-stream-{expected_stream}");

                    let (recv_len, info, recv_buf) = loop {
                        let recv_res = stream.recv_msg(current_buf, msg_size).await;
                        let (recv_len, meta) = recv_res.0.expect("server recv failed");
                        match meta {
                            SctpRecvMeta::Notification(_) => current_buf = recv_res.1,
                            SctpRecvMeta::Data(info) => break (recv_len, info, recv_res.1),
                        }
                    };
                    assert_eq!(recv_len, expected_payload.len());
                    assert_eq!(&recv_buf[..recv_len], expected_payload.as_bytes());
                    assert_eq!(info.stream_id, expected_stream);
                    assert_eq!(info.ppid, expected_ppid);

                    current_buf = recv_buf;
                    let (send_res, send_buf) = stream
                        .send_msg(
                            current_buf,
                            SctpSendInfo {
                                stream_id: expected_stream,
                                flags: 0,
                                ppid: expected_ppid,
                                context: round as u32,
                                assoc_id: 0,
                            },
                        )
                        .await;
                    assert_eq!(
                        send_res.expect("server send failed"),
                        expected_payload.len()
                    );
                    current_buf = send_buf;
                }
            })
            .expect("server spawn failed");

            let mut stream = connector
                .connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");

            let mut current_buf = vec![0u8; msg_size];
            for round in 0..rounds {
                let stream_id = (round % stream_count) as u16;
                let ppid = 0x0102_0304u32 + round as u32;
                let payload = format!("ping-{round:02}-stream-{stream_id}").into_bytes();

                let (send_res, _buf) = stream
                    .send_msg(
                        payload.clone(),
                        SctpSendInfo {
                            stream_id,
                            flags: 0,
                            ppid,
                            context: round as u32,
                            assoc_id: 0,
                        },
                    )
                    .await;
                assert_eq!(send_res.expect("client send failed"), payload.len());

                let (recv_len, info, recv_buf) = loop {
                    let recv_res = stream.recv_msg(current_buf, msg_size).await;
                    let (recv_len, meta) = recv_res.0.expect("client recv failed");
                    match meta {
                        SctpRecvMeta::Notification(_) => current_buf = recv_res.1,
                        SctpRecvMeta::Data(info) => break (recv_len, info, recv_res.1),
                    }
                };
                assert_eq!(recv_len, payload.len());
                assert_eq!(&recv_buf[..recv_len], payload.as_slice());
                assert_eq!(info.stream_id, stream_id);
                assert_eq!(info.ppid, ppid);
                current_buf = recv_buf;
            }
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_shutdown_write_peer_observes_terminal_state() {
    use std::net::{Ipv4Addr, SocketAddr};

    let init = SctpInitConfig::diameter_default();
    let mut listener = match SctpListener::bind(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        128,
        init,
    ) {
        Ok(listener) => listener,
        Err(err) => {
            if capability_unavailable(&err) {
                eprintln!(
                    "skipping runtime_sctp_shutdown_write_peer_observes_terminal_state: SCTP unsupported ({err})"
                );
                return;
            }
            panic!("failed to bind sctp listener: {err}");
        }
    };

    let mut executor = Executor::new().expect("failed to construct executor");
    let addr = listener.local_addr();
    let mut connector = SctpConnector::new(init);

    executor
        .run(async move {
            Executor::spawn(async move {
                let (mut stream, _remote) = listener.accept().await.expect("accept failed");
                let mut current_buf = vec![0u8; 256];
                let mut saw_data = false;
                let mut saw_shutdown_notification = false;
                let mut saw_eof = false;

                while !(saw_data && (saw_shutdown_notification || saw_eof)) {
                    let recv_res =
                        timeout(Duration::from_secs(1), stream.recv_msg(current_buf, 256))
                            .await
                            .expect("server recv timed out");
                    let (recv_len, meta) = recv_res.0.expect("server recv failed");
                    current_buf = recv_res.1;

                    match meta {
                        SctpRecvMeta::Notification(SctpNotification::Shutdown { .. }) => {
                            saw_shutdown_notification = true;
                        }
                        SctpRecvMeta::Notification(_) => {}
                        SctpRecvMeta::Data(info) => {
                            assert!(!saw_data, "unexpected extra data after shutdown");
                            assert_eq!(recv_len, 4);
                            assert_eq!(&current_buf[..recv_len], b"ping");
                            assert_eq!(info.stream_id, 1);
                            assert_eq!(info.ppid, 0x0102_0304);
                            saw_data = true;
                        }
                    }

                    if recv_len == 0 {
                        saw_eof = true;
                    }
                }

                assert!(
                    saw_data,
                    "peer did not receive the queued data before shutdown"
                );
                assert!(
                    saw_shutdown_notification || saw_eof,
                    "peer did not observe SCTP shutdown notification or EOF"
                );
            })
            .expect("server spawn failed");

            let mut stream = connector
                .connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");

            let (send_res, _buf) = stream
                .send_msg(
                    b"ping".to_vec(),
                    SctpSendInfo {
                        stream_id: 1,
                        flags: 0,
                        ppid: 0x0102_0304,
                        context: 0,
                        assoc_id: 0,
                    },
                )
                .await;
            assert_eq!(send_res.expect("client send failed"), 4);

            stream
                .shutdown(std::net::Shutdown::Write)
                .expect("shutdown failed");
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_reusable_dropped_connect_retains_socket_until_connect_cqe() {
    if std::env::var_os(SCTP_CONNECT_REUSE_CHILD_ENV).is_none() {
        common::run_exact_test_child_with_watchdog(
            SCTP_CONNECT_REUSE_TEST,
            SCTP_CONNECT_REUSE_CHILD_ENV,
            Duration::from_secs(8),
        );
        return;
    }

    const TEST_NAME: &str =
        "runtime_sctp_reusable_dropped_connect_retains_socket_until_connect_cqe";
    let init = SctpInitConfig::diameter_default();
    let first_listener =
        match SctpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128, init) {
            Ok(listener) => listener,
            Err(err) if capability_unavailable(&err) => {
                eprintln!("skipping {TEST_NAME}: SCTP unsupported ({err})");
                return;
            }
            Err(err) => panic!("failed to bind first SCTP listener for {TEST_NAME}: {err}"),
        };
    let mut second_listener =
        SctpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128, init).unwrap_or_else(
            |err| panic!("failed to bind second SCTP listener for {TEST_NAME}: {err}"),
        );
    let first_addr = first_listener.local_addr();
    let second_addr = second_listener.local_addr();
    assert_ne!(first_addr, second_addr);

    let mut executor = Executor::new().expect("failed to construct executor");
    let (first_fd, second_fd) = run_test_output(&mut executor, async move {
        let mut connector = SctpConnector::new(init);
        let first_fd = lowest_available_fd();
        let first = connector
            .connect(first_addr)
            .expect("first SCTP connect initialization failed");
        assert!(
            raw_fd_is_open(first_fd),
            "first SCTP connect did not acquire the predicted lowest descriptor",
        );
        poll_once_pending(first).await;
        assert!(
            raw_fd_is_open(first_fd),
            "dropped SCTP CONNECT released its socket before the queued target CQE",
        );

        let second_fd = lowest_available_fd();
        assert_ne!(
            second_fd, first_fd,
            "the still-kernel-visible SCTP CONNECT descriptor became reusable",
        );
        let second = connector
            .connect(second_addr)
            .expect("second SCTP connect initialization failed");
        assert!(
            raw_fd_is_open(second_fd),
            "second SCTP connect did not acquire the predicted descriptor",
        );
        let second = second.await.expect("second SCTP connect failed");
        assert_eq!(second.as_raw_fd(), second_fd);
        assert_eq!(second.peer_addr(), second_addr);
        let client_addr = second.local_addr().expect("SCTP client local_addr failed");
        let (_server, remote_addr) = timeout(Duration::from_secs(1), second_listener.accept())
            .await
            .expect("second SCTP listener accept timed out")
            .expect("second SCTP listener accept failed");
        assert_eq!(remote_addr, client_addr);
        (first_fd, second_fd)
    });

    assert!(
        !raw_fd_is_open(first_fd),
        "cancelled SCTP CONNECT retained its socket after target retirement",
    );
    assert!(
        !raw_fd_is_open(second_fd),
        "completed SCTP stream did not close exactly once after scope exit",
    );
    drop(first_listener);
}

#[test]
fn runtime_sctp_connect_timeout_success() {
    use std::net::{Ipv4Addr, SocketAddr};

    let init = SctpInitConfig::diameter_default();
    let mut listener =
        match SctpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128, init) {
            Ok(listener) => listener,
            Err(err) => {
                if capability_unavailable(&err) {
                    eprintln!(
                        "skipping runtime_sctp_connect_timeout_success: SCTP unsupported ({err})"
                    );
                    return;
                }
                panic!("failed to bind sctp listener: {err}");
            }
        };

    let mut executor = Executor::new().expect("failed to construct executor");
    let addr = listener.local_addr();
    let mut connector = SctpConnector::new(init);

    executor
        .run(async move {
            Executor::spawn(async move {
                let _ = listener.accept().await.expect("accept failed");
            })
            .expect("server spawn failed");

            let stream = connector
                .connect_timeout(addr, Duration::from_secs(1))
                .expect("connect_timeout init failed")
                .await
                .expect("connect_timeout failed");
            assert_eq!(stream.peer_addr(), addr);
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_accepted_option_failure_closes_once_and_reaccepts() {
    const TEST_NAME: &str = "runtime_sctp_accepted_option_failure_closes_once_and_reaccepts";
    let config = SctpSocketConfig::data(SctpInitConfig::diameter_default());
    let Some(mut listener) = bind_sctp_listener_or_skip(TEST_NAME, config) else {
        return;
    };
    let addr = listener.local_addr();
    let mut connector = SctpConnector::with_config(config);
    let mut executor = Executor::new().expect("failed to construct executor");

    let (mut listener, mut connector, first_client) = run_test_output(&mut executor, async move {
        let first_connect = Executor::spawn(async move {
            let first_client = connector
                .connect_timeout(addr, Duration::from_secs(1))
                .expect("first connect_timeout init failed")
                .await
                .expect("first SCTP connect failed");
            (connector, first_client)
        })
        .expect("first connect spawn failed");

        let (failed_accept, configure_calls) = timeout(
            Duration::from_secs(1),
            test_accept_with_established_config_error(&mut listener, libc::EIO),
        )
        .await
        .expect("injected SCTP accept timed out");
        let err = match failed_accept {
            Err(err) => err,
            Ok((server, _remote_addr)) => {
                drop(server);
                panic!("injected SCTP accepted-option failure returned a stream")
            }
        };
        assert_eq!(err.raw_os_error(), Some(libc::EIO));
        assert_eq!(configure_calls, 1);
        assert!(!listener.is_terminal());

        let (connector, first_client) = timeout(Duration::from_secs(1), first_connect)
            .await
            .expect("first SCTP connect task timed out")
            .expect("first SCTP connect task was cancelled");
        (listener, connector, first_client)
    });

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.close_ring_submissions, 1);
        assert_eq!(stats.close_ring_fallbacks, 0);
        assert_eq!(stats.close_direct_closes, 0);
        assert_eq!(stats.close_worker_admissions, 0);
        assert_eq!(stats.close_linger_queries, 0);
        assert_eq!(stats.close_linger_classification_failures, 0);
        assert_eq!(stats.close_worker_full_fallbacks, 0);
        assert_eq!(stats.close_worker_disconnected_fallbacks, 0);
        assert_eq!(stats.close_linger_waivers, 0);
        assert_eq!(stats.close_linger_waiver_failures, 0);
        assert_eq!(stats.sqe_submits, stats.cqe_completions);
    }

    let held = run_test_output(&mut executor, async move {
        let second_connect = Executor::spawn(async move {
            let second_client = connector
                .connect_timeout(addr, Duration::from_secs(1))
                .expect("second connect_timeout init failed")
                .await
                .expect("second SCTP connect failed");
            (connector, second_client)
        })
        .expect("second connect spawn failed");

        let (second_server, remote_addr) = timeout(Duration::from_secs(1), listener.accept())
            .await
            .expect("second SCTP accept timed out")
            .expect("second SCTP accept failed");
        let (connector, second_client) = timeout(Duration::from_secs(1), second_connect)
            .await
            .expect("second SCTP connect task timed out")
            .expect("second SCTP connect task was cancelled");
        assert_eq!(
            remote_addr,
            second_client
                .local_addr()
                .expect("second SCTP client local_addr failed")
        );
        assert!(!listener.is_terminal());

        (
            listener,
            connector,
            first_client,
            second_client,
            second_server,
        )
    });

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.close_ring_submissions, 0);
        assert_eq!(stats.close_ring_fallbacks, 0);
        assert_eq!(stats.close_direct_closes, 0);
        assert_eq!(stats.close_worker_admissions, 0);
        assert_eq!(stats.close_linger_queries, 0);
        assert_eq!(stats.sqe_submits, stats.cqe_completions);
    }

    drop(held);
    drop(executor);
}

/// Dropping an accept future before any association completes leaves the
/// listener reusable for a later connector.
#[test]
fn runtime_sctp_accept_drop_then_reaccepts() {
    use std::net::{Ipv4Addr, SocketAddr};

    let init = SctpInitConfig::diameter_default();
    let mut listener =
        match SctpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128, init) {
            Ok(listener) => listener,
            Err(err) => {
                if capability_unavailable(&err) {
                    eprintln!(
                        "skipping runtime_sctp_accept_drop_then_reaccepts: SCTP unsupported ({err})"
                    );
                    return;
                }
                panic!("failed to bind sctp listener: {err}");
            }
        };

    let mut executor = Executor::new().expect("failed to construct executor");
    let addr = listener.local_addr();
    let mut connector = SctpConnector::new(init);

    executor
        .run(async move {
            let mut accept = Box::pin(listener.accept());
            std::future::poll_fn(|cx| match Future::poll(accept.as_mut(), cx) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(_) => panic!("accept completed before test connector started"),
            })
            .await;
            drop(accept);

            let server = Executor::spawn(async move { listener.accept().await })
                .expect("server accept spawn failed");
            let stream = connector
                .connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");
            let (_server_stream, remote_addr) = server
                .await
                .expect("server accept task cancelled")
                .expect("second accept failed");
            assert_eq!(
                remote_addr,
                stream.local_addr().expect("client local_addr failed")
            );
        })
        .expect("executor run failed");
}

/// Cancelling readiness does not consume an established queued association;
/// the next accept receives it and the reusable slot remains usable.
#[test]
fn runtime_sctp_cancelled_accept_preserves_backlog_and_reaccepts() {
    use std::net::{Ipv4Addr, SocketAddr};

    let init = SctpInitConfig::diameter_default();
    let mut listener = match SctpListener::bind(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        128,
        init,
    ) {
        Ok(listener) => listener,
        Err(err) => {
            if capability_unavailable(&err) {
                eprintln!(
                    "skipping runtime_sctp_cancelled_accept_preserves_backlog_and_reaccepts: SCTP unsupported ({err})"
                );
                return;
            }
            panic!("failed to bind sctp listener: {err}");
        }
    };

    let mut executor = Executor::new().expect("failed to construct executor");
    let addr = listener.local_addr();
    let mut connector = SctpConnector::new(init);

    executor
        .run(async move {
            let mut accept = Box::pin(listener.accept());
            std::future::poll_fn(|cx| match Future::poll(accept.as_mut(), cx) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(_) => panic!("accept completed before test connector started"),
            })
            .await;

            let queued_stream = connector
                .connect_timeout(addr, Duration::from_secs(1))
                .expect("queued connect_timeout init failed")
                .await
                .expect("queued connect failed");
            let queued_addr = queued_stream
                .local_addr()
                .expect("queued client local_addr failed");
            drop(accept);

            let (queued_server, remote_addr) = timeout(Duration::from_secs(1), listener.accept())
                .await
                .expect("queued accept timed out")
                .expect("queued accept failed");
            assert_eq!(remote_addr, queued_addr);
            drop(queued_server);
            drop(queued_stream);

            let server = Executor::spawn(async move { listener.accept().await })
                .expect("server accept spawn failed");
            let stream = connector
                .connect_timeout(addr, Duration::from_secs(1))
                .expect("second connect_timeout init failed")
                .await
                .expect("second connect failed");
            let (_server_stream, remote_addr) = timeout(Duration::from_secs(1), server)
                .await
                .expect("second accept timed out")
                .expect("server accept task cancelled")
                .expect("second accept failed");
            assert_eq!(
                remote_addr,
                stream.local_addr().expect("client local_addr failed")
            );
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_accept_rearms_after_stale_readiness() {
    let config = SctpSocketConfig::data(SctpInitConfig::diameter_default());
    let Some(mut listener) =
        bind_sctp_listener_or_skip("runtime_sctp_accept_rearms_after_stale_readiness", config)
    else {
        return;
    };
    let addr = listener.local_addr();
    let listener_fd = listener.as_raw_fd();
    let mut connector = SctpConnector::with_config(config);
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            let mut accept = Box::pin(listener.accept());
            std::future::poll_fn(|cx| match Future::poll(accept.as_mut(), cx) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(_) => panic!("accept completed before first association"),
            })
            .await;

            let first_client = connector
                .connect_timeout(addr, Duration::from_secs(1))
                .expect("first connect_timeout init failed")
                .await
                .expect("first SCTP connect failed");
            sleep(Duration::from_millis(10))
                .await
                .expect("SCTP readiness wait failed");

            // This raw accept is an intentional test-only violation of the
            // documented no-concurrent-accept contract. It makes the completed
            // readiness stale before FlowIO performs its owner-thread accept4.
            let stolen = unsafe {
                libc::accept4(
                    listener_fd,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                )
            };
            assert!(
                stolen >= 0,
                "external SCTP accept should steal first readiness: {}",
                std::io::Error::last_os_error()
            );
            // SAFETY: the successful test accept4 returned one sole-owned fd.
            let stolen = unsafe { OwnedFd::from_raw_fd(stolen) };

            std::future::poll_fn(|cx| match Future::poll(accept.as_mut(), cx) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(_) => panic!("stale SCTP readiness was not rearmed"),
            })
            .await;

            let second_client = connector
                .connect_timeout(addr, Duration::from_secs(1))
                .expect("second connect_timeout init failed")
                .await
                .expect("second SCTP connect failed");
            let second_addr = second_client
                .local_addr()
                .expect("second SCTP client local_addr failed");
            let (_server, remote_addr) = timeout(Duration::from_secs(1), accept)
                .await
                .expect("rearmed SCTP accept timed out")
                .expect("rearmed SCTP accept failed");
            assert_eq!(remote_addr, second_addr);

            drop(stolen);
            drop(first_client);
            drop(second_client);
        })
        .expect("stale SCTP readiness run failed");
    #[cfg(debug_assertions)]
    {
        assert_eq!(executor.last_stats().accept_readiness_rearms, 1);
        assert_eq!(
            executor.last_stats().accept_descriptor_exhaustions,
            0,
            "ordinary stale readiness must not count descriptor exhaustion"
        );
    }
}

#[test]
fn runtime_sctp_completed_readiness_context_rejection_preserves_backlog() {
    let config = SctpSocketConfig::data(SctpInitConfig::diameter_default());
    let Some(mut listener) = bind_sctp_listener_or_skip(
        "runtime_sctp_completed_readiness_context_rejection_preserves_backlog",
        config,
    ) else {
        return;
    };
    let addr = listener.local_addr();
    let mut connector = SctpConnector::with_config(config);
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(async move {
            let mut accept = Box::pin(listener.accept());
            std::future::poll_fn(|cx| match Future::poll(accept.as_mut(), cx) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(_) => panic!("accept completed before queued association"),
            })
            .await;

            let client = connector
                .connect_timeout(addr, Duration::from_secs(1))
                .expect("queued connect_timeout init failed")
                .await
                .expect("queued SCTP connect failed");
            let client_addr = client
                .local_addr()
                .expect("queued SCTP client local_addr failed");
            sleep(Duration::from_millis(10))
                .await
                .expect("SCTP readiness wait failed");

            let mut invalid_cx = Context::from_waker(Waker::noop());
            let err = match Future::poll(accept.as_mut(), &mut invalid_cx) {
                Poll::Ready(Err(err)) => err,
                Poll::Ready(Ok(_)) => {
                    panic!("invalid-context SCTP accept unexpectedly succeeded")
                }
                Poll::Pending => panic!("completed SCTP readiness remained pending"),
            };
            assert_eq!(err.kind(), std::io::ErrorKind::NotConnected);
            drop(accept);

            let (_server, remote_addr) = timeout(Duration::from_secs(1), listener.accept())
                .await
                .expect("origin-context SCTP reaccept timed out")
                .expect("origin-context SCTP reaccept failed");
            assert_eq!(remote_addr, client_addr);
        })
        .expect("SCTP context-rejection run failed");
}

#[test]
fn runtime_sctp_connect_timeout_propagates_connect_error() {
    const TEST_NAME: &str = "runtime_sctp_connect_timeout_propagates_connect_error";

    let init = SctpInitConfig::diameter_default();
    let Some((refusal_guard, addr)) = bound_non_listening_sctp_endpoint_or_skip(TEST_NAME) else {
        return;
    };

    let mut executor = Executor::new().expect("failed to construct executor");
    let mut connector = SctpConnector::new(init);

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
    drop(refusal_guard);
}

#[test]
fn runtime_sctp_connect_timeout_preserves_timer_runtime_error() {
    let init = SctpInitConfig::diameter_default();
    let listener = match SctpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128, init) {
        Ok(listener) => listener,
        Err(err) => {
            if capability_unavailable(&err) {
                eprintln!(
                    "skipping runtime_sctp_connect_timeout_preserves_timer_runtime_error: SCTP unsupported ({err})"
                );
                return;
            }
            panic!("failed to bind sctp listener: {err}");
        }
    };
    let addr = listener.local_addr();
    let mut executor = Executor::new().expect("failed to construct executor");
    let mut connector = SctpConnector::new(init);

    executor
        .run(async move {
            test_hooks::fail_next_timer_alloc();
            let result = connector
                .connect_timeout(addr, Duration::from_secs(1))
                .expect("connect_timeout init failed")
                .await;
            let err = match result {
                Ok(_) => panic!("timer allocation failure should abort connect_timeout"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), std::io::ErrorKind::OutOfMemory);
        })
        .expect("executor run failed");
}

// ============================================================================
// IoBuffMut / IoBuff transport integration tests
// ============================================================================

/// SCTP ping-pong using IoBuffMut for send/recv instead of Vec<u8>.
#[test]
fn runtime_sctp_ping_pong_iobuff() {
    use std::net::{Ipv4Addr, SocketAddr};

    let init = SctpInitConfig::diameter_default();
    let mut listener =
        match SctpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128, init) {
            Ok(listener) => listener,
            Err(err) => {
                if capability_unavailable(&err) {
                    eprintln!("skipping runtime_sctp_ping_pong_iobuff: SCTP unsupported ({err})");
                    return;
                }
                panic!("failed to bind sctp listener: {err}");
            }
        };

    let mut executor = Executor::new().expect("failed to construct executor");
    let addr = listener.local_addr();
    let mut connector = SctpConnector::new(init);
    let msg_size = 256;

    executor
        .run(async move {
            Executor::spawn(async move {
                let (mut stream, _remote) = listener.accept().await.expect("accept failed");

                // Receive with IoBuffMut, skipping notifications. Reset before
                // reuse so published notification bytes do not become a prefix
                // of the eventual application data record.
                let mut current_buf = IoBuffMut::new(0, msg_size, 0);
                let (recv_len, meta, recv_buf) = loop {
                    let recv_res = stream.recv_msg(current_buf, msg_size).await;
                    let (recv_len, meta) = recv_res.0.expect("server recv failed");
                    match meta {
                        SctpRecvMeta::Notification(_) => {
                            let mut buf = recv_res.1;
                            buf.reset();
                            current_buf = buf;
                        }
                        SctpRecvMeta::Data(info) => {
                            break (recv_len, SctpRecvMeta::Data(info), recv_res.1);
                        }
                    }
                };
                assert_eq!(recv_len, 4);
                assert_eq!(recv_buf.payload_bytes()[..recv_len], *b"ping");
                match meta {
                    SctpRecvMeta::Data(info) => {
                        assert_eq!(info.stream_id, 1);
                        assert_eq!(info.ppid, 0x0102_0304);
                    }
                    _ => panic!("expected data, got notification"),
                }

                // Echo back using the received IoBuffMut directly.
                let (send_res, _buf) = stream
                    .send_msg(
                        recv_buf,
                        SctpSendInfo {
                            stream_id: 1,
                            flags: 0,
                            ppid: 0x0102_0304,
                            context: 0,
                            assoc_id: 0,
                        },
                    )
                    .await;
                send_res.expect("server send failed");
            })
            .expect("server spawn failed");

            let mut stream = connector
                .connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");

            // Send with IoBuffMut.
            let mut cli_send = IoBuffMut::new(0, msg_size, 0);
            cli_send.payload_append(b"ping").unwrap();
            let (send_res, _) = stream
                .send_msg(
                    cli_send,
                    SctpSendInfo {
                        stream_id: 1,
                        flags: 0,
                        ppid: 0x0102_0304,
                        context: 0,
                        assoc_id: 0,
                    },
                )
                .await;
            send_res.expect("client send failed");

            // Receive with IoBuffMut, skip notifications.
            let mut current_buf = IoBuffMut::new(0, msg_size + 4, 0);
            current_buf.payload_append(b"HEAD").unwrap();
            let (recv_len, meta, recv_buf) = loop {
                let recv_res = stream.recv_msg(current_buf, msg_size).await;
                let (recv_len, meta) = recv_res.0.expect("client recv failed");
                match meta {
                    SctpRecvMeta::Notification(_) => {
                        let mut buf = recv_res.1;
                        buf.reset();
                        buf.payload_append(b"HEAD").unwrap();
                        current_buf = buf;
                    }
                    SctpRecvMeta::Data(info) => {
                        break (recv_len, SctpRecvMeta::Data(info), recv_res.1);
                    }
                }
            };
            assert_eq!(recv_len, 4);
            assert_eq!(recv_buf.payload_bytes(), b"HEADping");
            match meta {
                SctpRecvMeta::Data(info) => {
                    assert_eq!(info.stream_id, 1);
                    assert_eq!(info.ppid, 0x0102_0304);
                }
                _ => panic!("expected data, got notification"),
            }
        })
        .expect("executor run failed");
}

#[test]
fn runtime_sctp_recv_msg_rejects_oversize_iobuff() {
    use std::net::{Ipv4Addr, SocketAddr};

    let init = SctpInitConfig::diameter_default();
    let mut listener = match SctpListener::bind(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        128,
        init,
    ) {
        Ok(listener) => listener,
        Err(err) => {
            if capability_unavailable(&err) {
                eprintln!(
                    "skipping runtime_sctp_recv_msg_rejects_oversize_iobuff: SCTP unsupported ({err})"
                );
                return;
            }
            panic!("failed to bind sctp listener: {err}");
        }
    };

    let mut executor = Executor::new().expect("failed to construct executor");
    let addr = listener.local_addr();
    let mut connector = SctpConnector::new(init);

    executor
        .run(async move {
            Executor::spawn(async move {
                let (_stream, _remote) = listener.accept().await.expect("accept failed");
            })
            .expect("accept spawn failed");

            let mut stream = connector
                .connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");

            let mut recv = IoBuffMut::new(0, 8, 0);
            recv.payload_append(b"HEAD").unwrap();
            let (res, buf) = stream.recv_msg(recv, 5).await;
            let err = res.expect_err("oversize recv_msg should fail");
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(buf.payload_bytes(), b"HEAD");
            assert_eq!(buf.payload_remaining(), 4);
        })
        .expect("executor run failed");
}

#[test]
#[cfg(all(target_os = "linux", target_pointer_width = "64", not(miri)))]
fn runtime_sctp_send_rejects_oversize_iobuff() {
    use std::net::{Ipv4Addr, SocketAddr};

    let oversized =
        SparseOversizedReadOnly::new().expect("failed to reserve sparse oversized mapping");
    let init = SctpInitConfig::diameter_default();
    let listener = match SctpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128, init) {
        Ok(listener) => listener,
        Err(err) => {
            if capability_unavailable(&err) {
                eprintln!(
                    "skipping runtime_sctp_send_rejects_oversize_iobuff: SCTP unsupported ({err})"
                );
                return;
            }
            panic!("failed to bind sctp listener: {err}");
        }
    };

    let mut executor = Executor::new().expect("failed to construct executor");
    let addr = listener.local_addr();
    let connector = SctpConnector::new(init);

    let (mut stream, server) =
        run_test_output(&mut executor, accepted_sctp_pair(listener, connector, addr));

    let (_oversized, _stream, _server) = run_test_output(&mut executor, async move {
        let (res, oversized) = stream.send(oversized).await;
        assert_oversized_send_rejected(res, &oversized);

        let (res, oversized) = stream.send_msg(oversized, SctpSendInfo::default()).await;
        assert_oversized_send_rejected(res, &oversized);

        (oversized, stream, server)
    });

    #[cfg(debug_assertions)]
    assert_eq!(
        executor.last_stats().sqe_submits,
        0,
        "oversized SCTP sends should submit no SQE"
    );
}

// ============================================================================
// Vectored I/O (send_msg_vectored / recv_msg_vectored) tests
// ============================================================================

const SCTP_TEST_ACTIVE_IOVEC_LIMIT: usize = 1024;
const SCTP_TEST_EXCESS_CAPACITY: usize = SCTP_TEST_ACTIVE_IOVEC_LIMIT + 1;

fn make_many_sctp_send_segments<const N: usize>(
    segments: usize,
    empty_segment: Option<usize>,
) -> (IoBuffVec<N>, Vec<u8>) {
    assert!(segments <= N);
    let mut chain = IoBuffVecMut::<N>::new();
    let mut expected = Vec::with_capacity(segments);
    for index in 0..segments {
        let empty = empty_segment == Some(index);
        let mut segment = IoBuffMut::new(0, usize::from(!empty), 0);
        if !empty {
            let byte = ((index * 17 + 3) % 251) as u8;
            segment
                .payload_append(&[byte])
                .expect("SCTP boundary send segment initialization failed");
            expected.push(byte);
        }
        chain
            .push(segment)
            .expect("SCTP boundary send chain push failed");
    }
    (chain.freeze(), expected)
}

fn make_many_sctp_recv_segments<const N: usize>(
    segments: usize,
    empty_segment: Option<usize>,
) -> IoBuffVecMut<N> {
    assert!(segments <= N);
    let mut chain = IoBuffVecMut::<N>::new();
    for index in 0..segments {
        let capacity = usize::from(empty_segment != Some(index));
        chain
            .push(IoBuffMut::new(0, capacity, 0))
            .expect("SCTP boundary receive chain push failed");
    }
    chain
}

fn collect_many_sctp_recv_segments<const N: usize>(chain: &IoBuffVecMut<N>) -> Vec<u8> {
    let mut bytes = Vec::new();
    for index in 0..chain.segments() {
        bytes.extend_from_slice(
            chain
                .get(index)
                .expect("SCTP boundary receive segment missing")
                .payload_bytes(),
        );
    }
    bytes
}

#[test]
fn runtime_sctp_active_iovec_limit_rejects_before_submission_and_returns_owners() {
    if std::env::var_os(SCTP_ACTIVE_IOVEC_REJECTION_CHILD_ENV).is_none() {
        common::run_exact_test_child_with_watchdog_env(
            SCTP_ACTIVE_IOVEC_REJECTION_TEST,
            SCTP_ACTIVE_IOVEC_REJECTION_CHILD_ENV,
            Duration::from_secs(10),
            &[("RUST_MIN_STACK", SCTP_ACTIVE_IOVEC_TEST_STACK_BYTES)],
        );
        return;
    }

    let (socket, _peer) =
        std::os::unix::net::UnixStream::pair().expect("Unix socket pair creation failed");
    socket
        .set_nonblocking(true)
        .expect("Unix test socket nonblocking setup failed");
    let mut stream =
        SctpStream::from_owned_fd(socket.into(), SocketAddr::from((Ipv4Addr::LOCALHOST, 3868)));
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(Box::pin(async move {
            let (send_chain, _) = make_many_sctp_send_segments::<SCTP_TEST_EXCESS_CAPACITY>(
                SCTP_TEST_EXCESS_CAPACITY,
                None,
            );
            let first_send_ptr = send_chain
                .get(0)
                .expect("first over-limit send segment missing")
                .as_ptr();
            let last_send_ptr = send_chain
                .get(SCTP_TEST_ACTIVE_IOVEC_LIMIT)
                .expect("last over-limit send segment missing")
                .as_ptr();
            let (send_result, returned_send) =
                Box::pin(stream.send_msg_vectored(send_chain, SctpSendInfo::default())).await;
            let send_err = send_result.expect_err("over-limit SCTP send should fail");
            assert_eq!(send_err.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(send_err.raw_os_error(), None);
            assert_eq!(returned_send.segments(), SCTP_TEST_EXCESS_CAPACITY);
            assert_eq!(
                returned_send
                    .get(0)
                    .expect("returned first over-limit send segment missing")
                    .as_ptr(),
                first_send_ptr
            );
            assert_eq!(
                returned_send
                    .get(SCTP_TEST_ACTIVE_IOVEC_LIMIT)
                    .expect("returned last over-limit send segment missing")
                    .as_ptr(),
                last_send_ptr
            );
            drop(returned_send);

            let mut recv_chain = make_many_sctp_recv_segments::<SCTP_TEST_EXCESS_CAPACITY>(
                SCTP_TEST_EXCESS_CAPACITY,
                None,
            );
            let first_recv_ptr = recv_chain
                .get_mut(0)
                .expect("first over-limit receive segment missing")
                .as_mut_ptr();
            let last_recv_ptr = recv_chain
                .get_mut(SCTP_TEST_ACTIVE_IOVEC_LIMIT)
                .expect("last over-limit receive segment missing")
                .as_mut_ptr();
            let (recv_result, mut returned_recv) =
                Box::pin(stream.recv_msg_vectored(recv_chain)).await;
            let recv_err = recv_result.expect_err("over-limit SCTP receive should fail");
            assert_eq!(recv_err.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(recv_err.raw_os_error(), None);
            assert_eq!(returned_recv.segments(), SCTP_TEST_EXCESS_CAPACITY);
            assert_eq!(
                returned_recv
                    .get_mut(0)
                    .expect("returned first over-limit receive segment missing")
                    .as_mut_ptr(),
                first_recv_ptr
            );
            assert_eq!(
                returned_recv
                    .get_mut(SCTP_TEST_ACTIVE_IOVEC_LIMIT)
                    .expect("returned last over-limit receive segment missing")
                    .as_mut_ptr(),
                last_recv_ptr
            );
            drop(returned_recv);
            drop(stream);
        }))
        .expect("SCTP active-iovec rejection run failed");

    #[cfg(debug_assertions)]
    {
        let stats = executor.last_stats();
        assert_eq!(stats.sqe_submits, stats.close_ring_submissions);
        assert_eq!(stats.retained_pooled_allocs, 0);
        assert_eq!(stats.retained_heap_fallbacks, 0);
    }
}

#[test]
fn runtime_sctp_active_iovec_boundary_accepts_sparse_and_excess_capacity_chains() {
    if std::env::var_os(SCTP_ACTIVE_IOVEC_BOUNDARY_CHILD_ENV).is_none() {
        common::run_exact_test_child_with_watchdog_env(
            SCTP_ACTIVE_IOVEC_BOUNDARY_TEST,
            SCTP_ACTIVE_IOVEC_BOUNDARY_CHILD_ENV,
            Duration::from_secs(15),
            &[("RUST_MIN_STACK", SCTP_ACTIVE_IOVEC_TEST_STACK_BYTES)],
        );
        return;
    }

    let init = SctpInitConfig::diameter_default();
    let config = SctpSocketConfig {
        notifications: SctpNotificationMask::none(),
        ..SctpSocketConfig::rich(init)
    };
    let Some(listener) = bind_sctp_listener_or_skip(SCTP_ACTIVE_IOVEC_BOUNDARY_TEST, config) else {
        return;
    };
    let addr = listener.local_addr();
    let connector = SctpConnector::with_config(config);
    let mut executor = Executor::new().expect("failed to construct executor");

    executor
        .run(Box::pin(async move {
            let (mut client, mut server) = accepted_sctp_pair(listener, connector, addr).await;

            let (send_chain, expected) = make_many_sctp_send_segments::<SCTP_TEST_EXCESS_CAPACITY>(
                SCTP_TEST_ACTIVE_IOVEC_LIMIT,
                None,
            );
            let (send_result, returned_send) = timeout(
                Duration::from_secs(1),
                Box::pin(client.send_msg_vectored(send_chain, SctpSendInfo::default())),
            )
            .await
            .expect("SCTP excess-capacity boundary send timed out");
            assert_eq!(
                send_result.expect("SCTP excess-capacity boundary send failed"),
                expected.len()
            );
            assert_eq!(returned_send.segments(), SCTP_TEST_ACTIVE_IOVEC_LIMIT);
            drop(returned_send);

            let recv_chain = make_many_sctp_recv_segments::<SCTP_TEST_EXCESS_CAPACITY>(
                SCTP_TEST_EXCESS_CAPACITY,
                Some(SCTP_TEST_ACTIVE_IOVEC_LIMIT / 2),
            );
            let (recv_result, returned_recv) = timeout(
                Duration::from_secs(1),
                Box::pin(server.recv_msg_vectored(recv_chain)),
            )
            .await
            .expect("SCTP sparse boundary receive timed out");
            let (received, meta) = recv_result.expect("SCTP sparse boundary receive failed");
            assert_eq!(received, expected.len());
            assert!(matches!(meta, SctpRecvMeta::Data(_)));
            assert_eq!(collect_many_sctp_recv_segments(&returned_recv), expected);
            drop(returned_recv);

            let (send_chain, expected) = make_many_sctp_send_segments::<SCTP_TEST_EXCESS_CAPACITY>(
                SCTP_TEST_EXCESS_CAPACITY,
                Some(SCTP_TEST_ACTIVE_IOVEC_LIMIT / 3),
            );
            let (send_result, returned_send) = timeout(
                Duration::from_secs(1),
                Box::pin(server.send_msg_vectored(send_chain, SctpSendInfo::default())),
            )
            .await
            .expect("SCTP sparse boundary send timed out");
            assert_eq!(
                send_result.expect("SCTP sparse boundary send failed"),
                expected.len()
            );
            assert_eq!(returned_send.segments(), SCTP_TEST_EXCESS_CAPACITY);
            drop(returned_send);

            let recv_chain = make_many_sctp_recv_segments::<SCTP_TEST_EXCESS_CAPACITY>(
                SCTP_TEST_ACTIVE_IOVEC_LIMIT,
                None,
            );
            let (recv_result, returned_recv) = timeout(
                Duration::from_secs(1),
                Box::pin(client.recv_msg_vectored(recv_chain)),
            )
            .await
            .expect("SCTP excess-capacity boundary receive timed out");
            let (received, meta) =
                recv_result.expect("SCTP excess-capacity boundary receive failed");
            assert_eq!(received, expected.len());
            assert!(matches!(meta, SctpRecvMeta::Data(_)));
            assert_eq!(collect_many_sctp_recv_segments(&returned_recv), expected);
        }))
        .expect("SCTP active-iovec boundary run failed");
}

/// SCTP ping-pong using vectored send/recv with IoBuffVecMut.
#[test]
fn runtime_sctp_ping_pong_vectored() {
    use std::net::{Ipv4Addr, SocketAddr};

    let init = SctpInitConfig::diameter_default();
    let mut listener =
        match SctpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), 128, init) {
            Ok(listener) => listener,
            Err(err) => {
                if capability_unavailable(&err) {
                    eprintln!("skipping runtime_sctp_ping_pong_vectored: SCTP unsupported ({err})");
                    return;
                }
                panic!("failed to bind sctp listener: {err}");
            }
        };

    let mut executor = Executor::new().expect("failed to construct executor");
    let addr = listener.local_addr();
    let mut connector = SctpConnector::new(init);

    executor
        .run(async move {
            Executor::spawn(async move {
                let (mut stream, _remote) = listener.accept().await.expect("accept failed");

                // Receive with single-buffer recv_msg, skip notifications.
                let mut current_buf = vec![0u8; 256];
                let (recv_len, _meta, _recv_buf) = loop {
                    let recv_res = stream.recv_msg(current_buf, 256).await;
                    let (recv_len, meta) = recv_res.0.expect("server recv failed");
                    match meta {
                        SctpRecvMeta::Notification(_) => current_buf = recv_res.1,
                        SctpRecvMeta::Data(info) => {
                            break (recv_len, SctpRecvMeta::Data(info), recv_res.1);
                        }
                    }
                };
                assert_eq!(recv_len, 11);

                // Echo back using vectored send: 2 segments.
                let mut seg1 = IoBuffMut::new(0, 16, 0);
                seg1.payload_append(b"reply:").unwrap();
                let mut seg2 = IoBuffMut::new(0, 16, 0);
                seg2.payload_append(b"ok").unwrap();

                let mut send_chain = IoBuffVecMut::<2>::new();
                send_chain.push(seg1).unwrap();
                send_chain.push(seg2).unwrap();
                let frozen = send_chain.freeze();

                let (send_res, _chain) = stream
                    .send_msg_vectored(
                        frozen,
                        SctpSendInfo {
                            stream_id: 1,
                            flags: 0,
                            ppid: 0x0102_0304,
                            context: 0,
                            assoc_id: 0,
                        },
                    )
                    .await;
                send_res.expect("server send_msg_vectored failed");
            })
            .expect("server spawn failed");

            let mut stream = connector
                .connect(addr)
                .expect("connect init failed")
                .await
                .expect("connect failed");

            // Send with vectored: 3 segments "hello" + " " + "world".
            let mut seg1 = IoBuffMut::new(0, 16, 0);
            seg1.payload_append(b"hello").unwrap();
            let mut seg2 = IoBuffMut::new(0, 16, 0);
            seg2.payload_append(b" ").unwrap();
            let mut seg3 = IoBuffMut::new(0, 16, 0);
            seg3.payload_append(b"world").unwrap();

            let mut send_chain = IoBuffVecMut::<3>::new();
            send_chain.push(seg1).unwrap();
            send_chain.push(seg2).unwrap();
            send_chain.push(seg3).unwrap();
            let frozen = send_chain.freeze();

            let (send_res, _chain) = stream
                .send_msg_vectored(
                    frozen,
                    SctpSendInfo {
                        stream_id: 1,
                        flags: 0,
                        ppid: 0x0102_0304,
                        context: 0,
                        assoc_id: 0,
                    },
                )
                .await;
            send_res.expect("client send_msg_vectored failed");

            // Receive reply with vectored recv, skip notifications.
            let mut recv_chain = IoBuffVecMut::<2>::new();
            recv_chain.push(IoBuffMut::new(0, 128, 0)).unwrap();
            recv_chain.push(IoBuffMut::new(0, 128, 0)).unwrap();

            let (recv_len, meta, _recv_chain) = loop {
                let recv_res = stream.recv_msg_vectored(recv_chain).await;
                let (recv_len, meta) = recv_res.0.expect("client recv_msg_vectored failed");
                match meta {
                    SctpRecvMeta::Notification(_) => {
                        // A notification returns the rented chain; rebuild it
                        // before the next recv attempt.
                        recv_chain = IoBuffVecMut::<2>::new();
                        recv_chain.push(IoBuffMut::new(0, 128, 0)).unwrap();
                        recv_chain.push(IoBuffMut::new(0, 128, 0)).unwrap();
                    }
                    SctpRecvMeta::Data(info) => {
                        break (recv_len, SctpRecvMeta::Data(info), recv_res.1);
                    }
                }
            };
            assert_eq!(recv_len, 8);
            match meta {
                SctpRecvMeta::Data(info) => {
                    assert_eq!(info.stream_id, 1);
                    assert_eq!(info.ppid, 0x0102_0304);
                }
                _ => panic!("expected data, got notification"),
            }
        })
        .expect("executor run failed");
}
