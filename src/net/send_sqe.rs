//! Shared socket SQE builders for TCP, Unix, UDP and SCTP transports.

use io_uring::{opcode, squeue, types};
use std::os::fd::RawFd;

/// Bounds a validated scalar request to the kernel's signed byte-count range.
#[inline(always)]
fn clamp_socket_io_len(len: u32) -> u32 {
    len.min(i32::MAX as u32)
}

/// Builds a positive-length receive SQE without message flags, capped at
/// `i32::MAX` bytes.
#[inline(always)]
pub(crate) fn build_recv_entry(fd: RawFd, ptr: *mut u8, len: u32, user_data: u64) -> squeue::Entry {
    debug_assert!(len > 0, "socket receives require a nonzero byte count");
    let entry = opcode::Recv::new(types::Fd(fd), ptr, clamp_socket_io_len(len))
        .build()
        .user_data(user_data);
    #[cfg(test)]
    test_support::observe_socket_entry(&entry);
    entry
}

/// Builds a receive-message SQE without message flags.
#[inline(always)]
pub(crate) fn build_recvmsg_entry(
    fd: RawFd,
    msg: *mut libc::msghdr,
    user_data: u64,
) -> squeue::Entry {
    opcode::RecvMsg::new(types::Fd(fd), msg)
        .build()
        .user_data(user_data)
}

/// Builds a single-buffer send SQE with the transport's message flags, capped at
/// `i32::MAX` bytes.
#[inline(always)]
pub(crate) fn build_send_entry(
    fd: RawFd,
    ptr: *const u8,
    len: u32,
    flags: i32,
    user_data: u64,
) -> squeue::Entry {
    let entry = opcode::Send::new(types::Fd(fd), ptr, clamp_socket_io_len(len))
        .flags(flags)
        .build()
        .user_data(user_data);
    #[cfg(test)]
    test_support::observe_socket_entry(&entry);
    entry
}

/// Builds a sendmsg SQE with the transport's message flags.
#[inline(always)]
pub(crate) fn build_sendmsg_entry(
    fd: RawFd,
    msg: *const libc::msghdr,
    flags: u32,
    user_data: u64,
) -> squeue::Entry {
    let entry = opcode::SendMsg::new(types::Fd(fd), msg)
        .flags(flags)
        .build()
        .user_data(user_data);
    #[cfg(test)]
    test_support::observe_socket_entry(&entry);
    entry
}

#[cfg(test)]
pub(crate) mod test_support {
    use crate::runtime::test_hooks;
    use io_uring::squeue;
    use std::future::Future;
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    thread_local! {
        static SOCKET_ENTRY: std::cell::RefCell<Option<Option<squeue::Entry>>> =
            const { std::cell::RefCell::new(None) };
    }

    pub(super) fn observe_socket_entry(entry: &squeue::Entry) {
        SOCKET_ENTRY.with(|observed| {
            if let Some(slot) = observed.borrow_mut().as_mut() {
                assert!(slot.is_none(), "socket operation built more than one entry");
                *slot = Some(entry.clone());
            }
        });
    }

    /// Captures one socket entry while restoring any outer observation.
    pub(crate) struct SocketEntryCapture {
        previous: Option<Option<squeue::Entry>>,
    }

    impl SocketEntryCapture {
        pub(crate) fn new() -> Self {
            Self {
                previous: SOCKET_ENTRY.with(|observed| observed.replace(Some(None))),
            }
        }

        pub(crate) fn take(&self) -> squeue::Entry {
            SOCKET_ENTRY.with(|observed| {
                observed
                    .borrow_mut()
                    .as_mut()
                    .expect("socket observation is not active")
                    .take()
                    .expect("socket operation did not build an entry")
            })
        }
    }

    impl Drop for SocketEntryCapture {
        fn drop(&mut self) {
            SOCKET_ENTRY.with(|observed| observed.replace(self.previous.take()));
        }
    }

    /// Predicts the next operation slot before the future allocates it.
    /// The pool reuses the most recently freed slot; no other slot may be
    /// allocated before the future's first poll.
    pub(crate) fn next_operation_user_data(
        owner: &std::rc::Rc<crate::runtime::executor::ExecutorOwner>,
    ) -> u64 {
        let reactor = owner.reactor_ptr();
        let state = unsafe { (&mut *reactor).alloc_op() };
        assert!(!state.is_null());
        unsafe { (&mut *reactor).free_op(state) };
        state as u64
    }

    pub(crate) struct ExpectedMessageEntry {
        fd: i32,
        addr: u64,
        msg_flags: u32,
        user_data: u64,
    }

    /// Predicts a retained header from its payload layout and the reused pool slot.
    pub(crate) fn next_message_entry<T: 'static>(
        owner: &std::rc::Rc<crate::runtime::executor::ExecutorOwner>,
        header_offset: usize,
        fd: i32,
        msg_flags: u32,
    ) -> ExpectedMessageEntry {
        use crate::runtime::reactor::Reactor;
        use crate::runtime::retained::with_raw_retained_slot;

        assert!(header_offset + std::mem::size_of::<libc::msghdr>() <= std::mem::size_of::<T>());
        // These small payloads use a pool class. The reservation is released
        // uninitialized, and no same-class allocation intervenes before polling.
        assert!(std::mem::size_of::<T>() <= 65_536);
        let pool = unsafe { Reactor::retained_payload_pool_ptr(owner.reactor_ptr()) };
        let addr = unsafe {
            with_raw_retained_slot::<T, _>(pool, |mut slot| {
                slot.as_mut_ptr() as usize + header_offset
            })
        } as u64;
        ExpectedMessageEntry {
            fd,
            addr,
            msg_flags,
            user_data: next_operation_user_data(owner),
        }
    }

    fn poll_rejected_submission<F, B>(
        cx: &mut Context<'_>,
        future: &mut F,
        kind: &str,
    ) -> (SocketEntryCapture, B)
    where
        F: Future<Output = (io::Result<usize>, B)> + Unpin,
    {
        let capture = SocketEntryCapture::new();
        test_hooks::fail_next_raw_sqe_submit();
        let Poll::Ready((result, owner)) = Pin::new(&mut *future).poll(cx) else {
            panic!("rejected {kind} submission did not return its owner");
        };
        let error = match result {
            Err(error) => error,
            Ok(value) => panic!("injected {kind} submission succeeded: {value:?}"),
        };
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(test_hooks::raw_sqe_submit_failures_remaining(), 0);
        (capture, owner)
    }

    /// Checks a message-send caller against independently predicted retained storage.
    pub(crate) fn assert_rejected_message_submission<F, B>(
        cx: &mut Context<'_>,
        future: &mut F,
        expected: ExpectedMessageEntry,
    ) -> B
    where
        F: Future<Output = (io::Result<usize>, B)> + Unpin,
    {
        let (capture, owner) = poll_rejected_submission(cx, future, "message");
        assert!(Pin::new(&mut *future).poll(cx).is_pending());
        let entry = capture.take();
        assert_entry_fields(
            &entry,
            io_uring::opcode::SendMsg::CODE,
            expected.fd,
            expected.addr,
            1,
            expected.msg_flags,
            expected.user_data,
        );
        owner
    }

    /// One scalar rental operation used by cross-operation test cases.
    pub(crate) type ScalarTestFuture<'a> =
        Pin<Box<dyn Future<Output = (io::Result<usize>, Vec<u8>)> + 'a>>;

    /// Independently expected identity and work for one scalar socket request.
    pub(crate) struct ExpectedScalarEntry {
        pub(crate) opcode: u8,
        pub(crate) fd: i32,
        pub(crate) addr: *const u8,
        pub(crate) owner_addr: *const u8,
        pub(crate) len: u32,
        pub(crate) msg_flags: u32,
        pub(crate) user_data: u64,
    }

    /// Inspects a rejected ringless submission and its returned owner.
    pub(crate) fn assert_rejected_scalar_submission<F>(
        cx: &mut Context<'_>,
        future: &mut F,
        expected: ExpectedScalarEntry,
        expected_bytes: &[u8],
    ) -> Vec<u8>
    where
        F: Future<Output = (io::Result<usize>, Vec<u8>)> + Unpin,
    {
        let (capture, owner) = poll_rejected_submission(cx, future, "scalar");
        assert_eq!(owner.as_ptr(), expected.owner_addr);
        assert_eq!(owner, expected_bytes);
        assert!(Pin::new(&mut *future).poll(cx).is_pending());
        let entry = capture.take();
        assert_entry_fields(
            &entry,
            expected.opcode,
            expected.fd,
            expected.addr as u64,
            expected.len,
            expected.msg_flags,
            expected.user_data,
        );
        owner
    }

    fn assert_entry_fields(
        entry: &squeue::Entry,
        opcode: u8,
        fd: i32,
        addr: u64,
        len: u32,
        msg_flags: u32,
        user_data: u64,
    ) {
        let fields = sqe_prefix(entry);
        assert_eq!(fields.opcode, opcode);
        assert_eq!(fields.flags, 0);
        assert_eq!(fields.ioprio, 0);
        assert_eq!(fields.fd, fd);
        assert_eq!(fields.off_or_addr2, 0);
        assert_eq!(fields.addr, addr);
        assert_eq!(fields.len, len);
        assert_eq!(fields.msg_flags, msg_flags);
        assert_eq!(fields.user_data, user_data);
    }

    #[repr(C)]
    /// Prefix of `io_uring_sqe` inspected by socket-builder unit tests.
    pub(crate) struct SqePrefix {
        /// io_uring opcode selected by the builder.
        pub(crate) opcode: u8,
        /// SQE submission flags.
        pub(crate) flags: u8,
        /// SQE I/O priority field.
        pub(crate) ioprio: u16,
        /// File descriptor encoded in the SQE.
        pub(crate) fd: i32,
        /// Opcode-dependent offset or secondary-address field.
        pub(crate) off_or_addr2: u64,
        /// Primary buffer or message-header address.
        pub(crate) addr: u64,
        /// Single-buffer byte count or opcode-dependent length.
        pub(crate) len: u32,
        /// Message flags encoded for socket operations.
        pub(crate) msg_flags: u32,
        /// Completion correlation value.
        pub(crate) user_data: u64,
    }

    /// Reinterprets the stable SQE prefix used by the test assertions.
    pub(crate) fn sqe_prefix(entry: &squeue::Entry) -> &SqePrefix {
        // SAFETY: `SqePrefix` mirrors the leading fields of the Linux SQE ABI
        // represented by `squeue::Entry`; tests read only that common prefix.
        unsafe { &*(entry as *const squeue::Entry as *const SqePrefix) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::{MaybeUninit, size_of};
    use test_support::sqe_prefix;

    fn sqe_bytes(entry: &squeue::Entry) -> &[u8; 64] {
        assert_eq!(size_of::<squeue::Entry>(), 64);
        // SAFETY: io_uring 0.7.13 represents Entry as its padding-free 64-byte
        // Linux SQE. Socket builders zero every byte before assigning fields.
        unsafe { &*(entry as *const squeue::Entry).cast::<[u8; 64]>() }
    }

    fn assert_socket_entries_equal(actual: squeue::Entry, expected: squeue::Entry) {
        assert_eq!(sqe_bytes(&actual), sqe_bytes(&expected));
        assert_eq!(&sqe_bytes(&actual)[40..], &[0; 24]);
    }

    #[test]
    fn receive_builders_match_inline_socket_entries() {
        let mut bytes = [0u8; 2];
        let mut header = MaybeUninit::<libc::msghdr>::uninit();
        for (index, fd, user_data) in [(0, 17, 0x1234_5678_9abc_def0), (1, -1, u64::MAX)] {
            let ptr = std::ptr::addr_of_mut!(bytes[index]);
            for len in [1, 4096, i32::MAX as u32, i32::MAX as u32 + 1, u32::MAX] {
                assert_socket_entries_equal(
                    build_recv_entry(fd, ptr, len, user_data),
                    opcode::Recv::new(types::Fd(fd), ptr, len.min(i32::MAX as u32))
                        .build()
                        .user_data(user_data),
                );
            }
            assert_socket_entries_equal(
                build_recvmsg_entry(fd, header.as_mut_ptr(), user_data),
                opcode::RecvMsg::new(types::Fd(fd), header.as_mut_ptr())
                    .build()
                    .user_data(user_data),
            );
        }
    }

    #[test]
    fn send_builders_match_inline_socket_entries_for_both_flag_policies() {
        let bytes = [0u8; 2];
        let header = MaybeUninit::<libc::msghdr>::uninit();
        for (index, fd, user_data) in [(0, 17, 0x1234_5678_9abc_def0), (1, -1, u64::MAX)] {
            let ptr = std::ptr::addr_of!(bytes[index]);
            for flags in [0, libc::MSG_NOSIGNAL] {
                for len in [0, 1, 4096, i32::MAX as u32, i32::MAX as u32 + 1, u32::MAX] {
                    let expected = if flags == 0 {
                        opcode::Send::new(types::Fd(fd), ptr, len.min(i32::MAX as u32)).build()
                    } else {
                        opcode::Send::new(types::Fd(fd), ptr, len.min(i32::MAX as u32))
                            .flags(libc::MSG_NOSIGNAL)
                            .build()
                    };
                    assert_socket_entries_equal(
                        build_send_entry(fd, ptr, len, flags, user_data),
                        expected.user_data(user_data),
                    );
                }
                let expected = if flags == 0 {
                    opcode::SendMsg::new(types::Fd(fd), header.as_ptr()).build()
                } else {
                    opcode::SendMsg::new(types::Fd(fd), header.as_ptr())
                        .flags(libc::MSG_NOSIGNAL as u32)
                        .build()
                };
                assert_socket_entries_equal(
                    build_sendmsg_entry(fd, header.as_ptr(), flags as u32, user_data),
                    expected.user_data(user_data),
                );
            }
        }
    }

    #[test]
    fn receive_entry_preserves_identity_and_clamps_signed_lengths() {
        let mut byte = 0u8;
        let ptr = &mut byte as *mut u8;
        for requested in [1, 4096, i32::MAX as u32, i32::MAX as u32 + 1, u32::MAX] {
            let entry = build_recv_entry(17, ptr, requested, 0x1234_5678_9abc_def0);
            let fields = sqe_prefix(&entry);
            assert_eq!(fields.opcode, opcode::Recv::CODE);
            assert_eq!(fields.fd, 17);
            assert_eq!(fields.addr, ptr as u64);
            assert_eq!(fields.len, requested.min(i32::MAX as u32));
            assert_eq!(fields.flags, 0);
            assert_eq!(fields.ioprio, 0);
            assert_eq!(fields.off_or_addr2, 0);
            assert_eq!(fields.msg_flags, 0);
            assert_eq!(fields.user_data, 0x1234_5678_9abc_def0);
        }
    }

    #[test]
    fn send_entry_preserves_flags_and_clamps_signed_lengths() {
        let byte = 0u8;
        let ptr = std::ptr::addr_of!(byte);
        let max = i32::MAX as u32;
        for flags in [0, libc::MSG_NOSIGNAL] {
            for (requested, expected) in [
                (0, 0),
                (1, 1),
                (4096, 4096),
                (max, max),
                (max + 1, max),
                (u32::MAX, max),
            ] {
                let entry = build_send_entry(17, ptr, requested, flags, 0x1234_5678_9abc_def0);
                let fields = sqe_prefix(&entry);
                assert_eq!(fields.opcode, opcode::Send::CODE);
                assert_eq!(fields.flags, 0);
                assert_eq!(fields.ioprio, 0);
                assert_eq!(fields.fd, 17);
                assert_eq!(fields.off_or_addr2, 0);
                assert_eq!(fields.addr, ptr as u64);
                assert_eq!(fields.len, expected);
                assert_eq!(fields.msg_flags, flags as u32);
                assert_eq!(fields.user_data, 0x1234_5678_9abc_def0);
                assert_eq!(&sqe_bytes(&entry)[40..], &[0; 24]);
            }
        }
    }
}
