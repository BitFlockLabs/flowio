//! Shared send-style SQE builders for stream transports.

use io_uring::{opcode, squeue, types};
use std::os::fd::RawFd;

const SEND_FLAGS: i32 = libc::MSG_NOSIGNAL;
const SENDMSG_FLAGS: u32 = libc::MSG_NOSIGNAL as u32;

/// Builds a single-buffer send SQE with `MSG_NOSIGNAL`.
#[inline(always)]
pub(crate) fn build_send_entry(
    fd: RawFd,
    ptr: *const u8,
    len: u32,
    user_data: u64,
) -> squeue::Entry {
    opcode::Send::new(types::Fd(fd), ptr, len)
        .flags(SEND_FLAGS)
        .build()
        .user_data(user_data)
}

/// Builds a sendmsg SQE with `MSG_NOSIGNAL`.
#[inline(always)]
pub(crate) fn build_sendmsg_entry(
    fd: RawFd,
    msg: *const libc::msghdr,
    user_data: u64,
) -> squeue::Entry {
    opcode::SendMsg::new(types::Fd(fd), msg)
        .flags(SENDMSG_FLAGS)
        .build()
        .user_data(user_data)
}

#[cfg(test)]
pub(crate) mod test_support {
    use io_uring::squeue;

    #[repr(C)]
    /// Prefix of `io_uring_sqe` inspected by send-builder unit tests.
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
        /// Message flags encoded for send-style operations.
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
