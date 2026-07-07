//! Shared send-style SQE builders for stream transports.

use io_uring::{opcode, squeue, types};
use std::os::fd::RawFd;

const SEND_FLAGS: i32 = libc::MSG_NOSIGNAL;
const SENDMSG_FLAGS: u32 = libc::MSG_NOSIGNAL as u32;

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
    pub(crate) struct SqePrefix {
        pub(crate) opcode: u8,
        pub(crate) flags: u8,
        pub(crate) ioprio: u16,
        pub(crate) fd: i32,
        pub(crate) off_or_addr2: u64,
        pub(crate) addr: u64,
        pub(crate) len: u32,
        pub(crate) msg_flags: u32,
        pub(crate) user_data: u64,
    }

    pub(crate) fn sqe_prefix(entry: &squeue::Entry) -> &SqePrefix {
        unsafe { &*(entry as *const squeue::Entry as *const SqePrefix) }
    }
}
