use crate::runtime::task::TaskHeader;
use crate::utils::memory::pool::InPlaceInit;
use std::mem::MaybeUninit;

/// Per-submission state shared between a pinned future and the io_uring CQE
/// completion path.
///
/// Allocated from the reactor's op pool; futures store only a raw pointer.
/// Each in-flight SQE owns one `CompletionState`. Sequential retry futures
/// may reuse the same slot after they have consumed the previous CQE. Dropping
/// a future with a completed state frees it immediately; dropping a future
/// with an in-flight state marks it orphaned and leaves reclamation to the
/// CQE path.
#[doc(hidden)]
pub struct CompletionState
{
    /// CQE result value, stored exactly as returned by the kernel.
    pub result: i32,
    /// CQE flags copied from the completion entry.
    pub cqe_flags: u32,
    /// Internal state bits such as completed/orphaned/detached.
    pub state_flags: u32,
    /// Task waiting on this operation, or null when no waiter is registered.
    pub waiter: *mut TaskHeader,
}

impl CompletionState
{
    pub const FLAG_COMPLETED: u32 = 1 << 0;
    pub const FLAG_ORPHANED: u32 = 1 << 1;
    pub const FLAG_DETACHED: u32 = 1 << 2;

    #[inline(always)]
    pub fn is_completed(&self) -> bool
    {
        self.state_flags & Self::FLAG_COMPLETED != 0
    }

    #[inline(always)]
    pub fn is_orphaned(&self) -> bool
    {
        self.state_flags & Self::FLAG_ORPHANED != 0
    }

    #[inline(always)]
    pub fn is_detached(&self) -> bool
    {
        self.state_flags & Self::FLAG_DETACHED != 0
    }

    #[inline(always)]
    pub fn set_completed(&mut self)
    {
        self.state_flags |= Self::FLAG_COMPLETED;
    }

    #[inline(always)]
    pub fn set_orphaned(&mut self)
    {
        self.state_flags |= Self::FLAG_ORPHANED;
    }

    #[inline(always)]
    pub fn set_detached(&mut self)
    {
        self.state_flags |= Self::FLAG_DETACHED;
    }

    #[inline(always)]
    pub fn register_waiter(&mut self, task: *mut TaskHeader)
    {
        self.waiter = task;
    }

    #[inline(always)]
    pub fn take_waiter(&mut self) -> *mut TaskHeader
    {
        let waiter = self.waiter;
        self.waiter = std::ptr::null_mut();
        waiter
    }

    #[inline(always)]
    pub fn clear_waiter(&mut self)
    {
        self.waiter = std::ptr::null_mut();
    }

    /// Reset a retired completion slot for the next sequential submission.
    ///
    /// This is only valid after the previous CQE has already been observed and
    /// fully consumed by the owning future. It must not be used while the slot
    /// still corresponds to an in-flight submission.
    #[inline(always)]
    pub fn reset_for_resubmit(&mut self)
    {
        self.result = 0;
        self.cqe_flags = 0;
        self.state_flags = 0;
        self.waiter = std::ptr::null_mut();
    }
}

impl InPlaceInit for CompletionState
{
    type Args = ();

    fn init_at(slot: &mut MaybeUninit<Self>, _: Self::Args)
    {
        unsafe {
            slot.as_mut_ptr().write(Self {
                result: 0,
                cqe_flags: 0,
                state_flags: 0,
                waiter: std::ptr::null_mut(),
            });
        }
    }
}
