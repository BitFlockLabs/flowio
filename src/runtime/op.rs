//! Per-submission completion state shared between io_uring CQE handling and
//! the runtime's concrete futures.

use crate::runtime::retained::{RetainedPayload, RetainedPayloadPool, RetainedPayloadVtable};
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
///
/// # Retained payload invariant
///
/// Any memory referenced by an in-flight SQE must remain retained until the
/// original CQE for that SQE is observed, even if the owning future is dropped.
/// Futures that submit SQEs pointing into caller-owned buffers or scratch
/// arrays attach an erased retained payload here. A live future detaches that
/// payload on normal completion and returns ownership to the caller; the
/// reactor drops it when an orphaned original CQE retires. Cancel CQEs use
/// `user_data == 0` and never release retained payloads.
///
/// Retained payload storage is backed by the reactor's private retained-payload
/// pool for common payload sizes. Oversized or over-aligned payloads use the
/// documented heap fallback carried by the same erased vtable.
#[doc(hidden)]
pub struct CompletionState {
    /// CQE result value, stored exactly as returned by the kernel.
    pub result: i32,
    /// CQE flags copied from the completion entry.
    pub cqe_flags: u32,
    /// Internal state bits such as completed/orphaned/detached.
    pub state_flags: u32,
    /// Task waiting on this operation, or null when no waiter is registered.
    pub waiter: *mut TaskHeader,
    /// Erased retained payload whose memory may be referenced by the in-flight
    /// SQE associated with this completion state.
    retained_payload: *mut (),
    /// Release vtable for `retained_payload`.
    retained_payload_vtable: Option<RetainedPayloadVtable>,
}

impl CompletionState {
    /// CQE has been observed and its result fields are valid.
    pub const FLAG_COMPLETED: u32 = 1 << 0;
    /// Owning future was dropped before the kernel retired the submission.
    pub const FLAG_ORPHANED: u32 = 1 << 1;
    /// Operation has no waiting task and should be reclaimed on completion.
    pub const FLAG_DETACHED: u32 = 1 << 2;

    #[inline(always)]
    pub(crate) fn empty() -> Self {
        Self {
            result: 0,
            cqe_flags: 0,
            state_flags: 0,
            waiter: std::ptr::null_mut(),
            retained_payload: std::ptr::null_mut(),
            retained_payload_vtable: None,
        }
    }

    #[inline(always)]
    pub fn is_completed(&self) -> bool {
        self.state_flags & Self::FLAG_COMPLETED != 0
    }

    #[inline(always)]
    pub fn is_orphaned(&self) -> bool {
        self.state_flags & Self::FLAG_ORPHANED != 0
    }

    #[inline(always)]
    pub fn is_detached(&self) -> bool {
        self.state_flags & Self::FLAG_DETACHED != 0
    }

    #[inline(always)]
    pub fn set_completed(&mut self) {
        self.state_flags |= Self::FLAG_COMPLETED;
    }

    #[inline(always)]
    pub fn set_orphaned(&mut self) {
        self.state_flags |= Self::FLAG_ORPHANED;
    }

    #[inline(always)]
    pub fn set_detached(&mut self) {
        self.state_flags |= Self::FLAG_DETACHED;
    }

    #[inline(always)]
    pub fn register_waiter(&mut self, task: *mut TaskHeader) {
        self.waiter = task;
    }

    #[inline(always)]
    pub fn take_waiter(&mut self) -> *mut TaskHeader {
        let waiter = self.waiter;
        self.waiter = std::ptr::null_mut();
        waiter
    }

    #[inline(always)]
    pub fn clear_waiter(&mut self) {
        self.waiter = std::ptr::null_mut();
    }

    /// Attaches a retained payload to this in-flight operation.
    ///
    /// The payload is owned by the completion state until a live future takes
    /// it back or the reactor drops it while retiring an orphaned original CQE.
    #[inline(always)]
    pub(crate) fn attach_retained_payload<T: 'static>(&mut self, payload: RetainedPayload<T>) {
        debug_assert!(
            self.retained_payload.is_null(),
            "CompletionState already has retained payload"
        );
        let (ptr, vtable) = payload.into_raw_parts();
        self.retained_payload = ptr;
        self.retained_payload_vtable = Some(vtable);
    }

    /// Returns a shared reference to the retained payload.
    ///
    /// # Safety
    ///
    /// The caller must request the exact concrete payload type that was
    /// attached to this completion state.
    #[inline(always)]
    pub(crate) unsafe fn retained_payload<T: 'static>(&self) -> &T {
        debug_assert!(
            !self.retained_payload.is_null(),
            "CompletionState retained payload missing"
        );
        unsafe { &*(self.retained_payload as *const T) }
    }

    /// Returns a mutable reference to the retained payload.
    ///
    /// # Safety
    ///
    /// The caller must request the exact concrete payload type that was
    /// attached to this completion state and must have exclusive logical
    /// access to the operation state.
    #[inline(always)]
    pub(crate) unsafe fn retained_payload_mut<T: 'static>(&mut self) -> &mut T {
        debug_assert!(
            !self.retained_payload.is_null(),
            "CompletionState retained payload missing"
        );
        unsafe { &mut *(self.retained_payload as *mut T) }
    }

    /// Detaches and returns the retained payload.
    ///
    /// # Safety
    ///
    /// The caller must request the exact concrete payload type that was
    /// attached to this completion state and provide the reactor-owned pool
    /// that allocated the retained storage.
    #[inline(always)]
    pub(crate) unsafe fn take_retained_payload<T: 'static>(
        &mut self,
        pool: &mut RetainedPayloadPool,
    ) -> T {
        debug_assert!(
            !self.retained_payload.is_null(),
            "CompletionState retained payload missing"
        );
        let ptr = self.retained_payload as *mut T;
        self.retained_payload = std::ptr::null_mut();
        let vtable = self
            .retained_payload_vtable
            .take()
            .expect("retained payload missing vtable");
        unsafe { RetainedPayload::from_raw_parts(ptr, vtable).take(pool) }
    }

    /// Drops any retained payload still attached to this completion state.
    #[inline(always)]
    pub(crate) unsafe fn drop_retained_payload(&mut self, pool: &mut RetainedPayloadPool) {
        if self.retained_payload.is_null() {
            return;
        }
        let ptr = self.retained_payload;
        let vtable = self
            .retained_payload_vtable
            .take()
            .expect("retained payload missing vtable");
        self.retained_payload = std::ptr::null_mut();
        unsafe { (vtable.drop_and_free)(ptr, pool) };
    }

    /// Reset a retired completion slot for the next sequential submission.
    ///
    /// This is only valid after the previous CQE has already been observed and
    /// fully consumed by the owning future. It must not be used while the slot
    /// still corresponds to an in-flight submission. Any retained payload is
    /// intentionally preserved so retrying futures can keep one caller-owned
    /// buffer alive across multiple sequential SQEs.
    #[inline(always)]
    pub fn reset_for_resubmit(&mut self) {
        self.result = 0;
        self.cqe_flags = 0;
        self.state_flags = 0;
        self.waiter = std::ptr::null_mut();
    }
}

impl InPlaceInit for CompletionState {
    type Args = ();

    fn init_at(slot: &mut MaybeUninit<Self>, _: Self::Args) {
        unsafe {
            slot.as_mut_ptr().write(Self::empty());
        }
    }
}
