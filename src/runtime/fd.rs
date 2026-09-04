//! Runtime-owned file descriptor wrapper with provenance-aware close behavior.
//!
//! Internally created sockets start with known nonpositive `SO_LINGER` and can
//! use the reactor's batched close path without a terminal `getsockopt`.
//! External adoption, public raw-fd exposure, aliases, and uncertain listener
//! inheritance monotonically taint that proof. Uncertain sockets are classified
//! at terminal drop: positive or unclassifiable linger goes to the executor's
//! bounded close worker, while proven nonpositive linger normally uses the
//! reactor close path. A final listener owner released during CQ reclamation
//! cannot re-enter the borrowed ring, so its nonpositive descriptor moves to
//! that reactor's bounded post-view close FIFO. Outside an executor, drop
//! preserves ordinary direct-close behavior.

use crate::runtime::executor::{
    CloseAdmission, CloseSubmission, completion_drain_active, has_active_close_context,
    note_close_direct, note_close_linger_classification_failure, note_close_linger_query,
    note_close_linger_waiver, try_admit_close, try_submit_close,
};
use crate::runtime::op::CompletionState;
use std::cell::Cell;
use std::io;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::ptr::NonNull;
use std::rc::Rc;
#[cfg(any(test, feature = "test-support"))]
use std::sync::atomic::{AtomicI32, Ordering};

#[cfg(any(test, feature = "test-support"))]
const DISTINCTIVE_TEST_FD_START: RawFd = 128;
#[cfg(any(test, feature = "test-support"))]
const DISTINCTIVE_TEST_FD_STRIDE: RawFd = 8;
#[cfg(any(test, feature = "test-support"))]
static NEXT_DISTINCTIVE_TEST_FD_BASE: AtomicI32 = AtomicI32::new(DISTINCTIVE_TEST_FD_START);

#[cfg(test)]
thread_local! {
    static FINAL_CORE_DROP_HOOK: Cell<Option<fn(RawFd)>> = const { Cell::new(None) };
}

#[cfg(test)]
struct FinalCoreDropHookScope {
    previous: Option<fn(RawFd)>,
    _owner_thread_only: PhantomData<Rc<()>>,
}

#[cfg(test)]
impl Drop for FinalCoreDropHookScope {
    fn drop(&mut self) {
        let previous = self.previous;
        let _ = FINAL_CORE_DROP_HOOK.try_with(|slot| slot.set(previous));
    }
}

#[cfg(test)]
pub(crate) fn with_final_core_drop_hook_for_test<R>(
    hook: fn(RawFd),
    body: impl FnOnce() -> R,
) -> R {
    let previous = FINAL_CORE_DROP_HOOK.with(|slot| slot.replace(Some(hook)));
    let _scope = FinalCoreDropHookScope {
        previous,
        _owner_thread_only: PhantomData,
    };
    body()
}

/// Last trustworthy knowledge about a runtime socket's linger state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LingerProvenance {
    /// FlowIO created the socket and no operation capable of changing its
    /// shared socket options has occurred.
    KnownNonPositive,
    /// External code or a socket alias may have enabled positive linger.
    Uncertain,
}

/// Sole descriptor owner shared by owner-thread handles and in-flight leases.
pub(crate) struct RuntimeFdCore {
    /// Raw descriptor value, or `INVALID` after ownership has been moved out.
    fd: RawFd,
    /// Owner-thread monotonic uncertainty bit updated through shared access.
    linger_uncertain: Cell<bool>,
}

impl RuntimeFdCore {
    const INVALID: RawFd = -1;

    #[inline(always)]
    fn new(fd: RawFd, provenance: LingerProvenance) -> Self {
        Self {
            fd,
            linger_uncertain: Cell::new(provenance == LingerProvenance::Uncertain),
        }
    }

    #[inline(always)]
    pub(crate) fn raw_fd(&self) -> RawFd {
        self.fd
    }

    #[inline(always)]
    fn mark_linger_uncertain(&self) {
        self.linger_uncertain.set(true);
    }

    #[inline(always)]
    fn linger_provenance(&self) -> LingerProvenance {
        if self.linger_uncertain.get() {
            LingerProvenance::Uncertain
        } else {
            LingerProvenance::KnownNonPositive
        }
    }

    /// Moves the raw descriptor and its provenance out, leaving this core
    /// empty so its ordinary destructor becomes a no-op.
    #[inline(always)]
    fn take_for_drop(&mut self) -> (RawFd, LingerProvenance) {
        let fd = self.fd;
        self.fd = Self::INVALID;
        (fd, self.linger_provenance())
    }

    /// Closes this sole core without immediately submitting to the active
    /// ring. This is reserved for a final listener lease released while a
    /// completion view still borrows that ring.
    fn close_without_ring(mut self) {
        self.close_taken(false);
    }

    fn close_taken(&mut self, allow_ring: bool) {
        let (fd, provenance) = self.take_for_drop();
        if fd == Self::INVALID {
            return;
        }
        #[cfg(test)]
        FINAL_CORE_DROP_HOOK.with(|hook| {
            if let Some(hook) = hook.get() {
                hook(fd);
            }
        });
        if fd < 0 {
            return;
        }

        // SAFETY: `take_for_drop` moved this core's sole valid descriptor
        // ownership into the temporary owner.
        let owned = unsafe { OwnedFd::from_raw_fd(fd) };

        if !has_active_close_context() {
            drop(owned);
            return;
        }

        close_owned_in_active_context(owned, provenance, allow_ring);
    }
}

impl Drop for RuntimeFdCore {
    fn drop(&mut self) {
        self.close_taken(true);
    }
}

/// One-word, four-byte-aligned owner-thread handle for a descriptor core.
///
/// `core` is the pointer returned by `Rc::into_raw` and therefore owns exactly
/// one non-atomic strong count. The packed alignment preserves the established
/// x86-64 public handle layout. Every access copies the pointer with an
/// unaligned raw read; code must never form a reference to the packed field.
/// Allocation failure follows `Rc::new` and the process global allocation-error
/// handler.
#[repr(C, packed(4))]
pub(crate) struct RuntimeFd {
    core: NonNull<RuntimeFdCore>,
    _owner_thread_only: PhantomData<Rc<()>>,
}

impl RuntimeFd {
    pub(crate) const INVALID: RawFd = RuntimeFdCore::INVALID;

    #[inline(always)]
    pub(crate) fn from_fresh_raw_fd(fd: RawFd) -> Self {
        Self::from_raw_fd_with_provenance(fd, LingerProvenance::KnownNonPositive)
    }

    #[inline(always)]
    pub(crate) fn from_fresh_owned(fd: OwnedFd) -> Self {
        Self::from_fresh_raw_fd(fd.into_raw_fd())
    }

    #[inline(always)]
    pub(crate) fn from_external_owned(fd: OwnedFd) -> Self {
        Self::from_owned_with_provenance(fd, LingerProvenance::Uncertain)
    }

    #[inline(always)]
    pub(crate) fn from_owned_with_provenance(fd: OwnedFd, provenance: LingerProvenance) -> Self {
        Self::from_raw_fd_with_provenance(fd.into_raw_fd(), provenance)
    }

    #[inline(always)]
    fn from_raw_fd_with_provenance(fd: RawFd, provenance: LingerProvenance) -> Self {
        let core = Rc::new(RuntimeFdCore::new(fd, provenance));
        // SAFETY: `Rc::into_raw` never returns null.
        let core = unsafe { NonNull::new_unchecked(Rc::into_raw(core).cast_mut()) };
        Self {
            core,
            _owner_thread_only: PhantomData,
        }
    }

    /// Copies the packed pointer without ever borrowing its under-aligned
    /// field. The returned pointer carries the allocation provenance created by
    /// `Rc::into_raw` and remains valid while this handle owns its strong count.
    #[inline(always)]
    fn core_ptr(&self) -> NonNull<RuntimeFdCore> {
        // SAFETY: `addr_of!` does not create a packed-field reference, and the
        // field is initialized for the full lifetime of `self`.
        unsafe { std::ptr::addr_of!(self.core).read_unaligned() }
    }

    #[inline(always)]
    fn core(&self) -> &RuntimeFdCore {
        // SAFETY: this handle owns one live strong count for `core_ptr()`.
        unsafe { self.core_ptr().as_ref() }
    }

    #[inline(always)]
    pub(crate) fn raw_fd(&self) -> RawFd {
        self.runtime_ref().raw_fd()
    }

    /// Returns the public raw descriptor and permanently invalidates the proof
    /// that shared socket options remain at their FlowIO-created defaults.
    #[inline(always)]
    pub(crate) fn expose_raw_fd(&self) -> RawFd {
        self.mark_linger_uncertain();
        self.raw_fd()
    }

    #[inline(always)]
    pub(crate) fn mark_linger_uncertain(&self) {
        self.core().mark_linger_uncertain();
    }

    #[inline(always)]
    pub(crate) fn linger_provenance(&self) -> LingerProvenance {
        self.core().linger_provenance()
    }

    #[inline(always)]
    pub(crate) fn runtime_ref(&self) -> RuntimeFdRef<'_> {
        RuntimeFdRef {
            core: self.core_ptr(),
            _borrow: PhantomData,
        }
    }

    #[inline(always)]
    pub(crate) fn op_state(&self) -> RuntimeFdOpState<'_> {
        self.runtime_ref().into_op_state()
    }

    #[inline(always)]
    pub(crate) fn lease(&self) -> RuntimeFdLease {
        self.runtime_ref().lease()
    }

    #[inline(always)]
    pub(crate) fn clone_handle(&self) -> Self {
        let core = self.core_ptr();
        // SAFETY: `core` is the exact provenance-preserving pointer returned by
        // `Rc::into_raw`, and this handle keeps its associated strong count live.
        unsafe { Rc::increment_strong_count(core.as_ptr()) };
        Self {
            core,
            _owner_thread_only: PhantomData,
        }
    }

    #[cfg(test)]
    #[inline(always)]
    pub(crate) fn strong_count_for_test(&self) -> usize {
        let core = self.core_ptr();
        // SAFETY: temporarily materialize one additional strong owner from the
        // original `Rc::into_raw` pointer, then drop it before returning.
        unsafe { Rc::increment_strong_count(core.as_ptr()) };
        let probe = unsafe { Rc::from_raw(core.as_ptr()) };
        let count = Rc::strong_count(&probe) - 1;
        drop(probe);
        count
    }

    #[cfg(all(test, not(miri)))]
    #[inline(always)]
    pub(crate) fn weak_for_test(&self) -> std::rc::Weak<RuntimeFdCore> {
        let core = self.core_ptr();
        // SAFETY: the temporary owner is paired with `drop` below, leaving this
        // handle's original raw strong owner unchanged.
        unsafe { Rc::increment_strong_count(core.as_ptr()) };
        let probe = unsafe { Rc::from_raw(core.as_ptr()) };
        let weak = Rc::downgrade(&probe);
        drop(probe);
        weak
    }
}

impl Drop for RuntimeFd {
    #[inline(always)]
    fn drop(&mut self) {
        let core = self.core_ptr();
        // SAFETY: each RuntimeFd owns exactly one raw strong count, created by
        // construction or `clone_handle`, and Drop consumes it exactly once.
        drop(unsafe { Rc::from_raw(core.as_ptr()) });
    }
}

// `Cell<bool>` records only monotonic owner-thread provenance. A panic cannot
// expose a partially updated multi-field invariant through these wrappers, so
// preserve the previous handle/future `UnwindSafe` contract. `RefUnwindSafe`
// deliberately remains absent because shared provenance can change.
impl std::panic::UnwindSafe for RuntimeFd {}

/// Copy, owner-local borrow of one runtime descriptor core.
#[derive(Clone, Copy)]
pub(crate) struct RuntimeFdRef<'a> {
    core: NonNull<RuntimeFdCore>,
    _borrow: PhantomData<&'a RuntimeFd>,
}

impl<'a> RuntimeFdRef<'a> {
    #[inline(always)]
    pub(crate) fn raw_fd(self) -> RawFd {
        // SAFETY: the borrowed RuntimeFd keeps one strong owner live for `'a`.
        unsafe { self.core.as_ref().raw_fd() }
    }

    #[inline(always)]
    pub(crate) fn lease(self) -> RuntimeFdLease {
        // SAFETY: this is the original provenance-preserving `Rc::into_raw`
        // pointer and the borrowed handle keeps its strong count live.
        unsafe { Rc::increment_strong_count(self.core.as_ptr()) };
        RuntimeFdLease::from_core(unsafe { Rc::from_raw(self.core.as_ptr()) })
    }

    #[inline(always)]
    pub(crate) fn into_op_state(self) -> RuntimeFdOpState<'a> {
        RuntimeFdOpState::from_borrowed(self)
    }
}

impl std::panic::UnwindSafe for RuntimeFdRef<'_> {}

/// Owned non-atomic strong lease retained through a target CQE.
#[repr(transparent)]
pub(crate) struct RuntimeFdLease {
    core: Rc<RuntimeFdCore>,
}

impl RuntimeFdLease {
    #[inline(always)]
    pub(crate) fn raw_fd(&self) -> RawFd {
        self.core.raw_fd()
    }

    #[cfg(test)]
    #[inline(always)]
    pub(crate) fn linger_provenance(&self) -> LingerProvenance {
        self.core.linger_provenance()
    }

    #[inline(always)]
    pub(crate) fn into_op_state(self) -> RuntimeFdOpState<'static> {
        RuntimeFdOpState::from_owned(self)
    }

    #[inline(always)]
    pub(crate) fn into_core(self) -> Rc<RuntimeFdCore> {
        self.core
    }

    #[inline(always)]
    pub(crate) fn from_core(core: Rc<RuntimeFdCore>) -> Self {
        Self { core }
    }

    #[cfg(test)]
    #[inline(always)]
    fn strong_count_for_test(&self) -> usize {
        Rc::strong_count(&self.core)
    }
}

impl std::panic::UnwindSafe for RuntimeFdLease {}

/// One-word descriptor-operation state replacing the future's raw state slot.
///
/// Before initial submission, an untagged core pointer borrows the parent
/// descriptor, while tag one owns a staged `Rc::into_raw` lease. After a
/// successful userspace-SQ push, tags two and three identify a live
/// [`CompletionState`] submitted from borrowed and owned initial state,
/// respectively. Retiring a borrowed submission restores its initial
/// capability so owner-borrowing internal futures can submit a fresh operation;
/// retiring an owned submission leaves terminal empty state. Thus public
/// futures retain their established raw-fd field and pointer-word footprint
/// instead of adding a capability field.
pub(crate) struct RuntimeFdOpState<'a> {
    tagged: Option<NonNull<()>>,
    _borrow: PhantomData<&'a RuntimeFd>,
}

impl<'a> RuntimeFdOpState<'a> {
    const TAG_MASK: usize = 0b11;
    const BORROWED_TAG: usize = 0;
    const OWNED_TAG: usize = 1;
    const SUBMITTED_BORROWED_TAG: usize = 2;
    const SUBMITTED_OWNED_TAG: usize = 3;

    #[inline(always)]
    fn from_borrowed(fd: RuntimeFdRef<'a>) -> Self {
        debug_assert_eq!(fd.core.addr().get() & Self::TAG_MASK, 0);
        Self {
            tagged: Some(fd.core.cast()),
            _borrow: PhantomData,
        }
    }

    #[inline(always)]
    fn from_owned(lease: RuntimeFdLease) -> RuntimeFdOpState<'static> {
        let ptr = unsafe { NonNull::new_unchecked(Rc::into_raw(lease.into_core()).cast_mut()) };
        debug_assert_eq!(ptr.addr().get() & Self::TAG_MASK, 0);
        RuntimeFdOpState {
            tagged: Some(Self::with_tag(ptr.cast(), Self::OWNED_TAG)),
            _borrow: PhantomData,
        }
    }

    #[inline(always)]
    pub(crate) fn raw_fd(&self) -> RawFd {
        match self.tag() {
            Some(Self::BORROWED_TAG) | Some(Self::OWNED_TAG) => {
                // SAFETY: both initial representations point at a live core;
                // borrowed lifetime or owned raw count keeps it allocated.
                unsafe { self.untagged_ptr::<RuntimeFdCore>().as_ref().raw_fd() }
            }
            Some(Self::SUBMITTED_BORROWED_TAG) | Some(Self::SUBMITTED_OWNED_TAG) => {
                // SAFETY: successful publication installs a live state pointer
                // whose typed submission invariant includes one fd lease.
                unsafe {
                    self.untagged_ptr::<CompletionState>()
                        .as_ref()
                        .fd_lease_raw_fd()
                }
            }
            _ => RuntimeFd::INVALID,
        }
    }

    /// Clones a borrowed capability or moves a staged owned capability.
    ///
    /// # Safety
    ///
    /// This operation state must still contain its initial borrowed or owned
    /// core representation. It must not be empty or already published.
    #[inline(always)]
    pub(crate) unsafe fn take_initial_lease(&mut self) -> RuntimeFdLease {
        let ptr = self.core_ptr().as_ptr();
        if self.tag() == Some(Self::OWNED_TAG) {
            self.tagged = None;
            // SAFETY: the owned representation was created by exactly one
            // `Rc::into_raw` and clearing the state transfers that ownership.
            return RuntimeFdLease::from_core(unsafe { Rc::from_raw(ptr) });
        }

        debug_assert_eq!(self.tag(), Some(Self::BORROWED_TAG));
        // SAFETY: the borrowed pointer is the original `Rc::into_raw` pointer,
        // and the capability lifetime keeps its associated owner live.
        unsafe { Rc::increment_strong_count(ptr) };
        // SAFETY: the increment above created exactly one transferable owner.
        RuntimeFdLease::from_core(unsafe { Rc::from_raw(ptr) })
    }

    #[inline(always)]
    fn core_ptr(&self) -> NonNull<RuntimeFdCore> {
        debug_assert!(
            matches!(self.tag(), Some(Self::BORROWED_TAG) | Some(Self::OWNED_TAG)),
            "fd operation state has no initial capability"
        );
        self.untagged_ptr()
    }

    #[inline(always)]
    pub(crate) fn state_ptr(&self) -> *mut CompletionState {
        if matches!(
            self.tag(),
            Some(Self::SUBMITTED_BORROWED_TAG) | Some(Self::SUBMITTED_OWNED_TAG)
        ) {
            self.untagged_ptr::<CompletionState>().as_ptr()
        } else {
            std::ptr::null_mut()
        }
    }

    #[inline(always)]
    pub(crate) fn is_null(&self) -> bool {
        self.state_ptr().is_null()
    }

    /// Publishes the completion state immediately after a successful SQ push.
    ///
    /// # Safety
    ///
    /// `state_ptr` must be a live, aligned completion state holding the lease
    /// just taken from this initial representation. A borrowed initial state
    /// must still carry its untagged core pointer; an owned initial state must
    /// be empty because taking its lease moved the raw owner. No fallible or
    /// panicking work may occur between the successful push and this call.
    #[inline(always)]
    pub(crate) unsafe fn publish_submitted_state(&mut self, state_ptr: *mut CompletionState) {
        let submitted_tag = if self.tag() == Some(Self::BORROWED_TAG) {
            Self::SUBMITTED_BORROWED_TAG
        } else {
            // SAFETY: callers promise that any non-borrowed representation is
            // the empty state left by moving an owned initial lease.
            Self::SUBMITTED_OWNED_TAG
        };
        // Deliberately no post-push assertion: publication must be infallible.
        unsafe { self.install_submitted_state(state_ptr, submitted_tag) };
    }

    #[inline(always)]
    unsafe fn install_submitted_state(&mut self, state_ptr: *mut CompletionState, tag: usize) {
        let ptr = unsafe { NonNull::new_unchecked(state_ptr) }.cast();
        self.tagged = Some(Self::with_tag(ptr, tag));
    }

    /// Takes a published completion pointer and restores its initial state.
    ///
    /// Borrowed submissions recover their provenance-preserving core pointer
    /// from the still-attached completion lease. Their lifetime guarantees the
    /// parent handle remains live after that lease is reclaimed. Owned
    /// submissions become terminal empty state because their sole staged owner
    /// moved into the completion.
    #[inline(always)]
    pub(crate) fn take_state_ptr(&mut self) -> *mut CompletionState {
        match self.tag() {
            Some(Self::SUBMITTED_BORROWED_TAG) => {
                let state_ptr = self.untagged_ptr::<CompletionState>();
                // SAFETY: the submitted-borrowed tag is installed only after
                // typed submission attached the matching descriptor lease.
                // `take_state_ptr` runs before completion-state reclamation.
                let core = unsafe { state_ptr.as_ref().fd_lease_core_ptr() };
                self.tagged = Some(core.cast());
                state_ptr.as_ptr()
            }
            Some(Self::SUBMITTED_OWNED_TAG) => {
                let state_ptr = self.untagged_ptr::<CompletionState>().as_ptr();
                self.tagged = None;
                state_ptr
            }
            _ => std::ptr::null_mut(),
        }
    }

    #[inline(always)]
    fn tag(&self) -> Option<usize> {
        self.tagged.map(|ptr| ptr.addr().get() & Self::TAG_MASK)
    }

    #[inline(always)]
    fn with_tag(ptr: NonNull<()>, tag: usize) -> NonNull<()> {
        ptr.map_addr(|addr| {
            // SAFETY: all source pointers have at least four-byte alignment;
            // replacing only those reserved low bits preserves nonzero.
            unsafe { NonZeroUsize::new_unchecked((addr.get() & !Self::TAG_MASK) | tag) }
        })
    }

    #[inline(always)]
    fn untagged_ptr<T>(&self) -> NonNull<T> {
        // SAFETY: callers require a nonempty state and select `T` from its tag.
        let tagged = unsafe { self.tagged.unwrap_unchecked() };
        tagged
            .map_addr(|addr| {
                // SAFETY: each representation began as an aligned nonzero
                // allocation pointer; this removes only our low tag bits.
                unsafe { NonZeroUsize::new_unchecked(addr.get() & !Self::TAG_MASK) }
            })
            .cast()
    }

    #[cfg(test)]
    #[inline(always)]
    fn owns_staged_lease_for_test(&self) -> bool {
        self.tag() == Some(Self::OWNED_TAG)
    }
}

impl Drop for RuntimeFdOpState<'_> {
    #[inline(always)]
    fn drop(&mut self) {
        if self.tag() != Some(Self::OWNED_TAG) {
            return;
        }
        let ptr = self.core_ptr().as_ptr();
        self.tagged = None;
        // SAFETY: this tagged representation still owns the one Rc raw owner
        // created by `from_owned`, and it has not been moved into a state.
        drop(unsafe { Rc::from_raw(ptr) });
    }
}

impl std::panic::UnwindSafe for RuntimeFdOpState<'_> {}

/// Test-support codegen probe for one descriptor-lease clone.
///
/// # Safety
///
/// `runtime_fd` must point to a live [`RuntimeFd`] on its owner thread. The
/// returned opaque pointer owns exactly one raw `Rc` strong count and must be
/// consumed once by the matching non-final-drop probe while another owner is
/// still live.
#[cfg(feature = "test-support")]
#[doc(hidden)]
#[unsafe(no_mangle)]
#[inline(never)]
pub unsafe extern "C" fn flowio_probe_clone_lease(runtime_fd: *const ()) -> *const () {
    let runtime_fd = unsafe { &*runtime_fd.cast::<RuntimeFd>() };
    Rc::into_raw(runtime_fd.lease().into_core()).cast()
}

/// Test-support codegen probe for one known-non-final lease release.
///
/// # Safety
///
/// `lease_core` must be the exact raw owner returned by
/// [`flowio_probe_clone_lease`], and another strong owner for that
/// same core must remain live until this function returns.
#[cfg(feature = "test-support")]
#[doc(hidden)]
#[unsafe(no_mangle)]
#[inline(never)]
pub unsafe extern "C" fn flowio_probe_drop_nonfinal_lease(lease_core: *const ()) {
    let core = lease_core.cast::<RuntimeFdCore>();
    // SAFETY: the probe contract transfers exactly one `Rc::into_raw` owner
    // into this matching release, while a separate base owner keeps it
    // non-final.
    drop(RuntimeFdLease::from_core(unsafe { Rc::from_raw(core) }));
}

const _: [(); std::mem::size_of::<usize>()] = [(); std::mem::size_of::<RuntimeFd>()];
const _: [(); std::mem::size_of::<usize>()] = [(); std::mem::size_of::<RuntimeFdRef<'static>>()];
const _: [(); std::mem::size_of::<usize>()] = [(); std::mem::size_of::<RuntimeFdLease>()];
const _: [(); std::mem::size_of::<usize>()] =
    [(); std::mem::size_of::<RuntimeFdOpState<'static>>()];
const _: () = assert!(std::mem::align_of::<RuntimeFd>() == std::mem::align_of::<RawFd>());
const _: () = assert!(std::mem::align_of::<RuntimeFdCore>() > RuntimeFdOpState::TAG_MASK);
const _: () = assert!(std::mem::align_of::<CompletionState>() > RuntimeFdOpState::TAG_MASK);

/// Listener owner retained by a readiness SQE.
///
/// TCP and SCTP listeners are owner-thread values while an accept operation is
/// live. If completion reclamation releases the final reference, nonpositive
/// close ownership is retained in the exact reactor's bounded post-view FIFO
/// rather than re-entering its borrowed completion ring.
pub(crate) struct RetainedListenerFd {
    fd: Option<RuntimeFdLease>,
}

impl RetainedListenerFd {
    #[inline(always)]
    pub(crate) fn new(fd: &RuntimeFd) -> Self {
        Self {
            fd: Some(fd.lease()),
        }
    }

    #[inline(always)]
    pub(crate) fn raw_fd(&self) -> RawFd {
        self.fd
            .as_ref()
            .map_or(RuntimeFd::INVALID, RuntimeFdLease::raw_fd)
    }
}

impl Drop for RetainedListenerFd {
    fn drop(&mut self) {
        let Some(fd) = self.fd.take() else {
            return;
        };

        match Rc::try_unwrap(fd.into_core()) {
            Ok(core) => core.close_without_ring(),
            Err(core) => drop(core),
        }
    }
}

fn close_owned_in_active_context(owned: OwnedFd, provenance: LingerProvenance, allow_ring: bool) {
    let route = match provenance {
        LingerProvenance::KnownNonPositive => CloseRoute::Ring,
        LingerProvenance::Uncertain => {
            note_close_linger_query();
            classify_close_linger(owned.as_raw_fd())
        }
    };
    close_owned_by_route(owned, route, allow_ring);
}

fn close_owned_by_route(owned: OwnedFd, route: CloseRoute, allow_ring: bool) {
    match route {
        CloseRoute::Ring => {
            if allow_ring || completion_drain_active() {
                match try_submit_close(owned) {
                    CloseSubmission::Submitted | CloseSubmission::Deferred => return,
                    CloseSubmission::OutsideExecutor(owned) | CloseSubmission::Rejected(owned) => {
                        close_direct(owned);
                        return;
                    }
                }
            } else {
                close_direct(owned);
                return;
            }
        }
        CloseRoute::Direct => {
            close_direct(owned);
            return;
        }
        CloseRoute::Unknown => note_close_linger_classification_failure(),
        CloseRoute::Worker => {}
    }

    match try_admit_close(owned) {
        CloseAdmission::Admitted => {}
        CloseAdmission::OutsideExecutor(owned) => drop(owned),
        CloseAdmission::Full(owned) | CloseAdmission::Disconnected(owned) => {
            match disable_linger_for_fallback(owned.as_raw_fd()) {
                Ok(()) => note_close_linger_waiver(true, false),
                Err(_) => note_close_linger_waiver(false, true),
            }
            close_direct(owned);
        }
    }
}

#[inline(always)]
fn close_direct(owned: OwnedFd) {
    note_close_direct();
    drop(owned);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CloseRoute {
    Ring,
    Direct,
    Worker,
    Unknown,
}

#[inline(always)]
fn classify_close_linger(fd: RawFd) -> CloseRoute {
    classify_linger_result(read_linger(fd))
}

#[inline(always)]
fn classify_linger_result(result: io::Result<libc::linger>) -> CloseRoute {
    match result {
        Ok(linger) => classify_linger_value(&linger),
        Err(err) if linger_is_unsupported_or_invalid(&err) => CloseRoute::Direct,
        Err(_) => CloseRoute::Unknown,
    }
}

fn read_linger(fd: RawFd) -> io::Result<libc::linger> {
    let mut linger = libc::linger {
        l_onoff: 0,
        l_linger: 0,
    };
    let mut len = std::mem::size_of::<libc::linger>() as libc::socklen_t;
    // SAFETY: `linger` and `len` provide writable storage of the exact option
    // type and size for this descriptor query.
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_LINGER,
            std::ptr::addr_of_mut!(linger).cast(),
            std::ptr::addr_of_mut!(len),
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    if len as usize != std::mem::size_of::<libc::linger>() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "SO_LINGER returned an unexpected option length",
        ));
    }
    Ok(linger)
}

#[inline(always)]
fn classify_linger_value(linger: &libc::linger) -> CloseRoute {
    // Linux converts the enabled linger interval to an unsigned internal
    // timeout. A negative value observed through this signed C ABI therefore
    // denotes a very large, blocking-capable interval and must not reach the
    // owner-thread ring.
    if linger.l_onoff != 0 && linger.l_linger != 0 {
        CloseRoute::Worker
    } else {
        CloseRoute::Ring
    }
}

#[inline(always)]
fn linger_is_unsupported_or_invalid(err: &io::Error) -> bool {
    matches!(
        err.raw_os_error(),
        Some(libc::ENOTSOCK) | Some(libc::EBADF) | Some(libc::ENOPROTOOPT)
    )
}

/// Neutralizes linger after bounded worker admission fails.
///
/// Known-positive and unclassifiable states both reach this exceptional path.
/// Setting the option directly avoids depending on a second successful query;
/// failure is recorded before the same sole owner closes.
fn disable_linger_for_fallback(fd: RawFd) -> io::Result<()> {
    let disabled = libc::linger {
        l_onoff: 0,
        l_linger: 0,
    };
    // SAFETY: `disabled` is initialized and borrowed for the exact option
    // byte count; the kernel copies it during this call.
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_LINGER,
            std::ptr::addr_of!(disabled).cast(),
            std::mem::size_of::<libc::linger>() as libc::socklen_t,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Creates a distinctive closeable fd for internal fd-close tests.
///
/// The duplicate is placed above the ordinary low fd range, within the current
/// soft fd limit, so parallel tests are unlikely to recycle the same numeric
/// descriptor before close assertions run.
#[doc(hidden)]
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn distinctive_closeable_test_fd() -> io::Result<RawFd> {
    let mut fds = [-1; 2];
    // SAFETY: `fds` provides writable storage for the two descriptors returned
    // by socketpair; no pointers outlive this call.
    let rc = unsafe {
        libc::socketpair(
            libc::AF_UNIX,
            libc::SOCK_STREAM | libc::SOCK_CLOEXEC,
            0,
            fds.as_mut_ptr(),
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: successful socketpair returned two distinct sole owners.
    let endpoint = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    let peer = unsafe { OwnedFd::from_raw_fd(fds[1]) };
    drop(peer);
    Ok(into_distinctive_test_fd(endpoint).into_raw_fd())
}

/// Moves a test descriptor above the ordinary low-fd range when possible.
///
/// `F_DUPFD_CLOEXEC` preserves the underlying open file description. If the
/// duplicate cannot be created, the original sole owner is returned.
#[cfg(any(test, feature = "test-support"))]
fn into_distinctive_test_fd(fd: OwnedFd) -> OwnedFd {
    let min_fd = next_distinctive_test_fd_floor(fd.as_raw_fd());
    // SAFETY: `fd` remains open for this call, and `min_fd` is bounded to the
    // process soft fd limit when available.
    let duplicate = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_DUPFD_CLOEXEC, min_fd) };
    if duplicate < 0 {
        return fd;
    }

    // SAFETY: successful F_DUPFD_CLOEXEC returned one new descriptor whose
    // sole ownership transfers here. The original owner drops on return.
    unsafe { OwnedFd::from_raw_fd(duplicate) }
}

#[doc(hidden)]
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn raw_fd_is_closed(fd: RawFd) -> bool {
    // SAFETY: F_GETFD reads descriptor metadata and accepts any integer fd;
    // EBADF is the expected result for a closed descriptor.
    let rc = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    rc == -1 && io::Error::last_os_error().raw_os_error() == Some(libc::EBADF)
}

#[inline(always)]
#[cfg(all(test, not(miri)))]
fn close_raw_fd(fd: RawFd) {
    // SAFETY: test helpers call this only after taking sole ownership of `fd`.
    unsafe {
        libc::close(fd);
    }
}

#[cfg(any(test, feature = "test-support"))]
fn next_distinctive_test_fd_floor(fd: RawFd) -> RawFd {
    let candidate = NEXT_DISTINCTIVE_TEST_FD_BASE
        .fetch_add(DISTINCTIVE_TEST_FD_STRIDE, Ordering::Relaxed)
        .saturating_add(fd.rem_euclid(DISTINCTIVE_TEST_FD_STRIDE));
    match soft_open_fd_limit() {
        Some(limit) if limit > DISTINCTIVE_TEST_FD_START => candidate.min(limit - 1),
        _ => fd,
    }
}

#[cfg(all(any(test, feature = "test-support"), not(miri)))]
fn soft_open_fd_limit() -> Option<RawFd> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is writable for the duration of getrlimit.
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };
    if rc != 0 {
        return None;
    }
    if limit.rlim_cur == libc::RLIM_INFINITY {
        return Some(RawFd::MAX);
    }
    RawFd::try_from(limit.rlim_cur).ok()
}

#[cfg(all(any(test, feature = "test-support"), miri))]
fn soft_open_fd_limit() -> Option<RawFd> {
    // Miri does not emulate getrlimit. Falling back to the socketpair fd keeps
    // close-ownership coverage while avoiding an unsupported host query.
    None
}

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;
    use crate::runtime::executor::Executor;

    fn set_linger(fd: RawFd, onoff: libc::c_int, seconds: libc::c_int) {
        let linger = libc::linger {
            l_onoff: onoff,
            l_linger: seconds,
        };
        // SAFETY: linger is initialized and borrowed for the exact option
        // byte count during this call.
        let rc = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_LINGER,
                std::ptr::addr_of!(linger).cast(),
                std::mem::size_of::<libc::linger>() as libc::socklen_t,
            )
        };
        assert_eq!(rc, 0, "setsockopt(SO_LINGER) failed");
    }

    #[test]
    fn distinctive_closeable_test_fd_does_not_immediately_reuse_closed_fd() {
        let first = distinctive_closeable_test_fd().expect("first distinctive fd failed");
        close_raw_fd(first);

        let second = distinctive_closeable_test_fd().expect("second distinctive fd failed");
        let reused_first = second == first;
        close_raw_fd(second);

        assert!(
            !reused_first,
            "distinctive test fd helper immediately reused a closed fd"
        );
        assert!(
            raw_fd_is_closed(first),
            "first distinctive fd should remain closed after second allocation"
        );
    }

    #[test]
    fn runtime_fd_drop_outside_executor_closes_directly() {
        let raw = distinctive_closeable_test_fd().expect("distinctive fd failed");
        drop(RuntimeFd::from_fresh_raw_fd(raw));
        assert!(
            raw_fd_is_closed(raw),
            "outside-executor RuntimeFd drop must close its sole descriptor"
        );
    }

    #[test]
    fn final_retained_listener_owner_closes_without_reentering_the_ring() {
        let raw = distinctive_closeable_test_fd().expect("distinctive fd failed");
        let listener = RuntimeFd::from_fresh_raw_fd(raw);
        let retained = RetainedListenerFd::new(&listener);
        drop(listener);

        let mut executor = Executor::new().expect("executor construction failed");
        executor
            .run(async move {
                drop(retained);
            })
            .expect("retained-listener close run failed");

        #[cfg(debug_assertions)]
        {
            let stats = executor.last_stats();
            assert_eq!(stats.close_ring_submissions, 0);
            assert_eq!(stats.close_direct_closes, 1);
            assert_eq!(stats.close_worker_admissions, 0);
        }
        assert!(
            raw_fd_is_closed(raw),
            "final retained listener owner must close its descriptor"
        );
    }

    #[test]
    fn unknown_linger_state_routes_conservatively_to_the_worker() {
        let raw = distinctive_closeable_test_fd().expect("distinctive fd failed");
        // SAFETY: the helper returned one open descriptor whose sole ownership
        // moves into this test owner.
        let owned = unsafe { OwnedFd::from_raw_fd(raw) };
        let mut executor = Executor::new().expect("executor construction failed");

        executor
            .run(async move {
                close_owned_by_route(owned, CloseRoute::Unknown, true);
            })
            .expect("unknown-linger worker run failed");
        #[cfg(debug_assertions)]
        {
            let stats = executor.last_stats();
            assert_eq!(stats.close_direct_closes, 0);
            assert_eq!(stats.close_worker_admissions, 1);
            assert_eq!(stats.close_linger_classification_failures, 1);
        }
        drop(executor);
        assert!(raw_fd_is_closed(raw));
    }

    #[test]
    fn non_socket_descriptor_has_no_linger_and_closes_directly() {
        let mut fds = [-1; 2];
        // SAFETY: fds provides exact writable storage for both pipe owners.
        let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) };
        assert_eq!(rc, 0, "pipe2 failed");
        // SAFETY: successful pipe2 returned two distinct sole owners.
        let endpoint = into_distinctive_test_fd(unsafe { OwnedFd::from_raw_fd(fds[0]) });
        let peer = into_distinctive_test_fd(unsafe { OwnedFd::from_raw_fd(fds[1]) });
        let raw = endpoint.as_raw_fd();
        assert_eq!(classify_close_linger(raw), CloseRoute::Direct);

        let mut executor = Executor::new().expect("executor construction failed");
        executor
            .run(async move {
                drop(RuntimeFd::from_external_owned(endpoint));
            })
            .expect("non-socket direct-close run failed");
        #[cfg(debug_assertions)]
        {
            let stats = executor.last_stats();
            assert_eq!(stats.close_direct_closes, 1);
            assert_eq!(stats.close_worker_admissions, 0);
        }
        assert!(raw_fd_is_closed(raw));
        drop(executor);
        drop(peer);
    }

    #[test]
    fn linger_classification_preserves_disabled_and_abortive_modes() {
        let mut fds = [-1; 2];
        // SAFETY: fds has exact writable space for a socketpair result.
        let rc = unsafe {
            libc::socketpair(
                libc::AF_UNIX,
                libc::SOCK_STREAM | libc::SOCK_CLOEXEC,
                0,
                fds.as_mut_ptr(),
            )
        };
        assert_eq!(rc, 0, "socketpair failed");
        // SAFETY: successful socketpair returned two distinct sole owners.
        let endpoint = unsafe { OwnedFd::from_raw_fd(fds[0]) };
        let peer = unsafe { OwnedFd::from_raw_fd(fds[1]) };

        assert_eq!(
            classify_close_linger(endpoint.as_raw_fd()),
            CloseRoute::Ring
        );
        let disabled = read_linger(endpoint.as_raw_fd()).expect("getsockopt(SO_LINGER) failed");
        assert_eq!(disabled.l_onoff, 0);

        set_linger(endpoint.as_raw_fd(), 1, 0);
        assert_eq!(
            classify_close_linger(endpoint.as_raw_fd()),
            CloseRoute::Ring
        );
        let abortive = read_linger(endpoint.as_raw_fd()).expect("getsockopt(SO_LINGER) failed");
        assert_ne!(abortive.l_onoff, 0);
        assert_eq!(abortive.l_linger, 0);

        set_linger(endpoint.as_raw_fd(), 1, 2);
        assert_eq!(
            classify_close_linger(endpoint.as_raw_fd()),
            CloseRoute::Worker
        );
        disable_linger_for_fallback(endpoint.as_raw_fd()).unwrap();
        let waived = read_linger(endpoint.as_raw_fd()).expect("getsockopt(SO_LINGER) failed");
        assert_eq!(waived.l_onoff, 0);

        drop(endpoint);
        drop(peer);
    }
}

#[cfg(test)]
mod policy_tests {
    use super::{
        CloseRoute, LingerProvenance, RetainedListenerFd, RuntimeFd, RuntimeFdLease,
        RuntimeFdOpState, RuntimeFdRef, classify_linger_result, classify_linger_value,
        with_final_core_drop_hook_for_test,
    };
    use crate::runtime::op::CompletionState;
    use static_assertions::{assert_impl_all, assert_not_impl_any};
    use std::cell::Cell;
    use std::io;
    use std::panic::{AssertUnwindSafe, UnwindSafe, catch_unwind};

    thread_local! {
        static OUTER_FINAL_CORE_DROPS: Cell<usize> = const { Cell::new(0) };
        static INNER_FINAL_CORE_DROPS: Cell<usize> = const { Cell::new(0) };
    }

    fn count_outer_final_core_drop(_: i32) {
        OUTER_FINAL_CORE_DROPS.with(|count| count.set(count.get() + 1));
    }

    fn count_inner_final_core_drop(_: i32) {
        INNER_FINAL_CORE_DROPS.with(|count| count.set(count.get() + 1));
    }

    assert_impl_all!(RuntimeFd: UnwindSafe);
    assert_impl_all!(RuntimeFdRef<'static>: UnwindSafe);
    assert_impl_all!(RuntimeFdLease: UnwindSafe);
    assert_impl_all!(RuntimeFdOpState<'static>: UnwindSafe);
    assert_not_impl_any!(RuntimeFd: Send, Sync, std::panic::RefUnwindSafe);
    assert_not_impl_any!(RuntimeFdRef<'static>: Send, Sync, std::panic::RefUnwindSafe);
    assert_not_impl_any!(RuntimeFdLease: Send, Sync, std::panic::RefUnwindSafe);
    assert_not_impl_any!(RuntimeFdOpState<'static>: Send, Sync, std::panic::RefUnwindSafe);

    #[test]
    fn final_core_drop_hook_scope_restores_previous_hook_after_unwind() {
        OUTER_FINAL_CORE_DROPS.with(|count| count.set(0));
        INNER_FINAL_CORE_DROPS.with(|count| count.set(0));

        let unwind = with_final_core_drop_hook_for_test(count_outer_final_core_drop, || {
            let unwind = catch_unwind(AssertUnwindSafe(|| {
                with_final_core_drop_hook_for_test(count_inner_final_core_drop, || {
                    drop(RuntimeFd::from_fresh_raw_fd(-2));
                    panic!("nested final-core hook panic");
                });
            }));

            drop(RuntimeFd::from_fresh_raw_fd(-2));
            unwind
        });

        drop(RuntimeFd::from_fresh_raw_fd(-2));

        assert!(unwind.is_err(), "nested hook scope did not unwind");
        INNER_FINAL_CORE_DROPS.with(|count| assert_eq!(count.get(), 1));
        OUTER_FINAL_CORE_DROPS.with(|count| assert_eq!(count.get(), 1));
    }

    #[test]
    fn runtime_fd_and_operation_state_layouts_match_frozen_words() {
        let word = std::mem::size_of::<usize>();
        assert_eq!(std::mem::size_of::<RuntimeFd>(), word);
        assert_eq!(std::mem::align_of::<RuntimeFd>(), 4);
        assert_eq!(std::mem::size_of::<RuntimeFdRef<'static>>(), word);
        assert_eq!(std::mem::size_of::<RuntimeFdLease>(), word);
        assert_eq!(std::mem::size_of::<RuntimeFdOpState<'static>>(), word);
    }

    #[test]
    fn borrowed_capability_clones_exactly_one_initial_lease() {
        let runtime = RuntimeFd::from_fresh_raw_fd(-1);
        assert_eq!(runtime.strong_count_for_test(), 1);
        let mut fd_state = runtime.op_state();
        assert_eq!(fd_state.raw_fd(), -1);
        assert_eq!(runtime.strong_count_for_test(), 1);

        let lease = unsafe { fd_state.take_initial_lease() };
        assert_eq!(runtime.strong_count_for_test(), 2);
        assert_eq!(lease.strong_count_for_test(), 2);
        assert_eq!(fd_state.raw_fd(), -1);

        drop(lease);
        assert_eq!(runtime.strong_count_for_test(), 1);
    }

    #[test]
    fn staged_capability_moves_one_lease_without_a_second_clone() {
        let runtime = RuntimeFd::from_fresh_raw_fd(-1);
        let lease = runtime.lease();
        assert_eq!(runtime.strong_count_for_test(), 2);
        let mut fd_state = lease.into_op_state();
        assert!(fd_state.owns_staged_lease_for_test());
        assert_eq!(runtime.strong_count_for_test(), 2);

        let lease = unsafe { fd_state.take_initial_lease() };
        assert!(!fd_state.owns_staged_lease_for_test());
        assert_eq!(runtime.strong_count_for_test(), 2);
        assert_eq!(lease.raw_fd(), -1);

        drop(lease);
        assert_eq!(runtime.strong_count_for_test(), 1);
    }

    #[test]
    fn unsubmitted_staged_capability_releases_its_owned_lease() {
        let runtime = RuntimeFd::from_fresh_raw_fd(-1);
        let fd_state = runtime.lease().into_op_state();
        assert!(fd_state.owns_staged_lease_for_test());
        assert_eq!(runtime.strong_count_for_test(), 2);
        drop(fd_state);
        assert_eq!(runtime.strong_count_for_test(), 1);
    }

    #[test]
    fn borrowed_submission_retirement_restores_fresh_capability() {
        let runtime = RuntimeFd::from_fresh_raw_fd(-7);
        let mut fd_state = runtime.op_state();
        let mut first_completion = CompletionState::empty();
        first_completion.attach_fd_lease(unsafe { fd_state.take_initial_lease() });
        let first_ptr = std::ptr::from_mut(&mut first_completion);
        unsafe { fd_state.publish_submitted_state(first_ptr) };

        assert_eq!(fd_state.take_state_ptr(), first_ptr);
        assert!(fd_state.state_ptr().is_null());
        assert_eq!(fd_state.raw_fd(), -7);
        drop(first_completion.take_fd_lease());
        assert_eq!(runtime.strong_count_for_test(), 1);

        let mut second_completion = CompletionState::empty();
        second_completion.attach_fd_lease(unsafe { fd_state.take_initial_lease() });
        let second_ptr = std::ptr::from_mut(&mut second_completion);
        unsafe { fd_state.publish_submitted_state(second_ptr) };

        assert_eq!(fd_state.state_ptr(), second_ptr);
        assert_eq!(fd_state.take_state_ptr(), second_ptr);
        assert_eq!(fd_state.raw_fd(), -7);
        drop(second_completion.take_fd_lease());
        assert_eq!(runtime.strong_count_for_test(), 1);
    }

    #[test]
    fn owned_submission_retirement_leaves_terminal_state() {
        let runtime = RuntimeFd::from_fresh_raw_fd(-7);
        let mut fd_state = runtime.lease().into_op_state();
        let mut completion = CompletionState::empty();
        completion.attach_fd_lease(unsafe { fd_state.take_initial_lease() });
        let completion_ptr = std::ptr::from_mut(&mut completion);
        unsafe { fd_state.publish_submitted_state(completion_ptr) };

        assert_eq!(fd_state.take_state_ptr(), completion_ptr);
        assert!(fd_state.state_ptr().is_null());
        assert_eq!(fd_state.raw_fd(), RuntimeFd::INVALID);
        drop(completion.take_fd_lease());
        assert_eq!(runtime.strong_count_for_test(), 1);
    }

    #[test]
    fn borrowed_and_owned_states_preserve_provenance_across_move_and_publication() {
        let runtime = RuntimeFd::from_fresh_raw_fd(-1);
        let moved_runtime = runtime;
        let borrowed = moved_runtime.op_state();
        let mut borrowed = borrowed;
        let mut completion = CompletionState::empty();
        completion.attach_fd_lease(unsafe { borrowed.take_initial_lease() });

        let completion_ptr = std::ptr::from_mut(&mut completion);
        unsafe { borrowed.publish_submitted_state(completion_ptr) };
        assert_eq!(borrowed.state_ptr(), completion_ptr);
        assert_eq!(borrowed.raw_fd(), -1);
        assert_eq!(borrowed.take_state_ptr(), completion_ptr);
        assert!(borrowed.state_ptr().is_null());

        let staged = moved_runtime.lease().into_op_state();
        assert!(staged.owns_staged_lease_for_test());
        let moved_staged = staged;
        drop(moved_staged);
        assert_eq!(moved_runtime.strong_count_for_test(), 2);

        drop(completion.take_fd_lease());
        assert_eq!(moved_runtime.strong_count_for_test(), 1);
    }

    #[test]
    fn linger_provenance_is_monotonic_after_raw_exposure() {
        let runtime = RuntimeFd::from_fresh_raw_fd(-1);
        assert_eq!(
            runtime.linger_provenance(),
            LingerProvenance::KnownNonPositive
        );
        assert_eq!(runtime.expose_raw_fd(), -1);
        assert_eq!(runtime.linger_provenance(), LingerProvenance::Uncertain);
        runtime.mark_linger_uncertain();
        assert_eq!(runtime.linger_provenance(), LingerProvenance::Uncertain);
    }

    #[test]
    fn retained_listener_observes_late_provenance_taint() {
        let listener = RuntimeFd::from_fresh_raw_fd(-1);
        let retained = RetainedListenerFd::new(&listener);

        listener.mark_linger_uncertain();
        assert_eq!(
            retained
                .fd
                .as_ref()
                .expect("retained listener owner")
                .linger_provenance(),
            LingerProvenance::Uncertain
        );

        drop(listener);
        drop(retained);
    }

    #[test]
    fn enabled_nonzero_linger_requires_the_worker() {
        for linger in [
            libc::linger {
                l_onoff: 0,
                l_linger: 3,
            },
            libc::linger {
                l_onoff: 1,
                l_linger: 0,
            },
        ] {
            assert_eq!(classify_linger_value(&linger), CloseRoute::Ring);
        }
        for linger in [
            libc::linger {
                l_onoff: 1,
                l_linger: -1,
            },
            libc::linger {
                l_onoff: 1,
                l_linger: 3,
            },
        ] {
            assert_eq!(classify_linger_value(&linger), CloseRoute::Worker);
        }
    }

    #[test]
    fn unsupported_or_invalid_descriptors_close_directly_but_other_failures_are_unknown() {
        for errno in [libc::ENOTSOCK, libc::EBADF, libc::ENOPROTOOPT] {
            assert_eq!(
                classify_linger_result(Err(io::Error::from_raw_os_error(errno))),
                CloseRoute::Direct
            );
        }
        assert_eq!(
            classify_linger_result(Err(io::Error::from_raw_os_error(libc::EIO))),
            CloseRoute::Unknown
        );
    }
}
