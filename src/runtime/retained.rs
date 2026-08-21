//! Internal retained operation-payload storage.
//!
//! This module provides fixed-size-class storage for operation payload structs
//! whose memory may be referenced by the kernel after the owning future is
//! dropped.
//!
//! Normal reclamation requires the referring target CQE. If bounded reactor
//! shutdown abandons a ring before that CQE is observed, the reactor
//! deliberately leaks this pool and every unresolved value rather than
//! deallocating or recycling memory the kernel may still reference.
//!
//! The common path is slab-backed and heap-free after warmup. Payloads larger
//! than 65536 bytes, payloads requiring alignment greater than 64 bytes, and
//! slab-allocation failures fall back to the global heap. That fallback is
//! intentional so I/O submission does not fail merely because a retained
//! payload is unusual, but it must stay visible through debug counters and
//! documentation because it is not the desired steady-state fast path.
//!
//! Retained vectored I/O scratch is separate from retained payload storage. The
//! scratch stores only kernel-facing `iovec` pointer/length metadata; message
//! bytes remain in the owned payload buffers. Scratch uses inline storage for
//! small submissions and size-classed slab storage for larger submissions. It
//! never uses heap fallback: oversized requests return `InvalidInput`, and
//! slab allocation failure returns `WouldBlock`.

use crate::utils::disarm_unwind_guard;
use crate::utils::list::intrusive::slist::{Link, SList};
use crate::utils::memory::provider::{BasicMemoryProvider, ProviderOwner};
use crate::utils::memory::slab::{SlabAllocator, SlabAllocatorConfigError, SlabPageChain};
use std::alloc::Layout;
use std::cell::UnsafeCell;
use std::io;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::rc::Rc;
use std::slice;

const RETAINED_BLOCK_ALIGN: usize = 64;
const RETAINED_SLAB_TARGET_BYTES: usize = 64 * 1024;
const RETAINED_SIZE_CLASSES: [usize; 11] = [
    64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536,
];
pub(crate) const RETAINED_IOVEC_INLINE_COUNT: usize = 16;
const RETAINED_IOVEC_SIZE_CLASSES: [usize; 4] = [64, 128, 512, 1024];
pub(crate) const RETAINED_IOVEC_MAX_COUNT: usize =
    RETAINED_IOVEC_SIZE_CLASSES[RETAINED_IOVEC_SIZE_CLASSES.len() - 1];
pub(crate) const RETAINED_IOVEC_OVERSIZE_MESSAGE: &str =
    "active iovec count exceeds retained scratch capacity";

const _: () = {
    assert!(RETAINED_BLOCK_ALIGN.is_power_of_two());
    let mut class_index = 0;
    while class_index < RETAINED_SIZE_CLASSES.len() {
        assert!(RETAINED_SIZE_CLASSES[class_index] >= std::mem::size_of::<Link>());
        assert!(RETAINED_SIZE_CLASSES[class_index].is_multiple_of(RETAINED_BLOCK_ALIGN));
        class_index += 1;
    }
    let mut iovec_class_index = 0;
    while iovec_class_index < RETAINED_IOVEC_SIZE_CLASSES.len() {
        let block_size =
            RETAINED_IOVEC_SIZE_CLASSES[iovec_class_index] * std::mem::size_of::<libc::iovec>();
        assert!(block_size >= std::mem::size_of::<Link>());
        assert!(block_size.is_multiple_of(RETAINED_BLOCK_ALIGN));
        iovec_class_index += 1;
    }
    assert!(RETAINED_IOVEC_INLINE_COUNT < RETAINED_IOVEC_SIZE_CLASSES[0]);
    assert!(RETAINED_IOVEC_SIZE_CLASSES[0] < RETAINED_IOVEC_SIZE_CLASSES[1]);
    assert!(RETAINED_IOVEC_SIZE_CLASSES[1] < RETAINED_IOVEC_SIZE_CLASSES[2]);
    assert!(RETAINED_IOVEC_SIZE_CLASSES[2] < RETAINED_IOVEC_SIZE_CLASSES[3]);
};

/// Invariant lifetime brand for one synchronous retained-slot scope.
///
/// The lifetime appears in both a contravariant argument and covariant return
/// position, so neither slot typestate can be widened and escape the
/// higher-ranked reservation closure.
type RetainedSlotBrand<'slot> = PhantomData<fn(&'slot ()) -> &'slot ()>;

#[cfg(test)]
const TEST_POISONED_HEAP_CAPACITY: usize = 8;

/// Test-only deferred cleanup for heap backing deliberately poisoned after a
/// partial write. The cleanup releases only `MaybeUninit<T>` storage and must
/// never run a destructor for the incomplete `T`.
#[cfg(test)]
#[derive(Clone, Copy)]
struct TestPoisonedHeap {
    ptr: *mut (),
    cleanup: unsafe fn(*mut ()),
}

#[derive(Clone, Copy)]
pub(crate) struct RetainedPayloadVtable {
    /// Drops the stored value and releases the backing allocation.
    pub(crate) drop_and_free: unsafe fn(*mut (), *mut RetainedPayloadPool),
    /// Releases backing storage after the value has been moved out.
    pub(crate) free_storage: unsafe fn(*mut (), *mut RetainedPayloadPool),
}

#[cfg(any(debug_assertions, feature = "test-support"))]
macro_rules! retained_payload_stat_fields {
    ($callback:ident) => {
        $callback! {
            pooled_allocs => retained_pooled_allocs:
                "Retained payload allocations served by size-class storage.";
            pooled_reuses => retained_pooled_reuses:
                "Retained payload allocations served from returned blocks.";
            pooled_frees => retained_pooled_frees:
                "Retained payload blocks returned to size-class free lists.";
            slab_allocs => retained_slab_allocs:
                "Retained payload slab pages requested from providers.";
            heap_fallbacks => retained_heap_fallbacks:
                "Retained payload allocations that used the heap fallback.";
            heap_frees => retained_heap_frees:
                "Heap fallback payload blocks released.";
            writev_scratch_inline_allocs => writev_scratch_inline_allocs:
                "Iovec scratch requests served from inline storage.";
            writev_scratch_pooled_allocs => writev_scratch_pooled_allocs:
                "Iovec scratch requests served by pooled sidecar storage.";
            writev_scratch_pooled_reuses => writev_scratch_pooled_reuses:
                "Pooled sidecar scratch requests served from returned blocks.";
            writev_scratch_pooled_frees => writev_scratch_pooled_frees:
                "Pooled sidecar scratch blocks returned to size-class free lists.";
            writev_scratch_slab_allocs => writev_scratch_slab_allocs:
                "Sidecar scratch slab pages requested from providers.";
            writev_scratch_oversize_rejections => writev_scratch_oversize_rejections:
                "Scratch requests rejected for exceeding the supported iovec count.";
            writev_scratch_alloc_failures => writev_scratch_alloc_failures:
                "Scratch requests rejected because no sidecar block was available.";
        }
    };
}

#[cfg(any(debug_assertions, feature = "test-support"))]
pub(crate) use retained_payload_stat_fields;

#[cfg(any(debug_assertions, feature = "test-support"))]
macro_rules! define_retained_payload_pool_stats {
    ($($field:ident => $runtime_field:ident: $doc:literal;)*) => {
        /// Debug/test-support counters for asserting retained-pool behavior in tests.
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
        pub(crate) struct RetainedPayloadPoolStats {
            $(
                #[doc = $doc]
                pub(crate) $field: usize,
            )*
        }
    };
}

#[cfg(any(debug_assertions, feature = "test-support"))]
retained_payload_stat_fields!(define_retained_payload_pool_stats);

#[cfg(any(debug_assertions, feature = "test-support"))]
macro_rules! impl_retained_payload_pool_stats_delta {
    ($($field:ident => $runtime_field:ident: $doc:literal;)*) => {
        impl RetainedPayloadPoolStats {
            /// Returns counter activity observed since `baseline` without mutating the
            /// retained pools. Saturating subtraction preserves debug bookkeeping if a
            /// counter has already saturated or a synthetic test baseline is newer.
            #[cfg_attr(
                all(not(debug_assertions), feature = "test-support"),
                allow(dead_code)
            )]
            pub(crate) fn saturating_delta_since(self, baseline: Self) -> Self {
                Self {
                    $(
                        $field: self.$field.saturating_sub(baseline.$field),
                    )*
                }
            }
        }
    };
}

#[cfg(any(debug_assertions, feature = "test-support"))]
retained_payload_stat_fields!(impl_retained_payload_pool_stats_delta);

#[cfg(any(debug_assertions, feature = "test-support"))]
#[inline(always)]
fn record_class_allocation(
    result: &ClassAllocResult,
    allocations: &mut usize,
    reuses: &mut usize,
    slab_allocations: &mut usize,
) {
    // The class has already removed the block from its free list. Saturation
    // keeps debug/test bookkeeping from panicking after that ownership change.
    *allocations = allocations.saturating_add(1);
    if result.reused {
        *reuses = reuses.saturating_add(1);
    }
    if result.new_slab {
        *slab_allocations = slab_allocations.saturating_add(1);
    }
}

/// Scratch-only counters stored with the heap-stable iovec sidecar owner.
#[cfg(any(debug_assertions, feature = "test-support"))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct RetainedIovecScratchStats {
    writev_scratch_inline_allocs: usize,
    writev_scratch_pooled_allocs: usize,
    writev_scratch_pooled_reuses: usize,
    writev_scratch_pooled_frees: usize,
    writev_scratch_slab_allocs: usize,
    writev_scratch_oversize_rejections: usize,
    writev_scratch_alloc_failures: usize,
}

#[cfg(any(debug_assertions, feature = "test-support"))]
impl RetainedIovecScratchStats {
    fn apply_to(self, stats: &mut RetainedPayloadPoolStats) {
        stats.writev_scratch_inline_allocs = self.writev_scratch_inline_allocs;
        stats.writev_scratch_pooled_allocs = self.writev_scratch_pooled_allocs;
        stats.writev_scratch_pooled_reuses = self.writev_scratch_pooled_reuses;
        stats.writev_scratch_pooled_frees = self.writev_scratch_pooled_frees;
        stats.writev_scratch_slab_allocs = self.writev_scratch_slab_allocs;
        stats.writev_scratch_oversize_rejections = self.writev_scratch_oversize_rejections;
        stats.writev_scratch_alloc_failures = self.writev_scratch_alloc_failures;
    }
}

/// Heap-stable owner for pooled iovec scratch classes.
///
/// The runtime is single-threaded. `UnsafeCell` permits a pooled scratch lease
/// to return its block after the outer retained-payload pool has moved or been
/// dropped; every such lease owns an `Rc` that keeps this allocation alive.
struct RetainedIovecScratchPool {
    state: UnsafeCell<RetainedIovecScratchPoolState>,
}

struct RetainedIovecScratchPoolState {
    classes: [RetainedSizeClass; RETAINED_IOVEC_SIZE_CLASSES.len()],
    /// One heap-stable provider shared by every iovec size class. This field
    /// follows `classes` so all slab allocators release their pages first.
    _provider: ProviderOwner<BasicMemoryProvider>,
    #[cfg(any(debug_assertions, feature = "test-support"))]
    stats: RetainedIovecScratchStats,
}

impl RetainedIovecScratchPool {
    fn new() -> io::Result<Rc<Self>> {
        let provider = ProviderOwner::new(BasicMemoryProvider::new());
        let classes = [
            RetainedSizeClass::new(&provider, iovec_class_block_size(0))?,
            RetainedSizeClass::new(&provider, iovec_class_block_size(1))?,
            RetainedSizeClass::new(&provider, iovec_class_block_size(2))?,
            RetainedSizeClass::new(&provider, iovec_class_block_size(3))?,
        ];
        Ok(Rc::new(Self {
            state: UnsafeCell::new(RetainedIovecScratchPoolState {
                classes,
                _provider: provider,
                #[cfg(any(debug_assertions, feature = "test-support"))]
                stats: RetainedIovecScratchStats::default(),
            }),
        }))
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[inline(always)]
    fn record_inline_alloc(&self) {
        // SAFETY: FlowIO confines the retained pool and every scratch lease to
        // one executor owner thread, and this synchronous update does not
        // overlap another access to the sidecar state.
        unsafe {
            let stats = &mut (*self.state.get()).stats;
            stats.writev_scratch_inline_allocs =
                stats.writev_scratch_inline_allocs.saturating_add(1);
        }
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[inline(always)]
    fn record_oversize_rejection(&self) {
        // SAFETY: same owner-thread-only access invariant as
        // `record_inline_alloc`.
        unsafe {
            let stats = &mut (*self.state.get()).stats;
            stats.writev_scratch_oversize_rejections =
                stats.writev_scratch_oversize_rejections.saturating_add(1);
        }
    }

    #[inline(always)]
    fn alloc_block(&self, class_index: usize) -> Option<ClassAllocResult> {
        // SAFETY: allocation is synchronous on the executor owner thread. No
        // scratch Drop can re-enter while this exclusive state access is live.
        let state = unsafe { &mut *self.state.get() };
        let Some(result) = state.classes[class_index].alloc_block() else {
            #[cfg(any(debug_assertions, feature = "test-support"))]
            {
                state.stats.writev_scratch_alloc_failures =
                    state.stats.writev_scratch_alloc_failures.saturating_add(1);
            }
            return None;
        };

        #[cfg(any(debug_assertions, feature = "test-support"))]
        {
            record_class_allocation(
                &result,
                &mut state.stats.writev_scratch_pooled_allocs,
                &mut state.stats.writev_scratch_pooled_reuses,
                &mut state.stats.writev_scratch_slab_allocs,
            );
        }

        Some(result)
    }

    #[inline(always)]
    /// Returns an iovec sidecar block to its size-class free list.
    ///
    /// # Safety
    ///
    /// `class_index` must identify the class that allocated `ptr`, and `ptr`
    /// must not already have been returned.
    unsafe fn free_block(&self, class_index: usize, ptr: *mut u8) {
        // SAFETY: Drop runs synchronously on the executor owner thread, the
        // lease's Rc keeps this state alive, and the caller supplies the exact
        // class/block pair recorded at allocation.
        let state = unsafe { &mut *self.state.get() };
        unsafe { state.classes[class_index].free_block(ptr) };
        #[cfg(any(debug_assertions, feature = "test-support"))]
        {
            state.stats.writev_scratch_pooled_frees =
                state.stats.writev_scratch_pooled_frees.saturating_add(1);
        }
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    fn stats(&self) -> RetainedIovecScratchStats {
        // SAFETY: snapshots are taken synchronously on the owner thread and do
        // not overlap allocation or release.
        unsafe { (*self.state.get()).stats }
    }
}

/// Raw, size-classed pool for retained operation payloads.
pub(crate) struct RetainedPayloadPool {
    /// Size classes for retained operation payload structs.
    classes: [RetainedSizeClass; RETAINED_SIZE_CLASSES.len()],
    /// One heap-stable provider shared by every retained-payload size class.
    /// This field follows `classes` so their slab pages are released first.
    _provider: ProviderOwner<BasicMemoryProvider>,
    /// Heap-stable owner for retained sidecar `iovec` scratch arrays.
    iovec_pool: Rc<RetainedIovecScratchPool>,
    #[cfg(any(debug_assertions, feature = "test-support"))]
    /// Debug/test-support counters exported through runtime stats and tests.
    stats: RetainedPayloadPoolStats,
    /// Fixed, nonallocating cleanup records used only so deliberate
    /// partial-initialization poison tests remain Miri leak-clean.
    #[cfg(test)]
    test_poisoned_heaps: [MaybeUninit<TestPoisonedHeap>; TEST_POISONED_HEAP_CAPACITY],
    /// Number of initialized entries in `test_poisoned_heaps`.
    #[cfg(test)]
    test_poisoned_heap_len: usize,
    /// Exact count of writing slots poisoned in this pool's unit tests.
    #[cfg(test)]
    test_poisoned_writes: usize,
}

impl RetainedPayloadPool {
    pub(crate) fn new() -> io::Result<Self> {
        let iovec_pool = RetainedIovecScratchPool::new()?;
        let provider = ProviderOwner::new(BasicMemoryProvider::new());
        let classes = [
            RetainedSizeClass::new(&provider, RETAINED_SIZE_CLASSES[0])?,
            RetainedSizeClass::new(&provider, RETAINED_SIZE_CLASSES[1])?,
            RetainedSizeClass::new(&provider, RETAINED_SIZE_CLASSES[2])?,
            RetainedSizeClass::new(&provider, RETAINED_SIZE_CLASSES[3])?,
            RetainedSizeClass::new(&provider, RETAINED_SIZE_CLASSES[4])?,
            RetainedSizeClass::new(&provider, RETAINED_SIZE_CLASSES[5])?,
            RetainedSizeClass::new(&provider, RETAINED_SIZE_CLASSES[6])?,
            RetainedSizeClass::new(&provider, RETAINED_SIZE_CLASSES[7])?,
            RetainedSizeClass::new(&provider, RETAINED_SIZE_CLASSES[8])?,
            RetainedSizeClass::new(&provider, RETAINED_SIZE_CLASSES[9])?,
            RetainedSizeClass::new(&provider, RETAINED_SIZE_CLASSES[10])?,
        ];
        Ok(Self {
            classes,
            _provider: provider,
            iovec_pool,
            #[cfg(any(debug_assertions, feature = "test-support"))]
            stats: RetainedPayloadPoolStats::default(),
            #[cfg(test)]
            test_poisoned_heaps: [MaybeUninit::uninit(); TEST_POISONED_HEAP_CAPACITY],
            #[cfg(test)]
            test_poisoned_heap_len: 0,
            #[cfg(test)]
            test_poisoned_writes: 0,
        })
    }

    /// Stores `value` in pointer-stable retained storage.
    ///
    /// The returned handle must be consumed exactly once by either
    /// [`RetainedPayload::take`] or [`RetainedPayload::drop_and_free`].
    #[inline(always)]
    pub(crate) fn alloc<T: 'static>(&mut self, value: T) -> RetainedPayload<T> {
        // Keep this established by-value path independent of the raw-slot RAII
        // guard below. The Slice 68 optimized-code oracle proved that routing
        // safe callers through that guard enlarged unaffected whole-payload
        // stream transfers; this direct shape preserves their existing codegen.
        match class_index_for::<T>() {
            Some(class_index) => {
                if let Some(result) = self.classes[class_index].alloc_block() {
                    #[cfg(any(debug_assertions, feature = "test-support"))]
                    record_class_allocation(
                        &result,
                        &mut self.stats.pooled_allocs,
                        &mut self.stats.pooled_reuses,
                        &mut self.stats.slab_allocs,
                    );

                    let ptr = result.ptr as *mut T;
                    unsafe { ptr.write(value) };
                    return unsafe {
                        RetainedPayload::from_raw_parts(ptr, pooled_vtable::<T>(class_index))
                    };
                }

                self.alloc_heap(value)
            }
            None => self.alloc_heap(value),
        }
    }

    #[inline(always)]
    fn alloc_heap<T: 'static>(&mut self, value: T) -> RetainedPayload<T> {
        #[cfg(any(debug_assertions, feature = "test-support"))]
        {
            self.stats.heap_fallbacks = self.stats.heap_fallbacks.saturating_add(1);
        }

        let ptr = Box::into_raw(Box::new(value));
        unsafe { RetainedPayload::from_raw_parts(ptr, heap_vtable::<T>()) }
    }

    /// Reserves uninitialized storage through the same pooled/heap policy as
    /// [`Self::alloc`] without retaining a Rust borrow across the caller's
    /// synchronous scope. The returned raw slot recycles untouched backing on
    /// Drop.
    ///
    /// # Safety
    ///
    /// `pool` must point to a live pool on its executor owner thread and must
    /// not move while the returned slot can drop. The invariant `'slot`
    /// lifetime must be chosen only for one invocation of
    /// [`with_raw_retained_slot`]'s higher-ranked closure.
    #[inline(always)]
    unsafe fn alloc_raw_slot_from_raw<'slot, T: 'static>(
        pool: NonNull<RetainedPayloadPool>,
    ) -> RawRetainedSlot<'slot, T> {
        if let Some(class_index) = class_index_for::<T>() {
            // SAFETY: guaranteed by the caller. This temporary borrow ends
            // before the returned guard can be observed or re-entered.
            let pool_ref = unsafe { &mut *pool.as_ptr() };
            if let Some(result) = pool_ref.classes[class_index].alloc_block() {
                #[cfg(any(debug_assertions, feature = "test-support"))]
                record_class_allocation(
                    &result,
                    &mut pool_ref.stats.pooled_allocs,
                    &mut pool_ref.stats.pooled_reuses,
                    &mut pool_ref.stats.slab_allocs,
                );

                return unsafe {
                    RawRetainedSlot::new(
                        result.ptr.cast::<MaybeUninit<T>>(),
                        pool,
                        pooled_vtable::<T>(class_index),
                        #[cfg(test)]
                        None,
                    )
                };
            }
        }

        unsafe { Self::alloc_heap_raw_slot_from_raw(pool) }
    }

    #[inline(always)]
    unsafe fn alloc_heap_raw_slot_from_raw<'slot, T: 'static>(
        pool: NonNull<RetainedPayloadPool>,
    ) -> RawRetainedSlot<'slot, T> {
        let ptr = Box::into_raw(Box::<T>::new_uninit());
        #[cfg(any(debug_assertions, feature = "test-support"))]
        {
            // SAFETY: guaranteed by the caller. The borrow ends before the
            // raw slot below becomes observable.
            let pool_ref = unsafe { &mut *pool.as_ptr() };
            pool_ref.stats.heap_fallbacks = pool_ref.stats.heap_fallbacks.saturating_add(1);
        }

        // SAFETY: `Box::into_raw` returns a non-null, correctly aligned live
        // `MaybeUninit<T>` allocation, including its aligned dangling value for
        // a zero-sized `T`.
        unsafe {
            RawRetainedSlot::new(
                ptr,
                pool,
                heap_vtable::<T>(),
                #[cfg(test)]
                Some(test_cleanup_poisoned_heap::<T>),
            )
        }
    }

    /// Records one deliberately poisoned writing slot without allocating or
    /// inspecting any partially initialized `T`.
    #[cfg(test)]
    #[inline(always)]
    unsafe fn record_test_poison(
        &mut self,
        ptr: *mut (),
        heap_cleanup: Option<unsafe fn(*mut ())>,
    ) {
        self.test_poisoned_writes = self.test_poisoned_writes.saturating_add(1);
        let Some(cleanup) = heap_cleanup else {
            return;
        };

        if self.test_poisoned_heap_len < TEST_POISONED_HEAP_CAPACITY {
            self.test_poisoned_heaps[self.test_poisoned_heap_len]
                .write(TestPoisonedHeap { ptr, cleanup });
            self.test_poisoned_heap_len += 1;
        } else {
            // Tests never require more than the fixed record capacity. Keep an
            // unexpected overflow leak-clean without dropping partial `T`.
            unsafe { cleanup(ptr) };
        }
    }

    #[inline(always)]
    /// Returns a payload block to its size-class free list.
    ///
    /// # Safety
    ///
    /// `class_index` must identify the class that allocated `ptr`, and `ptr`
    /// must be a live block that is not already on a free list.
    unsafe fn free_pooled_block(&mut self, class_index: usize, ptr: *mut u8) {
        unsafe { self.classes[class_index].free_block(ptr) };
        #[cfg(any(debug_assertions, feature = "test-support"))]
        {
            self.stats.pooled_frees = self.stats.pooled_frees.saturating_add(1);
        }
    }

    #[inline(always)]
    /// Releases heap backing after the stored `T` has been moved or dropped.
    ///
    /// # Safety
    ///
    /// `ptr` must be the live allocation returned by this pool's heap fallback
    /// for `T`. Any initialized `T` value must already have been consumed.
    unsafe fn free_heap_storage<T>(&mut self, ptr: *mut T) {
        if std::mem::size_of::<T>() != 0 {
            unsafe { std::alloc::dealloc(ptr as *mut u8, Layout::new::<T>()) };
        }
        #[cfg(any(debug_assertions, feature = "test-support"))]
        {
            self.stats.heap_frees = self.stats.heap_frees.saturating_add(1);
        }
    }

    /// Allocates retained kernel-facing `iovec` scratch for a vectored I/O
    /// submission.
    ///
    /// Scratch is sized by active iovec count, not by the
    /// const-generic chain capacity. It stores metadata only and has no heap
    /// fallback.
    #[inline(always)]
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn alloc_iovec_scratch(
        &mut self,
        iov_count: usize,
    ) -> io::Result<RetainedIovecScratch> {
        let init = self.alloc_iovec_scratch_init(iov_count)?;
        Ok(init.into_scratch())
    }

    /// Reserves the compact ownership state needed to initialize retained
    /// iovec scratch directly in its final payload allocation.
    ///
    /// The returned token owns any pooled sidecar until it is either dropped
    /// or transferred into a fully initialized [`RetainedIovecScratch`]. It
    /// deliberately carries no inline array, so payload-specific constructors
    /// can populate the final inline destination without staging those bytes.
    #[inline(always)]
    pub(crate) fn alloc_iovec_scratch_init(
        &mut self,
        iov_count: usize,
    ) -> io::Result<RetainedIovecScratchInit> {
        if iov_count <= RETAINED_IOVEC_INLINE_COUNT {
            #[cfg(any(debug_assertions, feature = "test-support"))]
            self.iovec_pool.record_inline_alloc();
            return Ok(RetainedIovecScratchInit::inline(iov_count));
        }

        let class_index = match iovec_class_index_for_count(iov_count) {
            Some(class_index) => class_index,
            None => {
                #[cfg(any(debug_assertions, feature = "test-support"))]
                self.iovec_pool.record_oversize_rejection();
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    RETAINED_IOVEC_OVERSIZE_MESSAGE,
                ));
            }
        };

        let Some(result) = self.iovec_pool.alloc_block(class_index) else {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        };

        Ok(unsafe {
            RetainedIovecScratchInit::pooled(
                result.ptr,
                iov_count,
                class_index,
                Rc::clone(&self.iovec_pool),
            )
        })
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    pub(crate) fn stats(&self) -> RetainedPayloadPoolStats {
        let mut stats = self.stats;
        self.iovec_pool.stats().apply_to(&mut stats);
        stats
    }
}

/// Runs `f` with one uninitialized retained slot carrying a fresh invariant
/// scope brand.
///
/// `f` may synchronously re-enter this function with the same pool pointer:
/// the temporary pool borrow used to reserve the block ends before `f` starts,
/// and the reserved block has already been removed from its free list.
///
/// # Safety
///
/// `pool` must identify a live, initialized retained pool for the complete
/// synchronous call. The call and every nested use must remain on the pool's
/// executor owner thread, and the pool allocation itself must not move while a
/// raw or writing slot can still drop.
#[inline(always)]
pub(crate) unsafe fn with_raw_retained_slot<T: 'static, R>(
    pool: NonNull<RetainedPayloadPool>,
    f: impl for<'slot> FnOnce(RawRetainedSlot<'slot, T>) -> R,
) -> R {
    // SAFETY: the caller guarantees pool liveness and owner-thread access. The
    // temporary exclusive borrow ends before the higher-ranked closure runs.
    let slot = unsafe { RetainedPayloadPool::alloc_raw_slot_from_raw::<T>(pool) };
    f(slot)
}

/// Uninitialized retained storage that still contains no ownership-bearing
/// `T` field.
///
/// Dropping this state recycles its untouched backing. The invariant closure
/// brand prevents the state from escaping its synchronous reservation scope;
/// the `Rc` marker preserves FlowIO's owner-thread-only contract.
#[must_use = "dropping a raw retained slot recycles its untouched storage"]
pub(crate) struct RawRetainedSlot<'slot, T: 'static> {
    ptr: NonNull<MaybeUninit<T>>,
    pool: NonNull<RetainedPayloadPool>,
    vtable: &'static RetainedPayloadVtable,
    _brand: RetainedSlotBrand<'slot>,
    _owner_thread_only: PhantomData<Rc<()>>,
    #[cfg(test)]
    poison_cleanup: Option<unsafe fn(*mut ())>,
}

impl<'slot, T: 'static> RawRetainedSlot<'slot, T> {
    /// # Safety
    ///
    /// `ptr` must own live, uninitialized storage for `T` allocated from
    /// `pool`, and `vtable.free_storage` must release exactly that backing.
    #[inline(always)]
    unsafe fn new(
        ptr: *mut MaybeUninit<T>,
        pool: NonNull<RetainedPayloadPool>,
        vtable: &'static RetainedPayloadVtable,
        #[cfg(test)] poison_cleanup: Option<unsafe fn(*mut ())>,
    ) -> Self {
        Self {
            // SAFETY: guaranteed by the constructor contract. Pooled blocks
            // are non-null, and Box supplies a non-null aligned dangling value
            // for zero-sized heap fallback.
            ptr: unsafe { NonNull::new_unchecked(ptr) },
            pool,
            vtable,
            _brand: PhantomData,
            _owner_thread_only: PhantomData,
            #[cfg(test)]
            poison_cleanup,
        }
    }

    /// Returns the compiler-layout pointer used to prepare non-owning fields.
    #[inline(always)]
    pub(crate) fn as_mut_ptr(&mut self) -> *mut T {
        self.ptr.as_ptr().cast::<T>()
    }

    /// Marks the point immediately before the first ownership-bearing field is
    /// written. From this point, unexpected Drop poisons rather than recycles
    /// the reservation.
    #[inline(always)]
    pub(crate) fn begin_writing(self) -> WritingRetainedSlot<'slot, T> {
        let this = ManuallyDrop::new(self);
        WritingRetainedSlot {
            ptr: this.ptr,
            #[cfg(test)]
            pool: this.pool,
            vtable: this.vtable,
            _brand: PhantomData,
            _owner_thread_only: PhantomData,
            #[cfg(test)]
            poison_cleanup: this.poison_cleanup,
        }
    }
}

impl<T: 'static> Drop for RawRetainedSlot<'_, T> {
    #[inline(always)]
    fn drop(&mut self) {
        unsafe { (self.vtable.free_storage)(self.ptr.as_ptr().cast::<()>(), self.pool.as_ptr()) };
    }
}

/// Retained storage whose payload-specific constructor may have transferred
/// ownership into an unknown subset of `T`'s fields.
///
/// Drop deliberately runs no `T` destructor and does not recycle or deallocate
/// the reservation. This leaks the unsubmitted partial payload rather than
/// risking an uninitialized drop, double drop, or later block reuse.
#[must_use = "a writing retained slot must be finished after every field is initialized"]
pub(crate) struct WritingRetainedSlot<'slot, T: 'static> {
    ptr: NonNull<MaybeUninit<T>>,
    #[cfg(test)]
    pool: NonNull<RetainedPayloadPool>,
    vtable: &'static RetainedPayloadVtable,
    _brand: RetainedSlotBrand<'slot>,
    _owner_thread_only: PhantomData<Rc<()>>,
    #[cfg(test)]
    poison_cleanup: Option<unsafe fn(*mut ())>,
}

impl<T: 'static> WritingRetainedSlot<'_, T> {
    /// Returns the compiler-layout pointer used to initialize fields in place.
    #[inline(always)]
    pub(crate) fn as_mut_ptr(&mut self) -> *mut T {
        self.ptr.as_ptr().cast::<T>()
    }

    /// Converts the slot to initialized retained ownership.
    ///
    /// # Safety
    ///
    /// Every field of `T` must be fully initialized, and no reference derived
    /// from the incomplete payload may remain live. The recorded allocation
    /// path and pool must still match this slot.
    #[inline(always)]
    pub(crate) unsafe fn finish(self) -> RetainedPayload<T> {
        let this = ManuallyDrop::new(self);
        RetainedPayload {
            ptr: this.ptr.cast::<T>(),
            vtable: this.vtable,
            _marker: PhantomData,
        }
    }
}

impl<T: 'static> Drop for WritingRetainedSlot<'_, T> {
    #[inline(always)]
    fn drop(&mut self) {
        #[cfg(test)]
        unsafe {
            // Test-only cleanup is deferred in the pool and releases only raw
            // heap backing. It never inspects or drops the partial `T`.
            (*self.pool.as_ptr())
                .record_test_poison(self.ptr.as_ptr().cast::<()>(), self.poison_cleanup);
        }
    }
}

#[cfg(test)]
impl Drop for RetainedPayloadPool {
    fn drop(&mut self) {
        for index in 0..self.test_poisoned_heap_len {
            // SAFETY: exactly the prefix tracked by
            // `test_poisoned_heap_len` was initialized once. Each record owns
            // one distinct poisoned heap backing allocation.
            let poisoned = unsafe { self.test_poisoned_heaps[index].assume_init_read() };
            unsafe { (poisoned.cleanup)(poisoned.ptr) };
        }
    }
}

/// Releases test-only poisoned heap backing as `MaybeUninit<T>` so no partial
/// field destructor can run.
#[cfg(test)]
unsafe fn test_cleanup_poisoned_heap<T>(ptr: *mut ()) {
    unsafe { drop(Box::from_raw(ptr.cast::<MaybeUninit<T>>())) };
}

/// Retained vectored I/O scratch storing kernel-facing `iovec` metadata.
///
/// This type is move-safe: inline storage is addressed from the enum field on
/// every access, while sidecar storage carries a stable slab pointer. The
/// sidecar pointer remains valid until the scratch handle is dropped because
/// every pooled handle retains the heap-stable sidecar owner. Pooled scratch
/// still carries the inline array so the handle remains one simple move-safe
/// value; the extra bytes are a deliberate tradeoff for cancellation-path
/// simplicity.
pub(crate) struct RetainedIovecScratch {
    /// Active iovec count visible through this scratch handle.
    len: usize,
    /// Inline storage used for small vectored submissions.
    inline: [MaybeUninit<libc::iovec>; RETAINED_IOVEC_INLINE_COUNT],
    /// Selects inline storage or a pooled sidecar block.
    storage: RetainedIovecScratchStorage,
}

enum RetainedIovecScratchStorage {
    /// Use the inline array stored inside `RetainedIovecScratch`.
    Inline,
    Pooled {
        /// Pointer to sidecar storage allocated from an iovec size class.
        ptr: NonNull<MaybeUninit<libc::iovec>>,
        /// Size-class index used to return `ptr` to the right free list.
        class_index: usize,
        /// Heap-stable sidecar pool that owns the block.
        owner: Rc<RetainedIovecScratchPool>,
    },
}

impl Drop for RetainedIovecScratchStorage {
    #[inline(always)]
    fn drop(&mut self) {
        if let Self::Pooled {
            ptr,
            class_index,
            owner,
        } = self
        {
            unsafe { owner.free_block(*class_index, ptr.as_ptr().cast::<u8>()) };
        }
    }
}

/// Compact allocation token for initializing retained iovec scratch in place.
///
/// Inline scratch carries only its active length. Pooled scratch additionally
/// owns one sidecar block and its heap-stable owner reference. Dropping an
/// unconsumed token releases that pooled ownership through the same storage
/// destructor used by the final scratch value.
#[must_use = "scratch initialization tokens own pooled sidecar storage"]
pub(crate) struct RetainedIovecScratchInit {
    /// Active iovec count for the final scratch value.
    len: usize,
    /// Inline selection or pooled sidecar ownership transferred on success.
    storage: RetainedIovecScratchStorage,
}

impl RetainedIovecScratchInit {
    #[inline(always)]
    fn inline(len: usize) -> Self {
        debug_assert!(len <= RETAINED_IOVEC_INLINE_COUNT);
        Self {
            len,
            storage: RetainedIovecScratchStorage::Inline,
        }
    }

    /// # Safety
    ///
    /// `ptr` must be a block allocated from `owner` class `class_index`, and it
    /// must be large enough for `len` `libc::iovec` values.
    #[inline(always)]
    unsafe fn pooled(
        ptr: *mut u8,
        len: usize,
        class_index: usize,
        owner: Rc<RetainedIovecScratchPool>,
    ) -> Self {
        debug_assert!(len > RETAINED_IOVEC_INLINE_COUNT);
        debug_assert!(len <= RETAINED_IOVEC_SIZE_CLASSES[class_index]);
        Self {
            len,
            storage: RetainedIovecScratchStorage::Pooled {
                ptr: unsafe { NonNull::new_unchecked(ptr as *mut MaybeUninit<libc::iovec>) },
                class_index,
                owner,
            },
        }
    }

    /// Returns the active iovec count carried into the final scratch value.
    #[cfg(test)]
    #[inline(always)]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Returns the exact final destination used to populate iovec metadata.
    ///
    /// Inline tokens address the inline field of `destination` directly;
    /// pooled tokens address their already-owned sidecar block. The token
    /// retains ownership for the complete borrow.
    ///
    /// # Safety
    ///
    /// `destination` must be non-null, correctly aligned writable storage for
    /// the final [`RetainedIovecScratch`], and it must remain live for the
    /// returned borrow. No other access may overlap the returned slice. The
    /// caller must end the borrow before initializing or abandoning that final
    /// scratch value.
    #[inline(always)]
    pub(crate) unsafe fn destination_mut(
        &mut self,
        destination: *mut RetainedIovecScratch,
    ) -> &mut [MaybeUninit<libc::iovec>] {
        debug_assert!(
            !destination.is_null(),
            "retained iovec destination must be non-null"
        );
        match &mut self.storage {
            RetainedIovecScratchStorage::Inline => unsafe {
                slice::from_raw_parts_mut(
                    std::ptr::addr_of_mut!((*destination).inline)
                        .cast::<MaybeUninit<libc::iovec>>(),
                    self.len,
                )
            },
            RetainedIovecScratchStorage::Pooled { ptr, .. } => unsafe {
                slice::from_raw_parts_mut(ptr.as_ptr(), self.len)
            },
        }
    }

    /// Transfers this token into an initialized scratch value at its final
    /// destination without copying or writing the inline array.
    ///
    /// # Safety
    ///
    /// `destination` must be non-null, correctly aligned writable storage for
    /// one uninitialized [`RetainedIovecScratch`]. Any initialized inline
    /// prefix must already reside in that value's `inline` field, every borrow
    /// returned by [`Self::destination_mut`] must have ended, and the caller
    /// must subsequently treat `destination` as one initialized scratch value.
    #[inline(always)]
    pub(crate) unsafe fn initialize_at(self, destination: *mut RetainedIovecScratch) {
        debug_assert!(
            !destination.is_null(),
            "retained iovec destination must be non-null"
        );
        let this = ManuallyDrop::new(self);
        unsafe {
            std::ptr::addr_of_mut!((*destination).len).write(this.len);
            let storage = std::ptr::addr_of!(this.storage).read();
            std::ptr::addr_of_mut!((*destination).storage).write(storage);
        }
    }

    /// Builds the established movable scratch value for unaffected safe
    /// callers while sharing the token's allocation and release policy.
    #[inline(always)]
    #[cfg(any(test, feature = "test-support"))]
    fn into_scratch(self) -> RetainedIovecScratch {
        let mut scratch = MaybeUninit::<RetainedIovecScratch>::uninit();
        unsafe {
            self.initialize_at(scratch.as_mut_ptr());
            scratch.assume_init()
        }
    }
}

impl RetainedIovecScratch {
    #[inline(always)]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub(crate) fn as_uninit_slice(&self) -> &[MaybeUninit<libc::iovec>] {
        match &self.storage {
            RetainedIovecScratchStorage::Inline => &self.inline[..self.len],
            RetainedIovecScratchStorage::Pooled { ptr, .. } => unsafe {
                slice::from_raw_parts(ptr.as_ptr(), self.len)
            },
        }
    }

    #[inline(always)]
    pub(crate) fn as_uninit_slice_mut(&mut self) -> &mut [MaybeUninit<libc::iovec>] {
        match &mut self.storage {
            RetainedIovecScratchStorage::Inline => &mut self.inline[..self.len],
            RetainedIovecScratchStorage::Pooled { ptr, .. } => unsafe {
                slice::from_raw_parts_mut(ptr.as_ptr(), self.len)
            },
        }
    }
}

#[cfg(target_pointer_width = "64")]
const _: [(); 288] = [(); std::mem::size_of::<RetainedIovecScratch>()];
#[cfg(target_pointer_width = "64")]
const _: [(); 24] = [(); std::mem::size_of::<RetainedIovecScratchStorage>()];
#[cfg(target_pointer_width = "64")]
const _: [(); 32] = [(); std::mem::size_of::<RetainedIovecScratchInit>()];

#[must_use = "retained payload handles own storage and must be consumed"]
pub(crate) struct RetainedPayload<T: 'static> {
    /// Pointer to initialized retained payload storage.
    ptr: NonNull<T>,
    /// Release hooks matching the allocation path for `ptr`.
    vtable: &'static RetainedPayloadVtable,
    /// Carries the concrete payload type for drop-checking and variance.
    _marker: PhantomData<T>,
}

impl<T: 'static> RetainedPayload<T> {
    /// # Safety
    ///
    /// `ptr` must point to initialized storage for `T`, and `vtable` must
    /// release that exact storage allocation path.
    #[inline(always)]
    pub(crate) unsafe fn from_raw_parts(
        ptr: *mut T,
        vtable: &'static RetainedPayloadVtable,
    ) -> Self {
        Self {
            ptr: {
                debug_assert!(!ptr.is_null(), "retained payload pointer must be non-null");
                unsafe { NonNull::new_unchecked(ptr) }
            },
            vtable,
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    /// Returns the stable address of the retained value.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn as_ptr(&self) -> *mut T {
        self.ptr.as_ptr()
    }

    #[inline(always)]
    /// Transfers the retained pointer and release hooks to erased ownership.
    ///
    /// The returned parts must later be reconstructed and consumed exactly
    /// once; this handle intentionally has no independent `Drop` path.
    pub(crate) fn into_raw_parts(self) -> (*mut (), &'static RetainedPayloadVtable) {
        (self.ptr.as_ptr() as *mut (), self.vtable)
    }

    /// Returns a shared reference to the retained payload.
    ///
    /// # Safety
    ///
    /// The caller must ensure the payload has not been taken or freed.
    #[cfg(any(test, feature = "test-support"))]
    #[inline(always)]
    pub(crate) unsafe fn as_ref(&self) -> &T {
        unsafe { self.ptr.as_ref() }
    }

    /// Returns a mutable reference to the retained payload.
    ///
    /// # Safety
    ///
    /// The caller must have exclusive logical access to the retained payload.
    #[cfg(test)]
    #[inline(always)]
    pub(crate) unsafe fn as_mut(&mut self) -> &mut T {
        unsafe { self.ptr.as_mut() }
    }

    /// Moves the payload value out and releases only the backing storage.
    ///
    /// # Safety
    ///
    /// `pool` must be the same retained pool that created this handle.
    #[inline(always)]
    pub(crate) unsafe fn take(self, pool: &mut RetainedPayloadPool) -> T {
        let value = unsafe { self.ptr.as_ptr().read() };
        unsafe { (self.vtable.free_storage)(self.ptr.as_ptr() as *mut (), pool) };
        value
    }

    /// Extracts selected data from the retained payload in place and releases
    /// only the backing storage.
    ///
    /// # Safety
    ///
    /// `pool` must be the same retained pool that created this handle.
    /// `extract` receives a pointer to the initialized payload and must move or
    /// drop every initialized field that requires destruction before returning.
    /// If `extract` unwinds, the backing allocation is intentionally leaked:
    /// the helper cannot know which payload fields remain initialized and
    /// therefore cannot safely release or reuse the storage.
    #[inline(always)]
    pub(crate) unsafe fn take_with<R>(
        self,
        pool: &mut RetainedPayloadPool,
        extract: impl FnOnce(*mut T) -> R,
    ) -> R {
        let ptr = self.ptr.as_ptr();
        let value = extract(ptr);
        unsafe { (self.vtable.free_storage)(ptr as *mut (), pool) };
        value
    }

    /// Drops the payload value and releases the backing storage.
    ///
    /// # Safety
    ///
    /// `pool` must be the same retained pool that created this handle.
    #[cfg(any(test, feature = "test-support"))]
    #[inline(always)]
    pub(crate) unsafe fn drop_and_free(self, pool: &mut RetainedPayloadPool) {
        unsafe { (self.vtable.drop_and_free)(self.ptr.as_ptr() as *mut (), pool) };
    }
}

struct RetainedSizeClass {
    /// Returned blocks ready for reuse.
    free_list: SList<u8>,
    /// Move-safe chain of slab pages owned by this class.
    slab_pages: SlabPageChain,
    /// Slab allocator that requests pages for this class.
    slab_factory: ManuallyDrop<SlabAllocator<'static, BasicMemoryProvider>>,
}

impl RetainedSizeClass {
    fn new(provider: &ProviderOwner<BasicMemoryProvider>, block_size: usize) -> io::Result<Self> {
        let blocks_per_slab = std::cmp::max(1, RETAINED_SLAB_TARGET_BYTES / block_size);
        let mut slab_factory = match unsafe {
            // SAFETY: the pool owns `provider` at a heap-stable address until
            // after every class is dropped. All class operations remain
            // serialized on the executor owner thread, so mutable provider
            // calls through the shared raw pointer never overlap.
            SlabAllocator::new_uninit_from_raw(
                provider.as_ptr(),
                block_size,
                RETAINED_BLOCK_ALIGN,
                blocks_per_slab,
            )
        } {
            Ok(slab_factory) => ManuallyDrop::new(slab_factory),
            Err(err) => {
                return Err(slab_config_error_to_io(err));
            }
        };
        slab_factory.init();

        Ok(Self {
            free_list: SList::new(),
            slab_pages: SlabPageChain::new(),
            slab_factory,
        })
    }

    #[cfg(test)]
    fn provider_ptr(&self) -> *mut BasicMemoryProvider {
        self.slab_factory.provider_ptr()
    }

    #[inline(always)]
    fn alloc_block(&mut self) -> Option<ClassAllocResult> {
        if let Some(ptr) = unsafe { self.free_list.pop_front() } {
            return Some(ClassAllocResult {
                ptr,
                #[cfg(any(debug_assertions, test, feature = "test-support"))]
                reused: true,
                #[cfg(any(debug_assertions, test, feature = "test-support"))]
                new_slab: false,
            });
        }

        let result = unsafe { self.slab_pages.alloc_or_grow(&mut self.slab_factory) }?;

        Some(ClassAllocResult {
            ptr: result.ptr,
            #[cfg(any(debug_assertions, test, feature = "test-support"))]
            reused: false,
            #[cfg(any(debug_assertions, test, feature = "test-support"))]
            new_slab: result.new_slab,
        })
    }

    #[inline(always)]
    /// Returns a block previously allocated by this size class to its free list.
    ///
    /// # Safety
    ///
    /// `ptr` must point to a live block from this class and must not currently
    /// be linked into any list.
    unsafe fn free_block(&mut self, ptr: *mut u8) {
        debug_assert!(!ptr.is_null(), "retained pool freeing null block");
        unsafe {
            let link = ptr as *mut Link;
            (*link).next = std::ptr::null_mut();
            self.free_list.push_front(link);
        }
    }
}

impl Drop for RetainedSizeClass {
    fn drop(&mut self) {
        unsafe { self.slab_pages.free_all(&mut self.slab_factory) };

        unsafe { ManuallyDrop::drop(&mut self.slab_factory) };
    }
}

struct ClassAllocResult {
    /// Raw block pointer returned to the caller.
    ptr: *mut u8,
    /// True when `ptr` came from the free list.
    #[cfg(any(debug_assertions, test, feature = "test-support"))]
    reused: bool,
    /// True when this allocation requested a fresh slab page.
    #[cfg(any(debug_assertions, test, feature = "test-support"))]
    new_slab: bool,
}

#[inline(always)]
fn class_index_for<T>() -> Option<usize> {
    let size = std::cmp::max(std::mem::size_of::<T>(), 1);
    let align = std::mem::align_of::<T>();
    if align > RETAINED_BLOCK_ALIGN {
        return None;
    }

    RETAINED_SIZE_CLASSES
        .iter()
        .position(|class_size| size <= *class_size)
}

#[inline(always)]
fn pooled_vtable<T: 'static>(class_index: usize) -> &'static RetainedPayloadVtable {
    match class_index {
        0 => &RetainedPayloadVtable {
            drop_and_free: pooled_drop_and_free::<T, 0>,
            free_storage: pooled_free_storage::<T, 0>,
        },
        1 => &RetainedPayloadVtable {
            drop_and_free: pooled_drop_and_free::<T, 1>,
            free_storage: pooled_free_storage::<T, 1>,
        },
        2 => &RetainedPayloadVtable {
            drop_and_free: pooled_drop_and_free::<T, 2>,
            free_storage: pooled_free_storage::<T, 2>,
        },
        3 => &RetainedPayloadVtable {
            drop_and_free: pooled_drop_and_free::<T, 3>,
            free_storage: pooled_free_storage::<T, 3>,
        },
        4 => &RetainedPayloadVtable {
            drop_and_free: pooled_drop_and_free::<T, 4>,
            free_storage: pooled_free_storage::<T, 4>,
        },
        5 => &RetainedPayloadVtable {
            drop_and_free: pooled_drop_and_free::<T, 5>,
            free_storage: pooled_free_storage::<T, 5>,
        },
        6 => &RetainedPayloadVtable {
            drop_and_free: pooled_drop_and_free::<T, 6>,
            free_storage: pooled_free_storage::<T, 6>,
        },
        7 => &RetainedPayloadVtable {
            drop_and_free: pooled_drop_and_free::<T, 7>,
            free_storage: pooled_free_storage::<T, 7>,
        },
        8 => &RetainedPayloadVtable {
            drop_and_free: pooled_drop_and_free::<T, 8>,
            free_storage: pooled_free_storage::<T, 8>,
        },
        9 => &RetainedPayloadVtable {
            drop_and_free: pooled_drop_and_free::<T, 9>,
            free_storage: pooled_free_storage::<T, 9>,
        },
        10 => &RetainedPayloadVtable {
            drop_and_free: pooled_drop_and_free::<T, 10>,
            free_storage: pooled_free_storage::<T, 10>,
        },
        _ => unreachable!("invalid retained payload size class"),
    }
}

#[inline(always)]
fn heap_vtable<T: 'static>() -> &'static RetainedPayloadVtable {
    &RetainedPayloadVtable {
        drop_and_free: heap_drop_and_free::<T>,
        free_storage: heap_free_storage::<T>,
    }
}

/// Statically selected release operation for one retained backing allocation.
///
/// Implementations contain no user code and must release the exact allocation
/// represented by `ptr` without inspecting a consumed `T`.
trait RetainedBackingCleanup {
    /// # Safety
    ///
    /// `ptr` and `pool` must identify one live retained allocation matching
    /// this cleanup implementation, with no initialized value remaining.
    unsafe fn free(ptr: *mut (), pool: *mut RetainedPayloadPool);
}

struct PooledBacking<T, const CLASS: usize>(PhantomData<T>);

impl<T, const CLASS: usize> RetainedBackingCleanup for PooledBacking<T, CLASS> {
    #[inline(always)]
    unsafe fn free(ptr: *mut (), pool: *mut RetainedPayloadPool) {
        unsafe { pooled_free_storage::<T, CLASS>(ptr, pool) };
    }
}

struct HeapBacking<T>(PhantomData<T>);

impl<T> RetainedBackingCleanup for HeapBacking<T> {
    #[inline(always)]
    unsafe fn free(ptr: *mut (), pool: *mut RetainedPayloadPool) {
        unsafe { heap_free_storage::<T>(ptr, pool) };
    }
}

/// Returns retained backing on both normal return and destructor unwind.
///
/// The common path consumes this guard through [`Self::finish`], which keeps
/// cleanup statically dispatched and inline. Its `Drop` exists only on the
/// exceptional path.
struct RetainedBackingGuard<C: RetainedBackingCleanup> {
    ptr: *mut (),
    pool: *mut RetainedPayloadPool,
    _cleanup: PhantomData<C>,
}

impl<C: RetainedBackingCleanup> RetainedBackingGuard<C> {
    #[inline(always)]
    fn new(ptr: *mut (), pool: *mut RetainedPayloadPool) -> Self {
        Self {
            ptr,
            pool,
            _cleanup: PhantomData,
        }
    }

    #[inline(always)]
    unsafe fn finish(self) {
        let this = disarm_unwind_guard(self);
        unsafe { C::free(this.ptr, this.pool) };
    }
}

impl<C: RetainedBackingCleanup> Drop for RetainedBackingGuard<C> {
    #[cold]
    #[inline(never)]
    fn drop(&mut self) {
        unsafe { C::free(self.ptr, self.pool) };
    }
}

/// Drops a pooled value and returns its block to the matching size class.
///
/// # Safety
///
/// `ptr` must address an initialized `T` allocated from `pool` class `CLASS`,
/// and both pointers must remain live for this call.
unsafe fn pooled_drop_and_free<T, const CLASS: usize>(
    ptr: *mut (),
    pool: *mut RetainedPayloadPool,
) {
    let backing = RetainedBackingGuard::<PooledBacking<T, CLASS>>::new(ptr, pool);
    unsafe { std::ptr::drop_in_place(ptr as *mut T) };
    unsafe { backing.finish() };
}

/// Returns pooled backing after its `T` value has already been consumed.
///
/// # Safety
///
/// `ptr` must be an unconsumed block allocated from `pool` class `CLASS`, and
/// no initialized `T` may remain in that block.
unsafe fn pooled_free_storage<T, const CLASS: usize>(ptr: *mut (), pool: *mut RetainedPayloadPool) {
    let _ = PhantomData::<T>;
    unsafe { (*pool).free_pooled_block(CLASS, ptr as *mut u8) };
}

/// Drops a heap-fallback value and records release in its owning pool.
///
/// # Safety
///
/// `ptr` must come from `Box<T>` in this pool's heap fallback and must still
/// contain an initialized `T`.
unsafe fn heap_drop_and_free<T>(ptr: *mut (), pool: *mut RetainedPayloadPool) {
    let backing = RetainedBackingGuard::<HeapBacking<T>>::new(ptr, pool);
    unsafe { std::ptr::drop_in_place(ptr as *mut T) };
    unsafe { backing.finish() };
}

/// Releases heap-fallback backing after its value has been consumed.
///
/// # Safety
///
/// `ptr` must be the live heap fallback allocation for `T` owned by `pool`,
/// with no initialized `T` remaining in the allocation.
unsafe fn heap_free_storage<T>(ptr: *mut (), pool: *mut RetainedPayloadPool) {
    unsafe { (*pool).free_heap_storage(ptr as *mut T) };
}

fn slab_config_error_to_io(err: SlabAllocatorConfigError) -> io::Error {
    let kind = match err {
        SlabAllocatorConfigError::ObjsPerSlabZero
        | SlabAllocatorConfigError::InvalidObjectAlign
        | SlabAllocatorConfigError::SizeOverflow => io::ErrorKind::InvalidInput,
    };
    io::Error::new(kind, err)
}

#[inline(always)]
fn iovec_class_block_size(class_index: usize) -> usize {
    RETAINED_IOVEC_SIZE_CLASSES[class_index] * std::mem::size_of::<libc::iovec>()
}

#[inline(always)]
fn iovec_class_index_for_count(iov_count: usize) -> Option<usize> {
    RETAINED_IOVEC_SIZE_CLASSES
        .iter()
        .position(|class_count| iov_count <= *class_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    #[test]
    fn static_vtable_refs_bind_exact_type_class_and_heap_hooks() {
        // Miri may materialize the same generic function item at distinct
        // abstract code addresses. Runtime dispatch behavior remains covered
        // by the pooled and heap backing-release tests below.
        #[cfg(not(miri))]
        {
            let pooled = pooled_vtable::<u32>(2);
            assert!(std::ptr::fn_addr_eq(
                pooled.drop_and_free,
                pooled_drop_and_free::<u32, 2> as unsafe fn(*mut (), *mut RetainedPayloadPool),
            ));
            assert!(std::ptr::fn_addr_eq(
                pooled.free_storage,
                pooled_free_storage::<u32, 2> as unsafe fn(*mut (), *mut RetainedPayloadPool),
            ));

            let heap = heap_vtable::<u64>();
            assert!(std::ptr::fn_addr_eq(
                heap.drop_and_free,
                heap_drop_and_free::<u64> as unsafe fn(*mut (), *mut RetainedPayloadPool),
            ));
            assert!(std::ptr::fn_addr_eq(
                heap.free_storage,
                heap_free_storage::<u64> as unsafe fn(*mut (), *mut RetainedPayloadPool),
            ));
        }

        assert_eq!(
            std::mem::size_of::<Option<&'static RetainedPayloadVtable>>(),
            std::mem::size_of::<usize>()
        );
    }

    struct ScratchPayload {
        _scratch: RetainedIovecScratch,
    }

    fn pooled_scratch_init_ptr(init: &RetainedIovecScratchInit) -> *mut MaybeUninit<libc::iovec> {
        match &init.storage {
            RetainedIovecScratchStorage::Inline => {
                panic!("expected pooled scratch initialization token")
            }
            RetainedIovecScratchStorage::Pooled { ptr, .. } => ptr.as_ptr(),
        }
    }

    fn test_iovec(len: usize) -> libc::iovec {
        libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: len,
        }
    }

    #[test]
    fn retained_stats_delta_is_field_complete_and_saturating() {
        let baseline = RetainedPayloadPoolStats {
            pooled_allocs: usize::MAX - 1,
            pooled_reuses: 20,
            pooled_frees: 30,
            slab_allocs: 40,
            heap_fallbacks: 50,
            heap_frees: 60,
            writev_scratch_inline_allocs: 70,
            writev_scratch_pooled_allocs: 80,
            writev_scratch_pooled_reuses: 90,
            writev_scratch_pooled_frees: 100,
            writev_scratch_slab_allocs: 110,
            writev_scratch_oversize_rejections: 120,
            writev_scratch_alloc_failures: 130,
        };
        let current = RetainedPayloadPoolStats {
            pooled_allocs: usize::MAX,
            pooled_reuses: 22,
            pooled_frees: 33,
            slab_allocs: 44,
            heap_fallbacks: 55,
            heap_frees: 66,
            writev_scratch_inline_allocs: 77,
            writev_scratch_pooled_allocs: 88,
            writev_scratch_pooled_reuses: 99,
            writev_scratch_pooled_frees: 110,
            writev_scratch_slab_allocs: 121,
            writev_scratch_oversize_rejections: 132,
            writev_scratch_alloc_failures: 143,
        };
        let expected = RetainedPayloadPoolStats {
            pooled_allocs: 1,
            pooled_reuses: 2,
            pooled_frees: 3,
            slab_allocs: 4,
            heap_fallbacks: 5,
            heap_frees: 6,
            writev_scratch_inline_allocs: 7,
            writev_scratch_pooled_allocs: 8,
            writev_scratch_pooled_reuses: 9,
            writev_scratch_pooled_frees: 10,
            writev_scratch_slab_allocs: 11,
            writev_scratch_oversize_rejections: 12,
            writev_scratch_alloc_failures: 13,
        };

        assert_eq!(current.saturating_delta_since(baseline), expected);
        assert_eq!(baseline.saturating_delta_since(current), Default::default());
        assert_eq!(current.saturating_delta_since(current), Default::default());
    }

    #[test]
    fn class_allocation_stats_record_flags_and_saturate() {
        let mut allocations = 7usize;
        let mut reuses = 11usize;
        let mut slab_allocations = 13usize;

        record_class_allocation(
            &ClassAllocResult {
                ptr: std::ptr::null_mut(),
                reused: false,
                new_slab: false,
            },
            &mut allocations,
            &mut reuses,
            &mut slab_allocations,
        );
        assert_eq!((allocations, reuses, slab_allocations), (8, 11, 13));

        record_class_allocation(
            &ClassAllocResult {
                ptr: std::ptr::null_mut(),
                reused: true,
                new_slab: false,
            },
            &mut allocations,
            &mut reuses,
            &mut slab_allocations,
        );
        assert_eq!((allocations, reuses, slab_allocations), (9, 12, 13));

        record_class_allocation(
            &ClassAllocResult {
                ptr: std::ptr::null_mut(),
                reused: false,
                new_slab: true,
            },
            &mut allocations,
            &mut reuses,
            &mut slab_allocations,
        );
        assert_eq!((allocations, reuses, slab_allocations), (10, 12, 14));

        allocations = usize::MAX;
        reuses = usize::MAX;
        slab_allocations = usize::MAX;
        record_class_allocation(
            &ClassAllocResult {
                ptr: std::ptr::null_mut(),
                reused: true,
                new_slab: true,
            },
            &mut allocations,
            &mut reuses,
            &mut slab_allocations,
        );
        assert_eq!(
            (allocations, reuses, slab_allocations),
            (usize::MAX, usize::MAX, usize::MAX)
        );
    }

    #[test]
    fn remaining_allocation_statistics_saturate() {
        #[repr(align(128))]
        struct HeapValue(u8);

        let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
        pool.stats.heap_fallbacks = usize::MAX;
        let heap = pool.alloc(HeapValue(0));
        assert_eq!(pool.stats().heap_fallbacks, usize::MAX);
        let value = unsafe { heap.take(&mut pool) };
        assert_eq!(value.0, 0);

        unsafe {
            let stats = &mut (*pool.iovec_pool.state.get()).stats;
            stats.writev_scratch_inline_allocs = usize::MAX;
            stats.writev_scratch_oversize_rejections = usize::MAX;
        }

        let inline = pool
            .alloc_iovec_scratch(RETAINED_IOVEC_INLINE_COUNT)
            .expect("inline scratch allocation failed");
        drop(inline);
        assert_eq!(pool.stats().writev_scratch_inline_allocs, usize::MAX);

        let err = match pool.alloc_iovec_scratch(RETAINED_IOVEC_MAX_COUNT + 1) {
            Ok(_) => panic!("oversized scratch should be rejected"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(pool.stats().writev_scratch_oversize_rejections, usize::MAX);

        let scratch_provider = unsafe {
            let state = &mut *pool.iovec_pool.state.get();
            state.stats.writev_scratch_alloc_failures = usize::MAX;
            state.classes[0].provider_ptr()
        };
        unsafe {
            <BasicMemoryProvider as crate::utils::memory::provider::MemoryProvider>::init(
                &mut *scratch_provider,
                1usize << (usize::BITS - 1),
            );
        }
        let err = match pool.alloc_iovec_scratch(RETAINED_IOVEC_INLINE_COUNT + 1) {
            Ok(_) => panic!("unrepresentable scratch slab should be rejected"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(pool.stats().writev_scratch_alloc_failures, usize::MAX);
    }

    #[test]
    fn release_statistics_saturate_after_backing_release() {
        #[repr(align(128))]
        struct HeapValue {
            _byte: u8,
        }

        let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");

        let pooled = pool.alloc(0_u8);
        pool.stats.pooled_frees = usize::MAX;
        unsafe { pooled.drop_and_free(&mut pool) };
        assert_eq!(pool.stats().pooled_frees, usize::MAX);

        let heap = pool.alloc(HeapValue { _byte: 0 });
        pool.stats.heap_frees = usize::MAX;
        unsafe { heap.drop_and_free(&mut pool) };
        assert_eq!(pool.stats().heap_frees, usize::MAX);

        let scratch = pool
            .alloc_iovec_scratch(RETAINED_IOVEC_INLINE_COUNT + 1)
            .expect("pooled scratch allocation failed");
        unsafe {
            (*pool.iovec_pool.state.get())
                .stats
                .writev_scratch_pooled_frees = usize::MAX;
        }
        drop(scratch);
        assert_eq!(pool.stats().writev_scratch_pooled_frees, usize::MAX);
    }

    #[test]
    fn retained_classes_share_one_provider_per_independent_lifetime() {
        let pool = RetainedPayloadPool::new().expect("retained pool init failed");
        let payload_provider = pool._provider.as_ptr();
        assert!(
            pool.classes
                .iter()
                .all(|class| class.provider_ptr() == payload_provider)
        );

        let scratch_state = unsafe { &*pool.iovec_pool.state.get() };
        let scratch_provider = scratch_state._provider.as_ptr();
        assert!(
            scratch_state
                .classes
                .iter()
                .all(|class| class.provider_ptr() == scratch_provider)
        );
        assert_ne!(
            payload_provider, scratch_provider,
            "independently lived pools must retain independent providers"
        );
    }

    #[test]
    fn retained_iovec_max_count_is_the_largest_size_class() {
        assert_eq!(RETAINED_IOVEC_MAX_COUNT, 1024);
        assert_eq!(
            iovec_class_index_for_count(RETAINED_IOVEC_MAX_COUNT),
            Some(RETAINED_IOVEC_SIZE_CLASSES.len() - 1)
        );
        assert_eq!(
            iovec_class_index_for_count(RETAINED_IOVEC_MAX_COUNT + 1),
            None
        );

        let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
        let scratch = pool
            .alloc_iovec_scratch(RETAINED_IOVEC_MAX_COUNT)
            .expect("largest retained iovec class should be accepted");
        assert_eq!(scratch.len(), RETAINED_IOVEC_MAX_COUNT);
        drop(scratch);

        let err = match pool.alloc_iovec_scratch(RETAINED_IOVEC_MAX_COUNT + 1) {
            Ok(_) => panic!("count above the largest retained iovec class should be rejected"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        let stats = pool.stats();
        assert_eq!(stats.writev_scratch_pooled_allocs, 1);
        assert_eq!(stats.writev_scratch_pooled_frees, 1);
        assert_eq!(stats.writev_scratch_oversize_rejections, 1);
    }

    struct DropProbe {
        drops: *const Cell<usize>,
    }

    impl DropProbe {
        fn new(drops: &Cell<usize>) -> Self {
            Self { drops }
        }
    }

    impl Drop for DropProbe {
        fn drop(&mut self) {
            // SAFETY: every test keeps its stack-local counter alive until all
            // fully initialized probes have either dropped or been
            // deliberately forgotten inside poisoned storage.
            let drops = unsafe { &*self.drops };
            drops.set(drops.get() + 1);
        }
    }

    struct PooledSlotPayload {
        value: usize,
        owner: DropProbe,
    }

    #[repr(align(128))]
    struct HeapSlotPayload {
        value: usize,
        owner: DropProbe,
    }

    struct PooledZst;

    #[repr(align(128))]
    struct HeapZst;

    #[derive(Debug)]
    struct RetainedPayloadDropPanic;

    struct RetainedDropBomb {
        drops: *const Cell<usize>,
        armed: *const Cell<bool>,
    }

    impl RetainedDropBomb {
        fn new(drops: &Cell<usize>, armed: &Cell<bool>) -> Self {
            Self { drops, armed }
        }
    }

    impl Drop for RetainedDropBomb {
        fn drop(&mut self) {
            // SAFETY: tests keep both stack-local cells live until every bomb
            // using them has been consumed.
            let drops = unsafe { &*self.drops };
            drops.set(drops.get() + 1);
            if unsafe { (*self.armed).get() } {
                std::panic::panic_any(RetainedPayloadDropPanic);
            }
        }
    }

    #[repr(align(128))]
    struct HeapRetainedDropBomb {
        _bomb: RetainedDropBomb,
    }

    #[test]
    fn pooled_retained_payload_drop_panic_recycles_backing_once() {
        let drops = Cell::new(0);
        let armed = Cell::new(true);
        let replacement_drops = Cell::new(0);
        let replacement_armed = Cell::new(false);
        let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
        let payload = pool.alloc(RetainedDropBomb::new(&drops, &armed));
        let payload_ptr = payload.as_ptr();

        let unwind = catch_unwind(AssertUnwindSafe(|| unsafe {
            payload.drop_and_free(&mut pool);
        }))
        .expect_err("retained payload destructor did not panic");
        assert!(
            unwind.downcast_ref::<RetainedPayloadDropPanic>().is_some(),
            "retained payload cleanup replaced the original panic"
        );
        assert_eq!(drops.get(), 1);
        let after_unwind = pool.stats();
        assert_eq!(after_unwind.pooled_allocs, 1);
        assert_eq!(after_unwind.pooled_frees, 1);

        let replacement = pool.alloc(RetainedDropBomb::new(
            &replacement_drops,
            &replacement_armed,
        ));
        assert_eq!(
            replacement.as_ptr(),
            payload_ptr,
            "pooled retained backing was not returned for exact reuse"
        );
        let after_reuse = pool.stats();
        assert_eq!(after_reuse.pooled_allocs, 2);
        assert_eq!(after_reuse.pooled_reuses, 1);
        unsafe { replacement.drop_and_free(&mut pool) };
        assert_eq!(replacement_drops.get(), 1);
        assert_eq!(pool.stats().pooled_frees, 2);
    }

    #[test]
    fn heap_retained_payload_drop_panic_deallocates_backing_once() {
        let drops = Cell::new(0);
        let armed = Cell::new(true);
        let replacement_drops = Cell::new(0);
        let replacement_armed = Cell::new(false);
        let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
        let payload = pool.alloc(HeapRetainedDropBomb {
            _bomb: RetainedDropBomb::new(&drops, &armed),
        });

        let unwind = catch_unwind(AssertUnwindSafe(|| unsafe {
            payload.drop_and_free(&mut pool);
        }))
        .expect_err("heap retained payload destructor did not panic");
        assert!(
            unwind.downcast_ref::<RetainedPayloadDropPanic>().is_some(),
            "heap retained cleanup replaced the original panic"
        );
        assert_eq!(drops.get(), 1);
        let after_unwind = pool.stats();
        assert_eq!(after_unwind.heap_fallbacks, 1);
        assert_eq!(after_unwind.heap_frees, 1);

        let replacement = pool.alloc(HeapRetainedDropBomb {
            _bomb: RetainedDropBomb::new(&replacement_drops, &replacement_armed),
        });
        unsafe { replacement.drop_and_free(&mut pool) };
        assert_eq!(replacement_drops.get(), 1);
        let after_replacement = pool.stats();
        assert_eq!(after_replacement.heap_fallbacks, 2);
        assert_eq!(after_replacement.heap_frees, 2);
    }

    #[test]
    fn raw_retained_slot_finishes_pooled_payload_exactly_once() {
        let drops = Cell::new(0);
        let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
        let pool_ptr = NonNull::from(&mut pool);

        let payload = unsafe {
            with_raw_retained_slot(pool_ptr, |slot: RawRetainedSlot<'_, PooledSlotPayload>| {
                let mut writing = slot.begin_writing();
                let dst = writing.as_mut_ptr();
                std::ptr::addr_of_mut!((*dst).value).write(41);
                std::ptr::addr_of_mut!((*dst).owner).write(DropProbe::new(&drops));
                writing.finish()
            })
        };

        assert_eq!(unsafe { payload.as_ref() }.value, 41);
        assert_eq!(drops.get(), 0);
        unsafe { payload.drop_and_free(&mut pool) };
        assert_eq!(drops.get(), 1);
        assert_eq!(pool.stats().pooled_allocs, 1);
        assert_eq!(pool.stats().pooled_frees, 1);
        assert_eq!(pool.test_poisoned_writes, 0);
    }

    #[test]
    fn raw_retained_slot_recycles_untouched_pooled_storage() {
        let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
        let pool_ptr = NonNull::from(&mut pool);

        let untouched = unsafe {
            with_raw_retained_slot::<PooledSlotPayload, _>(pool_ptr, |mut slot| slot.as_mut_ptr())
        };
        let drops = Cell::new(0);
        let replacement = pool.alloc(PooledSlotPayload {
            value: 7,
            owner: DropProbe::new(&drops),
        });

        assert_eq!(replacement.as_ptr(), untouched);
        let stats = pool.stats();
        assert_eq!(stats.pooled_allocs, 2);
        assert_eq!(stats.pooled_reuses, 1);
        assert_eq!(stats.pooled_frees, 1);

        unsafe { replacement.drop_and_free(&mut pool) };
        assert_eq!(drops.get(), 1);
        assert_eq!(pool.stats().pooled_frees, 2);
    }

    #[test]
    fn raw_retained_slot_poison_keeps_partial_pooled_payload_unreachable() {
        let drops = Cell::new(0);
        let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
        let pool_ptr = NonNull::from(&mut pool);
        let mut poisoned_ptr = std::ptr::null_mut();

        let unwind = catch_unwind(AssertUnwindSafe(|| unsafe {
            with_raw_retained_slot::<PooledSlotPayload, _>(pool_ptr, |slot| {
                let mut writing = slot.begin_writing();
                poisoned_ptr = writing.as_mut_ptr();
                std::ptr::addr_of_mut!((*poisoned_ptr).owner).write(DropProbe::new(&drops));
                panic!("inject partial pooled payload unwind");
            })
        }));
        assert!(unwind.is_err());
        assert_eq!(drops.get(), 0, "partial owner must not be dropped");

        let after_poison = pool.stats();
        assert_eq!(after_poison.pooled_allocs, 1);
        assert_eq!(after_poison.pooled_reuses, 0);
        assert_eq!(after_poison.pooled_frees, 0);
        assert_eq!(pool.test_poisoned_writes, 1);
        assert_eq!(pool.test_poisoned_heap_len, 0);

        let replacement = pool.alloc(PooledSlotPayload {
            value: 9,
            owner: DropProbe::new(&drops),
        });
        assert_ne!(replacement.as_ptr(), poisoned_ptr);
        assert_eq!(pool.stats().pooled_allocs, 2);
        assert_eq!(pool.stats().pooled_reuses, 0);
        unsafe { replacement.drop_and_free(&mut pool) };
        assert_eq!(pool.stats().pooled_frees, 1);
        assert_eq!(drops.get(), 1);

        drop(pool);
        assert_eq!(drops.get(), 1, "pool teardown must not drop partial T");
    }

    #[test]
    fn raw_retained_slot_finishes_heap_payload_exactly_once() {
        let drops = Cell::new(0);
        let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
        let pool_ptr = NonNull::from(&mut pool);

        let payload = unsafe {
            with_raw_retained_slot(pool_ptr, |slot: RawRetainedSlot<'_, HeapSlotPayload>| {
                let mut writing = slot.begin_writing();
                let dst = writing.as_mut_ptr();
                std::ptr::addr_of_mut!((*dst).value).write(73);
                std::ptr::addr_of_mut!((*dst).owner).write(DropProbe::new(&drops));
                writing.finish()
            })
        };

        assert_eq!(unsafe { payload.as_ref() }.value, 73);
        unsafe { payload.drop_and_free(&mut pool) };
        assert_eq!(drops.get(), 1);
        assert_eq!(pool.stats().heap_fallbacks, 1);
        assert_eq!(pool.stats().heap_frees, 1);
        assert_eq!(pool.test_poisoned_writes, 0);
    }

    #[test]
    fn raw_retained_slot_releases_untouched_heap_storage() {
        let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
        let pool_ptr = NonNull::from(&mut pool);

        unsafe {
            with_raw_retained_slot::<HeapSlotPayload, _>(pool_ptr, |_slot| {
                // Dropping the untouched raw slot releases only its Box-backed
                // `MaybeUninit<T>` allocation.
            });
        }

        assert_eq!(pool.stats().heap_fallbacks, 1);
        assert_eq!(pool.stats().heap_frees, 1);
        assert_eq!(pool.test_poisoned_writes, 0);
    }

    #[test]
    fn raw_retained_slot_poison_keeps_partial_heap_payload_unreachable() {
        let drops = Cell::new(0);
        let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
        let pool_ptr = NonNull::from(&mut pool);
        let mut poisoned_ptr = std::ptr::null_mut();

        let unwind = catch_unwind(AssertUnwindSafe(|| unsafe {
            with_raw_retained_slot::<HeapSlotPayload, _>(pool_ptr, |slot| {
                let mut writing = slot.begin_writing();
                poisoned_ptr = writing.as_mut_ptr();
                std::ptr::addr_of_mut!((*poisoned_ptr).owner).write(DropProbe::new(&drops));
                panic!("inject partial heap payload unwind");
            })
        }));
        assert!(unwind.is_err());
        assert_eq!(drops.get(), 0, "partial owner must not be dropped");

        let after_poison = pool.stats();
        assert_eq!(after_poison.heap_fallbacks, 1);
        assert_eq!(after_poison.heap_frees, 0);
        assert_eq!(pool.test_poisoned_writes, 1);
        assert_eq!(pool.test_poisoned_heap_len, 1);

        let replacement = pool.alloc(HeapSlotPayload {
            value: 11,
            owner: DropProbe::new(&drops),
        });
        assert_ne!(replacement.as_ptr(), poisoned_ptr);
        assert_eq!(pool.stats().heap_fallbacks, 2);
        assert_eq!(pool.stats().heap_frees, 0);
        unsafe { replacement.drop_and_free(&mut pool) };
        assert_eq!(pool.stats().heap_frees, 1);
        assert_eq!(drops.get(), 1);

        // The test-only pool Drop releases the first allocation strictly as
        // `MaybeUninit<T>`; it never calls the partial owner's destructor.
        drop(pool);
        assert_eq!(drops.get(), 1);
    }

    #[test]
    fn raw_retained_slot_preserves_pooled_and_overaligned_zst_provenance() {
        assert_eq!(std::mem::size_of::<PooledZst>(), 0);
        assert_eq!(std::mem::size_of::<HeapZst>(), 0);
        assert!(std::mem::align_of::<HeapZst>() > RETAINED_BLOCK_ALIGN);

        let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
        let pool_ptr = NonNull::from(&mut pool);
        let pooled = unsafe {
            with_raw_retained_slot(pool_ptr, |slot: RawRetainedSlot<'_, PooledZst>| {
                let mut writing = slot.begin_writing();
                writing.as_mut_ptr().write(PooledZst);
                writing.finish()
            })
        };
        let heap = unsafe {
            with_raw_retained_slot(pool_ptr, |mut slot: RawRetainedSlot<'_, HeapZst>| {
                assert_eq!(
                    slot.as_mut_ptr() as usize % std::mem::align_of::<HeapZst>(),
                    0
                );
                let mut writing = slot.begin_writing();
                writing.as_mut_ptr().write(HeapZst);
                writing.finish()
            })
        };

        let _pooled_value = unsafe { pooled.take(&mut pool) };
        let _heap_value = unsafe { heap.take(&mut pool) };
        let stats = pool.stats();
        assert_eq!(stats.pooled_allocs, 1);
        assert_eq!(stats.pooled_frees, 1);
        assert_eq!(stats.heap_fallbacks, 1);
        assert_eq!(stats.heap_frees, 1);
    }

    #[test]
    fn raw_retained_slot_allows_synchronous_same_pool_reentry() {
        let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
        let pool_ptr = NonNull::from(&mut pool);

        let outer = unsafe {
            with_raw_retained_slot(pool_ptr, |mut outer: RawRetainedSlot<'_, usize>| {
                let outer_ptr = outer.as_mut_ptr();
                let nested =
                    with_raw_retained_slot(pool_ptr, |slot: RawRetainedSlot<'_, usize>| {
                        let mut writing = slot.begin_writing();
                        writing.as_mut_ptr().write(19);
                        writing.finish()
                    });
                assert_ne!(outer_ptr, nested.as_ptr());
                assert_eq!(*nested.as_ref(), 19);
                nested.drop_and_free(&mut *pool_ptr.as_ptr());

                let mut writing = outer.begin_writing();
                writing.as_mut_ptr().write(23);
                writing.finish()
            })
        };

        assert_eq!(unsafe { *outer.as_ref() }, 23);
        unsafe { outer.drop_and_free(&mut pool) };
        let stats = pool.stats();
        assert_eq!(stats.pooled_allocs, 2);
        assert_eq!(stats.pooled_frees, 2);
        assert_eq!(stats.pooled_reuses, 0);
    }

    #[test]
    fn iovec_scratch_init_populates_inline_final_destination() {
        let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
        let mut init = pool
            .alloc_iovec_scratch_init(8)
            .expect("inline scratch token allocation failed");
        assert_eq!(init.len(), 8);
        assert!(matches!(&init.storage, RetainedIovecScratchStorage::Inline));

        let mut destination = MaybeUninit::<RetainedIovecScratch>::uninit();
        let final_ptr = destination.as_mut_ptr();
        let iovecs = unsafe { init.destination_mut(final_ptr) };
        assert_eq!(iovecs.len(), 8);
        iovecs[0].write(test_iovec(7));
        iovecs[7].write(test_iovec(13));

        unsafe { init.initialize_at(final_ptr) };
        let scratch = unsafe { destination.assume_init() };
        assert_eq!(scratch.len(), 8);
        assert!(matches!(
            &scratch.storage,
            RetainedIovecScratchStorage::Inline
        ));
        assert_eq!(
            unsafe { scratch.as_uninit_slice()[0].assume_init_ref() }.iov_len,
            7
        );
        assert_eq!(
            unsafe { scratch.as_uninit_slice()[7].assume_init_ref() }.iov_len,
            13
        );

        drop(scratch);
        let stats = pool.stats();
        assert_eq!(stats.writev_scratch_inline_allocs, 1);
        assert_eq!(stats.writev_scratch_pooled_allocs, 0);
        assert_eq!(stats.writev_scratch_pooled_frees, 0);
    }

    #[test]
    fn iovec_scratch_init_transfers_pooled_storage_exactly_once() {
        let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
        let mut init = pool
            .alloc_iovec_scratch_init(64)
            .expect("pooled scratch token allocation failed");
        let sidecar_ptr = pooled_scratch_init_ptr(&init);
        assert_eq!(init.len(), 64);
        assert_eq!(Rc::strong_count(&pool.iovec_pool), 2);

        let mut destination = MaybeUninit::<RetainedIovecScratch>::uninit();
        let final_ptr = destination.as_mut_ptr();
        let iovecs = unsafe { init.destination_mut(final_ptr) };
        assert_eq!(iovecs.as_mut_ptr(), sidecar_ptr);
        iovecs[0].write(test_iovec(17));
        iovecs[63].write(test_iovec(29));

        unsafe { init.initialize_at(final_ptr) };
        assert_eq!(pool.stats().writev_scratch_pooled_frees, 0);
        assert_eq!(Rc::strong_count(&pool.iovec_pool), 2);

        let scratch = unsafe { destination.assume_init() };
        assert_eq!(scratch.len(), 64);
        assert_eq!(scratch.as_uninit_slice().as_ptr(), sidecar_ptr);
        assert_eq!(
            unsafe { scratch.as_uninit_slice()[0].assume_init_ref() }.iov_len,
            17
        );
        assert_eq!(
            unsafe { scratch.as_uninit_slice()[63].assume_init_ref() }.iov_len,
            29
        );

        drop(scratch);
        assert_eq!(pool.stats().writev_scratch_pooled_frees, 1);
        assert_eq!(Rc::strong_count(&pool.iovec_pool), 1);

        let replacement = pool
            .alloc_iovec_scratch_init(64)
            .expect("replacement scratch token allocation failed");
        assert_eq!(pooled_scratch_init_ptr(&replacement), sidecar_ptr);
        assert_eq!(pool.stats().writev_scratch_pooled_reuses, 1);
        drop(replacement);
        assert_eq!(pool.stats().writev_scratch_pooled_frees, 2);
    }

    #[test]
    fn unconsumed_iovec_scratch_init_releases_pooled_storage() {
        let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
        let init = pool
            .alloc_iovec_scratch_init(64)
            .expect("pooled scratch token allocation failed");
        let sidecar_ptr = pooled_scratch_init_ptr(&init);
        assert_eq!(Rc::strong_count(&pool.iovec_pool), 2);

        drop(init);
        let after_drop = pool.stats();
        assert_eq!(after_drop.writev_scratch_pooled_allocs, 1);
        assert_eq!(after_drop.writev_scratch_pooled_frees, 1);
        assert_eq!(Rc::strong_count(&pool.iovec_pool), 1);

        let replacement = pool
            .alloc_iovec_scratch_init(64)
            .expect("replacement scratch token allocation failed");
        assert_eq!(pooled_scratch_init_ptr(&replacement), sidecar_ptr);
        assert_eq!(pool.stats().writev_scratch_pooled_reuses, 1);
        drop(replacement);
        assert_eq!(pool.stats().writev_scratch_pooled_frees, 2);
    }

    #[test]
    fn iovec_scratch_init_allows_same_pool_activity_while_destination_is_live() {
        let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
        let mut pool_ptr = NonNull::from(&mut pool);
        let mut outer = unsafe { pool_ptr.as_mut().alloc_iovec_scratch_init(64) }
            .expect("outer scratch token allocation failed");
        let outer_sidecar = pooled_scratch_init_ptr(&outer);
        let mut destination = MaybeUninit::<RetainedIovecScratch>::uninit();

        {
            let outer_iovecs = unsafe { outer.destination_mut(destination.as_mut_ptr()) };
            outer_iovecs[0].write(test_iovec(31));

            let nested = unsafe { pool_ptr.as_mut().alloc_iovec_scratch_init(64) }
                .expect("nested scratch token allocation failed");
            assert_ne!(pooled_scratch_init_ptr(&nested), outer_sidecar);
            drop(nested);

            outer_iovecs[63].write(test_iovec(37));
        }

        unsafe { outer.initialize_at(destination.as_mut_ptr()) };
        let scratch = unsafe { destination.assume_init() };
        assert_eq!(scratch.as_uninit_slice().as_ptr(), outer_sidecar);
        assert_eq!(
            unsafe { scratch.as_uninit_slice()[0].assume_init_ref() }.iov_len,
            31
        );
        assert_eq!(
            unsafe { scratch.as_uninit_slice()[63].assume_init_ref() }.iov_len,
            37
        );
        drop(scratch);

        let stats = pool.stats();
        assert_eq!(stats.writev_scratch_pooled_allocs, 2);
        assert_eq!(stats.writev_scratch_pooled_reuses, 0);
        assert_eq!(stats.writev_scratch_pooled_frees, 2);
        assert_eq!(Rc::strong_count(&pool.iovec_pool), 1);
    }

    #[test]
    fn poisoned_writing_slot_keeps_transferred_scratch_owned_and_unreused() {
        let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
        let mut init = pool
            .alloc_iovec_scratch_init(64)
            .expect("poisoned scratch token allocation failed");
        let pool_ptr = NonNull::from(&mut pool);
        let poisoned_sidecar = pooled_scratch_init_ptr(&init);
        let mut poisoned_scratch = std::ptr::null_mut();

        let unwind = catch_unwind(AssertUnwindSafe(|| unsafe {
            with_raw_retained_slot::<ScratchPayload, _>(pool_ptr, |mut slot| {
                let payload = slot.as_mut_ptr();
                poisoned_scratch = std::ptr::addr_of_mut!((*payload)._scratch);
                let iovecs = init.destination_mut(poisoned_scratch);
                iovecs[0].write(test_iovec(41));

                let mut writing = slot.begin_writing();
                init.initialize_at(std::ptr::addr_of_mut!((*writing.as_mut_ptr())._scratch));
                panic!("inject unwind after pooled scratch ownership transfer");
            })
        }));
        assert!(unwind.is_err());
        assert_eq!(pool.test_poisoned_writes, 1);
        assert_eq!(pool.stats().writev_scratch_pooled_frees, 0);
        assert_eq!(Rc::strong_count(&pool.iovec_pool), 2);

        let replacement = pool
            .alloc_iovec_scratch_init(64)
            .expect("post-poison scratch token allocation failed");
        assert_ne!(pooled_scratch_init_ptr(&replacement), poisoned_sidecar);
        assert_eq!(pool.stats().writev_scratch_pooled_reuses, 0);
        drop(replacement);
        assert_eq!(pool.stats().writev_scratch_pooled_frees, 1);
        assert_eq!(Rc::strong_count(&pool.iovec_pool), 2);

        // Production poison deliberately leaves the transferred owner in the
        // unreachable partial payload. This test knows that exact field was
        // fully initialized, so release it explicitly after proving nonreuse
        // to keep Miri's test allocation accounting clean.
        unsafe { std::ptr::drop_in_place(poisoned_scratch) };
        assert_eq!(pool.stats().writev_scratch_pooled_frees, 2);
        assert_eq!(Rc::strong_count(&pool.iovec_pool), 1);
    }

    #[test]
    fn pooled_iovec_scratch_survives_parent_move_inside_retained_payload() {
        let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
        let scratch = pool
            .alloc_iovec_scratch(512)
            .expect("pooled scratch allocation failed");
        let payload = pool.alloc(ScratchPayload { _scratch: scratch });

        let mut moved_pool = pool;
        unsafe { payload.drop_and_free(&mut moved_pool) };

        let stats = moved_pool.stats();
        assert_eq!(stats.writev_scratch_pooled_allocs, 1);
        assert_eq!(stats.writev_scratch_pooled_frees, 1);
        assert_eq!(stats.pooled_allocs, 1);
        assert_eq!(stats.pooled_frees, 1);
    }

    #[test]
    fn pooled_iovec_scratch_owner_outlives_dropped_parent_pool() {
        let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
        let owner = Rc::downgrade(&pool.iovec_pool);
        let mut scratch = pool
            .alloc_iovec_scratch(512)
            .expect("pooled scratch allocation failed");
        scratch.as_uninit_slice_mut()[0].write(libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 7,
        });
        scratch.as_uninit_slice_mut()[511].write(libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 11,
        });

        drop(pool);
        assert!(owner.upgrade().is_some());
        assert_eq!(
            unsafe { scratch.as_uninit_slice()[0].assume_init_ref() }.iov_len,
            7
        );
        assert_eq!(
            unsafe { scratch.as_uninit_slice()[511].assume_init_ref() }.iov_len,
            11
        );

        drop(scratch);
        assert!(owner.upgrade().is_none());
    }

    #[test]
    fn pooled_iovec_scratch_releases_before_parent_pool() {
        let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
        let owner = Rc::downgrade(&pool.iovec_pool);
        let scratch = pool
            .alloc_iovec_scratch(512)
            .expect("pooled scratch allocation failed");

        drop(scratch);
        assert!(owner.upgrade().is_some());
        assert_eq!(pool.stats().writev_scratch_pooled_frees, 1);

        drop(pool);
        assert!(owner.upgrade().is_none());
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn retained_iovec_scratch_layout_remains_fixed() {
        assert_eq!(std::mem::size_of::<RetainedIovecScratch>(), 288);
        assert_eq!(std::mem::size_of::<RetainedIovecScratchStorage>(), 24);
        assert_eq!(std::mem::size_of::<RetainedIovecScratchInit>(), 32);
    }
}
