//! Internal retained operation-payload storage.
//!
//! This module provides fixed-size-class storage for operation payload structs
//! whose memory may be referenced by the kernel after the owning future is
//! dropped.
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

#[derive(Clone, Copy)]
pub(crate) struct RetainedPayloadVtable {
    /// Drops the stored value and releases the backing allocation.
    pub(crate) drop_and_free: unsafe fn(*mut (), *mut RetainedPayloadPool),
    /// Releases backing storage after the value has been moved out.
    pub(crate) free_storage: unsafe fn(*mut (), *mut RetainedPayloadPool),
}

/// Debug/test-support counters for asserting retained-pool behavior in tests.
#[cfg(any(debug_assertions, feature = "test-support"))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RetainedPayloadPoolStats {
    /// Retained payload allocations served by size-class slabs.
    pub(crate) pooled_allocs: usize,
    /// Pooled payload allocations served from returned blocks.
    pub(crate) pooled_reuses: usize,
    /// Pooled payload blocks returned to size-class free lists.
    pub(crate) pooled_frees: usize,
    /// Retained payload slab pages requested from providers.
    pub(crate) slab_allocs: usize,
    /// Retained payload allocations that used the heap fallback.
    pub(crate) heap_fallbacks: usize,
    /// Heap fallback payload blocks released.
    pub(crate) heap_frees: usize,
    /// Iovec scratch requests served from inline storage.
    pub(crate) writev_scratch_inline_allocs: usize,
    /// Iovec scratch requests served by pooled sidecar storage.
    pub(crate) writev_scratch_pooled_allocs: usize,
    /// Pooled sidecar scratch requests served from returned blocks.
    pub(crate) writev_scratch_pooled_reuses: usize,
    /// Pooled sidecar scratch blocks returned to size-class free lists.
    pub(crate) writev_scratch_pooled_frees: usize,
    /// Sidecar scratch slab pages requested from providers.
    pub(crate) writev_scratch_slab_allocs: usize,
    /// Scratch requests rejected for exceeding the supported iovec count.
    pub(crate) writev_scratch_oversize_rejections: usize,
    /// Scratch requests rejected because no sidecar block was available.
    pub(crate) writev_scratch_alloc_failures: usize,
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
    #[cfg(any(debug_assertions, feature = "test-support"))]
    stats: RetainedIovecScratchStats,
}

impl RetainedIovecScratchPool {
    fn new() -> io::Result<Rc<Self>> {
        Ok(Rc::new(Self {
            state: UnsafeCell::new(RetainedIovecScratchPoolState {
                classes: [
                    RetainedSizeClass::new(iovec_class_block_size(0))?,
                    RetainedSizeClass::new(iovec_class_block_size(1))?,
                    RetainedSizeClass::new(iovec_class_block_size(2))?,
                    RetainedSizeClass::new(iovec_class_block_size(3))?,
                ],
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
            (*self.state.get()).stats.writev_scratch_inline_allocs += 1;
        }
    }

    #[cfg(any(debug_assertions, feature = "test-support"))]
    #[inline(always)]
    fn record_oversize_rejection(&self) {
        // SAFETY: same owner-thread-only access invariant as
        // `record_inline_alloc`.
        unsafe {
            (*self.state.get()).stats.writev_scratch_oversize_rejections += 1;
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
                state.stats.writev_scratch_alloc_failures += 1;
            }
            return None;
        };

        #[cfg(any(debug_assertions, feature = "test-support"))]
        {
            state.stats.writev_scratch_pooled_allocs += 1;
            if result.reused {
                state.stats.writev_scratch_pooled_reuses += 1;
            }
            if result.new_slab {
                state.stats.writev_scratch_slab_allocs += 1;
            }
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
            state.stats.writev_scratch_pooled_frees += 1;
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
    /// Heap-stable owner for retained sidecar `iovec` scratch arrays.
    iovec_pool: Rc<RetainedIovecScratchPool>,
    #[cfg(any(debug_assertions, feature = "test-support"))]
    /// Debug/test-support counters exported through runtime stats and tests.
    stats: RetainedPayloadPoolStats,
}

impl RetainedPayloadPool {
    pub(crate) fn new() -> io::Result<Self> {
        Ok(Self {
            classes: [
                RetainedSizeClass::new(RETAINED_SIZE_CLASSES[0])?,
                RetainedSizeClass::new(RETAINED_SIZE_CLASSES[1])?,
                RetainedSizeClass::new(RETAINED_SIZE_CLASSES[2])?,
                RetainedSizeClass::new(RETAINED_SIZE_CLASSES[3])?,
                RetainedSizeClass::new(RETAINED_SIZE_CLASSES[4])?,
                RetainedSizeClass::new(RETAINED_SIZE_CLASSES[5])?,
                RetainedSizeClass::new(RETAINED_SIZE_CLASSES[6])?,
                RetainedSizeClass::new(RETAINED_SIZE_CLASSES[7])?,
                RetainedSizeClass::new(RETAINED_SIZE_CLASSES[8])?,
                RetainedSizeClass::new(RETAINED_SIZE_CLASSES[9])?,
                RetainedSizeClass::new(RETAINED_SIZE_CLASSES[10])?,
            ],
            iovec_pool: RetainedIovecScratchPool::new()?,
            #[cfg(any(debug_assertions, feature = "test-support"))]
            stats: RetainedPayloadPoolStats::default(),
        })
    }

    /// Stores `value` in pointer-stable retained storage.
    ///
    /// The returned handle must be consumed exactly once by either
    /// [`RetainedPayload::take`] or [`RetainedPayload::drop_and_free`].
    #[inline(always)]
    pub(crate) fn alloc<T: 'static>(&mut self, value: T) -> RetainedPayload<T> {
        match class_index_for::<T>() {
            Some(class_index) => {
                if let Some(result) = self.classes[class_index].alloc_block() {
                    #[cfg(any(debug_assertions, feature = "test-support"))]
                    {
                        self.stats.pooled_allocs += 1;
                        if result.reused {
                            self.stats.pooled_reuses += 1;
                        }
                        if result.new_slab {
                            self.stats.slab_allocs += 1;
                        }
                    }

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
            self.stats.heap_fallbacks += 1;
        }

        let ptr = Box::into_raw(Box::new(value));
        unsafe { RetainedPayload::from_raw_parts(ptr, heap_vtable::<T>()) }
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
            self.stats.pooled_frees += 1;
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
            self.stats.heap_frees += 1;
        }
    }

    /// Allocates retained kernel-facing `iovec` scratch for a vectored I/O
    /// submission.
    ///
    /// Scratch is sized by active iovec count, not by the
    /// const-generic chain capacity. It stores metadata only and has no heap
    /// fallback.
    #[inline(always)]
    pub(crate) fn alloc_iovec_scratch(
        &mut self,
        iov_count: usize,
    ) -> io::Result<RetainedIovecScratch> {
        if iov_count <= RETAINED_IOVEC_INLINE_COUNT {
            #[cfg(any(debug_assertions, feature = "test-support"))]
            self.iovec_pool.record_inline_alloc();
            return Ok(RetainedIovecScratch::inline(iov_count));
        }

        let class_index = match iovec_class_index_for_count(iov_count) {
            Some(class_index) => class_index,
            None => {
                #[cfg(any(debug_assertions, feature = "test-support"))]
                self.iovec_pool.record_oversize_rejection();
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "active iovec count exceeds retained scratch capacity",
                ));
            }
        };

        let Some(result) = self.iovec_pool.alloc_block(class_index) else {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        };

        Ok(unsafe {
            RetainedIovecScratch::pooled(
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

impl RetainedIovecScratch {
    #[inline(always)]
    fn inline(len: usize) -> Self {
        debug_assert!(len <= RETAINED_IOVEC_INLINE_COUNT);
        Self {
            len,
            inline: uninit_iovec_inline(),
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
            inline: uninit_iovec_inline(),
            storage: RetainedIovecScratchStorage::Pooled {
                ptr: unsafe { NonNull::new_unchecked(ptr as *mut MaybeUninit<libc::iovec>) },
                class_index,
                owner,
            },
        }
    }

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

impl Drop for RetainedIovecScratch {
    fn drop(&mut self) {
        if let RetainedIovecScratchStorage::Pooled {
            ptr,
            class_index,
            owner,
        } = &self.storage
        {
            unsafe { owner.free_block(*class_index, ptr.as_ptr() as *mut u8) };
        }
    }
}

#[cfg(target_pointer_width = "64")]
const _: [(); 288] = [(); std::mem::size_of::<RetainedIovecScratch>()];
#[cfg(target_pointer_width = "64")]
const _: [(); 24] = [(); std::mem::size_of::<RetainedIovecScratchStorage>()];

#[must_use = "retained payload handles own storage and must be consumed"]
pub(crate) struct RetainedPayload<T: 'static> {
    /// Pointer to initialized retained payload storage.
    ptr: NonNull<T>,
    /// Release hooks matching the allocation path for `ptr`.
    vtable: RetainedPayloadVtable,
    /// Carries the concrete payload type for drop-checking and variance.
    _marker: PhantomData<T>,
}

impl<T: 'static> RetainedPayload<T> {
    /// # Safety
    ///
    /// `ptr` must point to initialized storage for `T`, and `vtable` must
    /// release that exact storage allocation path.
    #[inline(always)]
    pub(crate) unsafe fn from_raw_parts(ptr: *mut T, vtable: RetainedPayloadVtable) -> Self {
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
    pub(crate) fn as_ptr(&self) -> *mut T {
        self.ptr.as_ptr()
    }

    #[inline(always)]
    /// Returns the release hooks paired with this handle's allocation path.
    pub(crate) fn vtable(&self) -> RetainedPayloadVtable {
        self.vtable
    }

    #[inline(always)]
    /// Transfers the retained pointer and release hooks to erased ownership.
    ///
    /// The returned parts must later be reconstructed and consumed exactly
    /// once; this handle intentionally has no independent `Drop` path.
    pub(crate) fn into_raw_parts(self) -> (*mut (), RetainedPayloadVtable) {
        (self.ptr.as_ptr() as *mut (), self.vtable)
    }

    /// Returns a shared reference to the retained payload.
    ///
    /// # Safety
    ///
    /// The caller must ensure the payload has not been taken or freed.
    #[inline(always)]
    pub(crate) unsafe fn as_ref(&self) -> &T {
        unsafe { self.ptr.as_ref() }
    }

    /// Returns a mutable reference to the retained payload.
    ///
    /// # Safety
    ///
    /// The caller must have exclusive logical access to the retained payload.
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
    #[inline(always)]
    pub(crate) unsafe fn drop_and_free(self, pool: &mut RetainedPayloadPool) {
        unsafe { (self.vtable.drop_and_free)(self.ptr.as_ptr() as *mut (), pool) };
    }
}

struct RetainedSizeClass {
    /// Usable bytes in each block belonging to this class.
    block_size: usize,
    /// Returned blocks ready for reuse.
    free_list: SList<u8>,
    /// Move-safe chain of slab pages owned by this class.
    slab_pages: SlabPageChain,
    /// Slab allocator that requests pages for this class.
    slab_factory: ManuallyDrop<SlabAllocator<'static, BasicMemoryProvider>>,
    /// Stable provider backing `slab_factory` through its raw provider pointer.
    _provider: ProviderOwner<BasicMemoryProvider>,
}

impl RetainedSizeClass {
    fn new(block_size: usize) -> io::Result<Self> {
        let blocks_per_slab = std::cmp::max(1, RETAINED_SLAB_TARGET_BYTES / block_size);
        let provider = ProviderOwner::new(BasicMemoryProvider::new());
        let mut slab_factory = match unsafe {
            // SAFETY: provider.as_ptr() comes from a heap allocation owned by
            // ProviderOwner and remains stable until after slab_factory is
            // manually dropped.
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
            block_size,
            free_list: SList::new(),
            slab_pages: SlabPageChain::new(),
            slab_factory,
            _provider: provider,
        })
    }

    #[inline(always)]
    fn alloc_block(&mut self) -> Option<ClassAllocResult> {
        if let Some(ptr) = unsafe { self.free_list.pop_front() } {
            return Some(ClassAllocResult {
                ptr,
                reused: true,
                new_slab: false,
            });
        }

        let result = unsafe {
            self.slab_pages
                .alloc_or_grow(&mut self.slab_factory, self.block_size)
        }?;

        Some(ClassAllocResult {
            ptr: result.ptr,
            reused: false,
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
    reused: bool,
    /// True when this allocation requested a fresh slab page.
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
fn pooled_vtable<T: 'static>(class_index: usize) -> RetainedPayloadVtable {
    match class_index {
        0 => RetainedPayloadVtable {
            drop_and_free: pooled_drop_and_free::<T, 0>,
            free_storage: pooled_free_storage::<T, 0>,
        },
        1 => RetainedPayloadVtable {
            drop_and_free: pooled_drop_and_free::<T, 1>,
            free_storage: pooled_free_storage::<T, 1>,
        },
        2 => RetainedPayloadVtable {
            drop_and_free: pooled_drop_and_free::<T, 2>,
            free_storage: pooled_free_storage::<T, 2>,
        },
        3 => RetainedPayloadVtable {
            drop_and_free: pooled_drop_and_free::<T, 3>,
            free_storage: pooled_free_storage::<T, 3>,
        },
        4 => RetainedPayloadVtable {
            drop_and_free: pooled_drop_and_free::<T, 4>,
            free_storage: pooled_free_storage::<T, 4>,
        },
        5 => RetainedPayloadVtable {
            drop_and_free: pooled_drop_and_free::<T, 5>,
            free_storage: pooled_free_storage::<T, 5>,
        },
        6 => RetainedPayloadVtable {
            drop_and_free: pooled_drop_and_free::<T, 6>,
            free_storage: pooled_free_storage::<T, 6>,
        },
        7 => RetainedPayloadVtable {
            drop_and_free: pooled_drop_and_free::<T, 7>,
            free_storage: pooled_free_storage::<T, 7>,
        },
        8 => RetainedPayloadVtable {
            drop_and_free: pooled_drop_and_free::<T, 8>,
            free_storage: pooled_free_storage::<T, 8>,
        },
        9 => RetainedPayloadVtable {
            drop_and_free: pooled_drop_and_free::<T, 9>,
            free_storage: pooled_free_storage::<T, 9>,
        },
        10 => RetainedPayloadVtable {
            drop_and_free: pooled_drop_and_free::<T, 10>,
            free_storage: pooled_free_storage::<T, 10>,
        },
        _ => unreachable!("invalid retained payload size class"),
    }
}

#[inline(always)]
fn heap_vtable<T: 'static>() -> RetainedPayloadVtable {
    RetainedPayloadVtable {
        drop_and_free: heap_drop_and_free::<T>,
        free_storage: heap_free_storage::<T>,
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
    unsafe {
        std::ptr::drop_in_place(ptr as *mut T);
        (*pool).free_pooled_block(CLASS, ptr as *mut u8);
    }
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
    unsafe { drop(Box::from_raw(ptr as *mut T)) };
    #[cfg(not(any(debug_assertions, feature = "test-support")))]
    let _ = pool;
    #[cfg(any(debug_assertions, feature = "test-support"))]
    unsafe {
        (*pool).stats.heap_frees += 1;
    }
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

#[inline(always)]
fn uninit_iovec_inline() -> [MaybeUninit<libc::iovec>; RETAINED_IOVEC_INLINE_COUNT] {
    // SAFETY: an array of `MaybeUninit<libc::iovec>` may be left wholly
    // uninitialized; callers track which entries they initialize.
    unsafe { MaybeUninit::uninit().assume_init() }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ScratchPayload {
        _scratch: RetainedIovecScratch,
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
    }
}
