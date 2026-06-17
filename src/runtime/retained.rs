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
use crate::utils::memory::provider::BasicMemoryProvider;
use crate::utils::memory::slab::{Slab, SlabAllocator, SlabAllocatorConfigError};
use std::alloc::Layout;
use std::io;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::slice;

const RETAINED_BLOCK_ALIGN: usize = 64;
const RETAINED_SLAB_TARGET_BYTES: usize = 64 * 1024;
const RETAINED_SIZE_CLASSES: [usize; 11] = [
    64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536,
];
pub(crate) const RETAINED_IOVEC_INLINE_COUNT: usize = 16;
pub(crate) const RETAINED_IOVEC_MAX_COUNT: usize = 1024;
const RETAINED_IOVEC_SIZE_CLASSES: [usize; 4] = [64, 128, 512, 1024];

#[derive(Clone, Copy)]
pub(crate) struct RetainedPayloadVtable {
    /// Drops the stored value and releases the backing allocation.
    pub(crate) drop_and_free: unsafe fn(*mut (), *mut RetainedPayloadPool),
    /// Releases backing storage after the value has been moved out.
    pub(crate) free_storage: unsafe fn(*mut (), *mut RetainedPayloadPool),
}

/// Debug-only counters for asserting retained-pool behavior in tests.
#[cfg(debug_assertions)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RetainedPayloadPoolStats {
    /// Retained payload allocations served by size-class slabs.
    pub(crate) pooled_allocs: usize,
    /// Pooled payload allocations served from returned blocks.
    pub(crate) pooled_reuses: usize,
    /// Pooled payload blocks returned to size-class slabs.
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
    /// Pooled sidecar scratch blocks returned.
    pub(crate) writev_scratch_pooled_frees: usize,
    /// Sidecar scratch slab pages requested from providers.
    pub(crate) writev_scratch_slab_allocs: usize,
    /// Scratch requests rejected for exceeding the supported iovec count.
    pub(crate) writev_scratch_oversize_rejections: usize,
    /// Scratch requests rejected because no sidecar block was available.
    pub(crate) writev_scratch_alloc_failures: usize,
}

/// Raw, size-classed pool for retained operation payloads.
pub(crate) struct RetainedPayloadPool {
    /// Size classes for retained operation payload structs.
    classes: [RetainedSizeClass; RETAINED_SIZE_CLASSES.len()],
    /// Size classes for retained sidecar `iovec` scratch arrays.
    iovec_classes: [RetainedSizeClass; RETAINED_IOVEC_SIZE_CLASSES.len()],
    #[cfg(debug_assertions)]
    /// Debug counters exported through runtime stats and tests.
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
            iovec_classes: [
                RetainedSizeClass::new(iovec_class_block_size(0))?,
                RetainedSizeClass::new(iovec_class_block_size(1))?,
                RetainedSizeClass::new(iovec_class_block_size(2))?,
                RetainedSizeClass::new(iovec_class_block_size(3))?,
            ],
            #[cfg(debug_assertions)]
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
                    #[cfg(debug_assertions)]
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
        #[cfg(debug_assertions)]
        {
            self.stats.heap_fallbacks += 1;
        }

        let ptr = Box::into_raw(Box::new(value));
        unsafe { RetainedPayload::from_raw_parts(ptr, heap_vtable::<T>()) }
    }

    #[inline(always)]
    unsafe fn free_pooled_block(&mut self, class_index: usize, ptr: *mut u8) {
        unsafe { self.classes[class_index].free_block(ptr) };
        #[cfg(debug_assertions)]
        {
            self.stats.pooled_frees += 1;
        }
    }

    #[inline(always)]
    unsafe fn free_heap_storage<T>(&mut self, ptr: *mut T) {
        if std::mem::size_of::<T>() != 0 {
            unsafe { std::alloc::dealloc(ptr as *mut u8, Layout::new::<T>()) };
        }
        #[cfg(debug_assertions)]
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
            #[cfg(debug_assertions)]
            {
                self.stats.writev_scratch_inline_allocs += 1;
            }
            return Ok(RetainedIovecScratch::inline(iov_count));
        }

        let class_index = match iovec_class_index_for_count(iov_count) {
            Some(class_index) => class_index,
            None => {
                #[cfg(debug_assertions)]
                {
                    self.stats.writev_scratch_oversize_rejections += 1;
                }
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "active iovec count exceeds retained scratch capacity",
                ));
            }
        };

        let Some(result) = self.iovec_classes[class_index].alloc_block() else {
            #[cfg(debug_assertions)]
            {
                self.stats.writev_scratch_alloc_failures += 1;
            }
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        };

        #[cfg(debug_assertions)]
        {
            self.stats.writev_scratch_pooled_allocs += 1;
            if result.reused {
                self.stats.writev_scratch_pooled_reuses += 1;
            }
            if result.new_slab {
                self.stats.writev_scratch_slab_allocs += 1;
            }
        }

        Ok(unsafe {
            RetainedIovecScratch::pooled(result.ptr, iov_count, class_index, self as *mut Self)
        })
    }

    #[inline(always)]
    unsafe fn free_iovec_scratch_block(&mut self, class_index: usize, ptr: *mut u8) {
        unsafe { self.iovec_classes[class_index].free_block(ptr) };
        #[cfg(debug_assertions)]
        {
            self.stats.writev_scratch_pooled_frees += 1;
        }
    }

    #[cfg(debug_assertions)]
    pub(crate) fn stats(&self) -> RetainedPayloadPoolStats {
        self.stats
    }
}

/// Retained vectored I/O scratch storing kernel-facing `iovec` metadata.
///
/// This type is move-safe: inline storage is addressed from the enum field on
/// every access, while sidecar storage carries a stable slab pointer. The
/// sidecar pointer remains valid until the scratch handle is dropped, which is
/// tied to the retained vectored I/O payload lifetime. Pooled scratch still
/// carries the inline array so the handle remains one simple move-safe value;
/// the extra bytes are a deliberate tradeoff for cancellation-path simplicity.
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
        /// Retained pool that owns the sidecar block.
        pool: *mut RetainedPayloadPool,
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
    /// `ptr` must be a block allocated from `pool.iovec_classes[class_index]`
    /// and large enough for `len` `libc::iovec` values.
    #[inline(always)]
    unsafe fn pooled(
        ptr: *mut u8,
        len: usize,
        class_index: usize,
        pool: *mut RetainedPayloadPool,
    ) -> Self {
        debug_assert!(len > RETAINED_IOVEC_INLINE_COUNT);
        debug_assert!(len <= RETAINED_IOVEC_SIZE_CLASSES[class_index]);
        Self {
            len,
            inline: uninit_iovec_inline(),
            storage: RetainedIovecScratchStorage::Pooled {
                ptr: unsafe { NonNull::new_unchecked(ptr as *mut MaybeUninit<libc::iovec>) },
                class_index,
                pool,
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
            pool,
        } = &self.storage
        {
            let pool_ptr = *pool;
            debug_assert!(!pool_ptr.is_null(), "iovec scratch pool pointer is null");
            unsafe { (*pool_ptr).free_iovec_scratch_block(*class_index, ptr.as_ptr() as *mut u8) };
        }
    }
}

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
    pub(crate) fn as_ptr(&self) -> *mut T {
        self.ptr.as_ptr()
    }

    #[inline(always)]
    pub(crate) fn vtable(&self) -> RetainedPayloadVtable {
        self.vtable
    }

    #[inline(always)]
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
    /// Head of the singly linked list of slab pages owned by this class.
    slab_page_head: *mut Slab,
    /// Slab page currently used for bump allocation.
    current_slab: *mut Slab,
    /// Slab allocator that requests pages for this class.
    slab_factory: ManuallyDrop<SlabAllocator<'static, BasicMemoryProvider>>,
    /// Stable provider backing `slab_factory`.
    _provider: Box<BasicMemoryProvider>,
}

impl RetainedSizeClass {
    fn new(block_size: usize) -> io::Result<Self> {
        let blocks_per_slab = std::cmp::max(1, RETAINED_SLAB_TARGET_BYTES / block_size);
        let mut provider = Box::new(BasicMemoryProvider::new());
        let provider_ptr = &mut *provider as *mut BasicMemoryProvider;
        let mut slab_factory = ManuallyDrop::new(
            SlabAllocator::new_uninit(
                unsafe { &mut *provider_ptr },
                block_size,
                RETAINED_BLOCK_ALIGN,
                blocks_per_slab,
            )
            .map_err(slab_config_error_to_io)?,
        );
        slab_factory.init();

        Ok(Self {
            block_size,
            free_list: SList::new(),
            slab_page_head: std::ptr::null_mut(),
            current_slab: std::ptr::null_mut(),
            slab_factory,
            _provider: provider,
        })
    }

    #[inline(always)]
    fn alloc_block(&mut self) -> Option<ClassAllocResult> {
        if let Some(ptr) = unsafe { self.free_list.pop_front(0) } {
            return Some(ClassAllocResult {
                ptr,
                reused: true,
                new_slab: false,
            });
        }

        if !self.current_slab.is_null()
            && let Some(ptr) = unsafe { (*self.current_slab).try_alloc(self.block_size) }
        {
            return Some(ClassAllocResult {
                ptr,
                reused: false,
                new_slab: false,
            });
        }

        let slab_ptr = self.slab_factory.provide_slab()?;
        unsafe {
            (*slab_ptr).link.next = self.slab_page_head as *mut Link;
        }
        self.slab_page_head = slab_ptr;
        self.current_slab = slab_ptr;
        let ptr = unsafe { (*slab_ptr).try_alloc(self.block_size) }?;

        Some(ClassAllocResult {
            ptr,
            reused: false,
            new_slab: true,
        })
    }

    #[inline(always)]
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
        let mut current = self.slab_page_head;
        while !current.is_null() {
            let next = unsafe { (*current).link.next as *mut Slab };
            unsafe { self.slab_factory.free_slab(current as *mut u8) };
            current = next;
        }

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

unsafe fn pooled_drop_and_free<T, const CLASS: usize>(
    ptr: *mut (),
    pool: *mut RetainedPayloadPool,
) {
    unsafe {
        std::ptr::drop_in_place(ptr as *mut T);
        (*pool).free_pooled_block(CLASS, ptr as *mut u8);
    }
}

unsafe fn pooled_free_storage<T, const CLASS: usize>(ptr: *mut (), pool: *mut RetainedPayloadPool) {
    let _ = PhantomData::<T>;
    unsafe { (*pool).free_pooled_block(CLASS, ptr as *mut u8) };
}

unsafe fn heap_drop_and_free<T>(ptr: *mut (), pool: *mut RetainedPayloadPool) {
    unsafe { drop(Box::from_raw(ptr as *mut T)) };
    #[cfg(not(debug_assertions))]
    let _ = pool;
    #[cfg(debug_assertions)]
    unsafe {
        (*pool).stats.heap_frees += 1;
    }
}

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
    unsafe { MaybeUninit::uninit().assume_init() }
}
