//! Pool allocator for [`IoBuffMut`] buffers.
//!
//! [`IoBuffPool`] pre-allocates slab pages of identically-shaped buffer slots
//! using the library's slab allocator and memory provider.  Allocation is O(1)
//! (intrusive free-list pop).  When a pool-allocated buffer's last reference
//! drops, the slot is returned to the pool's free list — zero heap
//! alloc/dealloc on the fast path after warmup.
//!
//! Each pool produces buffers with a fixed headroom/payload/tailroom layout
//! configured at creation time.
//! Outstanding buffers keep the stable inner pool state alive, so checked-out
//! buffers remain valid even if the outer [`IoBuffPool`] handle is dropped.
//!
//! # Fast-Path Guidance
//!
//! Best fast-path choice:
//! - [`IoBuffPool`] is the intended steady-state buffer fast path for fixed
//!   layouts because it reuses stable slots and avoids heap alloc/dealloc
//!   after warmup.
//!
//! Prefer not to use on the fast path:
//! - Prefer not to use [`super::IoBuffMut::new`] when the same buffer
//!   geometry repeats over time. Use [`IoBuffPool`] instead.
//! - Prefer not to force pool-backed allocation when payload sizes or region
//!   layouts vary widely. In that case the heap-backed
//!   [`super::IoBuffMut::new`] path is usually the better fit.

use super::IoBuffError;
use super::iobuff::{IoBuffHeader, IoBuffMut};
use crate::utils::list::intrusive::slist::{Link as SListLink, SList};
use crate::utils::memory::provider::BasicMemoryProvider;
use crate::utils::memory::slab::{SlabAllocator, SlabPageChain};
use std::cell::Cell;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;

/// Configuration for [`IoBuffPool`] buffer layout.
///
/// This is setup-time configuration. Create and initialize pools before the
/// steady-state I/O path; use [`IoBuffPool::alloc`] to reuse fixed-shape
/// slots on the fast path.
///
/// # Example
/// ```
/// use flowio::runtime::buffer::pool::IoBuffPoolConfig;
///
/// let config = IoBuffPoolConfig {
///     headroom: 16,
///     payload: 1500,
///     tailroom: 0,
///     objs_per_slab: 64,
/// };
/// assert_eq!(config.payload, 1500);
/// ```
pub struct IoBuffPoolConfig {
    /// Reserved headroom bytes for prepending protocol headers.
    pub headroom: usize,
    /// Main payload region size.
    pub payload: usize,
    /// Reserved tailroom bytes for appending protocol trailers.
    pub tailroom: usize,
    /// Number of buffer slots per slab page.  Controls memory granularity:
    /// more slots per slab = fewer provider calls, larger pages.
    pub objs_per_slab: usize,
}

/// Errors returned when constructing an [`IoBuffPool`].
///
/// # Example
/// ```
/// use flowio::runtime::buffer::pool::{IoBuffPool, IoBuffPoolConfig, IoBuffPoolConfigError};
///
/// let result = IoBuffPool::new(IoBuffPoolConfig {
///     headroom: 0,
///     payload: 1,
///     tailroom: 0,
///     objs_per_slab: 0,
/// });
/// assert!(matches!(result, Err(IoBuffPoolConfigError::ObjsPerSlabZero)));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoBuffPoolConfigError {
    /// `objs_per_slab` must be greater than zero.
    ObjsPerSlabZero,
    /// Buffer slot layout overflowed addressable memory.
    LayoutOverflow,
}

impl std::fmt::Display for IoBuffPoolConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ObjsPerSlabZero => {
                f.write_str("IoBuffPoolConfig::objs_per_slab must be greater than zero")
            }
            Self::LayoutOverflow => {
                f.write_str("IoBuffPoolConfig produced a buffer layout that overflowed usize")
            }
        }
    }
}

impl std::error::Error for IoBuffPoolConfigError {}

/// Pool of identically-shaped [`IoBuffMut`] buffers for zero-alloc fast-path
/// operation.
///
/// Each buffer allocated from the pool has the same total capacity
/// (`headroom + payload + tailroom`) and starts with the offset positioned
/// after the headroom region.
///
/// This is the best buffer API to use in the crate's steady-state fast path
/// when buffer geometry is fixed and reusable. For variable-shape buffers,
/// prefer [`super::IoBuffMut::new`] instead of forcing everything through one
/// fixed pool layout.
///
/// # Example
/// ```no_run
/// use flowio::runtime::buffer::pool::{IoBuffPool, IoBuffPoolConfig};
///
/// let mut pool = IoBuffPool::new(IoBuffPoolConfig {
///     headroom: 16,
///     payload: 4096,
///     tailroom: 8,
///     objs_per_slab: 64,
/// })
/// .unwrap();
/// pool.init();
///
/// let mut buf = pool.alloc().unwrap();
/// buf.payload_append(b"fast path data").unwrap();
/// buf.headroom_prepend(b"HDR:").unwrap();
/// assert_eq!(buf.bytes(), b"HDR:fast path data");
/// // buf drops → slot returned to pool's free list (no heap dealloc)
/// ```
pub struct IoBuffPool {
    /// Stable heap-allocated pool state intentionally leaked from its `Box`
    /// and later reclaimed by `IoBuffPoolInner::destroy`.
    inner: NonNull<IoBuffPoolInner>,
}

/// Stable inner pool state referenced by pool-allocated buffer headers.
pub(crate) struct IoBuffPoolInner {
    /// Manually-dropped slab allocator that carves contiguous memory pages
    /// into fixed-size buffer slots and is destroyed with the leaked inner.
    /// Each slot holds one `IoBuffHeader` + data region.
    slab_factory: ManuallyDrop<SlabAllocator<'static, BasicMemoryProvider>>,
    /// Heap-allocated memory provider backing the slab allocator. Stored as a
    /// raw pointer so constructing the self-owning pool does not move a `Box`
    /// after `slab_factory` captures the provider address.
    provider: NonNull<BasicMemoryProvider>,
    /// Stable raw pointer to this inner allocation, stored in pool-backed
    /// buffer headers for final-slot release.
    pool_ptr: *mut IoBuffPoolInner,
    /// Intrusive free list of available buffer slots.  When a buffer is
    /// returned to the pool, its slot is pushed onto this list.  Alloc
    /// pops from here first (O(1)) before bump-allocating from a slab.
    free_list: SList<u8>,
    /// Move-safe chain of all allocated slab pages.
    slab_pages: SlabPageChain,
    /// Size of each slot in bytes: `sizeof(IoBuffHeader) + headroom +
    /// payload + tailroom`, rounded up for alignment.
    slot_size: usize,
    /// Headroom size for each buffer produced by this pool.
    headroom: usize,
    /// Payload size for each buffer produced by this pool.
    payload: usize,
    /// Tailroom size for each buffer produced by this pool.
    tailroom: usize,
    /// Whether `init()` has been called. `alloc()` returns
    /// [`IoBuffError::PoolNotInitialized`] until this becomes true.
    initialized: bool,
    /// Number of pool slots currently checked out to live buffers.
    live_slots: usize,
    /// Set when the public pool owner has been dropped. Outstanding buffers may
    /// still exist and will tear the pool down on final release.
    owner_dropped: bool,
}

impl IoBuffPoolInner {
    fn new(config: IoBuffPoolConfig) -> Result<Box<Self>, IoBuffPoolConfigError> {
        if config.objs_per_slab == 0 {
            return Err(IoBuffPoolConfigError::ObjsPerSlabZero);
        }

        let total_data = config
            .headroom
            .checked_add(config.payload)
            .and_then(|sum| sum.checked_add(config.tailroom))
            .ok_or(IoBuffPoolConfigError::LayoutOverflow)?;
        let slot_raw_size = std::mem::size_of::<IoBuffHeader>()
            .checked_add(total_data)
            .ok_or(IoBuffPoolConfigError::LayoutOverflow)?;
        // Ensure slots are at least large enough for the free-list link and
        // properly aligned.
        let slot_align = std::mem::align_of::<IoBuffHeader>();
        let link_size = std::mem::size_of::<SListLink>();
        let slot_min = std::cmp::max(slot_raw_size, link_size);
        let slot_size = crate::utils::size_up(slot_min, slot_align)
            .map_err(|_| IoBuffPoolConfigError::LayoutOverflow)?;

        let provider_ptr = Box::into_raw(Box::new(BasicMemoryProvider::new()));

        let slab_factory = match SlabAllocator::new_uninit(
            // SAFETY: provider_ptr comes from Box::into_raw and is not
            // reconstructed as a Box until after slab_factory is no longer
            // used. SlabAllocator stores only a raw pointer internally.
            unsafe { &mut *provider_ptr },
            slot_size,
            slot_align,
            config.objs_per_slab,
        ) {
            Ok(slab_factory) => ManuallyDrop::new(slab_factory),
            Err(_) => {
                unsafe { drop(Box::from_raw(provider_ptr)) };
                return Err(IoBuffPoolConfigError::LayoutOverflow);
            }
        };

        let provider = unsafe {
            // SAFETY: Box::into_raw never returns null.
            NonNull::new_unchecked(provider_ptr)
        };

        Ok(Box::new(Self {
            slab_factory,
            provider,
            pool_ptr: std::ptr::null_mut(),
            free_list: SList::new_uninit(),
            slab_pages: SlabPageChain::new(),
            slot_size,
            headroom: config.headroom,
            payload: config.payload,
            tailroom: config.tailroom,
            initialized: false,
            live_slots: 0,
            owner_dropped: false,
        }))
    }

    fn init(&mut self) {
        self.slab_factory.init();
        self.free_list.init();
        self.initialized = true;
    }

    unsafe fn alloc(pool_ptr: *mut Self) -> Result<IoBuffMut, IoBuffError> {
        debug_assert!(!pool_ptr.is_null());

        if unsafe { !(*pool_ptr).initialized } {
            return Err(IoBuffError::PoolNotInitialized);
        }

        let free_list = unsafe { std::ptr::addr_of_mut!((*pool_ptr).free_list) };
        let slot_ptr = if let Some(link_ptr) = unsafe { (*free_list).pop_front(0) } {
            link_ptr
        } else {
            let slot_size = unsafe { (*pool_ptr).slot_size };
            let slab_pages = unsafe { std::ptr::addr_of_mut!((*pool_ptr).slab_pages) };
            let mut ptr = unsafe { (*slab_pages).try_alloc_current(slot_size) };

            // Request new slab page if needed.
            if ptr.is_none() {
                #[cfg(debug_assertions)]
                if crate::runtime::test_hooks::take_iobuff_pool_slab_alloc_failure() {
                    return Err(IoBuffError::AllocFailed);
                }

                let slab_factory = unsafe { std::ptr::addr_of_mut!((*pool_ptr).slab_factory) };
                let new_ptr =
                    unsafe { (*slab_pages).alloc_from_new_slab(&mut *slab_factory, slot_size) }
                        .ok_or(IoBuffError::AllocFailed)?;
                ptr = Some(new_ptr);
            }

            ptr.ok_or(IoBuffError::AllocFailed)?
        };

        unsafe {
            (*pool_ptr).live_slots += 1;
        }

        // Initialize the header in the slot.
        let header = slot_ptr as *mut IoBuffHeader;
        unsafe {
            (*header).refcount = Cell::new(1);
            (*header).headroom_capacity = (*pool_ptr).headroom;
            (*header).payload_capacity = (*pool_ptr).payload;
            (*header).tailroom_capacity = (*pool_ptr).tailroom;
            (*header).pool = (*pool_ptr).pool_ptr;
        }

        Ok(IoBuffMut {
            header: unsafe { NonNull::new_unchecked(header) },
            offset: unsafe { (*pool_ptr).headroom },
            payload_len: 0,
            tailroom_len: 0,
        })
    }

    /// Returns a buffer slot to the pool, or destroys the pool if the owner
    /// has already been dropped and this was the last live slot.
    ///
    /// # Safety
    /// `pool_ptr` must be a valid pool inner allocation created by `new()`.
    /// `slot_ptr` must point to a slot originally allocated by that pool, and
    /// the buffer's refcount must already be zero.
    pub(crate) unsafe fn release_buffer(pool_ptr: *mut Self, slot_ptr: *mut u8) {
        debug_assert!(!pool_ptr.is_null());
        debug_assert!(!slot_ptr.is_null());

        let live_slots = unsafe { (*pool_ptr).live_slots };
        debug_assert!(live_slots > 0, "IoBuffPoolInner live_slots underflow");
        unsafe {
            (*pool_ptr).live_slots = live_slots - 1;
        }

        if unsafe { (*pool_ptr).owner_dropped } {
            if live_slots == 1 {
                unsafe { Self::destroy(pool_ptr) };
            }
            return;
        }

        let link_ptr = slot_ptr as *mut SListLink;
        let free_list = unsafe { std::ptr::addr_of_mut!((*pool_ptr).free_list) };
        unsafe { (*free_list).push_front_unchecked(link_ptr) };
    }

    /// Tears the pool down and frees every slab page.
    ///
    /// # Safety
    /// `pool_ptr` must be a valid pool inner allocation and there must be no
    /// outstanding live slots.
    unsafe fn destroy(pool_ptr: *mut Self) {
        debug_assert!(!pool_ptr.is_null());
        let mut inner = unsafe { Box::from_raw(pool_ptr) };
        debug_assert!(
            inner.live_slots == 0,
            "destroy called with {} live slots still outstanding",
            inner.live_slots
        );

        unsafe { inner.slab_pages.free_all(&mut inner.slab_factory) };

        unsafe { ManuallyDrop::drop(&mut inner.slab_factory) };
        unsafe { drop(Box::from_raw(inner.provider.as_ptr())) };
    }
}

impl IoBuffPool {
    /// Creates a new uninitialized buffer pool.  Must call [`init()`](Self::init)
    /// before allocating.
    ///
    /// This is a setup/control-plane API. Pools are typically created once
    /// and then reused for many allocations on the hot path.
    ///
    /// Returns an error if the configuration is invalid.
    pub fn new(config: IoBuffPoolConfig) -> Result<Self, IoBuffPoolConfigError> {
        let inner = IoBuffPoolInner::new(config)?;
        let inner_ptr = Box::into_raw(inner);
        unsafe {
            (*inner_ptr).pool_ptr = inner_ptr;
        }
        Ok(Self {
            inner: unsafe {
                // SAFETY: Box::into_raw never returns null.
                NonNull::new_unchecked(inner_ptr)
            },
        })
    }

    /// Initializes the pool's internal data structures.  Must be called once
    /// after the pool reaches its final memory location.
    ///
    /// This is also setup/control-plane work rather than part of steady-state
    /// allocation.
    pub fn init(&mut self) {
        unsafe { self.inner.as_mut() }.init();
    }

    /// Allocates a buffer from the pool.  O(1) when the free list is
    /// non-empty (pop from intrusive list).  If the free list is empty,
    /// bump-allocates from the current slab or requests a new slab page.
    ///
    /// Returns `IoBuffError::PoolNotInitialized` if [`init()`](Self::init)
    /// has not been called yet.
    ///
    /// Returns `IoBuffError::AllocFailed` if the memory provider cannot
    /// supply more slab pages.
    pub fn alloc(&mut self) -> Result<IoBuffMut, IoBuffError> {
        unsafe { IoBuffPoolInner::alloc(self.inner.as_ptr()) }
    }

    #[doc(hidden)]
    /// Test-only live-slot counter; not a stable public API.
    pub fn live_slots_for_test(&self) -> usize {
        unsafe { (*self.inner.as_ptr()).live_slots }
    }
}

impl Drop for IoBuffPool {
    fn drop(&mut self) {
        let inner_ptr = self.inner.as_ptr();
        unsafe {
            (*inner_ptr).owner_dropped = true;
        }
        if unsafe { (*inner_ptr).live_slots == 0 } {
            unsafe { IoBuffPoolInner::destroy(inner_ptr) };
        }
    }
}

#[cfg(all(test, debug_assertions))]
mod tests {
    use super::*;

    #[test]
    fn iobuff_pool_alloc_failed_seam_returns_alloc_failed_without_live_slot() {
        let mut pool = IoBuffPool::new(IoBuffPoolConfig {
            headroom: 4,
            payload: 16,
            tailroom: 4,
            objs_per_slab: 1,
        })
        .expect("pool config should be valid");
        pool.init();

        crate::runtime::test_hooks::fail_next_iobuff_pool_slab_alloc();
        let err = match pool.alloc() {
            Ok(_) => panic!("forced slab allocation failure unexpectedly succeeded"),
            Err(err) => err,
        };

        assert_eq!(err, IoBuffError::AllocFailed);
        assert_eq!(
            pool.live_slots_for_test(),
            0,
            "failed allocation must not check out a live slot"
        );

        let buf = pool
            .alloc()
            .expect("forced failure should not poison later allocation");
        assert_eq!(pool.live_slots_for_test(), 1);
        drop(buf);
        assert_eq!(pool.live_slots_for_test(), 0);
    }
}
