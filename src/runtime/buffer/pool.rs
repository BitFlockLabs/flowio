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

use super::IoBuffError;
use super::iobuff::{IoBuffHeader, IoBuffMut};
use crate::utils::list::intrusive::slist::{Link as SListLink, SList};
use crate::utils::memory::provider::BasicMemoryProvider;
use crate::utils::memory::slab::{self, SlabAllocator};
use std::cell::Cell;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;

/// Configuration for [`IoBuffPool`] buffer layout.
pub struct IoBuffPoolConfig
{
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoBuffPoolConfigError
{
    /// `objs_per_slab` must be greater than zero.
    ObjsPerSlabZero,
    /// Buffer slot layout overflowed addressable memory.
    LayoutOverflow,
}

impl std::fmt::Display for IoBuffPoolConfigError
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        match self
        {
            Self::ObjsPerSlabZero =>
            {
                f.write_str("IoBuffPoolConfig::objs_per_slab must be greater than zero")
            }
            Self::LayoutOverflow =>
            {
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
pub struct IoBuffPool
{
    /// Stable heap-allocated pool state shared with all outstanding buffers.
    inner: NonNull<IoBuffPoolInner>,
}

/// Stable inner pool state referenced by pool-allocated buffer headers.
pub(crate) struct IoBuffPoolInner
{
    /// Slab allocator that carves contiguous memory pages into fixed-size
    /// buffer slots.  Each slot holds one `IoBuffHeader` + data region.
    slab_factory: ManuallyDrop<SlabAllocator<'static, BasicMemoryProvider>>,
    /// Heap-allocated memory provider backing the slab allocator.  Boxed
    /// for a stable address that outlives the slab factory's reference.
    _provider: Box<BasicMemoryProvider>,
    /// Intrusive free list of available buffer slots.  When a buffer is
    /// returned to the pool, its slot is pushed onto this list.  Alloc
    /// pops from here first (O(1)) before bump-allocating from a slab.
    free_list: SList<u8>,
    /// Head of the singly-linked list of all allocated slab pages.
    /// Chained through each Slab header's link.next pointer.  No
    /// self-referential sentinel — the pool is safe to move.
    slab_page_head: *mut slab::Slab,
    /// The slab currently being bump-allocated from.  When exhausted,
    /// a new slab page is requested from the slab factory.
    current_slab: *mut slab::Slab,
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

impl IoBuffPoolInner
{
    fn new(config: IoBuffPoolConfig) -> Result<Box<Self>, IoBuffPoolConfigError>
    {
        if config.objs_per_slab == 0
        {
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

        let mut provider = Box::new(BasicMemoryProvider::new());
        let provider_ptr = &mut *provider as *mut BasicMemoryProvider;

        let slab_factory = ManuallyDrop::new(
            SlabAllocator::new_uninit(
                // SAFETY: provider_ptr is stable (Box) and outlives the slab factory.
                unsafe { &mut *provider_ptr },
                slot_size,
                slot_align,
                config.objs_per_slab,
            )
            .map_err(|_| IoBuffPoolConfigError::LayoutOverflow)?,
        );

        Ok(Box::new(Self {
            slab_factory,
            _provider: provider,
            free_list: SList::new_uninit(),
            slab_page_head: std::ptr::null_mut(),
            current_slab: std::ptr::null_mut(),
            slot_size,
            headroom: config.headroom,
            payload: config.payload,
            tailroom: config.tailroom,
            initialized: false,
            live_slots: 0,
            owner_dropped: false,
        }))
    }

    fn init(&mut self)
    {
        self.slab_factory.init();
        self.free_list.init();
        self.initialized = true;
    }

    fn alloc(&mut self) -> Result<IoBuffMut, IoBuffError>
    {
        if !self.initialized
        {
            return Err(IoBuffError::PoolNotInitialized);
        }

        let slot_ptr = if let Some(link_ptr) = unsafe { self.free_list.pop_front(0) }
        {
            link_ptr
        }
        else
        {
            // Try bump-alloc from current slab.
            let mut ptr = if !self.current_slab.is_null()
            {
                unsafe { (*self.current_slab).try_alloc(self.slot_size) }
            }
            else
            {
                None
            };

            // Request new slab page if needed.
            if ptr.is_none()
            {
                let slab_ptr = self
                    .slab_factory
                    .provide_slab()
                    .ok_or(IoBuffError::AllocFailed)?;
                // Link new slab into the singly-linked page list via
                // the Slab header's link.next pointer.
                unsafe {
                    (*slab_ptr).link.next =
                        self.slab_page_head as *mut crate::utils::list::intrusive::slist::Link;
                }
                self.slab_page_head = slab_ptr;
                self.current_slab = slab_ptr;
                ptr = unsafe { (*slab_ptr).try_alloc(self.slot_size) };
            }

            ptr.ok_or(IoBuffError::AllocFailed)?
        };

        self.live_slots += 1;

        // Initialize the header in the slot.
        let header = slot_ptr as *mut IoBuffHeader;
        unsafe {
            (*header).refcount = Cell::new(1);
            (*header).headroom_capacity = self.headroom;
            (*header).payload_capacity = self.payload;
            (*header).tailroom_capacity = self.tailroom;
            (*header).pool = self as *mut Self;
        }

        Ok(IoBuffMut {
            header: unsafe { NonNull::new_unchecked(header) },
            offset: self.headroom,
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
    pub(crate) unsafe fn release_buffer(pool_ptr: *mut Self, slot_ptr: *mut u8)
    {
        debug_assert!(!pool_ptr.is_null());
        debug_assert!(!slot_ptr.is_null());

        let pool = unsafe { &mut *pool_ptr };
        debug_assert!(pool.live_slots > 0, "IoBuffPoolInner live_slots underflow");
        pool.live_slots -= 1;

        if pool.owner_dropped
        {
            if pool.live_slots == 0
            {
                unsafe { Self::destroy(pool_ptr) };
            }
            return;
        }

        let link_ptr = slot_ptr as *mut SListLink;
        unsafe { pool.free_list.push_front_unchecked(link_ptr) };
    }

    /// Tears the pool down and frees every slab page.
    ///
    /// # Safety
    /// `pool_ptr` must be a valid pool inner allocation and there must be no
    /// outstanding live slots.
    unsafe fn destroy(pool_ptr: *mut Self)
    {
        debug_assert!(!pool_ptr.is_null());
        let mut inner = unsafe { Box::from_raw(pool_ptr) };
        debug_assert!(
            inner.live_slots == 0,
            "destroy called with {} live slots still outstanding",
            inner.live_slots
        );

        if inner.initialized
        {
            // Walk the singly-linked slab page list and free each page.
            let mut current = inner.slab_page_head;
            while !current.is_null()
            {
                let next = unsafe { (*current).link.next as *mut slab::Slab };
                unsafe { inner.slab_factory.free_slab(current as *mut u8) };
                current = next;
            }

            unsafe { ManuallyDrop::drop(&mut inner.slab_factory) };
        }
    }
}

impl IoBuffPool
{
    /// Creates a new uninitialized buffer pool.  Must call [`init()`](Self::init)
    /// before allocating.
    ///
    /// Returns an error if the configuration is invalid.
    pub fn new(config: IoBuffPoolConfig) -> Result<Self, IoBuffPoolConfigError>
    {
        let inner = IoBuffPoolInner::new(config)?;
        Ok(Self {
            inner: NonNull::from(Box::leak(inner)),
        })
    }

    /// Initializes the pool's internal data structures.  Must be called once
    /// after the pool reaches its final memory location.
    pub fn init(&mut self)
    {
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
    pub fn alloc(&mut self) -> Result<IoBuffMut, IoBuffError>
    {
        unsafe { self.inner.as_mut() }.alloc()
    }
}

impl Drop for IoBuffPool
{
    fn drop(&mut self)
    {
        let inner_ptr = self.inner.as_ptr();
        let inner = unsafe { &mut *inner_ptr };
        inner.owner_dropped = true;
        if inner.live_slots == 0
        {
            unsafe { IoBuffPoolInner::destroy(inner_ptr) };
        }
    }
}
