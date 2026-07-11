//! Slab-backed object pool with intrusive free-list reuse.

use crate::utils;
use std::mem::MaybeUninit;

/// Configuration error returned while constructing a slab-backed pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolConfigError {
    /// `objs_per_slab` must be greater than zero.
    ObjsPerSlabZero,
    /// Pool slot geometry overflowed addressable memory.
    SizeOverflow,
}

impl std::fmt::Display for PoolConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ObjsPerSlabZero => f.write_str("Pool::new_uninit requires objs_per_slab > 0"),
            Self::SizeOverflow => f.write_str("pool slot geometry overflowed usize"),
        }
    }
}

impl std::error::Error for PoolConfigError {}

/// Initializes an object directly inside pre-allocated memory.
pub trait InPlaceInit: Sized {
    /// Arguments consumed while constructing one slot value.
    type Args;
    /// Writes a fully initialized value into `slot`.
    fn init_at(slot: &mut MaybeUninit<Self>, args: Self::Args);
}

impl<const N: usize> InPlaceInit for [u8; N] {
    type Args = ();

    fn init_at(slot: &mut MaybeUninit<Self>, _args: Self::Args) {
        // Zero-fill byte arrays so newly allocated buffers start in a defined state.
        unsafe {
            std::ptr::write_bytes(slot.as_mut_ptr() as *mut u8, 0, N);
        }
    }
}

/// A memory pool that manages objects of type `T`.
///
/// It allocates large chunks of memory (slabs) from the `MemoryProvider`
/// and splits them into fixed-size slots. Freed slots are kept in an
/// intrusive singly linked free list for allocation-free reuse.
///
/// Slab pages are tracked via a singly-linked list through each Slab header's
/// `link.next` pointer.  This avoids the DList sentinel's self-referential
/// pointers, making the pool safe to move after initialization.
pub struct Pool<'a, T: InPlaceInit, P: super::provider::MemoryProvider> {
    /// Slab allocator responsible for requesting and formatting new pages.
    slab_factory: super::slab::SlabAllocator<'a, P>,
    /// Free-list of returned object slots ready for reuse.
    free_list: utils::list::intrusive::slist::SList<T>,
    /// Move-safe chain of allocated slab pages.
    slab_pages: super::slab::SlabPageChain,
    /// Size of each object slot after alignment and link accommodation.
    obj_size: usize,
    #[cfg(debug_assertions)]
    /// Number of slots allocated from this pool that have not been freed.
    live_slots: usize,
}

impl<'a, T: InPlaceInit, P: super::provider::MemoryProvider> Pool<'a, T, P> {
    /// Creates an uninitialized pool.
    ///
    /// Call [`Pool::init`] before the first allocation.
    ///
    /// # Errors
    /// Returns [`PoolConfigError::ObjsPerSlabZero`] when `objs_per_slab` is
    /// zero. Returns [`PoolConfigError::SizeOverflow`] if object slot or slab
    /// geometry overflows addressable memory.
    #[cfg(any(test, feature = "test-support"))]
    pub fn new_uninit(provider: &'a mut P, objs_per_slab: usize) -> Result<Self, PoolConfigError> {
        let provider = provider as *mut P;
        unsafe { Self::new_uninit_from_raw(provider, objs_per_slab) }
    }

    /// Creates an uninitialized pool from a raw provider pointer.
    ///
    /// # Safety
    /// `provider` must be non-null, valid for unique mutable access for `'a`,
    /// and must outlive the returned pool. The caller must ensure no other
    /// mutable access aliases the provider while this pool is used.
    pub(crate) unsafe fn new_uninit_from_raw(
        provider: *mut P,
        objs_per_slab: usize,
    ) -> Result<Self, PoolConfigError> {
        if objs_per_slab == 0 {
            return Err(PoolConfigError::ObjsPerSlabZero);
        }

        let raw_size = std::mem::size_of::<T>();
        let align = std::cmp::max(std::mem::align_of::<T>(), super::slab::SLAB_LINK_ALIGN);
        let base_size = std::cmp::max(raw_size, super::slab::SLAB_LINK_SIZE);
        let obj_size =
            crate::utils::size_up(base_size, align).map_err(|_| PoolConfigError::SizeOverflow)?;

        // Slab geometry is fixed up front and then reused across all slab
        // allocations from this pool.
        let slab_factory = unsafe {
            super::slab::SlabAllocator::new_uninit_from_raw(
                provider,
                obj_size,
                align,
                objs_per_slab,
            )
        }
        .map_err(|err| match err {
            super::slab::SlabAllocatorConfigError::ObjsPerSlabZero => {
                PoolConfigError::ObjsPerSlabZero
            }
            super::slab::SlabAllocatorConfigError::InvalidObjectAlign
            | super::slab::SlabAllocatorConfigError::SizeOverflow => PoolConfigError::SizeOverflow,
        })?;

        Ok(Self {
            slab_factory,
            free_list: utils::list::intrusive::slist::SList::new_uninit(),
            slab_pages: super::slab::SlabPageChain::new(),
            obj_size,
            #[cfg(debug_assertions)]
            live_slots: 0,
        })
    }

    /// Initializes the slab allocator and intrusive free list.
    pub fn init(&mut self) {
        self.slab_factory.init();
        self.free_list.init();
    }

    /// Allocates and initializes one object slot.
    ///
    /// Reuses a freed slot when available, otherwise bump-allocates from the
    /// current slab or requests one new slab page from the memory provider.
    /// Returns `None` if the provider cannot supply a required slab page.
    ///
    /// # Safety
    ///
    /// The raw provider captured by this pool must remain valid and uniquely
    /// accessible, and `InPlaceInit::init_at` must initialize the complete
    /// value without unwinding.
    pub unsafe fn alloc(&mut self, args: T::Args) -> Option<*mut T> {
        let raw_ptr = if let Some(link_ptr) = unsafe { self.free_list.pop_front() } {
            link_ptr
        } else {
            unsafe {
                self.slab_pages
                    .alloc_or_grow(&mut self.slab_factory, self.obj_size)?
                    .ptr as *mut T
            }
        };

        unsafe {
            let slot = &mut *(raw_ptr as *mut MaybeUninit<T>);
            T::init_at(slot, args)
        };

        #[cfg(debug_assertions)]
        {
            self.live_slots = self
                .live_slots
                .checked_add(1)
                .expect("Pool live-slot count overflowed");
        }

        Some(raw_ptr)
    }

    /// Drops one live object and returns its slot to the pool free list.
    ///
    /// Null pointers are ignored.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `obj` is a live object slot returned by this
    /// pool and has not already been freed.
    pub unsafe fn free(&mut self, obj: *mut T) {
        if obj.is_null() {
            return;
        }

        unsafe {
            std::ptr::drop_in_place(obj);

            #[cfg(debug_assertions)]
            {
                debug_assert!(
                    self.live_slots > 0,
                    "Pool freed more slots than it allocated"
                );
                self.live_slots = self.live_slots.saturating_sub(1);
            }

            let link_ptr = obj as *mut utils::list::intrusive::slist::Link;
            self.free_list.push_front_unchecked(link_ptr);
        }
    }

    /// Marks all currently live slots as intentionally abandoned for owner
    /// teardown.
    ///
    /// This is debug-only and reserved for owners that deliberately discard
    /// pooled object storage without running object destructors during a
    /// terminal shutdown path.
    #[cfg(debug_assertions)]
    pub(crate) fn abandon_live_slots_for_drop(&mut self) {
        self.live_slots = 0;
    }
}

impl<'a, T: InPlaceInit, P: super::provider::MemoryProvider> Drop for Pool<'a, T, P> {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        if !std::thread::panicking() {
            debug_assert_eq!(
                self.live_slots, 0,
                "Pool dropped with {} live slots still outstanding",
                self.live_slots
            );
        }

        // Pool teardown returns whole slabs and cannot discover live objects
        // from the free list. Callers must free live values first; any values
        // left in release builds are abandoned without running their drops.
        unsafe { self.slab_pages.free_all(&mut self.slab_factory) };
    }
}
