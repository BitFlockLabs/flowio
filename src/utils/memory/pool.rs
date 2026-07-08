//! Slab-backed object pool with intrusive free-list reuse.

use crate::utils;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::ops::{Deref, DerefMut};

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
    /// The caller must ensure the memory provider is valid and that
    /// the `InPlaceInit::init_at` implementation does not panic.
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
    /// The caller must ensure that `obj` is a valid pointer to a dynamically
    /// allocated object originally provided by this pool.
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

/// Owns a heap-allocated memory provider plus a pool that stores a raw pointer
/// to that provider.
///
/// The provider is kept as a raw `NonNull` instead of a moved `Box` so the pool
/// never holds a Stacked-Borrows-invalidating reference into an object that is
/// later moved. Drop order is pool first, then provider.
pub(crate) struct ProviderOwnedPool<T: InPlaceInit, P: super::provider::MemoryProvider + 'static> {
    provider: super::provider::ProviderOwner<P>,
    pool: ManuallyDrop<Pool<'static, T, P>>,
}

impl<T: InPlaceInit, P: super::provider::MemoryProvider + 'static> ProviderOwnedPool<T, P> {
    pub(crate) fn new(provider: P, objs_per_slab: usize) -> Result<Self, PoolConfigError> {
        let provider = super::provider::ProviderOwner::new(provider);
        let pool = match unsafe { Pool::new_uninit_from_raw(provider.as_ptr(), objs_per_slab) } {
            Ok(pool) => ManuallyDrop::new(pool),
            Err(err) => {
                return Err(err);
            }
        };
        Ok(Self { provider, pool })
    }

    #[cfg(debug_assertions)]
    #[inline(always)]
    pub(crate) fn provider_ref(&self) -> &P {
        self.provider.as_ref()
    }

    #[inline(always)]
    pub(crate) fn provider_mut(&mut self) -> &mut P {
        self.provider.as_mut()
    }
}

impl<T: InPlaceInit, P: super::provider::MemoryProvider + 'static> Deref
    for ProviderOwnedPool<T, P>
{
    type Target = Pool<'static, T, P>;

    fn deref(&self) -> &Self::Target {
        &self.pool
    }
}

impl<T: InPlaceInit, P: super::provider::MemoryProvider + 'static> DerefMut
    for ProviderOwnedPool<T, P>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.pool
    }
}

impl<T: InPlaceInit, P: super::provider::MemoryProvider + 'static> Drop
    for ProviderOwnedPool<T, P>
{
    fn drop(&mut self) {
        unsafe { ManuallyDrop::drop(&mut self.pool) };
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

        // Live objects are intentionally not dropped: executor teardown may
        // abandon task futures whose destructors require runtime TLS or pools
        // that are already being torn down.
        unsafe { self.slab_pages.free_all(&mut self.slab_factory) };
    }
}

#[cfg(test)]
mod tests {
    use super::{InPlaceInit, ProviderOwnedPool};
    use crate::utils::memory::provider::MemoryProvider;
    use std::cell::Cell;
    use std::mem::MaybeUninit;
    use std::rc::Rc;

    struct DropCountingProvider {
        drops: Rc<Cell<usize>>,
        storage: Box<[usize; 128]>,
        used: bool,
    }

    impl DropCountingProvider {
        fn new(drops: Rc<Cell<usize>>) -> Self {
            Self {
                drops,
                storage: Box::new([0; 128]),
                used: false,
            }
        }
    }

    impl Drop for DropCountingProvider {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    impl MemoryProvider for DropCountingProvider {
        fn init(&mut self, _required_align: usize) {
            self.used = false;
        }

        fn alignment_guarantee(&self) -> usize {
            std::mem::align_of::<usize>()
        }

        fn request_memory(&mut self, size: usize) -> Option<*mut u8> {
            let capacity = self.storage.len() * std::mem::size_of::<usize>();
            if self.used || size > capacity {
                return None;
            }
            self.used = true;
            Some(self.storage.as_mut_ptr().cast::<u8>())
        }

        unsafe fn free_memory(&mut self, _ptr: *mut u8, _size: usize) {}
    }

    struct TestSlot {
        #[allow(dead_code)]
        value: usize,
    }

    impl InPlaceInit for TestSlot {
        type Args = usize;

        fn init_at(slot: &mut MaybeUninit<Self>, value: Self::Args) {
            slot.write(Self { value });
        }
    }

    #[test]
    #[cfg(debug_assertions)]
    fn provider_owned_pool_drops_provider_when_pool_drop_panics() {
        let drops = Rc::new(Cell::new(0));
        let provider = DropCountingProvider::new(Rc::clone(&drops));
        let mut pool = ProviderOwnedPool::<TestSlot, _>::new(provider, 1)
            .expect("provider-owned pool construction failed");
        pool.init();

        let slot = unsafe { pool.alloc(7).expect("pool slot allocation failed") };
        unsafe {
            assert_eq!((*slot).value, 7);
        }

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(pool)));

        assert!(result.is_err(), "live slot should trigger pool drop panic");
        assert_eq!(drops.get(), 1, "provider must drop while unwinding");
    }

    #[test]
    fn provider_owned_pool_drops_provider_after_constructor_error() {
        let drops = Rc::new(Cell::new(0));
        let provider = DropCountingProvider::new(Rc::clone(&drops));

        let result = ProviderOwnedPool::<TestSlot, _>::new(provider, 0);

        assert!(matches!(
            result,
            Err(super::PoolConfigError::ObjsPerSlabZero)
        ));
        assert_eq!(
            drops.get(),
            1,
            "provider must drop when pool construction fails"
        );
    }
}
