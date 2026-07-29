//! Provider-owning wrapper around the slab-backed object pool.

use super::pool::{InPlaceInit, Pool, PoolConfigError};
use super::provider::{MemoryProvider, ProviderOwner};
use std::mem::ManuallyDrop;
use std::ops::Deref;

/// Narrow provider controls that are safe while a dependent pool is live.
///
/// # Safety
///
/// Implementations must not move or replace the provider, change allocation
/// geometry, invalidate a live allocation, or otherwise break the paired
/// [`MemoryProvider`] relationship. Controls may update observability state or
/// future allocation-admission policy only.
pub(crate) unsafe trait ProviderOwnedPoolControl {
    /// Clears provider observability counters without changing allocations.
    fn reset_debug_counts(&mut self);

    /// Sets a test-only cap on future provider requests.
    #[cfg(all(test, not(miri)))]
    fn set_max_request_count(&mut self, max_request_count: Option<usize>);
}

/// Owns a heap-allocated memory provider plus a pool that stores a raw pointer
/// to that provider.
///
/// The provider has a stable heap address, while the wrapper itself remains
/// movable. Drop order is pool first, then provider.
pub(crate) struct ProviderOwnedPool<T: InPlaceInit, P: MemoryProvider + 'static> {
    /// Owns the provider allocation referenced by the pool's slab allocator.
    provider: ProviderOwner<P>,
    /// Pool dropped manually before `provider` to preserve pointer validity.
    pool: ManuallyDrop<Pool<'static, T, P>>,
}

impl<T: InPlaceInit, P: MemoryProvider + 'static> ProviderOwnedPool<T, P> {
    /// Creates a pool and its stable, internally owned memory provider.
    pub(crate) fn new(provider: P, objs_per_slab: usize) -> Result<Self, PoolConfigError> {
        let provider = ProviderOwner::new(provider);
        let pool = match unsafe { Pool::new_uninit_from_raw(provider.as_ptr(), objs_per_slab) } {
            Ok(pool) => ManuallyDrop::new(pool),
            Err(err) => {
                return Err(err);
            }
        };
        Ok(Self { provider, pool })
    }

    /// Initializes the dependent pool without exposing it for replacement.
    #[inline(always)]
    pub(crate) fn init(&mut self) {
        self.pool.init();
    }

    /// Allocates and initializes one pool slot.
    ///
    /// # Safety
    ///
    /// The caller must uphold [`Pool::alloc`]'s object-initialization contract.
    #[inline(always)]
    pub(crate) unsafe fn alloc(&mut self, args: T::Args) -> Option<*mut T> {
        unsafe { self.pool.alloc(args) }
    }

    /// Drops and returns one live object to this pool.
    ///
    /// # Safety
    ///
    /// `obj` must be null or a live slot returned by this exact pool and not
    /// already freed. While `T::drop` runs, it must not re-enter this pool
    /// through any alias.
    #[inline(always)]
    pub(crate) unsafe fn free(&mut self, obj: *mut T) {
        unsafe { self.pool.free(obj) };
    }

    #[cfg(debug_assertions)]
    #[inline(always)]
    /// Borrows the backing provider for debug-only state inspection.
    pub(crate) fn provider_ref(&self) -> &P {
        self.provider.as_ref()
    }

    /// Resets provider counters through a relation-preserving narrow control.
    #[inline(always)]
    pub(crate) fn reset_provider_debug_counts(&mut self)
    where
        P: ProviderOwnedPoolControl,
    {
        self.provider.as_mut().reset_debug_counts();
    }

    /// Sets the test-only provider request limit without exposing the provider.
    #[cfg(all(test, not(miri)))]
    #[inline(always)]
    pub(crate) fn set_provider_max_request_count(&mut self, max_request_count: Option<usize>)
    where
        P: ProviderOwnedPoolControl,
    {
        self.provider
            .as_mut()
            .set_max_request_count(max_request_count);
    }
}

impl<T: InPlaceInit, P: MemoryProvider + 'static> Deref for ProviderOwnedPool<T, P> {
    type Target = Pool<'static, T, P>;

    fn deref(&self) -> &Self::Target {
        &self.pool
    }
}

impl<T: InPlaceInit, P: MemoryProvider + 'static> Drop for ProviderOwnedPool<T, P> {
    fn drop(&mut self) {
        // SAFETY: `pool` is initialized in `new`, has not been dropped, and
        // must release all provider-backed slabs before field drop reaches
        // `provider`.
        unsafe { ManuallyDrop::drop(&mut self.pool) };
    }
}

#[cfg(test)]
mod tests {
    use super::{InPlaceInit, ProviderOwnedPool};
    use crate::utils::memory::pool::PoolConfigError;
    use crate::utils::memory::provider::{BasicMemoryProvider, MemoryProvider};
    use std::cell::{Cell, RefCell};
    use std::mem::MaybeUninit;
    use std::rc::Rc;

    trait AmbiguousIfDerefMut<A> {
        fn marker() {}
    }

    impl<T: ?Sized> AmbiguousIfDerefMut<()> for T {}
    impl<T: ?Sized + std::ops::DerefMut> AmbiguousIfDerefMut<u8> for T {}

    struct DropCountingProvider {
        drops: Rc<Cell<usize>>,
        events: Rc<RefCell<Vec<&'static str>>>,
        provider: BasicMemoryProvider,
        live: Option<(*mut u8, usize)>,
    }

    impl DropCountingProvider {
        fn new(drops: Rc<Cell<usize>>) -> Self {
            Self::with_events(drops, Rc::new(RefCell::new(Vec::new())))
        }

        fn with_events(drops: Rc<Cell<usize>>, events: Rc<RefCell<Vec<&'static str>>>) -> Self {
            Self {
                drops,
                events,
                provider: BasicMemoryProvider::new(),
                live: None,
            }
        }
    }

    impl Drop for DropCountingProvider {
        fn drop(&mut self) {
            if let Some((ptr, size)) = self.live.take() {
                // SAFETY: `live` records the sole successful request that was
                // not returned before unwinding reached provider drop.
                unsafe { self.provider.free_memory(ptr, size) };
            }
            self.events.borrow_mut().push("provider_drop");
            self.drops.set(self.drops.get() + 1);
        }
    }

    // SAFETY: this fixture delegates every provider operation to the audited
    // BasicMemoryProvider without changing pointers, sizes, or lifetimes. It
    // permits at most one live allocation, records the exact pair, and reclaims
    // that pair if the pool's intentional drop panic bypasses normal teardown.
    unsafe impl MemoryProvider for DropCountingProvider {
        fn init(&mut self, required_align: usize) {
            self.provider.init(required_align);
        }

        fn alignment_guarantee(&self) -> usize {
            self.provider.alignment_guarantee()
        }

        fn request_memory(&mut self, size: usize) -> Option<*mut u8> {
            if self.live.is_some() {
                return None;
            }
            let ptr = self.provider.request_memory(size)?;
            self.live = Some((ptr, size));
            self.events.borrow_mut().push("request_memory");
            Some(ptr)
        }

        unsafe fn free_memory(&mut self, ptr: *mut u8, size: usize) {
            let Some((live_ptr, live_size)) = self.live.take() else {
                debug_assert!(false, "test provider freed without a live allocation");
                return;
            };
            debug_assert_eq!(live_ptr, ptr);
            debug_assert_eq!(live_size, size);
            unsafe { self.provider.free_memory(ptr, size) };
            self.events.borrow_mut().push("free_memory");
        }
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

    struct DropTrackedSlot {
        value: usize,
        drops: Rc<Cell<usize>>,
    }

    impl InPlaceInit for DropTrackedSlot {
        type Args = (usize, Rc<Cell<usize>>);

        fn init_at(slot: &mut MaybeUninit<Self>, (value, drops): Self::Args) {
            slot.write(Self { value, drops });
        }
    }

    impl Drop for DropTrackedSlot {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    struct DropBombPanic;

    struct DropBombSlot {
        value: usize,
        panic_on_drop: bool,
        drops: Rc<Cell<usize>>,
    }

    impl InPlaceInit for DropBombSlot {
        type Args = (usize, bool, Rc<Cell<usize>>);

        fn init_at(slot: &mut MaybeUninit<Self>, (value, panic_on_drop, drops): Self::Args) {
            slot.write(Self {
                value,
                panic_on_drop,
                drops,
            });
        }
    }

    impl Drop for DropBombSlot {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
            if self.panic_on_drop {
                std::panic::panic_any(DropBombPanic);
            }
        }
    }

    #[test]
    fn provider_owned_pool_has_no_broad_mutable_deref() {
        let _ =
            <ProviderOwnedPool<TestSlot, BasicMemoryProvider> as AmbiguousIfDerefMut<_>>::marker;
    }

    #[test]
    fn provider_owned_pool_allocates_frees_and_reuses_through_narrow_api() {
        let mut pool = ProviderOwnedPool::<TestSlot, _>::new(BasicMemoryProvider::new(), 1)
            .expect("provider-owned pool construction failed");
        pool.init();

        let first = unsafe { pool.alloc(7).expect("first pool slot allocation failed") };
        unsafe {
            assert_eq!((*first).value, 7);
            pool.free(first);
        }

        let reused = unsafe { pool.alloc(11).expect("reused pool slot allocation failed") };
        assert_eq!(reused, first);
        unsafe {
            assert_eq!((*reused).value, 11);
            pool.free(reused);
        }
    }

    #[test]
    fn provider_owned_pool_keeps_relation_across_move_and_drops_pool_first() {
        let provider_drops = Rc::new(Cell::new(0));
        let slot_drops = Rc::new(Cell::new(0));
        let events = Rc::new(RefCell::new(Vec::new()));
        let provider =
            DropCountingProvider::with_events(Rc::clone(&provider_drops), Rc::clone(&events));
        let pool = ProviderOwnedPool::<DropTrackedSlot, _>::new(provider, 2)
            .expect("provider-owned pool construction failed");
        let mut moved_pool = pool;
        moved_pool.init();

        let first = unsafe {
            moved_pool
                .alloc((7, Rc::clone(&slot_drops)))
                .expect("first pool slot allocation failed")
        };
        let second = unsafe {
            moved_pool
                .alloc((11, Rc::clone(&slot_drops)))
                .expect("second pool slot allocation failed")
        };
        assert_eq!(events.borrow().as_slice(), &["request_memory"]);

        unsafe { moved_pool.free(first) };
        assert_eq!(slot_drops.get(), 1);
        assert_eq!(events.borrow().as_slice(), &["request_memory"]);

        let reused = unsafe {
            moved_pool
                .alloc((13, Rc::clone(&slot_drops)))
                .expect("freed pool slot should be reused")
        };
        assert_eq!(reused, first);
        unsafe {
            assert_eq!((*reused).value, 13);
            moved_pool.free(second);
            moved_pool.free(reused);
        }
        assert_eq!(slot_drops.get(), 3);

        drop(moved_pool);
        assert_eq!(provider_drops.get(), 1);
        assert_eq!(
            events.borrow().as_slice(),
            &["request_memory", "free_memory", "provider_drop"]
        );
    }

    #[test]
    fn provider_owned_pool_recovers_slot_when_value_drop_panics() {
        let provider_drops = Rc::new(Cell::new(0));
        let slot_drops = Rc::new(Cell::new(0));
        let events = Rc::new(RefCell::new(Vec::new()));
        let provider =
            DropCountingProvider::with_events(Rc::clone(&provider_drops), Rc::clone(&events));
        let mut pool = ProviderOwnedPool::<DropBombSlot, _>::new(provider, 1)
            .expect("provider-owned pool construction failed");
        pool.init();

        let first = unsafe {
            pool.alloc((7, true, Rc::clone(&slot_drops)))
                .expect("first pool slot allocation failed")
        };
        assert_eq!(events.borrow().as_slice(), &["request_memory"]);

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
            pool.free(first);
        }))
        .expect_err("drop bomb must unwind through Pool::free");
        assert!(
            panic.is::<DropBombPanic>(),
            "Pool::free must preserve the original destructor panic"
        );
        assert_eq!(slot_drops.get(), 1, "panicking value must drop once");
        assert_eq!(
            events.borrow().as_slice(),
            &["request_memory"],
            "slot recycling must retain the live slab"
        );

        let reused = unsafe {
            pool.alloc((11, false, Rc::clone(&slot_drops)))
                .expect("panicking slot must remain reusable")
        };
        assert_eq!(reused, first, "free list must return the same raw slot");
        unsafe {
            assert_eq!((*reused).value, 11);
            pool.free(reused);
        }
        assert_eq!(slot_drops.get(), 2, "reused value must drop exactly once");
        assert_eq!(
            events.borrow().as_slice(),
            &["request_memory"],
            "slot reuse must not request a second slab"
        );

        let teardown = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(pool)));
        assert!(
            teardown.is_ok(),
            "balanced live-slot accounting must permit clean pool teardown"
        );
        assert_eq!(provider_drops.get(), 1);
        assert_eq!(
            events.borrow().as_slice(),
            &["request_memory", "free_memory", "provider_drop"],
            "pool teardown must return its sole slab before provider teardown"
        );
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

        assert!(matches!(result, Err(PoolConfigError::ObjsPerSlabZero)));
        assert_eq!(
            drops.get(),
            1,
            "provider must drop when pool construction fails"
        );
    }
}
