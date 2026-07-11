//! Provider-owning wrapper around the slab-backed object pool.

use super::pool::{InPlaceInit, Pool, PoolConfigError};
use super::provider::{MemoryProvider, ProviderOwner};
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};

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

    #[cfg(debug_assertions)]
    #[inline(always)]
    /// Borrows the backing provider for debug-only state inspection.
    pub(crate) fn provider_ref(&self) -> &P {
        self.provider.as_ref()
    }

    #[inline(always)]
    /// Mutably borrows the backing provider while the wrapper is exclusive.
    pub(crate) fn provider_mut(&mut self) -> &mut P {
        self.provider.as_mut()
    }
}

impl<T: InPlaceInit, P: MemoryProvider + 'static> Deref for ProviderOwnedPool<T, P> {
    type Target = Pool<'static, T, P>;

    fn deref(&self) -> &Self::Target {
        &self.pool
    }
}

impl<T: InPlaceInit, P: MemoryProvider + 'static> DerefMut for ProviderOwnedPool<T, P> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.pool
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

        assert!(matches!(result, Err(PoolConfigError::ObjsPerSlabZero)));
        assert_eq!(
            drops.get(),
            1,
            "provider must drop when pool construction fails"
        );
    }
}
