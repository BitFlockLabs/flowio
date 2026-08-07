//! Doc-hidden retained-payload white-box test support.

use std::io;
use std::marker::PhantomData;
use std::mem::MaybeUninit;

macro_rules! define_retained_payload_pool_stats {
    ($($field:ident => $runtime_field:ident: $doc:literal;)*) => {
        /// Debug counters for retained-pool white-box tests.
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
        pub struct RetainedPayloadPoolStats {
            $(
                #[doc = $doc]
                pub $field: usize,
            )*
        }
    };
}

super::retained::retained_payload_stat_fields!(define_retained_payload_pool_stats);

macro_rules! impl_retained_payload_pool_stats_conversion {
    ($($field:ident => $runtime_field:ident: $doc:literal;)*) => {
        impl From<super::retained::RetainedPayloadPoolStats> for RetainedPayloadPoolStats {
            fn from(stats: super::retained::RetainedPayloadPoolStats) -> Self {
                Self {
                    $($field: stats.$field,)*
                }
            }
        }
    };
}

super::retained::retained_payload_stat_fields!(impl_retained_payload_pool_stats_conversion);

/// Retained payload pool wrapper for integration tests.
pub struct RetainedPayloadPool {
    /// Production retained pool exercised through this white-box wrapper.
    inner: super::retained::RetainedPayloadPool,
}

impl RetainedPayloadPool {
    /// Creates an empty retained-payload pool for an integration test.
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            inner: super::retained::RetainedPayloadPool::new()?,
        })
    }

    /// Stores a value in the production retained-payload pool.
    pub fn alloc<T: 'static>(&mut self, value: T) -> RetainedPayload<T> {
        RetainedPayload {
            inner: self.inner.alloc(value),
        }
    }

    /// Allocates production iovec scratch for `iov_count` active entries.
    ///
    /// The safe test-support lease keeps this pool mutably borrowed until the
    /// scratch is dropped. Production scratch independently retains its
    /// heap-stable sidecar owner; this additional lifetime prevents white-box
    /// tests from expressing an invalid parent/lease order.
    ///
    /// ```
    /// use flowio::test_support::runtime::retained_test_support::{
    ///     RetainedIovecScratch, RetainedPayloadPool,
    /// };
    ///
    /// let mut pool = RetainedPayloadPool::new()?;
    /// let scratch: RetainedIovecScratch<'_> = pool.alloc_iovec_scratch(512)?;
    /// assert_eq!(scratch.as_uninit_slice().len(), 512);
    /// drop(scratch);
    /// drop(pool);
    /// # Ok::<(), std::io::Error>(())
    /// ```
    ///
    /// A lease cannot escape the pool that created it:
    ///
    /// ```compile_fail
    /// use flowio::test_support::runtime::retained_test_support::{
    ///     RetainedIovecScratch, RetainedPayloadPool,
    /// };
    ///
    /// fn escaped() -> RetainedIovecScratch<'static> {
    ///     let mut pool = RetainedPayloadPool::new().unwrap();
    ///     pool.alloc_iovec_scratch(512).unwrap()
    /// }
    /// ```
    ///
    /// The parent cannot move while its lease remains live:
    ///
    /// ```compile_fail
    /// use flowio::test_support::runtime::retained_test_support::RetainedPayloadPool;
    ///
    /// let mut pool = RetainedPayloadPool::new().unwrap();
    /// let scratch = pool.alloc_iovec_scratch(512).unwrap();
    /// let moved_pool = pool;
    /// drop(scratch);
    /// drop(moved_pool);
    /// ```
    ///
    /// The parent also cannot be dropped before its lease:
    ///
    /// ```compile_fail
    /// use flowio::test_support::runtime::retained_test_support::RetainedPayloadPool;
    ///
    /// let mut pool = RetainedPayloadPool::new().unwrap();
    /// let scratch = pool.alloc_iovec_scratch(512).unwrap();
    /// drop(pool);
    /// drop(scratch);
    /// ```
    pub fn alloc_iovec_scratch(
        &mut self,
        iov_count: usize,
    ) -> io::Result<RetainedIovecScratch<'_>> {
        Ok(RetainedIovecScratch {
            inner: self.inner.alloc_iovec_scratch(iov_count)?,
            _pool: PhantomData,
        })
    }

    /// Returns a snapshot of the pool's debug counters.
    pub fn stats(&self) -> RetainedPayloadPoolStats {
        self.inner.stats().into()
    }
}

/// Retained payload handle wrapper for integration tests.
#[must_use = "retained payload handles own storage and must be consumed"]
pub struct RetainedPayload<T: 'static> {
    /// Production retained handle whose lifetime this wrapper preserves.
    inner: super::retained::RetainedPayload<T>,
}

impl<T: 'static> RetainedPayload<T> {
    /// Returns the stable address of the retained value.
    pub fn as_ptr(&self) -> *mut T {
        self.inner.as_ptr()
    }

    /// Returns a shared reference to the retained payload.
    ///
    /// # Safety
    /// The caller must ensure the payload has not been taken or freed.
    pub unsafe fn as_ref(&self) -> &T {
        unsafe { self.inner.as_ref() }
    }

    /// Moves the payload value out and releases only backing storage.
    ///
    /// # Safety
    /// `pool` must be the same retained pool that created this handle.
    pub unsafe fn take(self, pool: &mut RetainedPayloadPool) -> T {
        unsafe { self.inner.take(&mut pool.inner) }
    }

    /// Extracts selected data from the retained payload in place.
    ///
    /// # Safety
    /// `pool` must be the same retained pool that created this handle.
    /// `extract` must move or drop every initialized field that requires
    /// destruction before returning.
    /// If `extract` unwinds, the backing allocation is intentionally leaked
    /// because its remaining initialized fields cannot be determined safely.
    pub unsafe fn take_with<R>(
        self,
        pool: &mut RetainedPayloadPool,
        extract: impl FnOnce(*mut T) -> R,
    ) -> R {
        unsafe { self.inner.take_with(&mut pool.inner, extract) }
    }

    /// Drops the payload value and releases backing storage.
    ///
    /// # Safety
    /// `pool` must be the same retained pool that created this handle.
    pub unsafe fn drop_and_free(self, pool: &mut RetainedPayloadPool) {
        unsafe { self.inner.drop_and_free(&mut pool.inner) };
    }
}

/// Retained iovec scratch wrapper for integration tests.
pub struct RetainedIovecScratch<'pool> {
    /// Production scratch handle whose allocation path is under test.
    inner: super::retained::RetainedIovecScratch,
    /// Prevents the safe wrapper from outliving or moving its parent pool.
    _pool: PhantomData<&'pool mut RetainedPayloadPool>,
}

impl RetainedIovecScratch<'_> {
    /// Returns the active scratch slots without assuming initialization.
    pub fn as_uninit_slice(&self) -> &[MaybeUninit<libc::iovec>] {
        self.inner.as_uninit_slice()
    }

    /// Returns the active scratch slots for test initialization.
    pub fn as_uninit_slice_mut(&mut self) -> &mut [MaybeUninit<libc::iovec>] {
        self.inner.as_uninit_slice_mut()
    }
}
