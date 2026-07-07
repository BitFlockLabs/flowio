//! Doc-hidden retained-payload white-box test support.

use std::io;
use std::mem::MaybeUninit;

/// Debug counters for retained-pool white-box tests.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RetainedPayloadPoolStats {
    pub pooled_allocs: usize,
    pub pooled_reuses: usize,
    pub pooled_frees: usize,
    pub slab_allocs: usize,
    pub heap_fallbacks: usize,
    pub heap_frees: usize,
    pub writev_scratch_inline_allocs: usize,
    pub writev_scratch_pooled_allocs: usize,
    pub writev_scratch_pooled_reuses: usize,
    pub writev_scratch_pooled_frees: usize,
    pub writev_scratch_slab_allocs: usize,
    pub writev_scratch_oversize_rejections: usize,
    pub writev_scratch_alloc_failures: usize,
}

impl From<super::retained::RetainedPayloadPoolStats> for RetainedPayloadPoolStats {
    fn from(stats: super::retained::RetainedPayloadPoolStats) -> Self {
        Self {
            pooled_allocs: stats.pooled_allocs,
            pooled_reuses: stats.pooled_reuses,
            pooled_frees: stats.pooled_frees,
            slab_allocs: stats.slab_allocs,
            heap_fallbacks: stats.heap_fallbacks,
            heap_frees: stats.heap_frees,
            writev_scratch_inline_allocs: stats.writev_scratch_inline_allocs,
            writev_scratch_pooled_allocs: stats.writev_scratch_pooled_allocs,
            writev_scratch_pooled_reuses: stats.writev_scratch_pooled_reuses,
            writev_scratch_pooled_frees: stats.writev_scratch_pooled_frees,
            writev_scratch_slab_allocs: stats.writev_scratch_slab_allocs,
            writev_scratch_oversize_rejections: stats.writev_scratch_oversize_rejections,
            writev_scratch_alloc_failures: stats.writev_scratch_alloc_failures,
        }
    }
}

/// Retained payload pool wrapper for integration tests.
pub struct RetainedPayloadPool {
    inner: super::retained::RetainedPayloadPool,
}

impl RetainedPayloadPool {
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            inner: super::retained::RetainedPayloadPool::new()?,
        })
    }

    pub fn alloc<T: 'static>(&mut self, value: T) -> RetainedPayload<T> {
        RetainedPayload {
            inner: self.inner.alloc(value),
        }
    }

    pub fn alloc_iovec_scratch(&mut self, iov_count: usize) -> io::Result<RetainedIovecScratch> {
        Ok(RetainedIovecScratch {
            inner: self.inner.alloc_iovec_scratch(iov_count)?,
        })
    }

    pub fn stats(&self) -> RetainedPayloadPoolStats {
        self.inner.stats().into()
    }
}

/// Retained payload handle wrapper for integration tests.
#[must_use = "retained payload handles own storage and must be consumed"]
pub struct RetainedPayload<T: 'static> {
    inner: super::retained::RetainedPayload<T>,
}

impl<T: 'static> RetainedPayload<T> {
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
pub struct RetainedIovecScratch {
    inner: super::retained::RetainedIovecScratch,
}

impl RetainedIovecScratch {
    pub fn as_uninit_slice(&self) -> &[MaybeUninit<libc::iovec>] {
        self.inner.as_uninit_slice()
    }

    pub fn as_uninit_slice_mut(&mut self) -> &mut [MaybeUninit<libc::iovec>] {
        self.inner.as_uninit_slice_mut()
    }
}
