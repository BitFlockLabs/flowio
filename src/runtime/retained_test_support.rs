//! Doc-hidden retained-payload white-box test support.

use std::io;
use std::mem::MaybeUninit;

/// Debug counters for retained-pool white-box tests.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RetainedPayloadPoolStats {
    /// Payload allocations served by retained size-class storage.
    pub pooled_allocs: usize,
    /// Pooled payload allocations served from a returned free-list block.
    pub pooled_reuses: usize,
    /// Pooled payload blocks returned to their size-class free lists.
    pub pooled_frees: usize,
    /// Slab pages requested for retained payload size classes.
    pub slab_allocs: usize,
    /// Payload allocations that fell back to the global heap.
    pub heap_fallbacks: usize,
    /// Heap-fallback payload allocations released.
    pub heap_frees: usize,
    /// Iovec scratch requests served by inline metadata storage.
    pub writev_scratch_inline_allocs: usize,
    /// Iovec scratch requests served by pooled sidecar blocks.
    pub writev_scratch_pooled_allocs: usize,
    /// Sidecar requests served from returned free-list blocks.
    pub writev_scratch_pooled_reuses: usize,
    /// Sidecar blocks returned to their size-class free lists.
    pub writev_scratch_pooled_frees: usize,
    /// Slab pages requested for iovec sidecar size classes.
    pub writev_scratch_slab_allocs: usize,
    /// Scratch requests rejected for exceeding the supported iovec count.
    pub writev_scratch_oversize_rejections: usize,
    /// Scratch requests rejected because a sidecar block was unavailable.
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
    pub fn alloc_iovec_scratch(&mut self, iov_count: usize) -> io::Result<RetainedIovecScratch> {
        Ok(RetainedIovecScratch {
            inner: self.inner.alloc_iovec_scratch(iov_count)?,
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
    /// Production scratch handle whose allocation path is under test.
    inner: super::retained::RetainedIovecScratch,
}

impl RetainedIovecScratch {
    /// Returns the active scratch slots without assuming initialization.
    pub fn as_uninit_slice(&self) -> &[MaybeUninit<libc::iovec>] {
        self.inner.as_uninit_slice()
    }

    /// Returns the active scratch slots for test initialization.
    pub fn as_uninit_slice_mut(&mut self) -> &mut [MaybeUninit<libc::iovec>] {
        self.inner.as_uninit_slice_mut()
    }
}
