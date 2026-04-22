//! Smart-pointer wrappers around the slab-backed pool so callers can work with
//! checked-out objects using ownership instead of raw pointers.

use super::pool::{InPlaceInit, Pool, PoolConfigError};
use super::provider::MemoryProvider;
use std::ops::{Deref, DerefMut};

/// Smart pointer for one slot checked out from an [`OwnedBufferPool`].
///
/// Dropping it returns the slot to the originating pool.
pub struct OwnedBuffer<'a, T: InPlaceInit, P: MemoryProvider> {
    /// Pointer to the pooled object storage.
    ptr: *mut T,
    /// Owning pool that will reclaim `ptr` on drop.
    pool: *mut OwnedBufferPool<'a, T, P>,
}

// Safety: OwnedBuffer is Send/Sync if the underlying data is.
unsafe impl<'a, T: InPlaceInit + Send, P: MemoryProvider> Send for OwnedBuffer<'a, T, P> {}
unsafe impl<'a, T: InPlaceInit + Sync, P: MemoryProvider> Sync for OwnedBuffer<'a, T, P> {}

impl<'a, T: InPlaceInit, P: MemoryProvider> OwnedBuffer<'a, T, P> {
    /// Consumes the smart pointer and returns the raw slot pointer without
    /// triggering `Drop`.
    ///
    /// The caller becomes responsible for returning the slot to the pool or
    /// reconstructing an [`OwnedBuffer`] later.
    pub fn into_raw(self) -> *mut T {
        let ptr = self.ptr;
        std::mem::forget(self);
        ptr
    }

    /// Consumes the smart pointer and returns both the raw slot pointer and
    /// the originating pool pointer.
    ///
    /// The caller becomes responsible for reconstructing the smart pointer or
    /// returning the slot correctly.
    pub fn into_raw_parts(self) -> (*mut T, *mut OwnedBufferPool<'a, T, P>) {
        let ptr = self.ptr;
        let pool = self.pool;
        std::mem::forget(self);
        (ptr, pool)
    }

    /// Reconstructs an [`OwnedBuffer`] from a raw pointer and pool pointer.
    ///
    /// This is the inverse of [`OwnedBuffer::into_raw_parts`].
    ///
    /// # Safety
    /// The pointer must have been previously obtained from the same pool and
    /// must still represent a live checked-out slot.
    pub unsafe fn from_raw(ptr: *mut T, pool: *mut OwnedBufferPool<'a, T, P>) -> Self {
        Self { ptr, pool }
    }
}

impl<'a, T: InPlaceInit, P: MemoryProvider> Deref for OwnedBuffer<'a, T, P> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.ptr }
    }
}

impl<'a, T: InPlaceInit, P: MemoryProvider> DerefMut for OwnedBuffer<'a, T, P> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.ptr }
    }
}

impl<'a, T: InPlaceInit, P: MemoryProvider> Drop for OwnedBuffer<'a, T, P> {
    fn drop(&mut self) {
        unsafe {
            // Return the slot to the originating pool when the smart pointer
            // goes out of scope.
            (*self.pool).inner.free(self.ptr);
        }
    }
}

/// Pool wrapper that returns [`OwnedBuffer`] smart pointers instead of raw slots.
pub struct OwnedBufferPool<'a, T: InPlaceInit, P: MemoryProvider> {
    /// Underlying slab-backed pool.
    inner: Pool<'a, T, P>,
}

impl<'a, T: InPlaceInit, P: MemoryProvider> OwnedBufferPool<'a, T, P> {
    /// Creates a new uninitialized pool.
    pub fn new_uninit(provider: &'a mut P, objs_per_slab: usize) -> Result<Self, PoolConfigError> {
        Ok(Self {
            inner: Pool::new_uninit(provider, objs_per_slab)?,
        })
    }

    /// Initializes the pool's intrusive linked lists.
    /// Must be called after moving the pool to its final memory location.
    pub fn init(&mut self) {
        self.inner.init();
    }

    /// Allocates an object and returns it wrapped in an [`OwnedBuffer`] smart
    /// pointer.
    pub fn alloc(&mut self, args: T::Args) -> Option<OwnedBuffer<'a, T, P>> {
        let ptr = unsafe { self.inner.alloc(args)? };
        Some(OwnedBuffer {
            ptr,
            pool: self as *mut Self,
        })
    }

    /// Directly frees a raw pointer, bypassing the smart-pointer wrapper.
    ///
    /// # Safety
    /// The pointer must belong to this pool.
    pub unsafe fn free_raw(&mut self, ptr: *mut T) {
        unsafe {
            self.inner.free(ptr);
        }
    }
}
