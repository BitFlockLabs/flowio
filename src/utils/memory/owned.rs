//! Smart-pointer wrappers around the slab-backed pool so callers can work with
//! checked-out objects using ownership instead of raw pointers.
//!
//! This is reserved low-level infrastructure for allocator-backed runtime
//! storage. Normal FlowIO buffer users should prefer the public runtime buffer
//! types unless they are explicitly working on pool internals.

use super::pool::{InPlaceInit, Pool, PoolConfigError};
use super::provider::MemoryProvider;
use std::cell::{Cell, UnsafeCell};
use std::marker::{PhantomData, PhantomPinned};
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::ptr::NonNull;

/// Smart pointer for one slot checked out from an [`OwnedBufferPool`].
///
/// Dropping it returns the slot to the originating pool.
///
/// The `OwnedBuffer` lifetime is tied to the pinned [`OwnedBufferPool`] borrow
/// used for allocation, so safe code cannot drop the pool while a checked-out
/// buffer still exists.
pub struct OwnedBuffer<'pool, 'a, T: InPlaceInit, P: MemoryProvider> {
    /// Pointer to the pooled object storage.
    ptr: *mut T,
    /// Owning pool that will reclaim `ptr` on drop.
    pool: NonNull<OwnedBufferPool<'a, T, P>>,
    /// Ties the checked-out buffer to the pinned pool borrow used by `alloc`.
    _pool: PhantomData<&'pool OwnedBufferPool<'a, T, P>>,
}

impl<'pool, 'a, T: InPlaceInit, P: MemoryProvider> OwnedBuffer<'pool, 'a, T, P> {
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
        let pool = self.pool.as_ptr();
        std::mem::forget(self);
        (ptr, pool)
    }

    /// Reconstructs an [`OwnedBuffer`] from a raw pointer and pool pointer.
    ///
    /// This is the inverse of [`OwnedBuffer::into_raw_parts`].
    ///
    /// # Safety
    /// The pointer must have been previously obtained from the same pool and
    /// must still represent a live checked-out slot. Reconstructing from raw
    /// does not increment the pool's outstanding-buffer count; it is intended
    /// only as the inverse of [`OwnedBuffer::into_raw_parts`].
    ///
    /// The reconstructed `'pool` lifetime is chosen by the caller and is not
    /// proven by the raw pointer. The caller must guarantee the pinned pool
    /// outlives the reconstructed handle.
    pub unsafe fn from_raw(ptr: *mut T, pool: *mut OwnedBufferPool<'a, T, P>) -> Self {
        debug_assert!(!pool.is_null(), "OwnedBuffer::from_raw pool was null");
        Self {
            ptr,
            pool: unsafe { NonNull::new_unchecked(pool) },
            _pool: PhantomData,
        }
    }
}

impl<'pool, 'a, T: InPlaceInit, P: MemoryProvider> Deref for OwnedBuffer<'pool, 'a, T, P> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.ptr }
    }
}

impl<'pool, 'a, T: InPlaceInit, P: MemoryProvider> DerefMut for OwnedBuffer<'pool, 'a, T, P> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.ptr }
    }
}

impl<'pool, 'a, T: InPlaceInit, P: MemoryProvider> Drop for OwnedBuffer<'pool, 'a, T, P> {
    fn drop(&mut self) {
        unsafe {
            // Return the slot to the originating pool when the smart pointer
            // goes out of scope.
            let pool = self.pool.as_ref();
            pool.release_checked_out();
            (*pool.inner.get()).free(self.ptr);
        }
    }
}

/// Pool wrapper that returns [`OwnedBuffer`] smart pointers instead of raw slots.
///
/// Checked-out buffers contain a raw pointer back to the pinned pool and carry
/// a borrow lifetime that prevents safe code from dropping the pool first.
/// Buffers converted to raw parts must still be reconstructed and dropped, or
/// returned with [`OwnedBufferPool::free_raw`], before the pool is dropped.
pub struct OwnedBufferPool<'a, T: InPlaceInit, P: MemoryProvider> {
    /// Underlying slab-backed pool.
    inner: UnsafeCell<Pool<'a, T, P>>,
    /// Number of checked-out buffers that still owe the pool a slot return.
    outstanding: Cell<usize>,
    /// Checked-out buffers store a raw pointer back to this pool.
    _pin: PhantomPinned,
}

impl<'a, T: InPlaceInit, P: MemoryProvider> OwnedBufferPool<'a, T, P> {
    /// Creates a new uninitialized pool.
    pub fn new_uninit(provider: &'a mut P, objs_per_slab: usize) -> Result<Self, PoolConfigError> {
        Ok(Self {
            inner: UnsafeCell::new(Pool::new_uninit(provider, objs_per_slab)?),
            outstanding: Cell::new(0),
            _pin: PhantomPinned,
        })
    }

    /// Initializes the pool's intrusive linked lists.
    /// Must be called after moving the pool to its final memory location.
    pub fn init(&mut self) {
        self.inner.get_mut().init();
    }

    /// Allocates an object and returns it wrapped in an [`OwnedBuffer`].
    ///
    /// The pool must be pinned before allocation because returned buffers keep
    /// a raw pointer to the originating pool for drop-time reclamation.
    pub fn alloc<'pool>(
        self: Pin<&'pool Self>,
        args: T::Args,
    ) -> Option<OwnedBuffer<'pool, 'a, T, P>> {
        let this = self.get_ref();
        let ptr = unsafe { (*this.inner.get()).alloc(args)? };
        let outstanding = this.outstanding.get();
        this.outstanding.set(outstanding.saturating_add(1));
        Some(OwnedBuffer {
            ptr,
            pool: NonNull::from(this),
            _pool: PhantomData,
        })
    }

    /// Directly frees a raw pointer, bypassing the smart-pointer wrapper.
    ///
    /// # Safety
    /// The pointer must belong to this pool.
    pub unsafe fn free_raw(self: Pin<&Self>, ptr: *mut T) {
        let this = self.get_ref();
        unsafe {
            this.release_checked_out();
            (*this.inner.get()).free(ptr);
        }
    }

    fn release_checked_out(&self) {
        let outstanding = self.outstanding.get();
        debug_assert!(
            outstanding > 0,
            "OwnedBufferPool released more buffers than it allocated"
        );
        self.outstanding.set(outstanding.saturating_sub(1));
    }
}

impl<'a, T: InPlaceInit, P: MemoryProvider> Drop for OwnedBufferPool<'a, T, P> {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            debug_assert_eq!(
                self.outstanding.get(),
                0,
                "OwnedBufferPool dropped before all OwnedBuffer handles were returned"
            );
        }
    }
}
