use super::pool::{InPlaceInit, Pool, PoolConfigError};
use super::provider::MemoryProvider;
use std::ops::{Deref, DerefMut};

/// Smart pointer for one slot checked out from an [`OwnedBufferPool`].
///
/// Dropping it returns the slot to the originating pool.
pub struct OwnedBuffer<'a, T: InPlaceInit, P: MemoryProvider>
{
    /// Pointer to the pooled object storage.
    ptr: *mut T,
    /// Owning pool that will reclaim `ptr` on drop.
    pool: *mut OwnedBufferPool<'a, T, P>,
}

// Safety: OwnedBuffer is Send/Sync if the underlying data is.
unsafe impl<'a, T: InPlaceInit + Send, P: MemoryProvider> Send for OwnedBuffer<'a, T, P> {}
unsafe impl<'a, T: InPlaceInit + Sync, P: MemoryProvider> Sync for OwnedBuffer<'a, T, P> {}

impl<'a, T: InPlaceInit, P: MemoryProvider> OwnedBuffer<'a, T, P>
{
    /// Consumes the OwnedBuffer and returns the raw pointer without triggering Drop.
    /// The caller is now responsible for returning it to the pool or re-wrapping it.
    pub fn into_raw(self) -> *mut T
    {
        let ptr = self.ptr;
        std::mem::forget(self);
        ptr
    }

    /// Consumes the OwnedBuffer and returns the raw pointer plus the originating pool.
    /// The caller is now responsible for reconstructing or returning the buffer correctly.
    pub fn into_raw_parts(self) -> (*mut T, *mut OwnedBufferPool<'a, T, P>)
    {
        let ptr = self.ptr;
        let pool = self.pool;
        std::mem::forget(self);
        (ptr, pool)
    }

    /// Reconstructs an OwnedBuffer from a raw pointer and a pool pointer.
    /// # Safety
    /// The pointer must have been previously obtained via `into_raw` from the same pool.
    pub unsafe fn from_raw(ptr: *mut T, pool: *mut OwnedBufferPool<'a, T, P>) -> Self
    {
        Self { ptr, pool }
    }
}

impl<'a, T: InPlaceInit, P: MemoryProvider> Deref for OwnedBuffer<'a, T, P>
{
    type Target = T;

    fn deref(&self) -> &Self::Target
    {
        unsafe { &*self.ptr }
    }
}

impl<'a, T: InPlaceInit, P: MemoryProvider> DerefMut for OwnedBuffer<'a, T, P>
{
    fn deref_mut(&mut self) -> &mut Self::Target
    {
        unsafe { &mut *self.ptr }
    }
}

impl<'a, T: InPlaceInit, P: MemoryProvider> Drop for OwnedBuffer<'a, T, P>
{
    fn drop(&mut self)
    {
        unsafe {
            // Automatically return the memory to the slab pool
            (*self.pool).inner.free(self.ptr);
        }
    }
}

/// Pool wrapper that returns [`OwnedBuffer`] smart pointers instead of raw slots.
pub struct OwnedBufferPool<'a, T: InPlaceInit, P: MemoryProvider>
{
    /// Underlying slab-backed pool.
    inner: Pool<'a, T, P>,
}

impl<'a, T: InPlaceInit, P: MemoryProvider> OwnedBufferPool<'a, T, P>
{
    /// Creates a new uninitialized pool.
    pub fn new_uninit(provider: &'a mut P, objs_per_slab: usize) -> Result<Self, PoolConfigError>
    {
        Ok(Self {
            inner: Pool::new_uninit(provider, objs_per_slab)?,
        })
    }

    /// Initializes the pool's intrusive linked lists.
    /// MUST be called after moving the pool to its final memory location.
    pub fn init(&mut self)
    {
        self.inner.init();
    }

    /// Allocates an object and returns it wrapped in an `OwnedBuffer` smart pointer.
    pub fn alloc(&mut self, args: T::Args) -> Option<OwnedBuffer<'a, T, P>>
    {
        let ptr = unsafe { self.inner.alloc(args)? };
        Some(OwnedBuffer {
            ptr,
            pool: self as *mut Self,
        })
    }

    /// Directly frees a raw pointer bypassing the smart pointer.
    /// # Safety
    /// The pointer must belong to this pool.
    pub unsafe fn free_raw(&mut self, ptr: *mut T)
    {
        unsafe {
            self.inner.free(ptr);
        }
    }
}
