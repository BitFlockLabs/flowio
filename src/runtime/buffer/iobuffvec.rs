//! Vectored I/O buffer chains for scatter-gather operations.
//!
//! [`IoBuffVecMut`] holds a fixed number of [`IoBuffMut`] segments for
//! `readv`/`recvmsg` operations. [`IoBuffVec`] holds frozen [`IoBuff`]
//! segments for `writev`/`sendmsg` operations.
//!
//! The segment count is a const generic determined at compile time. All
//! segment storage is inline; no heap allocation is performed by the chain
//! itself.
//!
//! These chain types own only buffer segments. Kernel-facing `iovec` arrays
//! are materialized into caller- or future-owned scratch storage at I/O
//! submission time instead of being cached inside the chain.
//!
//! # Example
//! ```no_run
//! use flowio::runtime::buffer::iobuffvec::{IoBuffVec, IoBuffVecMut};
//! use flowio::runtime::buffer::IoBuffMut;
//!
//! let mut seg1 = IoBuffMut::new(0, 16, 0).unwrap();
//! seg1.payload_append(b"hello").unwrap();
//! let mut seg2 = IoBuffMut::new(0, 16, 0).unwrap();
//! seg2.payload_append(b" world").unwrap();
//!
//! let chain = IoBuffVecMut::<2>::from_array([seg1, seg2]);
//! assert_eq!(chain.len(), 11);
//!
//! let frozen = chain.freeze();
//! assert_eq!(frozen.len(), 11);
//!
//! let thawed = match frozen.try_mut_all() {
//!     Ok(chain) => chain,
//!     Err((_err, _chain)) => unreachable!("segments are sole-owned"),
//! };
//! assert_eq!(thawed.get(0).unwrap().payload_bytes(), b"hello");
//!
//! let mut a = IoBuffMut::new(0, 8, 0).unwrap();
//! a.payload_append(b"hi").unwrap();
//! let mut b = IoBuffMut::new(0, 8, 0).unwrap();
//! b.payload_append(b"!").unwrap();
//! let frozen_chain: IoBuffVec<2> = [a.freeze(), b.freeze()].into();
//! assert_eq!(frozen_chain.len(), 3);
//! ```

use super::{IoBuff, IoBuffError, IoBuffMut, IoBuffReadOnly, IoBuffReadWrite};
use std::mem::MaybeUninit;

// ============================================================================
// IoBuffVecMut — mutable vectored buffer chain (const generic, inline)
// ============================================================================

/// Mutable vectored buffer chain with a fixed number of segments.
///
/// `N` is the maximum number of buffer segments, determined at compile time.
/// All segment-handle storage is inline and heap-free.
pub struct IoBuffVecMut<const N: usize>
{
    /// Inline array of buffer segment handles. Only indices `0..count`
    /// are initialized.
    buffers: [MaybeUninit<IoBuffMut>; N],
    /// Number of initialized segments (0..=N).
    count: usize,
}

impl<const N: usize> Default for IoBuffVecMut<N>
{
    fn default() -> Self
    {
        Self::new()
    }
}

impl<const N: usize> From<[IoBuffMut; N]> for IoBuffVecMut<N>
{
    fn from(buffers: [IoBuffMut; N]) -> Self
    {
        Self::from_array(buffers)
    }
}

impl<const N: usize> IoBuffVecMut<N>
{
    /// Creates an empty vectored buffer chain.
    pub fn new() -> Self
    {
        Self {
            buffers: unsafe { MaybeUninit::uninit().assume_init() },
            count: 0,
        }
    }

    /// Creates a fully-initialized chain from an array of mutable segments.
    pub fn from_array(buffers: [IoBuffMut; N]) -> Self
    {
        let mut out = Self::new();
        for (i, buf) in buffers.into_iter().enumerate()
        {
            out.buffers[i] = MaybeUninit::new(buf);
        }
        out.count = N;
        out
    }

    /// Returns the number of segments currently in the chain.
    #[inline(always)]
    pub fn segments(&self) -> usize
    {
        self.count
    }

    /// Returns the maximum number of segments this chain can hold.
    #[inline(always)]
    pub fn capacity(&self) -> usize
    {
        N
    }

    /// Adds a buffer segment to the chain. Takes ownership of the buffer.
    ///
    /// Returns `IoBuffError::ChainFull` if the chain is at capacity.
    pub fn push(&mut self, buf: IoBuffMut) -> Result<(), IoBuffError>
    {
        if self.count >= N
        {
            return Err(IoBuffError::ChainFull);
        }

        self.buffers[self.count] = MaybeUninit::new(buf);
        self.count += 1;
        Ok(())
    }

    /// Returns a reference to the buffer segment at the given index.
    #[inline(always)]
    pub fn get(&self, index: usize) -> Result<&IoBuffMut, IoBuffError>
    {
        if index >= self.count
        {
            return Err(IoBuffError::IndexOutOfBounds);
        }
        Ok(unsafe { self.buffers[index].assume_init_ref() })
    }

    /// Returns a mutable reference to the buffer segment at the given index.
    #[inline(always)]
    pub fn get_mut(&mut self, index: usize) -> Result<&mut IoBuffMut, IoBuffError>
    {
        if index >= self.count
        {
            return Err(IoBuffError::IndexOutOfBounds);
        }
        Ok(unsafe { self.buffers[index].assume_init_mut() })
    }

    /// Returns the total number of active bytes across all segments.
    #[inline(always)]
    pub fn len(&self) -> usize
    {
        let mut total = 0;
        for i in 0..self.count
        {
            total += unsafe { self.buffers[i].assume_init_ref() }.len();
        }
        total
    }

    /// Returns `true` if the chain has no segments or all segments are empty.
    #[inline(always)]
    pub fn is_empty(&self) -> bool
    {
        self.count == 0 || self.len() == 0
    }

    /// Returns the total writable capacity across all segments.
    #[inline(always)]
    pub fn writable_len(&self) -> usize
    {
        let mut total = 0;
        for i in 0..self.count
        {
            total += unsafe { self.buffers[i].assume_init_ref() }.payload_remaining();
        }
        total
    }

    /// Fills a caller-provided `iovec` scratch array for `readv`/`recvmsg`.
    ///
    /// Returns `(iov_count, total_writable)` for the initialized entries.
    pub(crate) fn fill_read_iovecs_and_writable_len(
        &mut self,
        dst: &mut [MaybeUninit<libc::iovec>; N],
    ) -> (usize, usize)
    {
        let mut total = 0;
        for (i, slot) in dst.iter_mut().enumerate().take(self.count)
        {
            let buf = unsafe { self.buffers[i].assume_init_mut() };
            let len = buf.writable_len();
            slot.write(libc::iovec {
                iov_base: buf.as_mut_ptr() as *mut libc::c_void,
                iov_len: len,
            });
            total += len;
        }
        (self.count, total)
    }

    /// Distributes `total_bytes` across the segments in order, setting each
    /// buffer's payload length. The kernel fills iovecs sequentially.
    ///
    /// # Safety
    /// The caller must guarantee that the first `total_bytes` bytes across
    /// the materialized iovec array have been initialized by the kernel.
    pub unsafe fn distribute_written(&mut self, total_bytes: usize)
    {
        let mut remaining = total_bytes;
        for i in 0..self.count
        {
            let buf = unsafe { self.buffers[i].assume_init_mut() };
            let cap = buf.payload_remaining();
            let written = if remaining >= cap { cap } else { remaining };
            buf.payload_len += written;
            remaining -= written;
            if remaining == 0
            {
                break;
            }
        }

        debug_assert!(
            remaining == 0,
            "distribute_written: {} bytes left over after filling all segments",
            remaining
        );
    }

    /// Freezes all buffer segments and returns an [`IoBuffVec`].
    /// Zero-copy.
    pub fn freeze(mut self) -> IoBuffVec<N>
    {
        let mut frozen_buffers: [MaybeUninit<IoBuff>; N] =
            unsafe { MaybeUninit::uninit().assume_init() };
        let count = self.count;

        for (i, slot) in frozen_buffers.iter_mut().enumerate().take(count)
        {
            let buf = unsafe { self.buffers[i].assume_init_read() };
            *slot = MaybeUninit::new(buf.freeze());
        }

        self.count = 0;

        IoBuffVec {
            buffers: frozen_buffers,
            count,
        }
    }
}

impl<const N: usize> Drop for IoBuffVecMut<N>
{
    fn drop(&mut self)
    {
        for i in 0..self.count
        {
            unsafe { self.buffers[i].assume_init_drop() };
        }
    }
}

// ============================================================================
// IoBuffVec — frozen vectored buffer chain (const generic, inline)
// ============================================================================

/// Frozen vectored buffer chain with a fixed number of segments.
///
/// Created by calling [`IoBuffVecMut::freeze`]. All segments are [`IoBuff`]
/// and cloning the chain clones each segment zero-copy. Like
/// [`IoBuffVecMut`], it stores only segment handles; `iovec` scratch belongs
/// to the calling I/O operation.
pub struct IoBuffVec<const N: usize>
{
    buffers: [MaybeUninit<IoBuff>; N],
    count: usize,
}

impl<const N: usize> Default for IoBuffVec<N>
{
    fn default() -> Self
    {
        Self::new()
    }
}

impl<const N: usize> From<[IoBuff; N]> for IoBuffVec<N>
{
    fn from(buffers: [IoBuff; N]) -> Self
    {
        Self::from_array(buffers)
    }
}

impl<const N: usize> IoBuffVec<N>
{
    /// Creates an empty frozen vectored buffer chain.
    pub fn new() -> Self
    {
        Self {
            buffers: unsafe { MaybeUninit::uninit().assume_init() },
            count: 0,
        }
    }

    /// Creates a fully-initialized chain from an array of frozen segments.
    pub fn from_array(buffers: [IoBuff; N]) -> Self
    {
        let mut out = Self::new();
        for (i, buf) in buffers.into_iter().enumerate()
        {
            out.buffers[i] = MaybeUninit::new(buf);
        }
        out.count = N;
        out
    }

    /// Returns the number of segments in the chain.
    #[inline(always)]
    pub fn segments(&self) -> usize
    {
        self.count
    }

    /// Returns the maximum number of segments this chain can hold.
    #[inline(always)]
    pub fn capacity(&self) -> usize
    {
        N
    }

    /// Returns the total number of readable bytes across all segments.
    #[inline(always)]
    pub fn len(&self) -> usize
    {
        let mut total = 0;
        for i in 0..self.count
        {
            total += unsafe { self.buffers[i].assume_init_ref() }.len();
        }
        total
    }

    /// Returns `true` if the chain has no segments or all segments are empty.
    #[inline(always)]
    pub fn is_empty(&self) -> bool
    {
        self.count == 0 || self.len() == 0
    }

    /// Returns a reference to the frozen buffer segment at the given index.
    #[inline(always)]
    pub fn get(&self, index: usize) -> Result<&IoBuff, IoBuffError>
    {
        if index >= self.count
        {
            return Err(IoBuffError::IndexOutOfBounds);
        }
        Ok(unsafe { self.buffers[index].assume_init_ref() })
    }

    /// Adds a frozen buffer segment to the chain. Takes ownership of the buffer.
    ///
    /// Returns `IoBuffError::ChainFull` if the chain is at capacity.
    pub fn push(&mut self, buf: IoBuff) -> Result<(), IoBuffError>
    {
        if self.count >= N
        {
            return Err(IoBuffError::ChainFull);
        }

        self.buffers[self.count] = MaybeUninit::new(buf);
        self.count += 1;
        Ok(())
    }

    /// Returns an iterator over the frozen buffer segments.
    pub fn iter(&self) -> impl Iterator<Item = &IoBuff>
    {
        (0..self.count).map(move |i| unsafe { self.buffers[i].assume_init_ref() })
    }

    /// Fills a caller-provided `iovec` scratch array for `writev`/`sendmsg`.
    ///
    /// Returns `(iov_count, total_len)` for the initialized entries.
    pub(crate) fn fill_write_iovecs_and_len(
        &self,
        dst: &mut [MaybeUninit<libc::iovec>; N],
    ) -> (usize, usize)
    {
        let mut total = 0;
        for (i, slot) in dst.iter_mut().enumerate().take(self.count)
        {
            let buf = unsafe { self.buffers[i].assume_init_ref() };
            let len = buf.len();
            slot.write(libc::iovec {
                iov_base: buf.as_ptr() as *mut libc::c_void,
                iov_len: len,
            });
            total += len;
        }
        (self.count, total)
    }

    /// Attempts to convert the frozen chain back to a mutable chain.
    ///
    /// Succeeds only if every segment has a reference count of 1.
    /// Returns `Err((IoBuffError::SharedBuffer, self))` with `self` intact
    /// if any segment is shared.
    pub fn try_mut_all(mut self) -> Result<IoBuffVecMut<N>, (IoBuffError, Self)>
    {
        for i in 0..self.count
        {
            let buf = unsafe { self.buffers[i].assume_init_ref() };
            if buf.ref_count() > 1
            {
                return Err((IoBuffError::SharedBuffer, self));
            }
        }

        let mut mut_buffers: [MaybeUninit<IoBuffMut>; N] =
            unsafe { MaybeUninit::uninit().assume_init() };
        let count = self.count;

        for (i, slot) in mut_buffers.iter_mut().enumerate().take(count)
        {
            let frozen = unsafe { self.buffers[i].assume_init_read() };
            let mutable = unsafe { frozen.try_mut().unwrap_unchecked() };
            *slot = MaybeUninit::new(mutable);
        }

        self.count = 0;

        Ok(IoBuffVecMut {
            buffers: mut_buffers,
            count,
        })
    }
}

impl<const N: usize> Clone for IoBuffVec<N>
{
    fn clone(&self) -> Self
    {
        let mut buffers: [MaybeUninit<IoBuff>; N] = unsafe { MaybeUninit::uninit().assume_init() };

        for (i, slot) in buffers.iter_mut().enumerate().take(self.count)
        {
            *slot = MaybeUninit::new(unsafe { self.buffers[i].assume_init_ref() }.clone());
        }

        IoBuffVec {
            buffers,
            count: self.count,
        }
    }
}

impl<const N: usize> Drop for IoBuffVec<N>
{
    fn drop(&mut self)
    {
        for i in 0..self.count
        {
            unsafe { self.buffers[i].assume_init_drop() };
        }
    }
}
