//! Vectored I/O buffer chains for scatter-gather operations.
//!
//! [`IoBuffVecMut`] holds up to a fixed capacity of [`IoBuffMut`] segments for
//! `readv`/`recvmsg` operations. [`IoBuffVec`] holds frozen [`IoBuff`]
//! segments for `writev`/`sendmsg` operations. [`IoBuffReadOnlyVec`] is the
//! generic send-side chain for already-owned buffers implementing
//! [`IoBuffReadOnly`].
//!
//! Segment capacity is a const generic determined at compile time. Buffer
//! handle storage is inline; the chain itself performs no heap allocation.
//! Individual segment implementations may own heap- or pool-backed storage.
//!
//! These are fixed-capacity containers. If a push would exceed capacity, the
//! operation returns [`PushError`] with the original segment so the caller
//! retains ownership; the container never drops a value it failed to insert.
//!
//! These chain types own only buffer segments. Kernel-facing `iovec` arrays
//! are materialized into caller- or future-owned scratch storage at I/O
//! submission time instead of being cached inside the chain.
//!
//! # Fast-Path Guidance
//!
//! Preferred on the fast path:
//! - Use these chain types when a protocol is already naturally segmented and
//!   that segmentation is worth preserving.
//! - Choose the smallest practical `N`: it sets both the inline handle
//!   footprint and the maximum segment count. I/O futures materialize bounded
//!   kernel `iovec` scratch only for active entries.
//!
//! Avoid on the fast path:
//! - A vectored chain adds no segmentation benefit for one already-contiguous
//!   payload; [`IoBuffMut`], [`IoBuff`], or another contiguous implementation
//!   represents that input directly.
//! - Do not choose a large `N` speculatively. Async transport operations reject
//!   more than 1024 active iovecs, and larger inline arrays increase the chain
//!   value's footprint even when only a few entries are used.
//!
//! # Example
//! ```
//! use flowio::runtime::buffer::iobuffvec::{IoBuffReadOnlyVec, IoBuffVec, IoBuffVecMut};
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
//!
//! let generic_chain: IoBuffReadOnlyVec<Vec<u8>, 2> =
//!     IoBuffReadOnlyVec::from_array([b"hello".to_vec(), b"!".to_vec()]);
//! assert_eq!(generic_chain.len(), 6);
//! ```

use super::{IoBuff, IoBuffError, IoBuffMut, IoBuffReadOnly, IoBuffReadWrite};
use std::io;
use std::mem::MaybeUninit;

pub(crate) const READ_IOVEC_SHAPE_CHANGED: &str =
    "read buffer chain shape changed before submission";

#[inline(always)]
pub(crate) fn invalid_read_iovec_shape() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, READ_IOVEC_SHAPE_CHANGED)
}

/// Error returned when a fixed-capacity vectored chain cannot accept a value.
///
/// The original value is returned intact so callers can retry, recycle, or
/// otherwise recover ownership. This is especially important for buffer
/// handles, where dropping on overflow would silently release caller-owned I/O
/// storage.
///
/// # Example
/// ```
/// use flowio::runtime::buffer::iobuffvec::IoBuffVecMut;
/// use flowio::runtime::buffer::{IoBuffMut, IoBuffReadWrite};
///
/// let mut chain = IoBuffVecMut::<1>::new();
/// chain.push(IoBuffMut::new(0, 8, 0).unwrap()).unwrap();
///
/// let err = chain.push(IoBuffMut::new(0, 8, 0).unwrap()).unwrap_err();
/// let recovered = err.into_value();
/// assert_eq!(recovered.writable_len(), 8);
/// ```
pub struct PushError<T> {
    /// Reason the value could not be pushed.
    error: IoBuffError,
    /// Original value returned to the caller intact.
    value: T,
}

impl<T> PushError<T> {
    /// Creates a new push error with the original value.
    #[inline(always)]
    fn new(error: IoBuffError, value: T) -> Self {
        Self { error, value }
    }

    /// Returns the reason the push failed.
    #[inline(always)]
    pub fn error(&self) -> IoBuffError {
        self.error
    }

    /// Returns a reference to the original value.
    #[inline(always)]
    pub fn value(&self) -> &T {
        &self.value
    }

    /// Returns a mutable reference to the original value.
    #[inline(always)]
    pub fn value_mut(&mut self) -> &mut T {
        &mut self.value
    }

    /// Consumes the error and returns the original value.
    #[inline(always)]
    pub fn into_value(self) -> T {
        self.value
    }

    /// Consumes the error and returns both the reason and original value.
    #[inline(always)]
    pub fn into_parts(self) -> (IoBuffError, T) {
        (self.error, self.value)
    }
}

impl<T> std::fmt::Debug for PushError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PushError")
            .field("error", &self.error)
            .field("value", &"<returned>")
            .finish()
    }
}

#[inline(always)]
fn uninit_inline_storage<T, const N: usize>() -> [MaybeUninit<T>; N] {
    // SAFETY: An array of `MaybeUninit<T>` may contain arbitrary uninitialized
    // bytes. Callers track the initialized prefix separately.
    unsafe { MaybeUninit::uninit().assume_init() }
}

fn inline_storage_from_array<T, const N: usize>(values: [T; N]) -> [MaybeUninit<T>; N] {
    let mut storage = uninit_inline_storage();
    for (i, value) in values.into_iter().enumerate() {
        storage[i] = MaybeUninit::new(value);
    }
    storage
}

#[inline(always)]
fn try_push_inline<T, const N: usize>(
    storage: &mut [MaybeUninit<T>; N],
    count: &mut usize,
    value: T,
) -> Result<(), PushError<T>> {
    if *count >= N {
        return Err(PushError::new(IoBuffError::ChainFull, value));
    }

    storage[*count] = MaybeUninit::new(value);
    *count += 1;
    Ok(())
}

#[inline(always)]
fn get_inline<T, const N: usize>(
    storage: &[MaybeUninit<T>; N],
    count: usize,
    index: usize,
) -> Result<&T, IoBuffError> {
    if index >= count {
        return Err(IoBuffError::IndexOutOfBounds);
    }
    Ok(unsafe { storage[index].assume_init_ref() })
}

#[inline(always)]
fn get_inline_mut<T, const N: usize>(
    storage: &mut [MaybeUninit<T>; N],
    count: usize,
    index: usize,
) -> Result<&mut T, IoBuffError> {
    if index >= count {
        return Err(IoBuffError::IndexOutOfBounds);
    }
    Ok(unsafe { storage[index].assume_init_mut() })
}

fn iter_inline<T, const N: usize>(
    storage: &[MaybeUninit<T>; N],
    count: usize,
) -> impl Iterator<Item = &T> {
    (0..count).map(move |i| unsafe { storage[i].assume_init_ref() })
}

#[inline(always)]
fn checked_length_sum<I>(lengths: I) -> Option<usize>
where
    I: IntoIterator<Item = usize>,
{
    lengths.into_iter().try_fold(0usize, usize::checked_add)
}

#[inline(always)]
pub(crate) fn checked_iovec_count_and_length_sum<I>(lengths: I) -> Option<(usize, usize)>
where
    I: IntoIterator<Item = usize>,
{
    let mut iov_count = 0;
    let mut total = 0usize;
    for len in lengths {
        total = total.checked_add(len)?;
        if len != 0 {
            iov_count += 1;
        }
    }
    Some((iov_count, total))
}

#[inline(always)]
fn checked_readable_len<'a, B, I>(iter: I) -> Option<usize>
where
    B: IoBuffReadOnly + 'a,
    I: IntoIterator<Item = &'a B>,
{
    checked_length_sum(iter.into_iter().map(IoBuffReadOnly::len))
}

/// Drops the initialized prefix of inline storage.
///
/// # Safety
///
/// `count` must not exceed `storage.len()`, and exactly the entries in
/// `storage[..count]` must contain live `T` values owned by the caller.
unsafe fn drop_initialized_inline<T>(storage: &mut [MaybeUninit<T>], count: usize) {
    debug_assert!(count <= storage.len());
    let initialized = std::ptr::slice_from_raw_parts_mut(storage.as_mut_ptr().cast::<T>(), count);
    unsafe { std::ptr::drop_in_place(initialized) };
}

// ============================================================================
// IoBuffVecMut — mutable vectored buffer chain (const generic, inline)
// ============================================================================

/// Mutable vectored buffer chain with fixed inline segment capacity.
///
/// `N` is the maximum number of buffer segments, determined at compile time.
/// All segment-handle storage is inline and heap-free.
///
/// This represents multiple writable destination segments for vectored
/// transport operations. A single [`IoBuffMut`] represents one contiguous
/// destination.
///
/// # Example
/// ```
/// use flowio::runtime::buffer::iobuffvec::IoBuffVecMut;
/// use flowio::runtime::buffer::IoBuffMut;
///
/// let chain = IoBuffVecMut::<2>::from_array([
///     IoBuffMut::new(0, 8, 0).unwrap(),
///     IoBuffMut::new(0, 8, 0).unwrap(),
/// ]);
/// assert!(chain.is_empty()); // no readable bytes yet
/// assert_eq!(chain.segments(), 2);
/// assert_eq!(chain.len(), 0);
/// assert_eq!(chain.writable_len(), 16);
/// ```
pub struct IoBuffVecMut<const N: usize> {
    /// Inline array of buffer segment handles. Only indices `0..count`
    /// are initialized.
    buffers: [MaybeUninit<IoBuffMut>; N],
    /// Number of initialized segments (0..=N).
    count: usize,
}

impl<const N: usize> Default for IoBuffVecMut<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> From<[IoBuffMut; N]> for IoBuffVecMut<N> {
    fn from(buffers: [IoBuffMut; N]) -> Self {
        Self::from_array(buffers)
    }
}

macro_rules! define_distribute_written {
    ($visibility:vis) => {
        /// Distributes `total_bytes` across the segments in order, adding to
        /// each buffer's payload length. The kernel fills iovecs sequentially.
        ///
        /// # Safety
        /// The caller must guarantee that the first `total_bytes` bytes across
        /// the materialized iovec array have been initialized by the kernel.
        $visibility unsafe fn distribute_written(&mut self, total_bytes: usize) {
            let writable = self.writable_len();
            debug_assert!(
                total_bytes <= writable,
                "distribute_written({}) exceeds writable capacity {}",
                total_bytes,
                writable
            );
            let mut remaining = std::cmp::min(total_bytes, writable);
            for i in 0..self.count {
                let buf = unsafe { self.buffers[i].assume_init_mut() };
                let cap = buf.payload_remaining();
                let written = if remaining >= cap { cap } else { remaining };
                let new_len = buf.payload_len() + written;
                // SAFETY: `written` is bounded by this segment's writable
                // capacity, and this method's caller guarantees that the
                // corresponding materialized iovec bytes were initialized.
                unsafe { buf.publish_initialized_len_unchecked(new_len) };
                remaining -= written;
                if remaining == 0 {
                    break;
                }
            }

            debug_assert!(
                remaining == 0,
                "distribute_written: {} bytes left over after filling all segments",
                remaining
            );
        }
    };
}

impl<const N: usize> IoBuffVecMut<N> {
    /// Creates an empty vectored buffer chain.
    pub fn new() -> Self {
        Self {
            buffers: uninit_inline_storage(),
            count: 0,
        }
    }

    /// Creates a fully-initialized chain from an array of mutable segments.
    pub fn from_array(buffers: [IoBuffMut; N]) -> Self {
        Self {
            buffers: inline_storage_from_array(buffers),
            count: N,
        }
    }

    /// Returns the number of segments currently in the chain.
    #[inline(always)]
    pub fn segments(&self) -> usize {
        self.count
    }

    /// Returns the maximum number of segments this chain can hold.
    #[inline(always)]
    pub fn capacity(&self) -> usize {
        N
    }

    /// Adds a buffer segment to the chain.
    ///
    /// Returns [`PushError`] containing `buf` if the chain is at capacity.
    pub fn push(&mut self, buf: IoBuffMut) -> Result<(), PushError<IoBuffMut>> {
        try_push_inline(&mut self.buffers, &mut self.count, buf)
    }

    /// Returns a reference to the buffer segment at the given index.
    #[inline(always)]
    pub fn get(&self, index: usize) -> Result<&IoBuffMut, IoBuffError> {
        get_inline(&self.buffers, self.count, index)
    }

    /// Returns a mutable reference to the buffer segment at the given index.
    #[inline(always)]
    pub fn get_mut(&mut self, index: usize) -> Result<&mut IoBuffMut, IoBuffError> {
        get_inline_mut(&mut self.buffers, self.count, index)
    }

    /// Returns the total number of active bytes across all segments.
    ///
    /// An unrepresentable aggregate saturates at `usize::MAX`.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.checked_len().unwrap_or(usize::MAX)
    }

    /// Returns the exact total active length, or `None` on `usize` overflow.
    #[inline(always)]
    pub fn checked_len(&self) -> Option<usize> {
        checked_readable_len(iter_inline(&self.buffers, self.count))
    }

    /// Returns `true` if the chain contains zero readable bytes.
    ///
    /// This does not describe segment presence or writable capacity: an empty
    /// mutable chain may still contain segments and accept data. Use
    /// [`Self::segments`] and [`Self::writable_len`] to query those properties.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        iter_inline(&self.buffers, self.count).all(IoBuffReadOnly::is_empty)
    }

    /// Returns the total writable capacity across all segments.
    ///
    /// An unrepresentable aggregate saturates at `usize::MAX`.
    #[inline(always)]
    pub fn writable_len(&self) -> usize {
        self.checked_writable_len().unwrap_or(usize::MAX)
    }

    /// Returns the exact total writable capacity, or `None` on `usize`
    /// overflow.
    #[inline(always)]
    pub fn checked_writable_len(&self) -> Option<usize> {
        checked_length_sum(
            iter_inline(&self.buffers, self.count).map(|buf| buf.payload_remaining()),
        )
    }

    /// Returns the number of non-empty read iovecs and their total capacity.
    #[inline(always)]
    pub(crate) fn checked_read_iovec_count_and_writable_len(&self) -> Option<(usize, usize)> {
        checked_iovec_count_and_length_sum(
            iter_inline(&self.buffers, self.count).map(|buf| buf.payload_remaining()),
        )
    }

    /// Fills caller-provided `iovec` scratch for `readv`/`recvmsg`.
    ///
    /// Zero-length destinations are skipped. Returns
    /// `(iov_count, total_writable)` for the initialized prefix. Returns
    /// `InvalidInput` if the scratch is too short or the materialized writable
    /// total cannot be represented by `usize`.
    pub(crate) fn fill_read_iovecs_and_writable_len(
        &mut self,
        dst: &mut [MaybeUninit<libc::iovec>],
    ) -> io::Result<(usize, usize)> {
        let mut iov_count = 0;
        let mut total = 0usize;
        for i in 0..self.count {
            let buf = unsafe { self.buffers[i].assume_init_mut() };
            let len = buf.writable_len();
            total = total
                .checked_add(len)
                .ok_or_else(invalid_read_iovec_shape)?;
            if len == 0 {
                continue;
            }
            let iovec = dst
                .get_mut(iov_count)
                .ok_or_else(invalid_read_iovec_shape)?;
            iovec.write(libc::iovec {
                iov_base: buf.as_mut_ptr() as *mut libc::c_void,
                iov_len: len,
            });
            iov_count += 1;
        }
        debug_assert_eq!(
            Some((iov_count, total)),
            self.checked_read_iovec_count_and_writable_len()
        );
        Ok((iov_count, total))
    }

    #[cfg(feature = "test-support")]
    define_distribute_written!(pub);

    #[cfg(not(feature = "test-support"))]
    define_distribute_written!(pub(crate));

    /// Freezes all buffer segments and returns an [`IoBuffVec`].
    /// Zero-copy.
    pub fn freeze(mut self) -> IoBuffVec<N> {
        let mut frozen_buffers = uninit_inline_storage();
        let count = self.count;

        for (i, slot) in frozen_buffers.iter_mut().enumerate().take(count) {
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

impl<const N: usize> Drop for IoBuffVecMut<N> {
    fn drop(&mut self) {
        unsafe { drop_initialized_inline(&mut self.buffers, self.count) };
    }
}

// ============================================================================
// IoBuffVec — frozen vectored buffer chain (const generic, inline)
// ============================================================================

/// Frozen vectored buffer chain with fixed inline segment capacity.
///
/// Created by calling [`IoBuffVecMut::freeze`]. All segments are [`IoBuff`]
/// and cloning the chain clones each segment zero-copy. Like
/// [`IoBuffVecMut`], it stores only segment handles; `iovec` scratch belongs
/// to the calling I/O operation.
///
/// This preserves existing send-side segmentation. A single [`IoBuff`] or
/// [`IoBuffMut`] represents one contiguous payload.
///
/// # Example
/// ```
/// use flowio::runtime::buffer::iobuffvec::IoBuffVec;
/// use flowio::runtime::buffer::IoBuffMut;
///
/// let mut first = IoBuffMut::new(0, 8, 0).unwrap();
/// first.payload_append(b"ab").unwrap();
/// let mut second = IoBuffMut::new(0, 8, 0).unwrap();
/// second.payload_append(b"cd").unwrap();
///
/// let chain: IoBuffVec<2> = [first.freeze(), second.freeze()].into();
/// assert_eq!(chain.segments(), 2);
/// assert_eq!(chain.len(), 4);
/// ```
pub struct IoBuffVec<const N: usize> {
    /// Inline array of frozen segment handles. Only indices `0..count` are
    /// initialized.
    buffers: [MaybeUninit<IoBuff>; N],
    /// Number of initialized frozen segments currently stored in the chain.
    count: usize,
}

impl<const N: usize> Default for IoBuffVec<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> From<[IoBuff; N]> for IoBuffVec<N> {
    fn from(buffers: [IoBuff; N]) -> Self {
        Self::from_array(buffers)
    }
}

impl<const N: usize> IoBuffVec<N> {
    /// Creates an empty frozen vectored buffer chain.
    pub fn new() -> Self {
        Self {
            buffers: uninit_inline_storage(),
            count: 0,
        }
    }

    /// Creates a fully-initialized chain from an array of frozen segments.
    pub fn from_array(buffers: [IoBuff; N]) -> Self {
        Self {
            buffers: inline_storage_from_array(buffers),
            count: N,
        }
    }

    /// Returns the number of segments in the chain.
    #[inline(always)]
    pub fn segments(&self) -> usize {
        self.count
    }

    /// Returns the maximum number of segments this chain can hold.
    #[inline(always)]
    pub fn capacity(&self) -> usize {
        N
    }

    /// Returns the total number of readable bytes across all segments.
    ///
    /// An unrepresentable aggregate saturates at `usize::MAX`.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.checked_len().unwrap_or(usize::MAX)
    }

    #[inline(always)]
    pub(crate) fn checked_len(&self) -> Option<usize> {
        checked_readable_len(iter_inline(&self.buffers, self.count))
    }

    /// Returns `true` if the chain contains zero readable bytes.
    ///
    /// A chain may still contain zero-length segments; use [`Self::segments`]
    /// to query segment presence.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        iter_inline(&self.buffers, self.count).all(IoBuffReadOnly::is_empty)
    }

    /// Returns a reference to the frozen buffer segment at the given index.
    #[inline(always)]
    pub fn get(&self, index: usize) -> Result<&IoBuff, IoBuffError> {
        get_inline(&self.buffers, self.count, index)
    }

    /// Adds a frozen buffer segment to the chain.
    ///
    /// Returns [`PushError`] containing `buf` if the chain is at capacity.
    pub fn push(&mut self, buf: IoBuff) -> Result<(), PushError<IoBuff>> {
        try_push_inline(&mut self.buffers, &mut self.count, buf)
    }

    /// Returns an iterator over the frozen buffer segments.
    pub fn iter(&self) -> impl Iterator<Item = &IoBuff> {
        iter_inline(&self.buffers, self.count)
    }

    /// Attempts to convert the frozen chain back to a mutable chain.
    ///
    /// Succeeds only if every segment has a reference count of 1.
    /// Returns `Err((IoBuffError::SharedBuffer, self))` with `self` intact
    /// if any segment is shared.
    pub fn try_mut_all(mut self) -> Result<IoBuffVecMut<N>, (IoBuffError, Self)> {
        let has_shared_segment =
            iter_inline(&self.buffers, self.count).any(|buf| buf.ref_count() > 1);
        if has_shared_segment {
            return Err((IoBuffError::SharedBuffer, self));
        }

        let mut mut_buffers = uninit_inline_storage();
        let count = self.count;

        for (i, slot) in mut_buffers.iter_mut().enumerate().take(count) {
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

impl<const N: usize> Clone for IoBuffVec<N> {
    fn clone(&self) -> Self {
        let mut buffers = uninit_inline_storage();

        for (i, slot) in buffers.iter_mut().enumerate().take(self.count) {
            *slot = MaybeUninit::new(unsafe { self.buffers[i].assume_init_ref() }.clone());
        }

        IoBuffVec {
            buffers,
            count: self.count,
        }
    }
}

impl<const N: usize> Drop for IoBuffVec<N> {
    fn drop(&mut self) {
        unsafe { drop_initialized_inline(&mut self.buffers, self.count) };
    }
}

// ============================================================================
// IoBuffReadOnlyVec — generic read-only vectored buffer chain
// ============================================================================

/// Generic owned read-only vectored buffer chain.
///
/// `B` is any concrete buffer type implementing [`IoBuffReadOnly`], and `N`
/// is the maximum number of segments stored inline. The chain itself performs
/// no heap allocation and stores only initialized entries in `0..segments()`.
///
/// The chain owns every buffer for the full lifetime of a vectored write
/// future. That preserves the [`IoBuffReadOnly`] pointer-stability contract
/// while an SQE is in flight, and the future returns the chain alongside the
/// I/O result so callers can recover or reuse the original buffers.
///
/// # Example
/// ```
/// use flowio::runtime::buffer::iobuffvec::IoBuffReadOnlyVec;
///
/// let chain: IoBuffReadOnlyVec<Vec<u8>, 2> =
///     IoBuffReadOnlyVec::from_array([b"ab".to_vec(), b"cd".to_vec()]);
/// assert_eq!(chain.segments(), 2);
/// assert_eq!(chain.checked_len(), Some(4));
/// assert_eq!(chain.len(), 4);
/// ```
pub struct IoBuffReadOnlyVec<B: IoBuffReadOnly, const N: usize> {
    /// Inline array of read-only buffer handles. Only indices `0..count` are
    /// initialized.
    buffers: [MaybeUninit<B>; N],
    /// Number of initialized read-only segments currently stored.
    count: usize,
}

impl<B: IoBuffReadOnly, const N: usize> Default for IoBuffReadOnlyVec<B, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<B: IoBuffReadOnly, const N: usize> From<[B; N]> for IoBuffReadOnlyVec<B, N> {
    fn from(buffers: [B; N]) -> Self {
        Self::from_array(buffers)
    }
}

impl<B: IoBuffReadOnly, const N: usize> IoBuffReadOnlyVec<B, N> {
    /// Creates an empty generic read-only vectored chain.
    pub fn new() -> Self {
        Self {
            buffers: uninit_inline_storage(),
            count: 0,
        }
    }

    /// Creates a fully-initialized chain from an array of read-only buffers.
    pub fn from_array(buffers: [B; N]) -> Self {
        Self {
            buffers: inline_storage_from_array(buffers),
            count: N,
        }
    }

    /// Returns the number of segments currently in the chain.
    #[inline(always)]
    pub fn segments(&self) -> usize {
        self.count
    }

    /// Returns the maximum number of segments this chain can hold.
    #[inline(always)]
    pub fn capacity(&self) -> usize {
        N
    }

    /// Adds a read-only buffer segment to the chain.
    ///
    /// Returns [`PushError`] containing `buf` if the chain is at capacity.
    pub fn push(&mut self, buf: B) -> Result<(), PushError<B>> {
        try_push_inline(&mut self.buffers, &mut self.count, buf)
    }

    /// Returns a reference to the buffer segment at the given index.
    #[inline(always)]
    pub fn get(&self, index: usize) -> Result<&B, IoBuffError> {
        get_inline(&self.buffers, self.count, index)
    }

    /// Returns a mutable reference to the buffer segment at the given index.
    ///
    /// This is intended for chain construction or recovery before submission
    /// and after completion. While a vectored write future owns the chain,
    /// callers cannot access the segments.
    #[inline(always)]
    pub fn get_mut(&mut self, index: usize) -> Result<&mut B, IoBuffError> {
        get_inline_mut(&mut self.buffers, self.count, index)
    }

    /// Returns the total number of readable bytes across all segments.
    ///
    /// If the exact aggregate cannot be represented by `usize`, this returns
    /// `usize::MAX`. Use [`Self::checked_len`] when overflow must be
    /// distinguished from that exact boundary value.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.checked_len().unwrap_or(usize::MAX)
    }

    /// Returns the exact total readable length, or `None` on `usize` overflow.
    #[inline(always)]
    pub fn checked_len(&self) -> Option<usize> {
        checked_readable_len(iter_inline(&self.buffers, self.count))
    }

    /// Returns `true` if the chain contains zero readable bytes.
    ///
    /// A chain may still contain zero-length segments; use [`Self::segments`]
    /// to query segment presence.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        iter_inline(&self.buffers, self.count).all(IoBuffReadOnly::is_empty)
    }

    /// Returns an iterator over the read-only buffer segments.
    pub fn iter(&self) -> impl Iterator<Item = &B> {
        iter_inline(&self.buffers, self.count)
    }
}

impl<B: IoBuffReadOnly, const N: usize> Drop for IoBuffReadOnlyVec<B, N> {
    fn drop(&mut self) {
        unsafe { drop_initialized_inline(&mut self.buffers, self.count) };
    }
}

impl<B: IoBuffReadOnly, const N: usize> IntoIterator for IoBuffReadOnlyVec<B, N> {
    type Item = B;
    type IntoIter = IoBuffReadOnlyVecIntoIter<B, N>;

    fn into_iter(mut self) -> Self::IntoIter {
        // Transfer ownership of the storage array with `ptr::read`, then clear
        // this chain's initialized count so its Drop does not release the
        // segments now owned by the iterator.
        let buffers = unsafe { std::ptr::read(&self.buffers) };
        let count = self.count;
        self.count = 0;

        IoBuffReadOnlyVecIntoIter {
            buffers,
            index: 0,
            count,
        }
    }
}

/// Consuming iterator over a generic read-only vectored chain.
///
/// # Example
/// ```
/// use flowio::runtime::buffer::iobuffvec::IoBuffReadOnlyVec;
///
/// let chain: IoBuffReadOnlyVec<Vec<u8>, 2> =
///     [b"ab".to_vec(), b"cd".to_vec()].into();
/// let pieces: Vec<Vec<u8>> = chain.into_iter().collect();
/// assert_eq!(pieces, vec![b"ab".to_vec(), b"cd".to_vec()]);
/// ```
pub struct IoBuffReadOnlyVecIntoIter<B: IoBuffReadOnly, const N: usize> {
    /// Inline array moved out of the source chain. Entries in `index..count`
    /// remain initialized until yielded or dropped.
    buffers: [MaybeUninit<B>; N],
    /// Next initialized segment to yield.
    index: usize,
    /// Total initialized segments moved from the source chain.
    count: usize,
}

impl<B: IoBuffReadOnly, const N: usize> Iterator for IoBuffReadOnlyVecIntoIter<B, N> {
    type Item = B;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.count {
            return None;
        }

        let item = unsafe { self.buffers[self.index].assume_init_read() };
        self.index += 1;
        Some(item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.count - self.index;
        (remaining, Some(remaining))
    }
}

impl<B: IoBuffReadOnly, const N: usize> ExactSizeIterator for IoBuffReadOnlyVecIntoIter<B, N> {}

impl<B: IoBuffReadOnly, const N: usize> Drop for IoBuffReadOnlyVecIntoIter<B, N> {
    fn drop(&mut self) {
        unsafe {
            drop_initialized_inline(&mut self.buffers[self.index..], self.count - self.index)
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_length_sum_accepts_exact_max_and_rejects_overflow() {
        assert_eq!(
            checked_length_sum([isize::MAX as usize, isize::MAX as usize, 1]),
            Some(usize::MAX)
        );
        assert_eq!(
            checked_length_sum([isize::MAX as usize, isize::MAX as usize, 2]),
            None
        );
        assert_eq!(checked_length_sum([0, 0]), Some(0));
    }

    #[test]
    fn checked_iovec_sizing_accepts_exact_max_rejects_overflow_and_compacts_zeros() {
        let half = isize::MAX as usize;
        assert_eq!(
            checked_iovec_count_and_length_sum([half, 0, half, 1]),
            Some((3, usize::MAX))
        );
        assert_eq!(checked_iovec_count_and_length_sum([half, half, 2]), None);
        assert_eq!(checked_iovec_count_and_length_sum([0, 7, 0]), Some((1, 7)));
    }

    #[test]
    fn mutable_chain_checked_lengths_match_active_and_writable_bytes() {
        let mut first = IoBuffMut::new(0, 8, 0).expect("first segment allocation failed");
        first
            .payload_append(b"abc")
            .expect("first segment initialization failed");
        let second = IoBuffMut::new(0, 5, 0).expect("second segment allocation failed");
        let chain = IoBuffVecMut::from_array([first, second]);

        assert_eq!(chain.checked_len(), Some(3));
        assert_eq!(chain.len(), 3);
        assert_eq!(chain.checked_writable_len(), Some(10));
        assert_eq!(chain.writable_len(), 10);
        assert!(!chain.is_empty());
    }

    #[test]
    fn read_iovec_fill_compacts_nonempty_segments_and_preserves_tail() {
        let mut full = IoBuffMut::new(0, 4, 0).expect("full segment allocation failed");
        full.payload_append(b"full")
            .expect("full segment initialization failed");
        let mut writable = IoBuffMut::new(0, 8, 0).expect("writable segment allocation failed");
        let zero = IoBuffMut::new(0, 0, 0).expect("zero segment allocation failed");
        let mut partial = IoBuffMut::new(0, 6, 0).expect("partial segment allocation failed");
        partial
            .payload_append(b"ok")
            .expect("partial segment initialization failed");

        let writable_ptr = writable.as_mut_ptr();
        let partial_ptr = partial.as_mut_ptr();
        let mut chain = IoBuffVecMut::from_array([full, writable, zero, partial]);
        assert_eq!(
            chain.checked_read_iovec_count_and_writable_len(),
            Some((2, 12))
        );

        let poison = libc::iovec {
            iov_base: std::ptr::NonNull::<u8>::dangling().as_ptr().cast(),
            iov_len: usize::MAX,
        };
        let mut scratch: [MaybeUninit<libc::iovec>; 4] =
            std::array::from_fn(|_| MaybeUninit::new(poison));
        let (iov_count, writable_len) = chain
            .fill_read_iovecs_and_writable_len(&mut scratch[..2])
            .expect("read iovec materialization failed");

        assert_eq!((iov_count, writable_len), (2, 12));
        let first = unsafe { scratch[0].assume_init_ref() };
        assert_eq!(first.iov_base, writable_ptr.cast());
        assert_eq!(first.iov_len, 8);
        let second = unsafe { scratch[1].assume_init_ref() };
        assert_eq!(second.iov_base, partial_ptr.cast());
        assert_eq!(second.iov_len, 4);
        for slot in &scratch[2..] {
            let untouched = unsafe { slot.assume_init_ref() };
            assert_eq!(untouched.iov_base, poison.iov_base);
            assert_eq!(untouched.iov_len, poison.iov_len);
        }
        assert_eq!(chain.segments(), 4);
        assert_eq!(
            chain.get(0).expect("full segment missing").payload_bytes(),
            b"full"
        );
        assert_eq!(
            chain
                .get(3)
                .expect("partial segment missing")
                .payload_bytes(),
            b"ok"
        );
    }

    #[test]
    fn read_iovec_fill_rejects_short_scratch_without_panicking() {
        let mut first = IoBuffMut::new(0, 8, 0).expect("first segment allocation failed");
        let second = IoBuffMut::new(0, 8, 0).expect("second segment allocation failed");
        let first_ptr = first.as_mut_ptr();
        let mut chain = IoBuffVecMut::from_array([first, second]);
        let poison = libc::iovec {
            iov_base: std::ptr::NonNull::<u8>::dangling().as_ptr().cast(),
            iov_len: usize::MAX,
        };
        let mut scratch = [MaybeUninit::new(poison)];

        let err = chain
            .fill_read_iovecs_and_writable_len(&mut scratch)
            .expect_err("short read iovec scratch must be rejected");

        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(err.to_string(), READ_IOVEC_SHAPE_CHANGED);
        let initialized = unsafe { scratch[0].assume_init_ref() };
        assert_eq!(initialized.iov_base, first_ptr.cast());
        assert_eq!(initialized.iov_len, 8);
        assert_eq!(
            chain.checked_read_iovec_count_and_writable_len(),
            Some((2, 16))
        );
    }
}
