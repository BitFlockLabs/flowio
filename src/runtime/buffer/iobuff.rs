//! Buffer traits and types for runtime I/O operations.
//!
//! The buffer system provides four core buffer types:
//!
//! - [`IoBuffMut`] — mutable, exclusively owned. Used for recv/read operations.
//! - [`IoBuff`] — frozen (immutable), reference-counted, cheaply clonable.
//!   Used for send/write operations where the same data goes to multiple destinations.
//! - [`IoBuffView`] — read-only byte subview. Useful for parsing/slicing
//!   without keeping region structure.
//! - [`IoBuffOwnedView`] — consuming read-only byte subview that owns the
//!   original [`IoBuff`] without taking another reference.
//!
//! Every buffer has three regions: headroom (for prepending protocol headers),
//! payload (the main data region), and tailroom (for appending trailers).
//! The total backing allocation is `headroom + payload + tailroom` bytes.
//!
//! Buffers can be created individually on the heap via [`IoBuffMut::new`], or
//! from a pool via [`super::pool::IoBuffPool`] for zero-alloc fast-path operation.
//!
//! All buffer types implement the [`IoBuffReadOnly`] and/or [`IoBuffReadWrite`] traits,
//! which are the generic interface used by all transport operations.
//!
//! `IoBuff` and pool-backed buffers use non-atomic bookkeeping and are
//! therefore intentionally thread-local today. Cross-thread transfer is
//! deferred to a later design.
//!
//! # Fast-Path Guidance
//!
//! Best fast-path choices:
//! - For fixed-shape steady-state transport I/O, prefer
//!   [`super::pool::IoBuffPool`] plus [`IoBuffMut`]. That is the intended
//!   zero-allocation buffer fast path after warmup.
//! - Freeze into [`IoBuff`] when data should be reused or fanned out to
//!   multiple send paths without copying.
//!
//! Prefer not to use on the fast path:
//! - Prefer not to use [`IoBuffMut::new`] for fixed-shape steady-state
//!   buffers. It is the convenience API for heap-backed buffers and is a good
//!   choice for setup code, tests, and variable-size workloads instead.
//! - Prefer not to replace the primary send/receive buffer with [`IoBuffView`]
//!   when the structured headroom/payload/tailroom layout still matters. Use
//!   [`IoBuffView`] for parsing and slicing convenience; keep the original
//!   [`IoBuff`] or [`IoBuffMut`] for transport reuse and later mutation.
//!
//! The examples below often use [`IoBuffMut::new`] because it keeps the code
//! short. On the fixed-shape hot path, prefer pool-backed allocation from
//! [`super::pool::IoBuffPool`].
//!
//! # Provided trait implementations
//!
//! | Type | [`IoBuffReadOnly`] | [`IoBuffReadWrite`] |
//! |------|---------------------|----------------------|
//! | [`IoBuff`] | yes | — |
//! | [`IoBuffOwnedView`] | yes | — |
//! | [`IoBuffMut`] | yes | yes |
//! | `Vec<u8>` | yes | yes |
//! | `Box<[u8]>` | yes | yes |
//! | `&'static [u8]` | yes | — |

use super::pool::IoBuffPoolInner;
use std::cell::Cell;
use std::ops::RangeBounds;
use std::ptr::NonNull;

// ============================================================================
// Error type
// ============================================================================

/// Error codes returned by buffer operations.
///
/// # Example
/// ```
/// use flowio::runtime::buffer::{IoBuffError, IoBuffMut};
///
/// let err = IoBuffMut::new(0, 4, 0)
///     .unwrap()
///     .freeze()
///     .into_owned_view(5..6);
/// assert!(matches!(err, Err((_buf, IoBuffError::SliceOutOfBounds))));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IoBuffError {
    /// Heap-backed buffer layout overflowed addressable memory.
    LayoutOverflow,
    /// Headroom region does not have enough space for the prepend.
    HeadroomFull,
    /// Payload region does not have enough space for the append or set_len.
    PayloadFull,
    /// Vectored buffer chain is already at segment capacity.
    ChainFull,
    /// Pool/provider failed to allocate backing storage for another buffer.
    AllocFailed,
    /// Buffer pool allocation requested before the pool was initialized.
    PoolNotInitialized,
    /// Payload can no longer grow because tailroom bytes have been written.
    PayloadSealed,
    /// Tailroom region does not have enough space for the append.
    TailroomFull,
    /// The requested tailroom-to-payload transfer exceeds tailroom capacity.
    TailroomInsufficient,
    /// The advance count exceeds the active window length.
    AdvanceOutOfBounds,
    /// Cannot convert to mutable: one or more segments have refcount > 1.
    SharedBuffer,
    /// Requested slice range is outside the readable window.
    SliceOutOfBounds,
    /// Requested segment index is outside the initialized chain length.
    IndexOutOfBounds,
}

#[inline(always)]
fn resolve_slice_bounds(
    range: impl RangeBounds<usize>,
    len: usize,
) -> Result<(usize, usize), IoBuffError> {
    let start = match range.start_bound() {
        std::ops::Bound::Included(&s) => s,
        std::ops::Bound::Excluded(&s) => s.checked_add(1).ok_or(IoBuffError::SliceOutOfBounds)?,
        std::ops::Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        std::ops::Bound::Included(&e) => e.checked_add(1).ok_or(IoBuffError::SliceOutOfBounds)?,
        std::ops::Bound::Excluded(&e) => e,
        std::ops::Bound::Unbounded => len,
    };
    if start > end || end > len {
        return Err(IoBuffError::SliceOutOfBounds);
    }

    Ok((start, end))
}

#[inline(always)]
fn payload_front_consumed(
    headroom_capacity: usize,
    payload_capacity: usize,
    offset: usize,
) -> usize {
    offset
        .saturating_sub(headroom_capacity)
        .min(payload_capacity)
}

#[inline(always)]
fn payload_start_offset(headroom_capacity: usize, payload_capacity: usize, offset: usize) -> usize {
    headroom_capacity + payload_front_consumed(headroom_capacity, payload_capacity, offset)
}

#[inline(always)]
fn payload_capacity_after_advance(
    headroom_capacity: usize,
    payload_capacity: usize,
    offset: usize,
) -> usize {
    payload_capacity - payload_front_consumed(headroom_capacity, payload_capacity, offset)
}

// ============================================================================
// Traits
// ============================================================================

/// A buffer that can be read by the runtime for write/send operations.
///
/// The runtime uses `as_ptr()` and `len()` to obtain the data to send over
/// the network.  The pointer must remain valid and stable across await points.
///
/// # Safety
///
/// `as_ptr()` must return a pointer that remains valid for at least `len()`
/// bytes for the entire lifetime of the buffer value.  The pointer must not
/// be invalidated by moves of the buffer value (i.e. the backing storage must
/// be heap- or pool-allocated, not inline).
///
/// # Example
/// ```
/// use flowio::runtime::buffer::IoBuffReadOnly;
///
/// fn readable_len<B: IoBuffReadOnly>(buffer: &B) -> usize {
///     buffer.len()
/// }
///
/// assert_eq!(readable_len(&b"payload".to_vec()), 7);
/// ```
pub unsafe trait IoBuffReadOnly: Unpin + 'static {
    /// Returns a raw pointer to the first readable byte exposed by this buffer.
    fn as_ptr(&self) -> *const u8;

    /// Returns the total number of readable bytes in the active window.
    fn len(&self) -> usize;

    /// Returns `true` if the active window contains no readable bytes.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A buffer that can be written into by the runtime for read/recv operations.
///
/// The runtime uses `as_mut_ptr()` and `writable_len()` to determine where
/// and how much the kernel can write. After a kernel read completes, the
/// runtime calls `set_written_len()` to publish the initialized bytes in the
/// returned buffer.
///
/// Implementations choose what their writable region means. [`IoBuffMut`]
/// writes into its unwritten payload region and updates structured payload
/// length. `Vec<u8>` exposes its full allocation starting at index 0 and
/// `set_written_len()` replaces its logical length with the number of bytes
/// written there. `Box<[u8]>` exposes its fixed slice and ignores
/// `set_written_len()` because there is no separate logical length to update.
///
/// # Safety
///
/// `as_mut_ptr()` must return a pointer that remains valid and writable for
/// at least `writable_len()` bytes for the entire lifetime of the buffer
/// value.  The pointer must not be invalidated by moves of the buffer value.
///
/// # Example
/// ```
/// use flowio::runtime::buffer::IoBuffReadWrite;
///
/// fn writable_len<B: IoBuffReadWrite>(buffer: &B) -> usize {
///     buffer.writable_len()
/// }
///
/// assert_eq!(writable_len(&vec![0u8; 16]), 16);
/// ```
pub unsafe trait IoBuffReadWrite: Unpin + 'static {
    /// Returns a raw pointer to the first writable byte exposed by this
    /// buffer.
    fn as_mut_ptr(&mut self) -> *mut u8;

    /// Returns the number of bytes available for writing through
    /// [`IoBuffReadWrite::as_mut_ptr`].
    fn writable_len(&self) -> usize;

    /// Called by the runtime after the kernel has written data into the
    /// buffer.
    ///
    /// Structured buffers use this to set the total payload length (absolute,
    /// not additive). `Vec<u8>` uses this as a replacement length for bytes
    /// written from index 0; existing contents beyond `len` are logically
    /// discarded. Fixed-size flat buffers such as `Box<[u8]>` may treat this
    /// as a no-op because their exposed slice length is not adjusted by
    /// FlowIO.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that the first `len` bytes of the payload
    /// region have been initialized.
    unsafe fn set_written_len(&mut self, len: usize);
}

// ============================================================================
// IoBuffHeader — per-buffer metadata (heap- or pool-allocated)
// ============================================================================

/// Metadata header for the backing storage shared by [`IoBuff`] and
/// [`IoBuffMut`] handles.
///
/// Allocated with a flexible trailing byte array: the actual allocation is
/// `size_of::<IoBuffHeader>() + headroom_capacity + payload_capacity +
/// tailroom_capacity` bytes.  The data region (headroom + payload + tailroom)
/// starts immediately after the header fields in memory.
#[repr(C)]
pub(crate) struct IoBuffHeader {
    /// Non-atomic reference count for single-threaded sharing.
    /// Incremented by `IoBuff::clone()`, decremented by drop.
    /// When it reaches zero, the backing allocation is freed (heap) or
    /// returned to the pool.  Uses `Cell` because `clone()`/`drop()`
    /// take `&self`.
    pub(crate) refcount: Cell<usize>,
    /// Headroom region size in bytes.  Space reserved before the payload
    /// for prepending protocol headers via `headroom_prepend()`.
    pub(crate) headroom_capacity: usize,
    /// Payload region size in bytes.  The main writable region where
    /// application data is stored.  May grow via
    /// `payload_extend_from_tailroom()` at the expense of tailroom.
    pub(crate) payload_capacity: usize,
    /// Tailroom region size in bytes.  Space reserved after the payload
    /// for appending protocol trailers via `tailroom_append()`.  May
    /// shrink when payload borrows from it.
    pub(crate) tailroom_capacity: usize,
    /// Owning pool pointer.  Non-null if this buffer was allocated from
    /// an [`super::pool::IoBuffPool`]; the buffer is returned to the pool's
    /// free list when the last reference drops. Null for heap-allocated
    /// buffers, which are freed via the global allocator.
    pub(crate) pool: *mut IoBuffPoolInner,
    // Flexible trailing data:
    // [u8; headroom_capacity + payload_capacity + tailroom_capacity]
    // follows immediately after this header in memory.
}

impl IoBuffHeader {
    /// Returns the total size of the trailing data region
    /// (headroom + payload + tailroom).
    #[inline(always)]
    fn total_capacity(&self) -> usize {
        self.headroom_capacity + self.payload_capacity + self.tailroom_capacity
    }

    /// Returns a pointer to the first byte of the trailing data region
    /// (the start of the headroom area), given a raw header pointer.
    ///
    /// Takes `*mut Self` rather than `&self` so callers can derive the data
    /// pointer without first materializing a `&IoBuffHeader` that would alias
    /// the bytes following the header.
    ///
    /// # Safety
    ///
    /// `header` must point to a live, properly aligned `IoBuffHeader` whose
    /// backing allocation includes the full trailing data region
    /// (`headroom + payload + tailroom` bytes) immediately after the header,
    /// as established by heap or pool allocation. The returned pointer is only
    /// valid for `total_capacity()` bytes.
    #[inline(always)]
    unsafe fn headroom_ptr_from_raw(header: *mut Self) -> *mut u8 {
        unsafe { (header as *mut u8).add(std::mem::size_of::<Self>()) }
    }

    fn try_layout(total_data_capacity: usize) -> Result<std::alloc::Layout, IoBuffError> {
        let total_size = std::mem::size_of::<Self>()
            .checked_add(total_data_capacity)
            .ok_or(IoBuffError::LayoutOverflow)?;

        std::alloc::Layout::from_size_align(total_size, std::mem::align_of::<Self>())
            .map_err(|_| IoBuffError::LayoutOverflow)
    }

    fn layout(total_data_capacity: usize) -> std::alloc::Layout {
        // Internal callers only use this after successful checked geometry
        // construction; keep the infallible form for deallocation paths.
        let result = Self::try_layout(total_data_capacity);
        debug_assert!(result.is_ok(), "IoBuffHeader layout invariant broken");
        unsafe { result.unwrap_unchecked() }
    }

    /// Allocates an `IoBuffHeader` on the heap with the given region sizes.
    fn heap_alloc(
        headroom: usize,
        payload: usize,
        tailroom: usize,
    ) -> Result<NonNull<Self>, IoBuffError> {
        let total = headroom
            .checked_add(payload)
            .and_then(|sum| sum.checked_add(tailroom))
            .ok_or(IoBuffError::LayoutOverflow)?;
        let layout = Self::try_layout(total)?;
        let ptr = unsafe { std::alloc::alloc(layout) } as *mut Self;
        let header = NonNull::new(ptr).ok_or(IoBuffError::AllocFailed)?;
        unsafe {
            (*header.as_ptr()).refcount = Cell::new(1);
            (*header.as_ptr()).headroom_capacity = headroom;
            (*header.as_ptr()).payload_capacity = payload;
            (*header.as_ptr()).tailroom_capacity = tailroom;
            (*header.as_ptr()).pool = std::ptr::null_mut();
        }
        Ok(header)
    }

    #[inline(always)]
    fn retain(&self) {
        self.refcount.set(self.refcount.get() + 1);
    }

    /// Decrements refcount and returns true if this was the last reference.
    #[inline(always)]
    fn release(&self) -> bool {
        let prev = self.refcount.get();
        debug_assert!(prev > 0, "IoBuffHeader refcount underflow");
        self.refcount.set(prev - 1);
        prev == 1
    }

    #[inline(always)]
    fn ref_count(&self) -> usize {
        self.refcount.get()
    }

    /// Frees the header allocation (heap or pool).
    ///
    /// # Safety
    /// Must only be called once, when refcount reaches zero.
    unsafe fn dealloc(ptr: NonNull<Self>) {
        let header = unsafe { ptr.as_ref() };
        let pool = header.pool;
        if pool.is_null() {
            // Heap-allocated: free via global allocator.
            let total = header.total_capacity();
            let layout = Self::layout(total);
            unsafe { std::alloc::dealloc(ptr.as_ptr() as *mut u8, layout) };
        } else {
            // Pool-allocated: return to pool's free list.
            unsafe { IoBuffPoolInner::release_buffer(pool, ptr.as_ptr() as *mut u8) };
        }
    }
}

// ============================================================================
// IoBuffMut — mutable, exclusively owned buffer
// ============================================================================

/// Mutable buffer with exclusive ownership.
///
/// `IoBuffMut` is the primary buffer type for recv/read operations.  It
/// implements both [`IoBuffReadOnly`] and [`IoBuffReadWrite`].
///
/// Every buffer has three regions: headroom (for prepending protocol headers),
/// payload (the main writable region), and tailroom (for appending trailers).
///
/// Created via [`IoBuffMut::new`] (heap) or returned by
/// [`super::pool::IoBuffPool::alloc`] (fast-path pool-backed reuse).
///
/// For fixed-shape steady-state workloads, the pool-backed path is preferred.
/// [`IoBuffMut::new`] is the simpler alternative when shapes vary or when the
/// allocation cost is not on the hot path.
///
/// # Example
/// ```
/// use flowio::runtime::buffer::IoBuffMut;
///
/// // Simple buffer: no headroom/tailroom, 4096 bytes payload.
/// let mut buf = IoBuffMut::new(0, 4096, 0).unwrap();
/// buf.payload_append(b"hello").unwrap();
/// assert_eq!(buf.payload_bytes(), b"hello");
/// assert_eq!(buf.payload_len(), 5);
///
/// // Buffer with headroom for protocol headers.
/// let mut framed = IoBuffMut::new(16, 4096, 0).unwrap();
/// framed.payload_append(b"payload").unwrap();
/// framed.headroom_prepend(b"HDR:").unwrap();
/// assert_eq!(framed.bytes(), b"HDR:payload");
/// ```
pub struct IoBuffMut {
    /// Pointer to the backing `IoBuffHeader` + data allocation (heap or pool).
    /// The header is followed by a contiguous byte array of
    /// `headroom + payload + tailroom` bytes.
    pub(crate) header: NonNull<IoBuffHeader>,
    /// Byte index into the data region where the active window starts.
    /// Initially `headroom_capacity` (the start of the payload region).
    /// Decreases when `headroom_prepend()` writes headers before the
    /// payload.  Increases when `advance()` consumes bytes from the front.
    pub(crate) offset: usize,
    /// Number of bytes written to the payload region.  This is the
    /// logical payload length, not the capacity.  Increased by
    /// `payload_append()` and `payload_set_len()`.
    pub(crate) payload_len: usize,
    /// Number of bytes written to the tailroom region.  Tailroom data
    /// is stored immediately after the last payload byte to keep the
    /// active window (headroom + payload + tailroom) contiguous.
    pub(crate) tailroom_len: usize,
}

impl IoBuffMut {
    /// Creates a new heap-allocated mutable buffer with the given region sizes.
    ///
    /// - `headroom` — reserved bytes before the payload for prepending headers.
    /// - `payload` — main writable region size.
    /// - `tailroom` — reserved bytes after the payload for appending trailers.
    ///
    /// The buffer starts with offset positioned at the beginning of the payload
    /// region and len = 0.
    pub fn new(headroom: usize, payload: usize, tailroom: usize) -> Result<Self, IoBuffError> {
        let header = IoBuffHeader::heap_alloc(headroom, payload, tailroom)?;
        Ok(Self {
            header,
            offset: headroom,
            payload_len: 0,
            tailroom_len: 0,
        })
    }

    /// Returns the number of headroom bytes that are part of the active
    /// window.  After `advance()` consumes headroom bytes, this decreases.
    #[inline(always)]
    fn headroom_len(&self) -> usize {
        let hdr_cap = unsafe { self.header.as_ref().headroom_capacity };
        hdr_cap.saturating_sub(self.offset)
    }

    /// Returns the total number of active bytes (headroom written + payload
    /// written + tailroom written).
    #[inline(always)]
    fn active_len(&self) -> usize {
        self.headroom_len() + self.payload_len + self.tailroom_len
    }

    #[inline(always)]
    fn active_end_offset(&self) -> usize {
        self.offset + self.active_len()
    }

    // -----------------------------------------------------------------------
    // Headroom operations
    // -----------------------------------------------------------------------

    /// Returns the original headroom region size configured at allocation.
    #[inline(always)]
    pub fn headroom_capacity(&self) -> usize {
        unsafe { self.header.as_ref().headroom_capacity }
    }

    /// Returns the configured pre-payload slack still available for
    /// prepending before the active window.
    ///
    /// After [`advance()`](Self::advance) consumes bytes into the payload
    /// region, the active window can no longer represent newly-prepended
    /// bytes in the consumed payload prefix as headroom, so this returns `0`.
    #[inline(always)]
    pub fn headroom_remaining(&self) -> usize {
        let headroom_capacity = unsafe { self.header.as_ref().headroom_capacity };
        if self.offset <= headroom_capacity {
            self.offset
        } else {
            0
        }
    }

    #[inline(always)]
    fn payload_start_offset(&self) -> usize {
        let hdr = unsafe { self.header.as_ref() };
        payload_start_offset(hdr.headroom_capacity, hdr.payload_capacity, self.offset)
    }

    #[inline(always)]
    fn payload_capacity_after_advance(&self) -> usize {
        let hdr = unsafe { self.header.as_ref() };
        payload_capacity_after_advance(hdr.headroom_capacity, hdr.payload_capacity, self.offset)
    }

    /// Prepends bytes into the headroom region before the current active
    /// window.  The offset moves backward, expanding the active window.
    ///
    /// Returns an error if the headroom does not have enough space.
    pub fn headroom_prepend(&mut self, data: &[u8]) -> Result<(), IoBuffError> {
        if data.len() > self.headroom_remaining() {
            return Err(IoBuffError::HeadroomFull);
        }
        self.offset -= data.len();
        unsafe {
            let dst = IoBuffHeader::headroom_ptr_from_raw(self.header.as_ptr()).add(self.offset);
            std::ptr::copy_nonoverlapping(data.as_ptr(), dst, data.len());
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Payload operations
    // -----------------------------------------------------------------------

    /// Returns the current payload region capacity.  This may be larger than
    /// the original if `payload_extend_from_tailroom()` was called.
    #[inline(always)]
    pub fn payload_capacity(&self) -> usize {
        unsafe { self.header.as_ref().payload_capacity }
    }

    /// Returns the number of bytes written to the payload region.
    #[inline(always)]
    pub fn payload_len(&self) -> usize {
        self.payload_len
    }

    /// Returns the number of bytes left to write in the payload region.
    ///
    /// Once tailroom bytes have been written, payload growth is sealed to keep
    /// the active window contiguous. In that state this returns `0`, even if
    /// the backing payload capacity has spare physical space.
    #[inline(always)]
    pub fn payload_remaining(&self) -> usize {
        if self.tailroom_len != 0 {
            return 0;
        }
        self.payload_capacity_after_advance()
            .saturating_sub(self.payload_len)
    }

    /// Returns `true` if no bytes have been written to the payload region.
    #[inline(always)]
    pub fn payload_is_empty(&self) -> bool {
        self.payload_len == 0
    }

    /// Returns the written payload as a read-only byte slice.
    #[inline(always)]
    pub fn payload_bytes(&self) -> &[u8] {
        unsafe {
            let ptr = IoBuffHeader::headroom_ptr_from_raw(self.header.as_ptr())
                .add(self.payload_start_offset());
            std::slice::from_raw_parts(ptr, self.payload_len)
        }
    }

    /// Returns the written payload as a mutable byte slice.
    #[inline(always)]
    pub fn payload_bytes_mut(&mut self) -> &mut [u8] {
        unsafe {
            let ptr = IoBuffHeader::headroom_ptr_from_raw(self.header.as_ptr())
                .add(self.payload_start_offset());
            std::slice::from_raw_parts_mut(ptr, self.payload_len)
        }
    }

    /// Returns the unwritten portion of the payload region as a mutable
    /// slice.  After writing directly into this slice, call
    /// [`payload_set_len()`](Self::payload_set_len) with the new total
    /// payload length (absolute, not additive) to update the buffer state.
    ///
    /// # Example
    /// ```
    /// use flowio::runtime::buffer::IoBuffMut;
    ///
    /// let mut buf = IoBuffMut::new(0, 64, 0).unwrap();
    /// let spare = buf.payload_unwritten_mut();
    /// spare[..5].copy_from_slice(b"hello");
    /// buf.payload_set_len(5).unwrap();
    /// assert_eq!(buf.payload_bytes(), b"hello");
    /// ```
    #[inline(always)]
    pub fn payload_unwritten_mut(&mut self) -> &mut [u8] {
        unsafe {
            let ptr = IoBuffHeader::headroom_ptr_from_raw(self.header.as_ptr())
                .add(self.payload_start_offset() + self.payload_len);
            std::slice::from_raw_parts_mut(ptr, self.payload_remaining())
        }
    }

    /// Appends bytes to the end of the written payload region.
    ///
    /// Returns an error if the payload does not have enough remaining space,
    /// or if tailroom data has already been written (because the tailroom
    /// sits immediately after the payload to keep the active window contiguous).
    pub fn payload_append(&mut self, data: &[u8]) -> Result<(), IoBuffError> {
        if self.tailroom_len != 0 && !data.is_empty() {
            return Err(IoBuffError::PayloadSealed);
        }
        if data.len() > self.payload_remaining() {
            return Err(IoBuffError::PayloadFull);
        }
        unsafe {
            let dst = IoBuffHeader::headroom_ptr_from_raw(self.header.as_ptr())
                .add(self.payload_start_offset() + self.payload_len);
            std::ptr::copy_nonoverlapping(data.as_ptr(), dst, data.len());
        }
        self.payload_len += data.len();
        Ok(())
    }

    /// Sets the total written payload length (absolute, not additive).
    ///
    /// This is typically called after writing directly into the pointer
    /// returned by [`payload_unwritten_mut()`](Self::payload_unwritten_mut)
    /// or after the runtime's kernel read fills the payload via
    /// [`IoBuffReadWrite::set_written_len()`].
    ///
    /// Returns an error if `new_len` exceeds the payload capacity, or if
    /// tailroom data has already been written and the requested length would
    /// change the payload size.
    pub fn payload_set_len(&mut self, new_len: usize) -> Result<(), IoBuffError> {
        if self.tailroom_len != 0 && new_len != self.payload_len {
            return Err(IoBuffError::PayloadSealed);
        }
        if new_len > self.payload_capacity_after_advance() {
            return Err(IoBuffError::PayloadFull);
        }
        self.payload_len = new_len;
        Ok(())
    }

    /// Extends the payload capacity by taking `amount` bytes from the
    /// tailroom region.  Any existing tailroom data in the taken region is
    /// logically discarded (tailroom_len is reset to 0).
    ///
    /// Returns an error if `amount` exceeds the current tailroom capacity.
    pub fn payload_extend_from_tailroom(&mut self, amount: usize) -> Result<(), IoBuffError> {
        let hdr = unsafe { self.header.as_mut() };
        if amount > hdr.tailroom_capacity {
            return Err(IoBuffError::TailroomInsufficient);
        }
        hdr.payload_capacity += amount;
        hdr.tailroom_capacity -= amount;
        self.tailroom_len = 0;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Tailroom operations
    // -----------------------------------------------------------------------

    /// Returns the current tailroom region capacity.
    #[inline(always)]
    pub fn tailroom_capacity(&self) -> usize {
        unsafe { self.header.as_ref().tailroom_capacity }
    }

    /// Returns the number of tailroom bytes still available for appending.
    ///
    /// After [`advance()`](Self::advance) consumes bytes into the tailroom
    /// region, those consumed trailer bytes stay outside the active window and
    /// are not reusable as append space.
    #[inline(always)]
    pub fn tailroom_remaining(&self) -> usize {
        let tailroom_region_end =
            self.payload_start_offset() + self.payload_len + self.tailroom_capacity();
        tailroom_region_end.saturating_sub(self.active_end_offset())
    }

    /// Appends bytes into the tailroom region immediately after the written
    /// payload.  The tailroom data is always contiguous with the payload,
    /// forming a single active window with any prepended headroom.
    ///
    /// Returns an error if the tailroom does not have enough space.
    pub fn tailroom_append(&mut self, data: &[u8]) -> Result<(), IoBuffError> {
        if data.len() > self.tailroom_remaining() {
            return Err(IoBuffError::TailroomFull);
        }
        // Tailroom is written right after the last payload byte to keep the
        // active window (headroom + payload + tailroom) contiguous.
        unsafe {
            let dst = IoBuffHeader::headroom_ptr_from_raw(self.header.as_ptr())
                .add(self.active_end_offset());
            std::ptr::copy_nonoverlapping(data.as_ptr(), dst, data.len());
        }
        self.tailroom_len += data.len();
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Full buffer operations
    // -----------------------------------------------------------------------

    /// Returns the full active window as a read-only byte slice.
    /// This includes any headroom written via `headroom_prepend()`, the
    /// written payload, and any tailroom written via `tailroom_append()`.
    /// This is the data that gets sent over the wire.
    #[inline(always)]
    pub fn bytes(&self) -> &[u8] {
        unsafe {
            let ptr = IoBuffHeader::headroom_ptr_from_raw(self.header.as_ptr()).add(self.offset);
            std::slice::from_raw_parts(ptr, self.active_len())
        }
    }

    /// Returns the full active window as a mutable byte slice.
    #[inline(always)]
    pub fn bytes_mut(&mut self) -> &mut [u8] {
        unsafe {
            let ptr = IoBuffHeader::headroom_ptr_from_raw(self.header.as_ptr()).add(self.offset);
            std::slice::from_raw_parts_mut(ptr, self.active_len())
        }
    }

    /// Returns the total number of active bytes (headroom written + payload
    /// written + tailroom written).
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.active_len()
    }

    /// Returns `true` if the active window contains no bytes.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.active_len() == 0
    }

    // -----------------------------------------------------------------------
    // Cursor operations
    // -----------------------------------------------------------------------

    /// Consumes `count` bytes from the front of the active window.
    /// The offset moves forward, shrinking the active window from the front.
    ///
    /// This is used after processing received data — for example, after
    /// parsing a protocol header from the front of the payload, `advance()`
    /// skips past it so the remaining data is at the front.
    ///
    /// # Example
    /// ```
    /// use flowio::runtime::buffer::IoBuffMut;
    ///
    /// let mut buf = IoBuffMut::new(0, 64, 0).unwrap();
    /// buf.payload_append(b"HDR:payload").unwrap();
    /// buf.advance(4).unwrap();  // skip past "HDR:"
    /// assert_eq!(buf.bytes(), b"payload");
    /// ```
    ///
    /// # Errors
    /// Returns [`IoBuffError::AdvanceOutOfBounds`] if `count` exceeds the
    /// active window length.
    pub fn advance(&mut self, count: usize) -> Result<(), IoBuffError> {
        if count > self.active_len() {
            return Err(IoBuffError::AdvanceOutOfBounds);
        }
        // Consume the active window in structural order: headroom, payload,
        // then tailroom.
        let headroom_len = self.headroom_len();
        let mut remaining = count;

        let headroom_consumed = remaining.min(headroom_len);
        remaining -= headroom_consumed;

        let payload_consumed = remaining.min(self.payload_len);
        remaining -= payload_consumed;

        let tailroom_consumed = remaining.min(self.tailroom_len);
        remaining -= tailroom_consumed;

        debug_assert!(
            remaining == 0,
            "advance({count}) left {remaining} bytes unconsumed despite bounds check"
        );

        self.offset += count;
        self.payload_len -= payload_consumed;
        self.tailroom_len -= tailroom_consumed;
        Ok(())
    }

    /// Resets the buffer to its initial state: offset = headroom_capacity,
    /// payload_len = 0, tailroom_len = 0.  All written data (headroom,
    /// payload, tailroom) is logically discarded.
    pub fn reset(&mut self) {
        let headroom = unsafe { self.header.as_ref().headroom_capacity };
        self.offset = headroom;
        self.payload_len = 0;
        self.tailroom_len = 0;
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /// Freezes this buffer, consuming it and returning an immutable [`IoBuff`].
    /// The backing storage is shared; no data is copied.
    pub fn freeze(self) -> IoBuff {
        let buf = IoBuff {
            header: self.header,
            offset: self.offset,
            payload_len: self.payload_len,
            tailroom_len: self.tailroom_len,
        };
        std::mem::forget(self);
        buf
    }
}

impl Drop for IoBuffMut {
    fn drop(&mut self) {
        let hdr = unsafe { self.header.as_ref() };
        if hdr.release() {
            unsafe { IoBuffHeader::dealloc(self.header) };
        }
    }
}

impl AsRef<[u8]> for IoBuffMut {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self.bytes()
    }
}

impl AsMut<[u8]> for IoBuffMut {
    #[inline(always)]
    fn as_mut(&mut self) -> &mut [u8] {
        self.bytes_mut()
    }
}

impl std::ops::Deref for IoBuffMut {
    type Target = [u8];

    #[inline(always)]
    fn deref(&self) -> &[u8] {
        self.bytes()
    }
}

impl std::ops::DerefMut for IoBuffMut {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut [u8] {
        self.bytes_mut()
    }
}

// ============================================================================
// IoBuff — frozen (immutable), reference-counted buffer
// ============================================================================

/// Frozen (immutable) buffer with reference-counted sharing.
///
/// `IoBuff` is cheaply clonable — each clone shares the same backing storage
/// and increments a non-atomic reference count.  Implements [`IoBuffReadOnly`]
/// only (no mutable access).
/// The frozen handle preserves structured headroom/payload/tailroom metadata,
/// so zero-copy `try_mut()` remains exact for full frozen buffers.
///
/// Created by calling [`IoBuffMut::freeze`] on a mutable buffer.
///
/// This is the right send-path representation when the same bytes need to be
/// reused or fanned out. It is not the receive fast path; use [`IoBuffMut`]
/// there.
///
/// # Example
/// ```
/// use flowio::runtime::buffer::IoBuffMut;
///
/// let mut buf = IoBuffMut::new(0, 64, 0).unwrap();
/// buf.payload_append(b"shared data").unwrap();
/// let frozen = buf.freeze();
/// let clone1 = frozen.clone();
/// let clone2 = frozen.clone();
/// assert_eq!(frozen.bytes(), b"shared data");
/// assert_eq!(clone1.bytes(), b"shared data");
/// assert_eq!(clone2.bytes(), b"shared data");
/// ```
pub struct IoBuff {
    /// Pointer to the shared `IoBuffHeader` + data allocation.  Multiple
    /// `IoBuff` handles can point to the same header (reference-counted).
    header: NonNull<IoBuffHeader>,
    /// Byte index into the backing data region where the active window starts.
    /// This preserves any prepended headroom bytes exactly as they existed
    /// when the mutable buffer was frozen.
    offset: usize,
    /// Number of active payload bytes preserved from the mutable source.
    payload_len: usize,
    /// Number of active trailer bytes preserved after the payload.
    tailroom_len: usize,
}

impl std::fmt::Debug for IoBuff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IoBuff")
            .field("len", &self.len())
            .field("bytes", &self.bytes())
            .finish()
    }
}

impl IoBuff {
    #[inline(always)]
    fn payload_start_offset(&self) -> usize {
        let hdr = unsafe { self.header.as_ref() };
        payload_start_offset(hdr.headroom_capacity, hdr.payload_capacity, self.offset)
    }

    /// Returns the number of headroom bytes in the active window.
    #[inline(always)]
    pub fn headroom_len(&self) -> usize {
        let hdr_cap = unsafe { self.header.as_ref().headroom_capacity };
        hdr_cap.saturating_sub(self.offset)
    }

    /// Returns the number of payload bytes in the active window.
    #[inline(always)]
    pub fn payload_len(&self) -> usize {
        self.payload_len
    }

    /// Returns the number of tailroom bytes in the active window.
    #[inline(always)]
    pub fn tailroom_len(&self) -> usize {
        self.tailroom_len
    }

    /// Returns the total number of bytes in the active window.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.headroom_len() + self.payload_len + self.tailroom_len
    }

    /// Returns `true` if the active window contains no bytes.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the full active window as a read-only byte slice.
    #[inline(always)]
    pub fn bytes(&self) -> &[u8] {
        unsafe {
            let ptr = IoBuffHeader::headroom_ptr_from_raw(self.header.as_ptr()).add(self.offset);
            std::slice::from_raw_parts(ptr, self.len())
        }
    }

    /// Returns the written payload as a read-only slice.
    #[inline(always)]
    pub fn payload_bytes(&self) -> &[u8] {
        unsafe {
            let ptr = IoBuffHeader::headroom_ptr_from_raw(self.header.as_ptr())
                .add(self.payload_start_offset());
            std::slice::from_raw_parts(ptr, self.payload_len)
        }
    }

    /// Returns a sub-range view of this buffer.  The new [`IoBuffView`] shares the
    /// same backing storage with an incremented refcount.  Zero-copy.
    ///
    /// # Errors
    /// Returns [`IoBuffError::SliceOutOfBounds`] if `range` is outside the
    /// active window.
    ///
    /// # Example
    /// ```
    /// use flowio::runtime::buffer::IoBuffMut;
    ///
    /// let mut buf = IoBuffMut::new(0, 64, 0).unwrap();
    /// buf.payload_append(b"Hello, World!").unwrap();
    /// let frozen = buf.freeze();
    /// let hello = frozen.slice(0..5).unwrap();
    /// assert_eq!(hello.bytes(), b"Hello");
    /// ```
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Result<IoBuffView, IoBuffError> {
        let (start, end) = resolve_slice_bounds(range, self.len())?;
        unsafe { self.header.as_ref().retain() };
        Ok(IoBuffView {
            header: self.header,
            offset: self.offset + start,
            len: end - start,
        })
    }

    /// Consumes this buffer and returns an owned read-only view over `range`.
    ///
    /// Unlike [`IoBuff::slice`], this does not increment the backing
    /// reference count because the returned [`IoBuffOwnedView`] owns the
    /// original `IoBuff` handle. Creation is O(1), zero-copy, and allocation
    /// free.
    ///
    /// On bounds error, the original buffer is returned with the error so the
    /// caller retains ownership.
    ///
    /// The returned view is a raw immutable byte range. It does not preserve
    /// headroom/payload/tailroom structure; call [`IoBuffOwnedView::into_inner`]
    /// to recover the original structured buffer.
    pub fn into_owned_view(
        self,
        range: impl RangeBounds<usize>,
    ) -> Result<IoBuffOwnedView, (IoBuff, IoBuffError)> {
        let (start, end) = match resolve_slice_bounds(range, self.len()) {
            Ok(bounds) => bounds,
            Err(err) => return Err((self, err)),
        };
        let offset = self.offset + start;
        Ok(IoBuffOwnedView {
            buffer: self,
            offset,
            len: end - start,
        })
    }

    /// Attempts to convert back to a mutable buffer.  Succeeds only if this
    /// is the sole reference (refcount == 1), making it a zero-copy operation.
    ///
    /// Returns `Err(self)` if the buffer is shared (refcount > 1).
    ///
    /// # Example
    /// ```
    /// use flowio::runtime::buffer::IoBuffMut;
    ///
    /// let mut buf = IoBuffMut::new(0, 64, 0).unwrap();
    /// buf.payload_append(b"data").unwrap();
    /// let frozen = buf.freeze();
    /// // Sole owner — try_mut succeeds (zero-copy).
    /// let mut unfrozen = frozen.try_mut().unwrap();
    /// unfrozen.payload_append(b"!").unwrap();
    /// ```
    pub fn try_mut(self) -> Result<IoBuffMut, IoBuff> {
        let hdr = unsafe { self.header.as_ref() };
        if hdr.ref_count() != 1 {
            return Err(self);
        }
        let buf = IoBuffMut {
            header: self.header,
            offset: self.offset,
            payload_len: self.payload_len,
            tailroom_len: self.tailroom_len,
        };
        std::mem::forget(self);
        Ok(buf)
    }

    /// Returns the reference count of the backing storage.
    #[inline(always)]
    pub(crate) fn ref_count(&self) -> usize {
        unsafe { self.header.as_ref().ref_count() }
    }

    /// Converts to a mutable buffer.  If this is the sole reference, the
    /// conversion is zero-copy.  If the buffer is shared (refcount > 1),
    /// the active data is copied into a new heap-allocated buffer
    /// (copy-on-write).
    ///
    /// # Example
    /// ```
    /// use flowio::runtime::buffer::IoBuffMut;
    ///
    /// let mut buf = IoBuffMut::new(0, 64, 0).unwrap();
    /// buf.payload_append(b"original").unwrap();
    /// let frozen = buf.freeze();
    /// let _clone = frozen.clone();  // refcount > 1
    /// // make_mut copies because buffer is shared.
    /// let exclusive = frozen.make_mut().unwrap();
    /// assert_eq!(exclusive.bytes(), b"original");
    /// ```
    pub fn make_mut(self) -> Result<IoBuffMut, IoBuffError> {
        match self.try_mut() {
            Ok(buf) => Ok(buf),
            Err(frozen) => {
                let hdr = unsafe { frozen.header.as_ref() };
                let mut new = IoBuffMut::new(
                    hdr.headroom_capacity,
                    hdr.payload_capacity,
                    hdr.tailroom_capacity,
                )?;
                if frozen.headroom_len() > 0 {
                    let head_len = frozen.headroom_len();
                    new.headroom_prepend(&frozen.bytes()[..head_len])?;
                }
                if frozen.payload_len > 0 {
                    let head_len = frozen.headroom_len();
                    new.payload_append(&frozen.bytes()[head_len..head_len + frozen.payload_len])?;
                }
                if frozen.tailroom_len > 0 {
                    let tail_start = frozen.headroom_len() + frozen.payload_len;
                    new.tailroom_append(&frozen.bytes()[tail_start..])?;
                }
                Ok(new)
            }
        }
    }
}

impl Clone for IoBuff {
    fn clone(&self) -> Self {
        unsafe { self.header.as_ref().retain() };
        IoBuff {
            header: self.header,
            offset: self.offset,
            payload_len: self.payload_len,
            tailroom_len: self.tailroom_len,
        }
    }
}

impl Drop for IoBuff {
    fn drop(&mut self) {
        let hdr = unsafe { self.header.as_ref() };
        if hdr.release() {
            unsafe { IoBuffHeader::dealloc(self.header) };
        }
    }
}

impl AsRef<[u8]> for IoBuff {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self.bytes()
    }
}

impl std::ops::Deref for IoBuff {
    type Target = [u8];

    #[inline(always)]
    fn deref(&self) -> &[u8] {
        self.bytes()
    }
}

/// Consuming read-only byte-range view over an [`IoBuff`].
///
/// `IoBuffOwnedView` owns the original frozen buffer and exposes only a
/// validated subrange of its active byte window. It is intended for parsers
/// that consume an `IoBuff` and need a narrowed immutable view without the
/// extra retain/release performed by [`IoBuffView`].
///
/// This type is O(1), zero-copy, allocation-free, and has no public unsafe API.
/// Internally it keeps the original `IoBuff` alive, so the backing allocation
/// remains valid for the lifetime of the view.
///
/// # Example
/// ```
/// use flowio::runtime::buffer::IoBuffMut;
///
/// let mut buf = IoBuffMut::new(2, 16, 2).unwrap();
/// buf.payload_append(b"hello").unwrap();
/// buf.headroom_prepend(b"H:").unwrap();
/// let frozen = buf.freeze();
///
/// let owned = frozen.into_owned_view(2..7).unwrap();
/// assert_eq!(owned.bytes(), b"hello");
///
/// let frozen = owned.into_inner();
/// assert_eq!(frozen.bytes(), b"H:hello");
/// ```
pub struct IoBuffOwnedView {
    /// Original structured buffer. Owning this handle keeps the backing
    /// allocation alive without incrementing its refcount.
    buffer: IoBuff,
    /// Byte offset of the view start within the shared data region.
    offset: usize,
    /// Length of the visible byte range.
    len: usize,
}

impl std::fmt::Debug for IoBuffOwnedView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IoBuffOwnedView")
            .field("len", &self.len)
            .field("bytes", &self.bytes())
            .finish()
    }
}

impl IoBuffOwnedView {
    /// Returns the number of bytes visible through this owned view.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` when this owned view contains no bytes.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the visible byte range.
    #[inline(always)]
    pub fn bytes(&self) -> &[u8] {
        unsafe {
            let ptr =
                IoBuffHeader::headroom_ptr_from_raw(self.buffer.header.as_ptr()).add(self.offset);
            std::slice::from_raw_parts(ptr, self.len)
        }
    }

    /// Recovers the original structured buffer.
    ///
    /// This is useful when a parser needs to inspect a narrowed range and then
    /// return to APIs such as [`IoBuff::try_mut`] or [`IoBuff::make_mut`].
    #[inline(always)]
    pub fn into_inner(self) -> IoBuff {
        self.buffer
    }
}

impl AsRef<[u8]> for IoBuffOwnedView {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self.bytes()
    }
}

impl std::ops::Deref for IoBuffOwnedView {
    type Target = [u8];

    #[inline(always)]
    fn deref(&self) -> &[u8] {
        self.bytes()
    }
}

/// Read-only shared byte-range view into a buffer backing allocation.
///
/// Unlike [`IoBuff`], this type does not preserve headroom/payload/tailroom
/// structure. It is a raw byte subview and is therefore not zero-copy
/// reversible back into [`IoBuffMut`].
///
/// This is primarily a parsing helper. For steady-state transport I/O, keep
/// the original [`IoBuff`] or [`IoBuffMut`] unless you specifically need a
/// sliced read-only byte view.
///
/// # Example
/// ```
/// use flowio::runtime::buffer::IoBuffMut;
///
/// let mut buf = IoBuffMut::new(2, 16, 2).unwrap();
/// buf.payload_append(b"hello").unwrap();
/// buf.headroom_prepend(b"H:").unwrap();
/// let frozen = buf.freeze();
///
/// let payload = frozen.slice(2..7).unwrap();
/// assert_eq!(payload.bytes(), b"hello");
///
/// let copied = payload.make_mut().unwrap();
/// assert_eq!(copied.bytes(), b"hello");
/// assert_eq!(copied.headroom_capacity(), 0);
/// assert_eq!(copied.tailroom_capacity(), 0);
/// ```
pub struct IoBuffView {
    /// Shared backing allocation referenced by this byte-range view.
    header: NonNull<IoBuffHeader>,
    /// Byte offset of the view start within the shared data region.
    offset: usize,
    /// Length of the visible byte range.
    len: usize,
}

impl std::fmt::Debug for IoBuffView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IoBuffView")
            .field("len", &self.len)
            .field("bytes", &self.bytes())
            .finish()
    }
}

impl IoBuffView {
    /// Returns the number of bytes visible through this borrowed view.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` when this borrowed view contains no bytes.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the visible byte range.
    #[inline(always)]
    pub fn bytes(&self) -> &[u8] {
        unsafe {
            let ptr = IoBuffHeader::headroom_ptr_from_raw(self.header.as_ptr()).add(self.offset);
            std::slice::from_raw_parts(ptr, self.len)
        }
    }

    /// Returns a zero-copy subview of this view.
    ///
    /// # Errors
    /// Returns [`IoBuffError::SliceOutOfBounds`] if `range` is outside the
    /// visible byte range.
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Result<IoBuffView, IoBuffError> {
        let (start, end) = resolve_slice_bounds(range, self.len)?;
        unsafe { self.header.as_ref().retain() };
        Ok(IoBuffView {
            header: self.header,
            offset: self.offset + start,
            len: end - start,
        })
    }

    /// Copies the viewed bytes into a new tight payload-only mutable buffer.
    pub fn make_mut(&self) -> Result<IoBuffMut, IoBuffError> {
        let mut new = IoBuffMut::new(0, self.len, 0)?;
        new.payload_append(self.bytes())?;
        Ok(new)
    }
}

impl Clone for IoBuffView {
    fn clone(&self) -> Self {
        unsafe { self.header.as_ref().retain() };
        IoBuffView {
            header: self.header,
            offset: self.offset,
            len: self.len,
        }
    }
}

impl Drop for IoBuffView {
    fn drop(&mut self) {
        let hdr = unsafe { self.header.as_ref() };
        if hdr.release() {
            unsafe { IoBuffHeader::dealloc(self.header) };
        }
    }
}

impl AsRef<[u8]> for IoBuffView {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self.bytes()
    }
}

impl std::ops::Deref for IoBuffView {
    type Target = [u8];

    #[inline(always)]
    fn deref(&self) -> &[u8] {
        self.bytes()
    }
}

// ============================================================================
// IoBuffReadOnly / IoBuffReadWrite trait implementations
// ============================================================================

// --- IoBuff (frozen, read-only) ---

// SAFETY: IoBuff's backing storage is heap/pool-allocated and pointer-stable.
unsafe impl IoBuffReadOnly for IoBuff {
    #[inline(always)]
    fn as_ptr(&self) -> *const u8 {
        unsafe { IoBuffHeader::headroom_ptr_from_raw(self.header.as_ptr()).add(self.offset) }
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.len()
    }
}

// --- IoBuffView (read-only byte subview) ---

// SAFETY: IoBuffView's backing storage is heap/pool-allocated and pointer-stable.
unsafe impl IoBuffReadOnly for IoBuffView {
    #[inline(always)]
    fn as_ptr(&self) -> *const u8 {
        unsafe { IoBuffHeader::headroom_ptr_from_raw(self.header.as_ptr()).add(self.offset) }
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.len
    }
}

// --- IoBuffOwnedView (consuming read-only byte subview) ---

// SAFETY: IoBuffOwnedView owns an IoBuff whose backing storage is heap/pool-
// allocated and pointer-stable. Its offset and len are validated at creation.
unsafe impl IoBuffReadOnly for IoBuffOwnedView {
    #[inline(always)]
    fn as_ptr(&self) -> *const u8 {
        unsafe { IoBuffHeader::headroom_ptr_from_raw(self.buffer.header.as_ptr()).add(self.offset) }
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.len
    }
}

// --- IoBuffMut (mutable, exclusive) ---

// SAFETY: IoBuffMut's backing storage is heap/pool-allocated and pointer-stable.
unsafe impl IoBuffReadOnly for IoBuffMut {
    #[inline(always)]
    fn as_ptr(&self) -> *const u8 {
        unsafe { IoBuffHeader::headroom_ptr_from_raw(self.header.as_ptr()).add(self.offset) }
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.active_len()
    }
}

// SAFETY: IoBuffMut has exclusive ownership — mutable pointer is valid.
unsafe impl IoBuffReadWrite for IoBuffMut {
    #[inline(always)]
    fn as_mut_ptr(&mut self) -> *mut u8 {
        unsafe {
            IoBuffHeader::headroom_ptr_from_raw(self.header.as_ptr())
                .add(self.payload_start_offset() + self.payload_len())
        }
    }

    #[inline(always)]
    fn writable_len(&self) -> usize {
        self.payload_remaining()
    }

    #[inline(always)]
    unsafe fn set_written_len(&mut self, len: usize) {
        let capacity = self.payload_capacity_after_advance();
        debug_assert!(
            len <= capacity,
            "set_written_len({}) exceeds payload capacity {}",
            len,
            capacity
        );
        debug_assert!(
            self.tailroom_len == 0 || len == self.payload_len,
            "set_written_len: cannot change payload length after tailroom_append"
        );
        if self.tailroom_len != 0 && len != self.payload_len {
            return;
        }
        self.payload_len = std::cmp::min(len, capacity);
    }
}

// ============================================================================
// Standard type trait implementations
// ============================================================================

// --- Vec<u8> ---

// SAFETY: Vec's heap buffer does not move when the Vec value is moved.
unsafe impl IoBuffReadOnly for Vec<u8> {
    #[inline(always)]
    fn as_ptr(&self) -> *const u8 {
        Vec::as_ptr(self)
    }

    #[inline(always)]
    fn len(&self) -> usize {
        Vec::len(self)
    }
}

// SAFETY: Same heap stability.
unsafe impl IoBuffReadWrite for Vec<u8> {
    #[inline(always)]
    fn as_mut_ptr(&mut self) -> *mut u8 {
        Vec::as_mut_ptr(self)
    }

    #[inline(always)]
    fn writable_len(&self) -> usize {
        Vec::capacity(self)
    }

    #[inline(always)]
    unsafe fn set_written_len(&mut self, len: usize) {
        debug_assert!(
            len <= Vec::capacity(self),
            "Vec::set_written_len({}) exceeds capacity {}",
            len,
            Vec::capacity(self)
        );
        let len = std::cmp::min(len, Vec::capacity(self));
        // SAFETY: caller guarantees the first `len` bytes are written.
        unsafe { self.set_len(len) };
    }
}

// --- Box<[u8]> ---

// SAFETY: Box heap allocation does not move when the Box value is moved.
unsafe impl IoBuffReadOnly for Box<[u8]> {
    #[inline(always)]
    fn as_ptr(&self) -> *const u8 {
        <[u8]>::as_ptr(self)
    }

    #[inline(always)]
    fn len(&self) -> usize {
        <[u8]>::len(self)
    }
}

// SAFETY: Same heap stability.
unsafe impl IoBuffReadWrite for Box<[u8]> {
    #[inline(always)]
    fn as_mut_ptr(&mut self) -> *mut u8 {
        <[u8]>::as_mut_ptr(self)
    }

    #[inline(always)]
    fn writable_len(&self) -> usize {
        <[u8]>::len(self)
    }

    #[inline(always)]
    unsafe fn set_written_len(&mut self, _len: usize) {
        // Box<[u8]> has no internal length to update.
    }
}

// --- &'static [u8] (read-only, useful for sending constant data) ---

// SAFETY: A static reference is valid for the entire program lifetime.
unsafe impl IoBuffReadOnly for &'static [u8] {
    #[inline(always)]
    fn as_ptr(&self) -> *const u8 {
        <[u8]>::as_ptr(self)
    }

    #[inline(always)]
    fn len(&self) -> usize {
        <[u8]>::len(self)
    }
}
