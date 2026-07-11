//! Pluggable raw-memory providers used by slab- and pool-backed allocators.

use std::alloc::{Layout, alloc, dealloc};
use std::ptr::NonNull;

#[derive(Clone, Copy)]
#[repr(C)]
struct BasicAllocationHeader {
    /// Byte distance from the returned payload pointer back to the allocated
    /// base pointer.
    base_offset: usize,
    /// Total allocation size passed to the global allocator.
    total_size: usize,
    /// Allocation alignment passed to the global allocator.
    total_align: usize,
    /// User-requested payload size for debug validation on free.
    requested_size: usize,
}

/// Raw memory source used by the slab/pool allocators.
///
/// This keeps allocator policy separate from the global heap so callers can
/// substitute hugepages, mmap-backed regions, NUMA-aware allocators, or other
/// custom memory sources.
pub trait MemoryProvider {
    /// Raises the minimum alignment that future allocations must satisfy.
    /// `required_align` must be a non-zero power of two.
    fn init(&mut self, required_align: usize);

    /// Returns the provider's current guaranteed alignment.
    fn alignment_guarantee(&self) -> usize;

    /// Requests `size` writable bytes aligned to `alignment_guarantee()`.
    ///
    /// A non-null returned pointer remains owned by the caller until passed
    /// back exactly once to [`MemoryProvider::free_memory`] on this provider.
    fn request_memory(&mut self, size: usize) -> Option<*mut u8>;

    /// Returns a memory chunk previously allocated by `request_memory`.
    ///
    /// # Safety
    /// `ptr` must have been returned by a prior `request_memory` call on this
    /// provider, `size` must match the original allocation size, and no live
    /// reference may use the allocation after this call.
    unsafe fn free_memory(&mut self, ptr: *mut u8, size: usize);
}

/// Owns a heap-allocated memory provider whose stable raw address is handed
/// to slab and pool allocators.
pub(crate) struct ProviderOwner<P: MemoryProvider + 'static> {
    /// Stable address of the provider allocation owned by this wrapper.
    provider: NonNull<P>,
}

impl<P: MemoryProvider + 'static> ProviderOwner<P> {
    /// Moves `provider` into a stable heap allocation.
    pub(crate) fn new(provider: P) -> Self {
        let provider_ptr = Box::into_raw(Box::new(provider));
        Self {
            provider: unsafe {
                // SAFETY: Box::into_raw never returns null.
                NonNull::new_unchecked(provider_ptr)
            },
        }
    }

    #[inline(always)]
    /// Returns the stable provider pointer for dependent allocators.
    pub(crate) fn as_ptr(&self) -> *mut P {
        self.provider.as_ptr()
    }

    #[cfg(debug_assertions)]
    #[inline(always)]
    /// Borrows the provider for debug-only state inspection.
    pub(crate) fn as_ref(&self) -> &P {
        // SAFETY: `provider` remains owned and live for `self`'s lifetime.
        unsafe { self.provider.as_ref() }
    }

    #[inline(always)]
    /// Mutably borrows the owned provider while this wrapper is exclusive.
    pub(crate) fn as_mut(&mut self) -> &mut P {
        // SAFETY: `&mut self` excludes access through dependent owners for the
        // duration of the returned borrow.
        unsafe { self.provider.as_mut() }
    }
}

impl<P: MemoryProvider + 'static> Drop for ProviderOwner<P> {
    fn drop(&mut self) {
        // SAFETY: this is the unique pointer produced by `Box::into_raw` in
        // `new`, and dependent allocators must be dropped first.
        unsafe { drop(Box::from_raw(self.provider.as_ptr())) };
    }
}

/// Heap-backed [`MemoryProvider`] using the global allocator.
pub struct BasicMemoryProvider {
    /// Minimum alignment guaranteed for future allocations from this provider.
    alignment: usize,
}

impl BasicMemoryProvider {
    /// Creates a provider aligned at least to machine word size.
    pub fn new() -> Self {
        Self {
            alignment: std::mem::align_of::<usize>(),
        }
    }
}

impl Default for BasicMemoryProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryProvider for BasicMemoryProvider {
    fn init(&mut self, required_align: usize) {
        self.alignment = std::cmp::max(self.alignment, required_align);
    }

    fn alignment_guarantee(&self) -> usize {
        self.alignment
    }

    fn request_memory(&mut self, size: usize) -> Option<*mut u8> {
        let header_size = std::mem::size_of::<BasicAllocationHeader>();
        let header_align = std::mem::align_of::<BasicAllocationHeader>();
        let total_align = std::cmp::max(self.alignment, header_align);
        let align_mask = total_align.checked_sub(1)?;
        let payload_offset = header_size.checked_add(align_mask)? & !align_mask;
        let total_size = payload_offset.checked_add(size)?;
        let layout = Layout::from_size_align(total_size, total_align).ok()?;
        let base = unsafe { alloc(layout) };
        if base.is_null() {
            return None;
        }

        let payload = unsafe { base.add(payload_offset) };
        let header_ptr = unsafe { payload.sub(header_size) as *mut BasicAllocationHeader };
        unsafe {
            std::ptr::write(
                header_ptr,
                BasicAllocationHeader {
                    base_offset: payload_offset,
                    total_size,
                    total_align,
                    requested_size: size,
                },
            );
        }
        Some(payload)
    }

    unsafe fn free_memory(&mut self, ptr: *mut u8, size: usize) {
        if ptr.is_null() {
            return;
        }

        let header_size = std::mem::size_of::<BasicAllocationHeader>();
        let header_ptr = unsafe { ptr.sub(header_size) as *const BasicAllocationHeader };
        let header = unsafe { std::ptr::read(header_ptr) };
        debug_assert_eq!(
            header.requested_size, size,
            "BasicMemoryProvider free size did not match allocation size"
        );
        if let Ok(layout) = Layout::from_size_align(header.total_size, header.total_align) {
            let base = unsafe { ptr.sub(header.base_offset) };
            unsafe { dealloc(base, layout) };
        }
    }
}
