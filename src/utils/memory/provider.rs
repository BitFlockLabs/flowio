//! Pluggable raw-memory providers used by slab- and pool-backed allocators.

use std::alloc::{Layout, alloc, dealloc};

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
    fn init(&mut self, required_align: usize);

    /// Returns the provider's current guaranteed alignment.
    fn alignment_guarantee(&self) -> usize;

    /// Requests a block of raw memory aligned to `alignment_guarantee()`.
    fn request_memory(&mut self, size: usize) -> Option<*mut u8>;

    /// Returns a memory chunk previously allocated by `request_memory`.
    ///
    /// # Safety
    /// `ptr` must have been returned by a prior `request_memory` call on this
    /// provider, and `size` must match the original allocation size.
    unsafe fn free_memory(&mut self, ptr: *mut u8, size: usize);
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
