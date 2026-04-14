/// Raw memory source used by the slab/pool allocators.
///
/// This keeps allocator policy separate from the global heap so callers can
/// substitute hugepages, mmap-backed regions, NUMA-aware allocators, or other
/// custom memory sources.
pub trait MemoryProvider
{
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
pub struct BasicMemoryProvider
{
    alignment: usize,
}

impl BasicMemoryProvider
{
    pub fn new() -> Self
    {
        Self {
            alignment: std::mem::align_of::<usize>(),
        }
    }
}

impl Default for BasicMemoryProvider
{
    fn default() -> Self
    {
        Self::new()
    }
}

impl MemoryProvider for BasicMemoryProvider
{
    fn init(&mut self, required_align: usize)
    {
        self.alignment = std::cmp::max(self.alignment, required_align);
    }

    fn alignment_guarantee(&self) -> usize
    {
        self.alignment
    }

    fn request_memory(&mut self, size: usize) -> Option<*mut u8>
    {
        let layout = std::alloc::Layout::from_size_align(size, self.alignment).ok()?;
        let ptr = unsafe { std::alloc::alloc(layout) };
        if ptr.is_null() { None } else { Some(ptr) }
    }

    unsafe fn free_memory(&mut self, ptr: *mut u8, size: usize)
    {
        if let Ok(layout) = std::alloc::Layout::from_size_align(size, self.alignment)
        {
            unsafe { std::alloc::dealloc(ptr, layout) };
        }
    }
}
