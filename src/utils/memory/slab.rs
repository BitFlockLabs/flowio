use crate::utils;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlabAllocatorConfigError
{
    /// `objs_per_slab` must be greater than zero.
    ObjsPerSlabZero,
    /// Object alignment must be a non-zero power of two.
    InvalidObjectAlign,
    /// Slab geometry overflowed addressable memory.
    SizeOverflow,
}

impl std::fmt::Display for SlabAllocatorConfigError
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        match self
        {
            Self::ObjsPerSlabZero =>
            {
                f.write_str("SlabAllocator::new_uninit requires objs_per_slab > 0")
            }
            Self::InvalidObjectAlign =>
            {
                f.write_str("SlabAllocator::new_uninit requires power-of-two object alignment")
            }
            Self::SizeOverflow => f.write_str("slab geometry overflowed usize"),
        }
    }
}

impl std::error::Error for SlabAllocatorConfigError {}

/// Size of the [`Slab`] header before payload padding.
pub const SLAB_HDR_SIZE: usize = std::mem::size_of::<Slab>();
/// Size of the intrusive singly-linked `Link` reused at the front of a slab.
pub const SLAB_LINK_SIZE: usize = std::mem::size_of::<utils::list::intrusive::slist::Link>();
/// Alignment of the intrusive singly-linked `Link` reused at the front of a slab.
pub const SLAB_LINK_ALIGN: usize = std::mem::align_of::<utils::list::intrusive::slist::Link>();

/// One contiguous page of allocator-managed memory.
///
/// A slab starts out as a simple bump allocator over its payload region. Once
/// individual objects are freed, their slots are tracked by the higher-level
/// pool free list instead.
#[repr(C)]
pub struct Slab
{
    /// Intrusive link used to chain slabs in a singly-linked list.
    /// This must stay at offset 0 because other code reinterprets a slab as
    /// its link head when chaining pages.
    pub link: utils::list::intrusive::slist::Link,
    /// Current bump-allocation cursor inside the slab payload area.
    pub bump_ptr: *mut u8,
    /// End of the slab payload area.
    pub end_ptr: *mut u8,
}

impl Slab
{
    #[inline(always)]
    pub fn try_alloc(&mut self, obj_size: usize) -> Option<*mut u8>
    {
        let remaining = self.end_ptr as usize - self.bump_ptr as usize;
        if remaining < obj_size
        {
            return None;
        }
        let ptr = self.bump_ptr;
        self.bump_ptr = unsafe { self.bump_ptr.add(obj_size) };
        Some(ptr)
    }
}

/// The underlying allocator responsible for requesting large chunks (slabs)
/// of memory from the generic `MemoryProvider` and formatting the slab's
/// intrusive header.
pub struct SlabAllocator<'a, P: super::provider::MemoryProvider>
{
    provider: &'a mut P,
    total_slab_size: usize,
    header_padded_size: usize,
    slab_align: usize,
}

impl<'a, P: super::provider::MemoryProvider> SlabAllocator<'a, P>
{
    pub fn new_uninit(
        provider: &'a mut P,
        obj_size: usize,
        obj_align: usize,
        objs_per_slab: usize,
    ) -> Result<Self, SlabAllocatorConfigError>
    {
        if objs_per_slab == 0
        {
            return Err(SlabAllocatorConfigError::ObjsPerSlabZero);
        }

        let provider_align = provider.alignment_guarantee();

        // The provider only needs to guarantee the stricter of the two
        let slab_align = std::cmp::max(provider_align, obj_align);

        if !obj_align.is_power_of_two()
        {
            return Err(SlabAllocatorConfigError::InvalidObjectAlign);
        }

        // TIGHT HEADER: We only need to pad the header to the OBJECT'S alignment.
        // Since the Slab starts at a 'slab_align' boundary (which is a multiple of obj_align),
        // rounding the header to obj_align is sufficient!
        let header_padded_size =
            crate::utils::size_up(SLAB_HDR_SIZE, obj_align).map_err(|err| match err
            {
                utils::SizeUpError::InvalidAlign => SlabAllocatorConfigError::InvalidObjectAlign,
                utils::SizeUpError::SizeOverflow => SlabAllocatorConfigError::SizeOverflow,
            })?;

        let payload_bytes = obj_size
            .checked_mul(objs_per_slab)
            .ok_or(SlabAllocatorConfigError::SizeOverflow)?;
        let min_required = header_padded_size
            .checked_add(payload_bytes)
            .ok_or(SlabAllocatorConfigError::SizeOverflow)?;
        let total_slab_size =
            crate::utils::size_up(min_required, slab_align).map_err(|err| match err
            {
                utils::SizeUpError::InvalidAlign => SlabAllocatorConfigError::InvalidObjectAlign,
                utils::SizeUpError::SizeOverflow => SlabAllocatorConfigError::SizeOverflow,
            })?;

        Ok(Self {
            provider,
            total_slab_size,
            header_padded_size,
            slab_align,
        })
    }

    pub fn init(&mut self)
    {
        debug_assert!(self.slab_align.is_power_of_two());

        // Tell the provider to use this alignment from now on
        self.provider.init(self.slab_align);
    }

    pub fn provide_slab(&mut self) -> Option<*mut Slab>
    {
        // Request memory specifically aligned for this Slab's requirements
        let raw_mem = self.provider.request_memory(self.total_slab_size)?;

        unsafe {
            let slab_ptr = raw_mem as *mut Slab;

            std::ptr::write(
                slab_ptr,
                Slab {
                    link: utils::list::intrusive::slist::Link::new_unlinked(),
                    bump_ptr: raw_mem.add(self.header_padded_size),
                    end_ptr: raw_mem.add(self.total_slab_size),
                },
            );
            Some(slab_ptr)
        }
    }

    pub fn get_slab_alignment(&self) -> usize
    {
        self.slab_align
    }

    /// Returns a slab to the backing memory provider.
    ///
    /// # Safety
    /// `slab_ptr` must have been allocated by this `SlabAllocator`'s provider
    /// and must not be in use by any live pool objects.
    pub unsafe fn free_slab(&mut self, slab_ptr: *mut u8)
    {
        unsafe { self.provider.free_memory(slab_ptr, self.total_slab_size) };
    }
}
