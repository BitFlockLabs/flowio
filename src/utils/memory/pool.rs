use crate::utils;
use std::mem::MaybeUninit;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolConfigError
{
    /// `objs_per_slab` must be greater than zero.
    ObjsPerSlabZero,
    /// Pool slot geometry overflowed addressable memory.
    SizeOverflow,
}

impl std::fmt::Display for PoolConfigError
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result
    {
        match self
        {
            Self::ObjsPerSlabZero => f.write_str("Pool::new_uninit requires objs_per_slab > 0"),
            Self::SizeOverflow => f.write_str("pool slot geometry overflowed usize"),
        }
    }
}

impl std::error::Error for PoolConfigError {}

/// Initializes an object directly inside pre-allocated memory.
pub trait InPlaceInit: Sized
{
    type Args;
    /// Writes a fully initialized value into `slot`.
    fn init_at(slot: &mut MaybeUninit<Self>, args: Self::Args);
}

impl<const N: usize> InPlaceInit for [u8; N]
{
    type Args = ();

    fn init_at(slot: &mut MaybeUninit<Self>, _args: Self::Args)
    {
        // Zero-fill byte arrays so newly allocated buffers start in a defined state.
        unsafe {
            std::ptr::write_bytes(slot.as_mut_ptr() as *mut u8, 0, N);
        }
    }
}

/// A memory pool that manages objects of type `T`.
///
/// It allocates large chunks of memory (slabs) from the `MemoryProvider`
/// and chunks them into slots. Freed slots are kept in an intrusive single-linked list
/// (`free_list`) for zero-allocation reuse.
///
/// Slab pages are tracked via a singly-linked list through each Slab header's
/// `link.next` pointer.  This avoids the DList sentinel's self-referential
/// pointers, making the pool safe to move after initialization.
pub struct Pool<'a, T: InPlaceInit, P: super::provider::MemoryProvider>
{
    /// Slab allocator responsible for requesting and formatting new pages.
    slab_factory: super::slab::SlabAllocator<'a, P>,
    /// Free-list of returned object slots ready for reuse.
    free_list: utils::list::intrusive::slist::SList<T>,
    /// Head of the singly-linked list of allocated slab pages, chained
    /// through each Slab header's `link.next` pointer.
    slab_page_head: *mut super::slab::Slab,
    /// Slab currently being bump-allocated from.
    current_slab: *mut super::slab::Slab,
    /// Size of each object slot after alignment and link accommodation.
    obj_size: usize,
}

impl<'a, T: InPlaceInit, P: super::provider::MemoryProvider> Pool<'a, T, P>
{
    pub fn new_uninit(provider: &'a mut P, objs_per_slab: usize) -> Result<Self, PoolConfigError>
    {
        if objs_per_slab == 0
        {
            return Err(PoolConfigError::ObjsPerSlabZero);
        }

        let raw_size = std::mem::size_of::<T>();
        let align = std::cmp::max(std::mem::align_of::<T>(), super::slab::SLAB_LINK_ALIGN);
        let base_size = std::cmp::max(raw_size, super::slab::SLAB_LINK_SIZE);
        let obj_size =
            crate::utils::size_up(base_size, align).map_err(|_| PoolConfigError::SizeOverflow)?;

        // Factory creates geometry based purely on the provider's inherent alignment
        let slab_factory =
            super::slab::SlabAllocator::new_uninit(provider, obj_size, align, objs_per_slab)
                .map_err(|err| match err
                {
                    super::slab::SlabAllocatorConfigError::ObjsPerSlabZero =>
                    {
                        PoolConfigError::ObjsPerSlabZero
                    }
                    super::slab::SlabAllocatorConfigError::InvalidObjectAlign
                    | super::slab::SlabAllocatorConfigError::SizeOverflow =>
                    {
                        PoolConfigError::SizeOverflow
                    }
                })?;

        Ok(Self {
            slab_factory,
            free_list: utils::list::intrusive::slist::SList::new_uninit(),
            slab_page_head: std::ptr::null_mut(),
            current_slab: std::ptr::null_mut(),
            obj_size,
        })
    }

    pub fn init(&mut self)
    {
        self.slab_factory.init();
        self.free_list.init();
    }

    /// # Safety
    ///
    /// The caller must ensure the memory provider is valid and that
    /// the `InPlaceInit::init_at` implementation does not panic.
    pub unsafe fn alloc(&mut self, args: T::Args) -> Option<*mut T>
    {
        let raw_ptr = if let Some(link_ptr) = unsafe { self.free_list.pop_front(0) }
        {
            link_ptr
        }
        else
        {
            // B. Try Current Slab
            let mut ptr = if !self.current_slab.is_null()
            {
                unsafe { (*self.current_slab).try_alloc(self.obj_size) }
            }
            else
            {
                None
            };

            // C. Request New Slab
            if ptr.is_none()
            {
                let slab_ptr = self.slab_factory.provide_slab()?;
                // Link new slab into the singly-linked page list.
                unsafe {
                    (*slab_ptr).link.next =
                        self.slab_page_head as *mut utils::list::intrusive::slist::Link;
                }
                self.slab_page_head = slab_ptr;
                self.current_slab = slab_ptr;
                ptr = unsafe { (*slab_ptr).try_alloc(self.obj_size) };
            }

            ptr? as *mut T
        };

        unsafe {
            let slot = &mut *(raw_ptr as *mut MaybeUninit<T>);
            T::init_at(slot, args)
        };

        Some(raw_ptr)
    }

    /// # Safety
    ///
    /// The caller must ensure that `obj` is a valid pointer to a dynamically
    /// allocated object originally provided by this pool.
    pub unsafe fn free(&mut self, obj: *mut T)
    {
        if obj.is_null()
        {
            return;
        }

        unsafe {
            std::ptr::drop_in_place(obj);

            let link_ptr = obj as *mut utils::list::intrusive::slist::Link;
            self.free_list.push_front_unchecked(link_ptr);
        }
    }
}

impl<'a, T: InPlaceInit, P: super::provider::MemoryProvider> Drop for Pool<'a, T, P>
{
    fn drop(&mut self)
    {
        // Walk the singly-linked slab page list and free each page.
        let mut current = self.slab_page_head;
        while !current.is_null()
        {
            let next = unsafe { (*current).link.next as *mut super::slab::Slab };
            let slab_ptr = current as *mut u8;
            unsafe { self.slab_factory.free_slab(slab_ptr) };
            current = next;
        }
    }
}
