//! Slab-page allocator used by the higher-level object pool.

use crate::utils;
use std::marker::PhantomData;
use std::ptr::NonNull;

/// Configuration error returned while constructing a slab allocator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlabAllocatorConfigError {
    /// `objs_per_slab` must be greater than zero.
    ObjsPerSlabZero,
    /// Object alignment must be a non-zero power of two.
    InvalidObjectAlign,
    /// Slab geometry overflowed addressable memory.
    SizeOverflow,
}

impl std::fmt::Display for SlabAllocatorConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ObjsPerSlabZero => {
                f.write_str("SlabAllocator::new_uninit requires objs_per_slab > 0")
            }
            Self::InvalidObjectAlign => {
                f.write_str("SlabAllocator::new_uninit requires power-of-two object alignment")
            }
            Self::SizeOverflow => f.write_str("slab geometry overflowed usize"),
        }
    }
}

impl std::error::Error for SlabAllocatorConfigError {}

/// Size of the [`Slab`] header before payload padding.
pub const SLAB_HDR_SIZE: usize = std::mem::size_of::<Slab>();
/// Alignment required by the [`Slab`] header itself.
pub const SLAB_ALIGN: usize = std::mem::align_of::<Slab>();
/// Size of the intrusive singly-linked `Link` reused at the front of a slab.
pub const SLAB_LINK_SIZE: usize = std::mem::size_of::<utils::list::intrusive::slist::Link>();
/// Alignment of the intrusive singly-linked `Link` reused at the front of a slab.
pub const SLAB_LINK_ALIGN: usize = std::mem::align_of::<utils::list::intrusive::slist::Link>();

/// One contiguous page of allocator-managed memory.
///
/// A slab starts out as a simple bump allocator over its payload region. Once
/// individual objects are freed, their slots are tracked by the higher-level
/// pool free list instead. Its raw cursor and link fields are intentionally
/// opaque so safe feature consumers cannot forge allocator state.
///
/// The type remains available for alignment and pointer-based test support:
///
/// ```
/// use flowio::test_support::utils::memory::slab::Slab;
///
/// assert!(std::mem::align_of::<Slab>().is_power_of_two());
/// ```
///
/// Raw cursor state cannot be inspected through safe code:
///
/// ```compile_fail
/// use flowio::test_support::utils::memory::slab::Slab;
///
/// fn bump_cursor(slab: &Slab) -> *mut u8 {
///     slab.bump_ptr
/// }
/// ```
///
/// Nor can safe code construct a forged slab header:
///
/// ```compile_fail
/// use flowio::test_support::utils::list::intrusive::slist::Link;
/// use flowio::test_support::utils::memory::slab::Slab;
///
/// let _forged = Slab {
///     link: Link::new_unlinked(),
///     bump_ptr: std::ptr::null_mut(),
///     end_ptr: std::ptr::null_mut(),
/// };
/// ```
#[repr(C)]
pub struct Slab {
    /// Intrusive link used to chain slabs in a singly-linked list.
    /// This must stay at offset 0 because other code reinterprets a slab as
    /// its link head when chaining pages.
    link: utils::list::intrusive::slist::Link,
    /// Current bump-allocation cursor inside the slab payload area.
    bump_ptr: *mut u8,
    /// End of the slab payload area.
    end_ptr: *mut u8,
}

impl Slab {
    /// Bump-allocates one fixed-size object slot from this slab page.
    ///
    /// Returns `None` when fewer than `obj_size` payload bytes remain.
    #[inline(always)]
    pub fn try_alloc(&mut self, obj_size: usize) -> Option<*mut u8> {
        let remaining = self.end_ptr as usize - self.bump_ptr as usize;
        if remaining < obj_size {
            return None;
        }
        let ptr = self.bump_ptr;
        self.bump_ptr = unsafe { self.bump_ptr.add(obj_size) };
        Some(ptr)
    }
}

/// Allocation result from a slab-page chain.
pub(crate) struct SlabPageAlloc {
    /// Raw slot pointer returned to the caller.
    pub(crate) ptr: *mut u8,
    /// True when this allocation requested a fresh slab page.
    pub(crate) new_slab: bool,
}

/// Move-safe owner of a singly-linked chain of slab pages.
///
/// The chain uses each slab header's intrusive `link.next` pointer, avoiding
/// self-referential sentinels while giving pools one shared implementation for
/// current-page bump allocation, page growth, and teardown.
pub(crate) struct SlabPageChain {
    /// Head of the singly-linked list of allocated slab pages.
    head: *mut Slab,
    /// Slab currently being bump-allocated from.
    current: *mut Slab,
}

impl SlabPageChain {
    /// Creates an empty slab-page chain.
    pub(crate) const fn new() -> Self {
        Self {
            head: std::ptr::null_mut(),
            current: std::ptr::null_mut(),
        }
    }

    /// Attempts to bump-allocate from the current slab page.
    ///
    /// # Safety
    /// Any live current slab must belong to this chain and must have been
    /// allocated by the same slab allocator used to grow and free the chain.
    #[inline(always)]
    pub(crate) unsafe fn try_alloc_current(&mut self, obj_size: usize) -> Option<*mut u8> {
        if self.current.is_null() {
            return None;
        }

        unsafe { (*self.current).try_alloc(obj_size) }
    }

    /// Requests one new slab page, links it into the chain, and allocates the
    /// first object slot from it.
    ///
    /// # Safety
    /// `slab_factory` must be the allocator that owns all pages in this chain.
    /// The returned slot must not outlive the chain or be used after
    /// [`Self::free_all`] returns the pages to the provider.
    #[inline(always)]
    pub(crate) unsafe fn alloc_from_new_slab<P: super::provider::MemoryProvider>(
        &mut self,
        slab_factory: &mut SlabAllocator<'_, P>,
        obj_size: usize,
    ) -> Option<*mut u8> {
        let slab_ptr = slab_factory.provide_slab()?;
        unsafe {
            (*slab_ptr).link.next = self.head as *mut utils::list::intrusive::slist::Link;
        }
        self.head = slab_ptr;
        self.current = slab_ptr;

        unsafe { (*slab_ptr).try_alloc(obj_size) }
    }

    /// Allocates from the current slab page or grows the chain by one page.
    ///
    /// # Safety
    /// `slab_factory` must be the allocator that owns all pages in this chain.
    #[inline(always)]
    pub(crate) unsafe fn alloc_or_grow<P: super::provider::MemoryProvider>(
        &mut self,
        slab_factory: &mut SlabAllocator<'_, P>,
        obj_size: usize,
    ) -> Option<SlabPageAlloc> {
        unsafe { self.alloc_or_grow_inner(slab_factory, obj_size, || false) }
    }

    /// Debug-only allocation variant that can reject a required page growth
    /// before the backing provider is called.
    ///
    /// Current-page allocations never consult `reject_growth`; the callback
    /// runs only after the current page cannot satisfy the request.
    ///
    /// # Safety
    /// `slab_factory` must be the allocator that owns all pages in this chain.
    #[cfg(debug_assertions)]
    #[inline(always)]
    pub(crate) unsafe fn alloc_or_grow_with_debug_rejection<
        P: super::provider::MemoryProvider,
        F: FnOnce() -> bool,
    >(
        &mut self,
        slab_factory: &mut SlabAllocator<'_, P>,
        obj_size: usize,
        reject_growth: F,
    ) -> Option<SlabPageAlloc> {
        unsafe { self.alloc_or_grow_inner(slab_factory, obj_size, reject_growth) }
    }

    #[inline(always)]
    unsafe fn alloc_or_grow_inner<P: super::provider::MemoryProvider, F: FnOnce() -> bool>(
        &mut self,
        slab_factory: &mut SlabAllocator<'_, P>,
        obj_size: usize,
        reject_growth: F,
    ) -> Option<SlabPageAlloc> {
        if let Some(ptr) = unsafe { self.try_alloc_current(obj_size) } {
            return Some(SlabPageAlloc {
                ptr,
                new_slab: false,
            });
        }

        if reject_growth() {
            return None;
        }

        let ptr = unsafe { self.alloc_from_new_slab(slab_factory, obj_size) }?;
        Some(SlabPageAlloc {
            ptr,
            new_slab: true,
        })
    }

    /// Returns every slab page in this chain to the backing provider.
    ///
    /// # Safety
    /// `slab_factory` must be the allocator that owns all pages in this chain,
    /// and no live objects may reference any slot in the chain.
    pub(crate) unsafe fn free_all<P: super::provider::MemoryProvider>(
        &mut self,
        slab_factory: &mut SlabAllocator<'_, P>,
    ) {
        let mut current = self.head;
        self.head = std::ptr::null_mut();
        self.current = std::ptr::null_mut();

        while !current.is_null() {
            let next = unsafe { (*current).link.next as *mut Slab };
            unsafe { slab_factory.free_slab(current as *mut u8) };
            current = next;
        }
    }
}

/// The underlying allocator responsible for requesting large chunks (slabs)
/// of memory from the generic `MemoryProvider` and formatting the slab's
/// intrusive header.
pub struct SlabAllocator<'a, P: super::provider::MemoryProvider> {
    /// Raw memory source from which full slab pages are requested.
    ///
    /// This is stored as a raw pointer rather than a long-lived `&'a mut P` so
    /// a provider-owning wrapper can retain ownership while this allocator
    /// borrows the provider only for individual operations.
    provider: NonNull<P>,
    /// Ties the allocator to the caller-provided provider lifetime without
    /// storing a real Rust reference.
    _provider_lifetime: PhantomData<&'a mut P>,
    /// Total bytes requested for each slab page, including padding.
    total_slab_size: usize,
    /// Header size rounded up to the object alignment.
    header_padded_size: usize,
    /// Usable payload bytes reserved for configured object slots.
    payload_bytes: usize,
    /// Alignment required for the slab allocation as a whole.
    slab_align: usize,
    /// Whether the provider has accepted this allocator's alignment contract.
    initialized: bool,
}

impl<'a, P: super::provider::MemoryProvider> SlabAllocator<'a, P> {
    /// Creates an uninitialized slab allocator for fixed-size object slots.
    ///
    /// Call [`SlabAllocator::init`] before requesting slabs so the provider's
    /// alignment guarantee is raised to the computed slab alignment.
    ///
    /// # Errors
    /// Returns [`SlabAllocatorConfigError::ObjsPerSlabZero`] when
    /// `objs_per_slab` is zero.
    /// Returns [`SlabAllocatorConfigError::InvalidObjectAlign`] when
    /// `obj_align` is not a non-zero power of two.
    /// Returns [`SlabAllocatorConfigError::SizeOverflow`] when slab geometry
    /// overflows addressable memory.
    #[cfg(any(test, feature = "test-support"))]
    pub fn new_uninit(
        provider: &'a mut P,
        obj_size: usize,
        obj_align: usize,
        objs_per_slab: usize,
    ) -> Result<Self, SlabAllocatorConfigError> {
        let provider = provider as *mut P;
        unsafe { Self::new_uninit_from_raw(provider, obj_size, obj_align, objs_per_slab) }
    }

    /// Creates an uninitialized slab allocator from a raw provider pointer.
    ///
    /// # Safety
    /// `provider` must be non-null, valid for unique mutable access for `'a`,
    /// and must outlive the returned allocator. The caller must ensure no
    /// other mutable access aliases the provider while this allocator is used.
    pub(crate) unsafe fn new_uninit_from_raw(
        provider: *mut P,
        obj_size: usize,
        obj_align: usize,
        objs_per_slab: usize,
    ) -> Result<Self, SlabAllocatorConfigError> {
        debug_assert!(
            !provider.is_null(),
            "SlabAllocator::new_uninit_from_raw requires a non-null provider"
        );
        let provider = unsafe { NonNull::new_unchecked(provider) };
        if objs_per_slab == 0 {
            return Err(SlabAllocatorConfigError::ObjsPerSlabZero);
        }

        let provider_align = unsafe { provider.as_ref() }.alignment_guarantee();

        // The slab page must satisfy the provider's base guarantee, the slab
        // header alignment, and the requested object alignment.
        let slab_align = std::cmp::max(std::cmp::max(provider_align, SLAB_ALIGN), obj_align);

        if !obj_align.is_power_of_two() {
            return Err(SlabAllocatorConfigError::InvalidObjectAlign);
        }

        // The slab page itself starts at `slab_align`, so the header only
        // needs to be rounded up to the object alignment before payload slots
        // begin.
        let header_padded_size =
            crate::utils::size_up(SLAB_HDR_SIZE, obj_align).map_err(|err| match err {
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
            crate::utils::size_up(min_required, slab_align).map_err(|err| match err {
                utils::SizeUpError::InvalidAlign => SlabAllocatorConfigError::InvalidObjectAlign,
                utils::SizeUpError::SizeOverflow => SlabAllocatorConfigError::SizeOverflow,
            })?;

        Ok(Self {
            provider,
            _provider_lifetime: PhantomData,
            total_slab_size,
            header_padded_size,
            payload_bytes,
            slab_align,
            initialized: false,
        })
    }

    /// Applies the allocator's computed alignment requirement to the backing
    /// memory provider once.
    ///
    /// Repeated calls are no-ops so an initialized provider is never
    /// reconfigured while slab allocations remain live.
    pub fn init(&mut self) {
        if self.initialized {
            return;
        }

        debug_assert!(self.slab_align.is_power_of_two());

        // Raise the provider's minimum alignment to the slab alignment for all
        // future slab-page allocations.
        unsafe { self.provider.as_mut() }.init(self.slab_align);
        #[cfg(debug_assertions)]
        {
            let provider_align = unsafe { self.provider.as_ref() }.alignment_guarantee();
            debug_assert!(
                provider_align.is_power_of_two(),
                "MemoryProvider returned an invalid alignment guarantee"
            );
            debug_assert!(
                provider_align >= self.slab_align,
                "MemoryProvider::init did not raise its alignment guarantee"
            );
        }
        self.initialized = true;
    }

    /// Requests, formats, and returns a fresh slab page.
    ///
    /// Returns `None` before [`SlabAllocator::init`] or when the provider
    /// cannot supply the required memory.
    pub fn provide_slab(&mut self) -> Option<*mut Slab> {
        if !self.initialized {
            return None;
        }

        // Request raw memory already aligned for this slab page.
        let raw_mem = unsafe { self.provider.as_mut() }.request_memory(self.total_slab_size)?;

        unsafe {
            let slab_ptr = raw_mem as *mut Slab;

            std::ptr::write(
                slab_ptr,
                Slab {
                    link: utils::list::intrusive::slist::Link::new_unlinked(),
                    bump_ptr: raw_mem.add(self.header_padded_size),
                    end_ptr: raw_mem.add(self.header_padded_size + self.payload_bytes),
                },
            );
            Some(slab_ptr)
        }
    }

    /// Returns the alignment required for each slab page allocation.
    #[cfg(any(test, feature = "test-support"))]
    pub fn get_slab_alignment(&self) -> usize {
        self.slab_align
    }

    /// Returns a slab to the backing memory provider.
    ///
    /// # Safety
    /// `slab_ptr` must have been allocated by this `SlabAllocator`'s provider
    /// and must not be in use by any live pool objects.
    pub unsafe fn free_slab(&mut self, slab_ptr: *mut u8) {
        unsafe {
            self.provider
                .as_mut()
                .free_memory(slab_ptr, self.total_slab_size)
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::memory::provider::MemoryProvider;
    use std::alloc::{Layout, alloc, dealloc};
    use std::cell::Cell;
    use std::rc::Rc;

    #[derive(Default)]
    struct CountingStats {
        inits: Cell<usize>,
        requests: Cell<usize>,
        frees: Cell<usize>,
    }

    struct CountingProvider {
        alignment: usize,
        stats: Rc<CountingStats>,
    }

    impl CountingProvider {
        fn new(stats: Rc<CountingStats>) -> Self {
            Self {
                alignment: 1,
                stats,
            }
        }
    }

    // SAFETY: this private fixture is used only through SlabAllocator, which
    // initializes it once before requesting storage. Each request uses a valid
    // Layout with the then-stable alignment, global-allocator allocations are
    // disjoint, and free reconstructs the same Layout from the exact size.
    unsafe impl MemoryProvider for CountingProvider {
        fn init(&mut self, required_align: usize) {
            self.stats.inits.set(self.stats.inits.get() + 1);
            self.alignment = std::cmp::max(self.alignment, required_align);
        }

        fn alignment_guarantee(&self) -> usize {
            self.alignment
        }

        fn request_memory(&mut self, size: usize) -> Option<*mut u8> {
            if size == 0 {
                return None;
            }

            let layout = Layout::from_size_align(size, self.alignment).ok()?;
            let ptr = unsafe { alloc(layout) };
            if ptr.is_null() {
                return None;
            }
            self.stats.requests.set(self.stats.requests.get() + 1);
            Some(ptr)
        }

        unsafe fn free_memory(&mut self, ptr: *mut u8, size: usize) {
            self.stats.frees.set(self.stats.frees.get() + 1);
            let layout = Layout::from_size_align(size, self.alignment)
                .expect("test provider layout should be valid");
            // SAFETY: the caller returns the exact live pointer and size from
            // this provider, and allocator initialization is idempotent.
            unsafe { dealloc(ptr, layout) };
        }
    }

    #[test]
    fn slab_allocator_rejects_preinit_and_initializes_provider_once() {
        let stats = Rc::new(CountingStats::default());
        let mut provider = CountingProvider::new(Rc::clone(&stats));
        let mut allocator = SlabAllocator::new_uninit(&mut provider, 1, 1, 1)
            .expect("slab allocator config should be valid");

        assert!(
            allocator.provide_slab().is_none(),
            "pre-init slab acquisition must fail safely"
        );
        assert_eq!(stats.requests.get(), 0, "provider must not be consulted");

        allocator.init();
        allocator.init();
        assert_eq!(stats.inits.get(), 1, "provider init must be idempotent");

        let slab = allocator
            .provide_slab()
            .expect("initialized allocator should provide one slab");
        assert_eq!((slab as usize) % std::mem::align_of::<Slab>(), 0);
        unsafe { allocator.free_slab(slab.cast::<u8>()) };
        assert_eq!(stats.requests.get(), 1);
        assert_eq!(stats.frees.get(), 1);
    }

    #[test]
    fn slab_page_chain_allocates_across_pages_and_frees_all() {
        let stats = Rc::new(CountingStats::default());
        let mut provider = CountingProvider::new(Rc::clone(&stats));
        let mut allocator = SlabAllocator::new_uninit(&mut provider, 16, 8, 2)
            .expect("slab allocator config should be valid");
        allocator.init();
        assert_eq!(stats.inits.get(), 1);

        let mut chain = SlabPageChain::new();

        let first = unsafe {
            chain
                .alloc_or_grow(&mut allocator, 16)
                .expect("first slot should allocate")
        };
        assert!(first.new_slab);
        let second = unsafe {
            chain
                .alloc_or_grow(&mut allocator, 16)
                .expect("second slot should allocate")
        };
        assert!(!second.new_slab);
        let third = unsafe {
            chain
                .alloc_or_grow(&mut allocator, 16)
                .expect("third slot should allocate from a new slab")
        };
        assert!(third.new_slab);

        assert_ne!(first.ptr, second.ptr);
        assert_ne!(first.ptr, third.ptr);
        assert_eq!(stats.requests.get(), 2);

        unsafe { chain.free_all(&mut allocator) };
        assert_eq!(stats.frees.get(), 2);

        unsafe { chain.free_all(&mut allocator) };
        assert_eq!(stats.frees.get(), 2, "free_all must leave the chain empty");
    }
}
