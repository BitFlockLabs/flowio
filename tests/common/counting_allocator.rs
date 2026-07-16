use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Process-wide allocation counter used by dedicated integration-test binaries.
pub struct CountingAllocator;

static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static DEALLOCS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        DEALLOCS.fetch_add(1, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !ptr.is_null() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            DEALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        ptr
    }
}

#[derive(Clone, Copy)]
pub struct AllocationSnapshot {
    allocs: usize,
    deallocs: usize,
}

impl AllocationSnapshot {
    pub fn current() -> Self {
        Self {
            allocs: ALLOCS.load(Ordering::Relaxed),
            deallocs: DEALLOCS.load(Ordering::Relaxed),
        }
    }

    pub fn assert_unchanged_since(self, before: Self) {
        assert_eq!(
            self.allocs, before.allocs,
            "measured path performed heap allocations"
        );
        assert_eq!(
            self.deallocs, before.deallocs,
            "measured path performed heap deallocations"
        );
    }
}
