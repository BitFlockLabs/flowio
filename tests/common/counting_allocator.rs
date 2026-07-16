use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Process-wide allocation counter used by dedicated integration-test binaries.
pub struct CountingAllocator;

static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static DEALLOCS: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static FAIL_NEXT_SIZE: Cell<usize> = const { Cell::new(0) };
}

fn should_fail(size: usize) -> bool {
    FAIL_NEXT_SIZE.with(|target| {
        if target.get() != size {
            return false;
        }
        target.set(0);
        true
    })
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if should_fail(layout.size()) {
            return std::ptr::null_mut();
        }
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if should_fail(layout.size()) {
            return std::ptr::null_mut();
        }
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
        if should_fail(new_size) {
            return std::ptr::null_mut();
        }
        let ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !ptr.is_null() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            DEALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        ptr
    }
}

/// Makes the next allocation of exactly `size` bytes on this thread fail.
#[allow(dead_code)]
pub fn fail_next_allocation_of_size(size: usize) {
    assert!(size > 0, "allocation failure size must be nonzero");
    FAIL_NEXT_SIZE.with(|target| {
        assert_eq!(target.replace(size), 0, "allocation failure already armed");
    });
}

/// Asserts that a size-specific allocation failure was observed, then disarms
/// it before reporting a test failure.
#[allow(dead_code)]
pub fn assert_allocation_failure_consumed() {
    FAIL_NEXT_SIZE.with(|target| {
        let pending = target.replace(0);
        assert_eq!(pending, 0, "armed allocation failure was not consumed");
    });
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

    #[allow(dead_code)]
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

    #[allow(dead_code)]
    pub fn assert_delta_since(self, before: Self, allocations: usize, deallocations: usize) {
        assert_eq!(
            self.allocs - before.allocs,
            allocations,
            "unexpected allocation-count delta"
        );
        assert_eq!(
            self.deallocs - before.deallocs,
            deallocations,
            "unexpected deallocation-count delta"
        );
    }
}
