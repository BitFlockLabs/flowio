use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::LocalKey;

/// Test-only allocator with process-wide and calling-thread accounting.
///
/// This is integration-test instrumentation, not a production allocator.
pub struct CountingAllocator;

static PROCESS_ALLOCS: AtomicUsize = AtomicUsize::new(0);
static PROCESS_DEALLOCS: AtomicUsize = AtomicUsize::new(0);
static THREAD_DESTRUCTION_PROBES: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Copy)]
struct AllocationCounts {
    allocs: usize,
    deallocs: usize,
}

impl AllocationCounts {
    const ZERO: Self = Self {
        allocs: 0,
        deallocs: 0,
    };

    fn assert_unchanged_since(self, before: Self) {
        assert_eq!(
            self.allocs, before.allocs,
            "measured path performed heap allocations"
        );
        assert_eq!(
            self.deallocs, before.deallocs,
            "measured path performed heap deallocations"
        );
    }

    fn assert_delta_since(self, before: Self, allocations: usize, deallocations: usize) {
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

struct ThreadAllocatorState {
    fail_next_allocation: Cell<bool>,
    fail_next_size: Cell<usize>,
    count_size_allocs: Cell<(usize, usize)>,
    count_size_zeroed_allocs: Cell<(usize, usize)>,
    counts: Cell<AllocationCounts>,
}

impl ThreadAllocatorState {
    const fn new() -> Self {
        Self {
            fail_next_allocation: Cell::new(false),
            fail_next_size: Cell::new(0),
            count_size_allocs: Cell::new((0, 0)),
            count_size_zeroed_allocs: Cell::new((0, 0)),
            counts: Cell::new(AllocationCounts::ZERO),
        }
    }

    fn add_counts(&self, allocations: usize, deallocations: usize) {
        let counts = self.counts.get();
        self.counts.set(AllocationCounts {
            allocs: counts.allocs.saturating_add(allocations),
            deallocs: counts.deallocs.saturating_add(deallocations),
        });
    }
}

// Allocator callbacks access this direct const TLS. Keeping it destructor-free
// avoids destructor registration or lazy initialization re-entering allocation.
const _: () = assert!(!std::mem::needs_drop::<ThreadAllocatorState>());

struct ThreadDestructionProbe {
    armed: Cell<bool>,
}

impl Drop for ThreadDestructionProbe {
    fn drop(&mut self) {
        if self.armed.get() {
            let inaccessible = try_with_local(&THREAD_DESTRUCTION_PROBE, |_| ()).is_none();
            let before = current_process_counts();
            let allocation = Box::new([0_u8; 257]);
            std::hint::black_box(&allocation);
            drop(allocation);
            let after = current_process_counts();
            let exact_allocator_delta = after.allocs.wrapping_sub(before.allocs) == 1
                && after.deallocs.wrapping_sub(before.deallocs) == 1;
            if inaccessible && exact_allocator_delta {
                THREAD_DESTRUCTION_PROBES.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

thread_local! {
    static THREAD_STATE: ThreadAllocatorState = const { ThreadAllocatorState::new() };
    static THREAD_DESTRUCTION_PROBE: ThreadDestructionProbe = const {
        ThreadDestructionProbe {
            armed: Cell::new(false),
        }
    };
}

fn try_with_local<T, R>(key: &'static LocalKey<T>, f: impl FnOnce(&T) -> R) -> Option<R> {
    key.try_with(f).ok()
}

fn should_fail(size: usize) -> bool {
    try_with_local(&THREAD_STATE, |state| {
        if state.fail_next_allocation.replace(false) {
            return true;
        }
        if state.fail_next_size.get() != size {
            return false;
        }
        state.fail_next_size.set(0);
        true
    })
    .unwrap_or(false)
}

fn record_allocation(size: usize, zeroed: bool) {
    PROCESS_ALLOCS.fetch_add(1, Ordering::Relaxed);
    let _ = try_with_local(&THREAD_STATE, |state| {
        state.add_counts(1, 0);

        let (target, allocations) = state.count_size_allocs.get();
        if target == size {
            state
                .count_size_allocs
                .set((target, allocations.saturating_add(1)));
        }

        if zeroed {
            let (target, allocations) = state.count_size_zeroed_allocs.get();
            if target == size {
                state
                    .count_size_zeroed_allocs
                    .set((target, allocations.saturating_add(1)));
            }
        }
    });
}

fn record_deallocation() {
    PROCESS_DEALLOCS.fetch_add(1, Ordering::Relaxed);
    let _ = try_with_local(&THREAD_STATE, |state| state.add_counts(0, 1));
}

fn record_reallocation(new_size: usize) {
    PROCESS_ALLOCS.fetch_add(1, Ordering::Relaxed);
    PROCESS_DEALLOCS.fetch_add(1, Ordering::Relaxed);
    let _ = try_with_local(&THREAD_STATE, |state| {
        state.add_counts(1, 1);
        let (target, allocations) = state.count_size_allocs.get();
        if target == new_size {
            state
                .count_size_allocs
                .set((target, allocations.saturating_add(1)));
        }
    });
}

fn with_thread_state<R>(f: impl FnOnce(&ThreadAllocatorState) -> R) -> R {
    try_with_local(&THREAD_STATE, f)
        .expect("allocation test instrumentation is unavailable during thread destruction")
}

fn current_thread_counts() -> AllocationCounts {
    with_thread_state(|state| state.counts.get())
}

fn current_process_counts() -> AllocationCounts {
    AllocationCounts {
        allocs: PROCESS_ALLOCS.load(Ordering::Relaxed),
        deallocs: PROCESS_DEALLOCS.load(Ordering::Relaxed),
    }
}

/// Arms one allocation/deallocation pair from a dedicated TLS destructor.
/// Used to prove allocator callbacks tolerate thread teardown.
#[allow(dead_code)]
pub fn allocate_during_tls_destruction() {
    THREAD_DESTRUCTION_PROBE
        .try_with(|probe| {
            assert!(
                !probe.armed.replace(true),
                "thread-destruction allocation probe already armed"
            );
        })
        .expect("thread-destruction allocation probe is already being destroyed");
}

/// Returns the number of completed TLS-destruction probes.
#[allow(dead_code)]
pub fn completed_tls_destruction_probes() -> usize {
    THREAD_DESTRUCTION_PROBES.load(Ordering::Relaxed)
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if should_fail(layout.size()) {
            return std::ptr::null_mut();
        }
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            record_allocation(layout.size(), false);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if should_fail(layout.size()) {
            return std::ptr::null_mut();
        }
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record_allocation(layout.size(), true);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        record_deallocation();
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if should_fail(new_size) {
            return std::ptr::null_mut();
        }
        let ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !ptr.is_null() {
            record_reallocation(new_size);
        }
        ptr
    }
}

/// Makes the next allocation on this thread fail, regardless of its layout.
#[allow(dead_code)]
pub fn fail_next_allocation() {
    with_thread_state(|state| {
        assert_eq!(
            state.fail_next_size.get(),
            0,
            "size-specific allocation failure armed"
        );
        assert!(
            !state.fail_next_allocation.replace(true),
            "allocation failure already armed"
        );
    });
}

/// Makes the next allocation of exactly `size` bytes on this thread fail.
#[allow(dead_code)]
pub fn fail_next_allocation_of_size(size: usize) {
    assert!(size > 0, "allocation failure size must be nonzero");
    with_thread_state(|state| {
        assert!(
            !state.fail_next_allocation.get(),
            "layout-independent allocation failure armed"
        );
        assert_eq!(
            state.fail_next_size.replace(size),
            0,
            "allocation failure already armed"
        );
    });
}

/// Asserts that the armed allocation failure was observed, then disarms any
/// remaining failure gate before reporting a test failure.
#[allow(dead_code)]
pub fn assert_allocation_failure_consumed() {
    with_thread_state(|state| {
        let any_pending = state.fail_next_allocation.replace(false);
        let pending = state.fail_next_size.replace(0);
        assert!(
            !any_pending && pending == 0,
            "armed allocation failure was not consumed"
        );
    });
}

/// Starts counting successful allocations of exactly `size` bytes on this
/// thread. Reallocations to that size count as allocations.
#[allow(dead_code)]
pub fn start_counting_allocations_of_size(size: usize) {
    assert!(size > 0, "allocation count size must be nonzero");
    with_thread_state(|state| {
        let previous = state.count_size_allocs.replace((size, 0));
        assert_eq!(previous.0, 0, "allocation-size counter already armed");
    });
}

/// Stops the active size-specific counter and returns its allocation count.
#[allow(dead_code)]
pub fn finish_counting_allocations_of_size() -> usize {
    with_thread_state(|state| {
        let (size, allocations) = state.count_size_allocs.replace((0, 0));
        assert_ne!(size, 0, "allocation-size counter was not armed");
        allocations
    })
}

/// Starts counting successful zero-initializing allocations of exactly `size`
/// bytes on this thread.
#[allow(dead_code)]
pub fn start_counting_zeroed_allocations_of_size(size: usize) {
    assert!(size > 0, "allocation count size must be nonzero");
    with_thread_state(|state| {
        let previous = state.count_size_zeroed_allocs.replace((size, 0));
        assert_eq!(
            previous.0, 0,
            "zeroed allocation-size counter already armed"
        );
    });
}

/// Stops the active zero-initializing allocation counter.
#[allow(dead_code)]
pub fn finish_counting_zeroed_allocations_of_size() -> usize {
    with_thread_state(|state| {
        let (size, allocations) = state.count_size_zeroed_allocs.replace((0, 0));
        assert_ne!(size, 0, "zeroed allocation-size counter was not armed");
        allocations
    })
}

macro_rules! define_allocation_snapshot {
    ($(#[$meta:meta])* $name:ident, $current:path, $marker:ty) => {
        $(#[$meta])*
        #[derive(Clone, Copy)]
        pub struct $name(AllocationCounts, PhantomData<$marker>);

        impl $name {
            #[allow(dead_code)]
            pub fn current() -> Self {
                Self($current(), PhantomData)
            }

            #[allow(dead_code)]
            pub fn assert_unchanged_since(self, before: Self) {
                self.0.assert_unchanged_since(before.0);
            }

            #[allow(dead_code)]
            pub fn assert_delta_since(
                self,
                before: Self,
                allocations: usize,
                deallocations: usize,
            ) {
                self.0
                    .assert_delta_since(before.0, allocations, deallocations);
            }
        }
    };
}

define_allocation_snapshot!(
    /// Snapshot of allocator callbacks executed by the calling thread.
    ///
    /// A cross-thread deallocation belongs to the thread that executes the
    /// deallocation callback, not the thread that originally allocated.
    ThreadLocalAllocationSnapshot,
    current_thread_counts,
    Rc<()>
);

define_allocation_snapshot!(
    /// Snapshot of allocator callbacks executed anywhere in the test process.
    ProcessWideAllocationSnapshot,
    current_process_counts,
    ()
);
