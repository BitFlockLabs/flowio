#[path = "common/counting_allocator.rs"]
mod counting_allocator;

use counting_allocator::{
    CountingAllocator, ProcessWideAllocationSnapshot, ThreadLocalAllocationSnapshot,
    allocate_during_tls_destruction, completed_tls_destruction_probes,
    finish_counting_allocations_of_size, finish_counting_zeroed_allocations_of_size,
    start_counting_allocations_of_size, start_counting_zeroed_allocations_of_size,
};
use std::alloc::{Layout, alloc, alloc_zeroed, dealloc, realloc};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

const STARTING: u8 = 0;
const WORKER_READY: u8 = 1;
const ALLOCATE: u8 = 2;
const ALLOCATION_HELD: u8 = 3;
const DEALLOCATE: u8 = 4;
const DEALLOCATION_COMPLETE: u8 = 5;
const EXIT: u8 = 6;

fn wait_for(state: &AtomicU8, expected: u8) {
    let started = Instant::now();
    while state.load(Ordering::Acquire) != expected {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "allocator scope worker did not reach state {expected}"
        );
        std::thread::yield_now();
    }
}

#[cfg(not(miri))]
fn identifier_count(source: &str, identifier: &str) -> usize {
    source
        .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
        .filter(|token| *token == identifier)
        .count()
}

#[cfg(not(miri))]
fn assert_snapshot_consumer_routes_are_explicit() {
    let thread_local_consumers = [
        ("iobuff_cow_alloc", include_str!("iobuff_cow_alloc.rs")),
        (
            "resolver_candidate_alloc",
            include_str!("resolver_candidate_alloc.rs"),
        ),
        (
            "resolver_dedup_alloc",
            include_str!("resolver_dedup_alloc.rs"),
        ),
        (
            "resolver_name_alloc",
            include_str!("resolver_name_alloc.rs"),
        ),
        ("tls_scratch_alloc", include_str!("tls_scratch_alloc.rs")),
    ];
    for (name, source) in thread_local_consumers {
        assert!(
            identifier_count(source, "ThreadLocalAllocationSnapshot") > 0,
            "{name} no longer uses calling-thread allocation snapshots"
        );
        assert_eq!(
            identifier_count(source, "ProcessWideAllocationSnapshot"),
            0,
            "{name} must not use process-wide allocation snapshots"
        );
        assert_eq!(identifier_count(source, "AllocationSnapshot"), 0);
    }

    let process_wide_consumer = include_str!("alloc_steady_state.rs");
    assert!(
        identifier_count(process_wide_consumer, "ProcessWideAllocationSnapshot") > 0,
        "alloc_steady_state no longer observes the complete test process"
    );
    assert_eq!(
        identifier_count(process_wide_consumer, "ThreadLocalAllocationSnapshot"),
        0,
        "alloc_steady_state must not hide peer-thread allocations"
    );
    assert_eq!(
        identifier_count(process_wide_consumer, "AllocationSnapshot"),
        0
    );
}

fn assert_snapshot_scopes_distinguish_worker_activity() {
    let state = Arc::new(AtomicU8::new(STARTING));
    let worker_state = Arc::clone(&state);
    let worker = std::thread::spawn(move || {
        let layout = Layout::from_size_align(257, 8).expect("probe layout must be valid");
        let local_before = ThreadLocalAllocationSnapshot::current();
        worker_state.store(WORKER_READY, Ordering::Release);

        wait_for(&worker_state, ALLOCATE);
        // SAFETY: `layout` is nonzero and valid. The pointer is checked before
        // use and released exactly once with the same layout below.
        let ptr = unsafe { alloc(layout) };
        assert!(!ptr.is_null(), "worker probe allocation failed");
        // The allocation itself is the behavior under observation. Make the
        // pointer externally opaque so release optimization cannot remove the
        // otherwise unused allocation/deallocation pair.
        std::hint::black_box(ptr);
        ThreadLocalAllocationSnapshot::current().assert_delta_since(local_before, 1, 0);
        worker_state.store(ALLOCATION_HELD, Ordering::Release);

        wait_for(&worker_state, DEALLOCATE);
        // SAFETY: `ptr` came from `alloc(layout)`, remains live, and has not
        // previously been deallocated.
        unsafe { dealloc(ptr, layout) };
        ThreadLocalAllocationSnapshot::current().assert_delta_since(local_before, 1, 1);
        worker_state.store(DEALLOCATION_COMPLETE, Ordering::Release);

        wait_for(&worker_state, EXIT);
    });

    wait_for(&state, WORKER_READY);
    let local_before = ThreadLocalAllocationSnapshot::current();
    let process_before = ProcessWideAllocationSnapshot::current();

    state.store(ALLOCATE, Ordering::Release);
    wait_for(&state, ALLOCATION_HELD);
    ThreadLocalAllocationSnapshot::current().assert_unchanged_since(local_before);
    ProcessWideAllocationSnapshot::current().assert_delta_since(process_before, 1, 0);

    state.store(DEALLOCATE, Ordering::Release);
    wait_for(&state, DEALLOCATION_COMPLETE);
    ThreadLocalAllocationSnapshot::current().assert_unchanged_since(local_before);
    ProcessWideAllocationSnapshot::current().assert_delta_since(process_before, 1, 1);

    state.store(EXIT, Ordering::Release);
    worker.join().expect("allocation probe worker panicked");
}

fn assert_zeroed_and_reallocation_accounting() {
    let zeroed_layout = Layout::from_size_align(263, 8).expect("zeroed layout must be valid");
    start_counting_allocations_of_size(zeroed_layout.size());
    start_counting_zeroed_allocations_of_size(zeroed_layout.size());
    let local_before = ThreadLocalAllocationSnapshot::current();
    let process_before = ProcessWideAllocationSnapshot::current();

    // SAFETY: `zeroed_layout` is valid and nonzero. The result is checked and
    // deallocated exactly once with the same layout below.
    let zeroed = unsafe { alloc_zeroed(zeroed_layout) };
    assert!(!zeroed.is_null(), "zeroed accounting probe failed");
    // SAFETY: the successful allocation owns `zeroed_layout.size()` readable,
    // initialized bytes until the matching deallocation below.
    assert!(
        unsafe { std::slice::from_raw_parts(zeroed, zeroed_layout.size()) }
            .iter()
            .all(|byte| *byte == 0)
    );
    ThreadLocalAllocationSnapshot::current().assert_delta_since(local_before, 1, 0);
    ProcessWideAllocationSnapshot::current().assert_delta_since(process_before, 1, 0);
    assert_eq!(finish_counting_allocations_of_size(), 1);
    assert_eq!(finish_counting_zeroed_allocations_of_size(), 1);
    // SAFETY: `zeroed` remains live and came from `alloc_zeroed(zeroed_layout)`.
    unsafe { dealloc(zeroed, zeroed_layout) };
    ThreadLocalAllocationSnapshot::current().assert_delta_since(local_before, 1, 1);
    ProcessWideAllocationSnapshot::current().assert_delta_since(process_before, 1, 1);

    let original_layout = Layout::from_size_align(127, 8).expect("source layout must be valid");
    const NEW_SIZE: usize = 509;
    // SAFETY: `original_layout` is valid and nonzero. The result is checked
    // before it becomes the source of the matching realloc below.
    let original = unsafe { alloc(original_layout) };
    assert!(!original.is_null(), "reallocation source probe failed");
    start_counting_allocations_of_size(NEW_SIZE);
    let local_before = ThreadLocalAllocationSnapshot::current();
    let process_before = ProcessWideAllocationSnapshot::current();

    // SAFETY: `original` is live from `alloc(original_layout)`, and `NEW_SIZE`
    // is nonzero. On success the returned pointer becomes the sole owner.
    let resized = unsafe { realloc(original, original_layout, NEW_SIZE) };
    assert!(!resized.is_null(), "reallocation accounting probe failed");
    ThreadLocalAllocationSnapshot::current().assert_delta_since(local_before, 1, 1);
    ProcessWideAllocationSnapshot::current().assert_delta_since(process_before, 1, 1);
    assert_eq!(finish_counting_allocations_of_size(), 1);

    let resized_layout =
        Layout::from_size_align(NEW_SIZE, original_layout.align()).expect("resized layout invalid");
    // SAFETY: `resized` is the live result of the successful realloc and
    // `resized_layout` describes its new size and unchanged alignment.
    unsafe { dealloc(resized, resized_layout) };
    ThreadLocalAllocationSnapshot::current().assert_delta_since(local_before, 1, 2);
    ProcessWideAllocationSnapshot::current().assert_delta_since(process_before, 1, 2);
}

fn assert_tls_destructor_allocator_activity_is_non_panicking() {
    let before = completed_tls_destruction_probes();
    let worker = std::thread::spawn(allocate_during_tls_destruction);
    let started = Instant::now();
    while completed_tls_destruction_probes() != before + 1 {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "TLS-destruction allocation probe did not complete"
        );
        std::thread::yield_now();
    }
    worker
        .join()
        .expect("allocator access from a TLS destructor panicked");
    assert_eq!(
        completed_tls_destruction_probes(),
        before + 1,
        "TLS destruction did not exercise an inaccessible local key and exact allocator delta"
    );
}

#[test]
fn allocation_snapshots_have_explicit_scopes_and_safe_tls_teardown() {
    let source = include_str!("common/counting_allocator.rs");
    assert!(
        !source.contains(".with("),
        "allocator test TLS access must remain fallible"
    );

    #[cfg(not(miri))]
    assert_snapshot_consumer_routes_are_explicit();
    assert_snapshot_scopes_distinguish_worker_activity();
    assert_zeroed_and_reallocation_accounting();
    assert_tls_destructor_allocator_activity_is_non_panicking();
}
