//! Frozen-chain length overflow over one real, readable sparse mapping.
//!
//! The mapping requires native Linux virtual-memory facilities, so this test
//! does not run under Miri. Smaller chain initialization, ownership, and
//! arithmetic tests do run under Miri.
#![cfg(not(miri))]

mod common;

use flowio::runtime::buffer::iobuffvec::IoBuffVec;
use flowio::runtime::buffer::{IoBuff, IoBuffMut, IoBuffReadOnly, IoBuffReadWrite};
use std::alloc::{GlobalAlloc, Layout, System};
use std::ptr;
use std::sync::atomic::{AtomicI32, AtomicPtr, AtomicUsize, Ordering};
use std::time::Duration;

const FROZEN_OVERFLOW_TEST: &str = "frozen_checked_len_reports_concrete_overflow";
const FROZEN_OVERFLOW_CHILD_ENV: &str = "FLOWIO_FROZEN_OVERFLOW_CHILD";
const FROZEN_OVERFLOW_STACK_BYTES: &str = "33554432";
const FROZEN_OVERFLOW_SEGMENTS: usize = 1usize << 18;
const SPARSE_PAYLOAD_BYTES: usize = 1usize << 46;
// The backing header's size/alignment are also asserted by the buffer module.
const SPARSE_LAYOUT_BYTES: usize = SPARSE_PAYLOAD_BYTES + 40;
const SPARSE_LAYOUT_ALIGNMENT: usize = 8;
const SENTINELS: [(usize, u8); 3] = [
    (0, 0x31),
    (SPARSE_PAYLOAD_BYTES / 2, 0x72),
    (SPARSE_PAYLOAD_BYTES - 1, 0xb4),
];

const BAD_LAYOUT: usize = 1;
const REPEATED_MAPPING: usize = 2;
const MAP_FAILED: usize = 4;
const BAD_DEALLOCATION: usize = 8;
const UNMAP_FAILED: usize = 16;
const LARGE_REALLOCATION: usize = 32;

static MAPPING_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
static MAPPINGS: AtomicUsize = AtomicUsize::new(0);
static UNMAPPINGS: AtomicUsize = AtomicUsize::new(0);
static ACTIVE_MAPPING: AtomicPtr<u8> = AtomicPtr::new(ptr::null_mut());
static ALLOCATOR_ERRORS: AtomicUsize = AtomicUsize::new(0);
static MAPPING_ERRNO: AtomicI32 = AtomicI32::new(0);

struct SparseFixtureAllocator;

#[global_allocator]
static ALLOCATOR: SparseFixtureAllocator = SparseFixtureAllocator;

fn exact_sparse_layout(layout: Layout) -> bool {
    layout.size() == SPARSE_LAYOUT_BYTES && layout.align() == SPARSE_LAYOUT_ALIGNMENT
}

fn record_allocator_error(error: usize) {
    ALLOCATOR_ERRORS.fetch_or(error, Ordering::Relaxed);
}

// SAFETY: Small allocations retain System's layout/alignment contracts. The
// single large allocation owns a real read/write anonymous mapping covering its
// complete requested layout, whose alignment is below the system page size.
// Exact pointer/layout pairing routes only that mapping to munmap. Callbacks
// use syscall/atomic operations without formatting, allocation, or unwinding.
unsafe impl GlobalAlloc for SparseFixtureAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.size() < SPARSE_PAYLOAD_BYTES {
            return unsafe { System.alloc(layout) };
        }
        if !exact_sparse_layout(layout) {
            record_allocator_error(BAD_LAYOUT);
            return ptr::null_mut();
        }
        if MAPPING_ATTEMPTS.fetch_add(1, Ordering::Relaxed) != 0 {
            record_allocator_error(REPEATED_MAPPING);
            return ptr::null_mut();
        }
        // SAFETY: No fixed address or existing mapping is replaced. Anonymous
        // memory starts zero-initialized across the entire requested range.
        let mapped = unsafe {
            libc::mmap(
                ptr::null_mut(),
                layout.size(),
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                -1,
                0,
            )
        };
        if mapped == libc::MAP_FAILED {
            MAPPING_ERRNO.store(unsafe { *libc::__errno_location() }, Ordering::Relaxed);
            record_allocator_error(MAP_FAILED);
            return ptr::null_mut();
        }
        // A null address cannot represent a successful GlobalAlloc allocation.
        if mapped.is_null() {
            if unsafe { libc::munmap(mapped, layout.size()) } != 0 {
                record_allocator_error(UNMAP_FAILED);
            }
            record_allocator_error(MAP_FAILED);
            return ptr::null_mut();
        }
        let mapped = mapped.cast::<u8>();
        ACTIVE_MAPPING.store(mapped, Ordering::Relaxed);
        MAPPINGS.fetch_add(1, Ordering::Relaxed);
        mapped
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if layout.size() < SPARSE_PAYLOAD_BYTES {
            unsafe { System.alloc_zeroed(layout) }
        } else {
            // Anonymous mmap already initializes the complete large range.
            unsafe { self.alloc(layout) }
        }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        let active = ACTIVE_MAPPING.load(Ordering::Relaxed);
        if layout.size() < SPARSE_PAYLOAD_BYTES && pointer != active {
            unsafe { System.dealloc(pointer, layout) };
            return;
        }
        if !exact_sparse_layout(layout) || pointer != active || active.is_null() {
            record_allocator_error(BAD_DEALLOCATION);
            return;
        }
        // SAFETY: This is the original live pointer and the exact layout size
        // recorded at allocation; only its final buffer owner reaches here.
        if unsafe { libc::munmap(pointer.cast(), layout.size()) } != 0 {
            MAPPING_ERRNO.store(unsafe { *libc::__errno_location() }, Ordering::Relaxed);
            record_allocator_error(UNMAP_FAILED);
            return;
        }
        ACTIVE_MAPPING.store(ptr::null_mut(), Ordering::Relaxed);
        UNMAPPINGS.fetch_add(1, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if layout.size() >= SPARSE_PAYLOAD_BYTES || new_size >= SPARSE_PAYLOAD_BYTES {
            record_allocator_error(LARGE_REALLOCATION);
            return ptr::null_mut();
        }
        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

fn assert_allocator_clean() {
    assert_eq!(
        ALLOCATOR_ERRORS.load(Ordering::Relaxed),
        0,
        "sparse allocator error; errno={}",
        MAPPING_ERRNO.load(Ordering::Relaxed)
    );
}

#[test]
fn frozen_checked_len_reports_concrete_overflow() {
    if std::env::var_os(FROZEN_OVERFLOW_CHILD_ENV).is_none() {
        common::run_exact_test_child_with_watchdog_env(
            FROZEN_OVERFLOW_TEST,
            FROZEN_OVERFLOW_CHILD_ENV,
            Duration::from_secs(30),
            &[("RUST_MIN_STACK", FROZEN_OVERFLOW_STACK_BYTES)],
        );
        return;
    }

    assert_eq!(usize::BITS, 64);
    assert_eq!(std::mem::size_of::<IoBuff>(), 32);
    assert_eq!(MAPPING_ATTEMPTS.load(Ordering::Relaxed), 0);
    let mut buffer = IoBuffMut::new(0, SPARSE_PAYLOAD_BYTES, 0).unwrap_or_else(|error| {
        panic!(
            "sparse buffer allocation failed: {error}; flags={}, errno={}",
            ALLOCATOR_ERRORS.load(Ordering::Relaxed),
            MAPPING_ERRNO.load(Ordering::Relaxed)
        )
    });
    assert_allocator_clean();
    assert_eq!(MAPPING_ATTEMPTS.load(Ordering::Relaxed), 1);
    assert_eq!(MAPPINGS.load(Ordering::Relaxed), 1);
    assert_eq!(buffer.writable_len(), SPARSE_PAYLOAD_BYTES);
    let payload = buffer.as_mut_ptr();
    let mapped = ACTIVE_MAPPING.load(Ordering::Relaxed);
    assert!(!mapped.is_null());
    // SAFETY: The real header and complete payload share one mapping. Only
    // these three in-range bytes are read/written, without a whole-range slice.
    unsafe {
        assert_eq!(payload, mapped.add(40));
        for (offset, value) in SENTINELS {
            assert_eq!(payload.add(offset).read_volatile(), 0);
            payload.add(offset).write_volatile(value);
        }
        // mmap initialized every payload byte, including untouched zero pages.
        buffer
            .payload_set_len_initialized(SPARSE_PAYLOAD_BYTES)
            .expect("the initialized mapped payload should publish");
    }
    let source = buffer.freeze();
    assert_eq!(source.as_ptr(), payload);
    assert_eq!(source.len(), SPARSE_PAYLOAD_BYTES);
    let mut chain = IoBuffVec::<FROZEN_OVERFLOW_SEGMENTS>::new_boxed_empty();
    assert_eq!(std::mem::size_of_val(&*chain), 8_388_616);
    assert_eq!(chain.capacity(), FROZEN_OVERFLOW_SEGMENTS);
    assert_eq!(chain.segments(), 0);
    assert_eq!(chain.checked_len(), Some(0));

    for _ in 0..FROZEN_OVERFLOW_SEGMENTS - 1 {
        chain
            .push(source.clone())
            .expect("the pre-overflow prefix should fit");
    }
    assert_eq!(chain.segments(), FROZEN_OVERFLOW_SEGMENTS - 1);
    assert_eq!(
        chain.checked_len(),
        Some(usize::MAX - (SPARSE_PAYLOAD_BYTES - 1))
    );
    chain
        .push(source.clone())
        .expect("the final segment should fit");
    assert_eq!(chain.segments(), FROZEN_OVERFLOW_SEGMENTS);
    assert_eq!(chain.checked_len(), None);
    assert_eq!(chain.len(), usize::MAX);
    for segment in chain.iter() {
        assert_eq!(segment.as_ptr(), payload);
        assert_eq!(segment.len(), SPARSE_PAYLOAD_BYTES);
    }
    let source = match source.try_mut() {
        Err(source) => source,
        Ok(_) => panic!("the populated chain must retain shared ownership"),
    };
    assert_eq!(UNMAPPINGS.load(Ordering::Relaxed), 0);
    drop(chain);
    assert_eq!(UNMAPPINGS.load(Ordering::Relaxed), 0);
    let source = match source.try_mut() {
        Ok(source) => source,
        Err(_) => panic!("dropping the chain must release every shared owner"),
    };
    assert_eq!(source.as_ptr(), payload);
    assert_eq!(source.payload_len(), SPARSE_PAYLOAD_BYTES);
    // SAFETY: The sole remaining owner still holds the complete mapping.
    for (offset, value) in SENTINELS {
        assert_eq!(
            unsafe { source.as_ptr().add(offset).read_volatile() },
            value
        );
    }
    drop(source);
    assert_allocator_clean();
    assert_eq!(MAPPING_ATTEMPTS.load(Ordering::Relaxed), 1);
    assert_eq!(MAPPINGS.load(Ordering::Relaxed), 1);
    assert_eq!(UNMAPPINGS.load(Ordering::Relaxed), 1);
    assert!(ACTIVE_MAPPING.load(Ordering::Relaxed).is_null());
}
