use flowio::test_support::utils::list::intrusive::slist::Link;
use flowio::test_support::utils::memory::pool::*;
use flowio::test_support::utils::memory::provider::{BasicMemoryProvider, MemoryProvider};
use flowio::test_support::utils::memory::slab::{Slab, SlabAllocator};
use std::mem::{MaybeUninit, align_of, size_of};

// verbose memory provider
struct VerboseProvider {
    buffer: Vec<u8>,
    offset: usize,
    alignment: usize,
}

impl VerboseProvider {
    fn new(size: usize, align: usize) -> Self {
        assert!(
            align.is_power_of_two(),
            "test provider alignment must be valid"
        );
        Self {
            buffer: vec![0u8; size],
            offset: 0,
            alignment: align,
        }
    }
}

// SAFETY: the backing Vec is fully materialized once and never resized, so
// returned pointers retain stable Vec provenance. Checked bump allocation
// produces disjoint ranges aligned to the current power-of-two guarantee; the
// no-op free never invalidates another live range.
unsafe impl MemoryProvider for VerboseProvider {
    fn init(&mut self, required_align: usize) {
        // Escalation: The provider adapts to the stricter requirement
        self.alignment = core::cmp::max(self.alignment, required_align);
        println!(
            "  [Step] Provider: Initialized with Alignment Guarantee: {}",
            self.alignment
        );
    }

    fn alignment_guarantee(&self) -> usize {
        self.alignment
    }

    fn request_memory(&mut self, size: usize) -> Option<*mut u8> {
        if size == 0 {
            return None;
        }

        // SAFETY: `offset` advances only to a checked end within `buffer` and
        // the Vec is never resized.
        let current = unsafe { self.buffer.as_mut_ptr().add(self.offset) };
        let padding = current.align_offset(self.alignment);
        if padding == usize::MAX {
            return None;
        }
        let end = self.offset.checked_add(padding)?.checked_add(size)?;

        if end > self.buffer.len() {
            println!(
                "  [Actual] Provider: Failed to allocate {} bytes (OOM)",
                size
            );
            return None;
        }

        self.offset = end;
        // SAFETY: the checked `end` proves `padding` stays within the same Vec
        // allocation and leaves `size` bytes available from the result.
        let ptr = unsafe { current.add(padding) };
        println!(
            "  [Actual] Provider: Allocated {} bytes at {:p} (Padding: {} bytes)",
            size, ptr, padding
        );
        Some(ptr)
    }

    unsafe fn free_memory(&mut self, _ptr: *mut u8, size: usize) {
        println!(
            "  [Actual] Provider: Freeing {} bytes (noop in bump allocator)",
            size
        );
    }
}

// Small test object; the pool rounds the slot up to the 8-byte slab link size.
struct Task {
    id: u32,
}
impl InPlaceInit for Task {
    type Args = u32;
    fn init_at(slot: &mut MaybeUninit<Self>, id: Self::Args) {
        slot.write(Task { id });
    }
}

// 64 bytes test object
#[repr(C, align(64))]
struct HardwareTask {
    id: u32,
}
impl InPlaceInit for HardwareTask {
    type Args = u32;
    fn init_at(slot: &mut MaybeUninit<Self>, id: Self::Args) {
        slot.write(HardwareTask { id });
    }
}

#[test]
fn slab_allocator_aligns_header_even_for_byte_slots() {
    let mut provider = VerboseProvider::new(1024, 1);
    let mut allocator =
        SlabAllocator::new_uninit(&mut provider, 1, 1, 1).expect("slab allocator init");

    assert!(
        allocator.get_slab_alignment() >= std::mem::align_of::<Slab>(),
        "slab alignment must satisfy the Slab header alignment"
    );

    allocator.init();
    let slab = allocator
        .provide_slab()
        .expect("byte-slot slab should allocate");
    assert_eq!(
        (slab as usize) % std::mem::align_of::<Slab>(),
        0,
        "slab header pointer must be properly aligned"
    );
    unsafe { allocator.free_slab(slab.cast::<u8>()) };
}

#[test]
fn basic_provider_frees_with_alloc_time_alignment_after_reinit() {
    let mut provider = BasicMemoryProvider::new();
    provider.init(8);
    let first = provider
        .request_memory(32)
        .expect("first provider allocation failed");
    assert_eq!((first as usize) % 8, 0);

    provider.init(128);
    let second = provider
        .request_memory(32)
        .expect("second provider allocation failed");
    assert_eq!((second as usize) % 128, 0);

    unsafe {
        provider.free_memory(first, 32);
        provider.free_memory(second, 32);
    }
}

#[test]
fn basic_provider_rejects_request_size_overflow() {
    let mut provider = BasicMemoryProvider::new();
    provider.init(64);

    assert!(
        provider.request_memory(usize::MAX).is_none(),
        "header plus payload overflow must fail before allocation"
    );
}

#[test]
fn basic_provider_returns_aligned_nonoverlapping_live_ranges() {
    const ALIGNMENT: usize = 256;
    const FIRST_SIZE: usize = 33;
    const SECOND_SIZE: usize = 257;

    let mut provider = BasicMemoryProvider::new();
    provider.init(ALIGNMENT);
    let first = provider
        .request_memory(FIRST_SIZE)
        .expect("first provider allocation failed");
    let second = provider
        .request_memory(SECOND_SIZE)
        .expect("second provider allocation failed");

    assert_eq!((first as usize) % ALIGNMENT, 0);
    assert_eq!((second as usize) % ALIGNMENT, 0);
    let first_range = first as usize..(first as usize + FIRST_SIZE);
    let second_range = second as usize..(second as usize + SECOND_SIZE);
    assert!(
        first_range.end <= second_range.start || second_range.end <= first_range.start,
        "simultaneously live provider allocations must not overlap"
    );

    unsafe {
        std::ptr::write_bytes(first, 0x11, FIRST_SIZE);
        std::ptr::write_bytes(second, 0x22, SECOND_SIZE);
        assert!((0..FIRST_SIZE).all(|index| *first.add(index) == 0x11));
        assert!((0..SECOND_SIZE).all(|index| *second.add(index) == 0x22));
        provider.free_memory(second, SECOND_SIZE);
        provider.free_memory(first, FIRST_SIZE);
    }
}

#[test]
fn pool_free_null_is_noop() {
    let mut provider = BasicMemoryProvider::new();
    let mut pool = Pool::<Task, _>::new_uninit(&mut provider, 1).unwrap();
    pool.init();

    unsafe { pool.free(std::ptr::null_mut()) };

    let task = unsafe { pool.alloc(902).expect("pool must remain usable") };
    assert_eq!(unsafe { (*task).id }, 902);
    unsafe { pool.free(task) };
}

#[test]
fn pool_rejects_slot_count_overflow() {
    let mut provider = BasicMemoryProvider::new();
    assert!(size_of::<Task>() <= size_of::<Link>());
    assert!(align_of::<Task>() <= align_of::<Link>());
    let slot_stride = size_of::<Link>();
    let first_overflowing_slot_count = usize::MAX / slot_stride + 1;
    assert!(
        slot_stride
            .checked_mul(first_overflowing_slot_count)
            .is_none(),
        "the selected count must overflow slot-count multiplication"
    );
    assert_eq!(
        slot_stride.wrapping_mul(first_overflowing_slot_count),
        0,
        "unchecked multiplication would leave valid downstream slab geometry"
    );

    assert_eq!(
        Pool::<Task, _>::new_uninit(&mut provider, first_overflowing_slot_count)
            .err()
            .expect("pool slot geometry must reject multiplication overflow"),
        PoolConfigError::SizeOverflow
    );

    let last_multiplication_safe_count = first_overflowing_slot_count - 1;
    let last_payload_size = slot_stride
        .checked_mul(last_multiplication_safe_count)
        .expect("the adjacent lower count must fit slot-count multiplication");
    assert!(
        size_of::<Slab>().checked_add(last_payload_size).is_none(),
        "the adjacent lower count must fail only in later slab geometry"
    );
    assert_eq!(
        Pool::<Task, _>::new_uninit(&mut provider, last_multiplication_safe_count)
            .err()
            .expect("later slab geometry must reject its own overflow"),
        PoolConfigError::SizeOverflow
    );
}

#[test]
fn pool_drop_allows_balanced_raw_slots() {
    let mut provider = VerboseProvider::new(4096, 64);
    let mut pool = Pool::<Task, _>::new_uninit(&mut provider, 1).unwrap();
    pool.init();

    let task = unsafe { pool.alloc(900).expect("task allocation failed") };
    unsafe { pool.free(task) };
}

#[test]
fn pool_provider_exhaustion_still_reuses_returned_slot() {
    let unrounded_slab_size = std::mem::size_of::<Slab>() + 2 * std::mem::size_of::<usize>();
    let slab_size = (unrounded_slab_size + 7) & !7;
    // Leave enough base-alignment slack for exactly one slab on both 32- and
    // 64-bit targets, while making a second provider request fail.
    let mut provider = VerboseProvider::new(slab_size + 7, 8);
    let mut pool = Pool::<Task, _>::new_uninit(&mut provider, 2).unwrap();
    pool.init();

    let first = unsafe { pool.alloc(1).expect("first slot should allocate") };
    let second = unsafe { pool.alloc(2).expect("second slot should allocate") };
    assert!(
        unsafe { pool.alloc(3) }.is_none(),
        "provider exhaustion must be reported"
    );

    unsafe { pool.free(first) };
    let reused = unsafe {
        pool.alloc(4)
            .expect("a returned slot must remain reusable after exhaustion")
    };
    assert_eq!(reused, first);

    unsafe {
        pool.free(second);
        pool.free(reused);
    }
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "Pool dropped with 1 live slots still outstanding")]
fn pool_drop_debug_asserts_on_live_raw_slots() {
    let mut provider = VerboseProvider::new(4096, 64);
    let mut pool = Pool::<Task, _>::new_uninit(&mut provider, 1).unwrap();
    pool.init();
    let _task = unsafe { pool.alloc(901).expect("task allocation failed") };
}

#[test]
fn test_verbose_pool_logic() {
    println!("\n=== TEST CASE: Lifecycle & Alignment Verification ===");

    // Setup
    let mut provider = VerboseProvider::new(16384, 4096);
    let mut pool = Pool::<Task, _>::new_uninit(&mut provider, 2).unwrap();

    // ---------------------------------------------------------
    println!("\nStep 1: Initialize Pool");
    println!("  [Expected] Provider.init() called, lists initialized.");
    pool.init();

    // ---------------------------------------------------------
    println!("\nStep 2: Allocate first object (T1)");
    println!("  [Expected] Provider should be asked for a ~4096 byte slab.");
    let t1 = unsafe { pool.alloc(101).expect("T1 failed") };
    println!("  [Actual] T1 address: {:p}", t1);

    unsafe {
        assert_eq!((*t1).id, 101);
    }

    // ---------------------------------------------------------
    println!("\nStep 3: Allocate second object (T2)");
    println!("  [Expected] Should use existing slab (Bump allocation). No Provider call.");
    let t2 = unsafe { pool.alloc(102).expect("T2 failed") };
    println!("  [Actual] T2 address: {:p}", t2);

    // Verify the second object came from the next slot in the same slab.
    let dist = (t2 as usize) - (t1 as usize);
    println!("  [Actual] Distance T1->T2: {} bytes", dist);
    assert_eq!(
        dist,
        size_of::<Link>(),
        "T2 did not use the next slist-sized pool slot"
    );

    // ---------------------------------------------------------
    println!("\nStep 4: Free T1 and Re-allocate (Recycling)");
    println!("  [Expected] T1 moves to free_list. New allocation should return T1's address.");
    unsafe {
        pool.free(t1);
    }
    let t3 = unsafe { pool.alloc(103).expect("T3 failed") };
    println!("  [Actual] T3 address: {:p}", t3);
    assert_eq!(
        t1, t3,
        "Recycling failed: T3 did not reuse T1's memory slot"
    );

    // ---------------------------------------------------------
    println!("\nStep 5: Exhaust configured slab slots and trigger growth");
    println!("  [Expected] objs_per_slab=2, so the next live allocation needs a new slab.");

    let t4 = unsafe { pool.alloc(104).expect("T4 failed") };
    println!("  [Actual] T4 address: {:p}", t4);
    assert!(
        (t4 as usize).abs_diff(t2 as usize) > 1024,
        "T4 should come from a new slab, not first-slab alignment padding"
    );

    println!("\n=== TEST COMPLETED SUCCESSFULLY ===");
    unsafe {
        pool.free(t2);
        pool.free(t3);
        pool.free(t4);
    }
}

#[test]
fn test_strict_alignment_64() {
    println!("\n=== TEST CASE: Strict 64-Byte Hardware Alignment ===");

    let mut provider = VerboseProvider::new(4096, 64);
    let mut pool = Pool::<HardwareTask, _>::new_uninit(&mut provider, 4).unwrap();
    pool.init();

    println!("\nStep 1: Request Slab");
    println!("  [Expected] Provider returns address divisible by 64.");
    let t1 = unsafe { pool.alloc(1).unwrap() };
    let addr = t1 as usize;
    println!(
        "  [Actual] Object address: {:p} (Mod 64: {})",
        t1,
        addr % 64
    );

    assert_eq!(
        addr % 64,
        0,
        "Memory failed to respect 64-byte alignment guarantee"
    );

    println!("\n=== TEST COMPLETED SUCCESSFULLY ===");
    unsafe { pool.free(t1) };
}

#[test]
fn slab_does_not_allocate_from_alignment_padding() {
    let mut provider = VerboseProvider::new(12288, 4096);
    let mut pool = Pool::<Task, _>::new_uninit(&mut provider, 1).unwrap();
    pool.init();

    let first = unsafe { pool.alloc(1).expect("first slot should allocate") };
    let second = unsafe {
        pool.alloc(2)
            .expect("second slot should allocate from new slab")
    };

    let first_addr = first as usize;
    let second_addr = second as usize;
    assert!(
        second_addr.abs_diff(first_addr) >= 4096,
        "second allocation reused alignment padding instead of requesting a new slab"
    );
    unsafe {
        pool.free(first);
        pool.free(second);
    }
}

#[test]
fn test_alignment_conflict_resolution() {
    println!("\n=== TEST CASE: Alignment Conflict (Obj 64, Provider 8) ===");

    // Provider only guarantees 8-byte alignment
    let mut provider = VerboseProvider::new(8192, 8);

    #[repr(align(64))]
    struct BigAlign {
        _inner: u64,
    }
    impl InPlaceInit for BigAlign {
        type Args = ();
        fn init_at(slot: &mut MaybeUninit<Self>, _: ()) {
            slot.write(BigAlign { _inner: 0 });
        }
    }

    let mut pool = Pool::<BigAlign, _>::new_uninit(&mut provider, 2).unwrap();
    pool.init();

    let t1 = unsafe { pool.alloc(()).unwrap() };
    let addr = t1 as usize;
    println!("  [Actual] Obj Address: {:p} (Mod 64: {})", t1, addr % 64);

    assert_eq!(addr % 64, 0, "Failed to resolve alignment conflict!");
    println!("=== TEST COMPLETED SUCCESSFULLY ===");
    unsafe { pool.free(t1) };
}
