use flowio::utils::memory::owned::{OwnedBuffer, OwnedBufferPool};
use flowio::utils::memory::pool::*;
use static_assertions::assert_not_impl_any;
use std::mem::MaybeUninit;

// verbose memory provider
struct VerboseProvider {
    buffer: Vec<u8>,
    offset: usize,
    alignment: usize,
}

impl VerboseProvider {
    fn new(size: usize, align: usize) -> Self {
        Self {
            buffer: vec![0u8; size],
            offset: 0,
            alignment: align,
        }
    }
}

impl flowio::utils::memory::provider::MemoryProvider for VerboseProvider {
    fn init(&mut self, required_align: usize) {
        // Escalation: The provider adapts to the stricter requirement
        self.alignment = core::cmp::max(self.alignment, required_align);
        self.offset = 0;
        println!(
            "  [Step] Provider: Initialized with Alignment Guarantee: {}",
            self.alignment
        );
    }

    fn alignment_guarantee(&self) -> usize {
        self.alignment
    }

    fn request_memory(&mut self, size: usize) -> Option<*mut u8> {
        let base = self.buffer.as_mut_ptr() as usize;
        let current = base + self.offset;
        let aligned = (current + self.alignment - 1) & !(self.alignment - 1);
        let padding = aligned - current;

        if self.offset + padding + size > self.buffer.len() {
            println!(
                "  [Actual] Provider: Failed to allocate {} bytes (OOM)",
                size
            );
            return None;
        }

        self.offset += padding + size;
        let ptr = aligned as *mut u8;
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

/// Pool is !Unpin because buffers keep a raw back-pointer to it; checked-out
/// OwnedBuffer handles are !Send/!Sync and stay on the runtime thread.
#[test]
fn owned_buffer_pool_requires_pin_and_buffers_stay_single_threaded() {
    assert_not_impl_any!(OwnedBufferPool<'static, Task, VerboseProvider>: Unpin);
    assert_not_impl_any!(OwnedBuffer<'static, 'static, Task, VerboseProvider>: Send, Sync);
}

#[test]
fn owned_buffer_pool_allows_multiple_live_buffers_from_shared_pin() {
    let mut provider = VerboseProvider::new(4096, 64);
    let mut pool = OwnedBufferPool::<Task, _>::new_uninit(&mut provider, 2).unwrap();
    pool.init();

    let pool = Box::pin(pool);
    let first = pool
        .as_ref()
        .alloc(404)
        .expect("first owned buffer alloc failed");
    let second = pool
        .as_ref()
        .alloc(405)
        .expect("second owned buffer alloc failed");
    assert_eq!(first.id, 404);
    assert_eq!(second.id, 405);

    drop(first);
    drop(second);

    let reused = pool.as_ref().alloc(406).expect("owned buffer reuse failed");
    assert_eq!(reused.id, 406);
}

/// into_raw_parts forgets the handle without changing the outstanding count;
/// from_raw reconstructs it, so the reconstructed Drop returns the slot.
#[test]
fn owned_buffer_raw_parts_round_trip_returns_slot_to_pool() {
    let mut provider = VerboseProvider::new(127, 64);
    let mut pool = OwnedBufferPool::<Task, _>::new_uninit(&mut provider, 1).unwrap();
    pool.init();

    let pool = Box::pin(pool);
    let buffer = pool.as_ref().alloc(700).expect("owned buffer alloc failed");
    let (ptr, pool_ptr) = buffer.into_raw_parts();

    let recovered = unsafe { OwnedBuffer::from_raw(ptr, pool_ptr) };
    assert_eq!(recovered.id, 700);
    drop(recovered);

    let reused = pool
        .as_ref()
        .alloc(701)
        .expect("raw round-trip did not return slot to pool");
    assert_eq!(reused.id, 701);
}

/// into_raw forgets the handle; free_raw returns the slot and decrements the
/// outstanding count so the pool drop invariant remains balanced.
#[test]
fn owned_buffer_free_raw_returns_slot_to_pool() {
    let mut provider = VerboseProvider::new(127, 64);
    let mut pool = OwnedBufferPool::<Task, _>::new_uninit(&mut provider, 1).unwrap();
    pool.init();

    let pool = Box::pin(pool);
    let buffer = pool.as_ref().alloc(800).expect("owned buffer alloc failed");
    let ptr = buffer.into_raw();

    unsafe { pool.as_ref().free_raw(ptr) };

    let reused = pool
        .as_ref()
        .alloc(801)
        .expect("free_raw did not return slot to pool");
    assert_eq!(reused.id, 801);
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

    // Verify distance (Should be obj_size = 8 with slist-based slab header link)
    let dist = (t2 as usize) - (t1 as usize);
    println!("  [Actual] Distance T1->T2: {} bytes", dist);
    assert!(dist >= 8);

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
}
