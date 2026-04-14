use flowio::utils::memory::pool::*;
use std::mem::MaybeUninit;

// verbose memory provider
struct VerboseProvider
{
    buffer: Vec<u8>,
    offset: usize,
    alignment: usize,
}

impl VerboseProvider
{
    fn new(size: usize, align: usize) -> Self
    {
        Self {
            buffer: vec![0u8; size],
            offset: 0,
            alignment: align,
        }
    }
}

impl flowio::utils::memory::provider::MemoryProvider for VerboseProvider
{
    fn init(&mut self, required_align: usize)
    {
        // Escalation: The provider adapts to the stricter requirement
        self.alignment = core::cmp::max(self.alignment, required_align);
        self.offset = 0;
        println!(
            "  [Step] Provider: Initialized with Alignment Guarantee: {}",
            self.alignment
        );
    }

    fn alignment_guarantee(&self) -> usize
    {
        self.alignment
    }

    fn request_memory(&mut self, size: usize) -> Option<*mut u8>
    {
        let base = self.buffer.as_mut_ptr() as usize;
        let current = base + self.offset;
        let aligned = (current + self.alignment - 1) & !(self.alignment - 1);
        let padding = aligned - current;

        if self.offset + padding + size > self.buffer.len()
        {
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

    unsafe fn free_memory(&mut self, _ptr: *mut u8, size: usize)
    {
        println!(
            "  [Actual] Provider: Freeing {} bytes (noop in bump allocator)",
            size
        );
    }
}

// 32 bytes test object
struct Task
{
    id: u32,
}
impl InPlaceInit for Task
{
    type Args = u32;
    fn init_at(slot: &mut MaybeUninit<Self>, id: Self::Args)
    {
        slot.write(Task { id });
    }
}

// 64 bytes test object
#[repr(C, align(64))]
struct HardwareTask
{
    id: u32,
}
impl InPlaceInit for HardwareTask
{
    type Args = u32;
    fn init_at(slot: &mut MaybeUninit<Self>, id: Self::Args)
    {
        slot.write(HardwareTask { id });
    }
}

#[test]
fn test_verbose_pool_logic()
{
    println!("\n=== TEST CASE: Lifecycle & Alignment Verification ===");

    // Setup
    let mut provider = VerboseProvider::new(8192, 4096);
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
    println!("\nStep 5: Exhaust slab and trigger growth");
    println!("  [Expected] Filling the 4KB slab until a NEW slab is requested from Provider.");

    unsafe {
        let mut count = 0;
        // We know from math ~254 objects fit in a 4KB slab.
        // We loop until a new request is seen in the logs.
        for i in 0..300
        {
            if pool.alloc(i).is_none()
            {
                println!("  [Actual] Pool reached OOM at {} objects", count);
                break;
            }
            count += 1;
        }
        assert!(count > 250);
    }

    println!("\n=== TEST COMPLETED SUCCESSFULLY ===");
}

#[test]
fn test_strict_alignment_64()
{
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
fn test_alignment_conflict_resolution()
{
    println!("\n=== TEST CASE: Alignment Conflict (Obj 64, Provider 8) ===");

    // Provider only guarantees 8-byte alignment
    let mut provider = VerboseProvider::new(8192, 8);

    #[repr(align(64))]
    struct BigAlign
    {
        _inner: u64,
    }
    impl InPlaceInit for BigAlign
    {
        type Args = ();
        fn init_at(slot: &mut MaybeUninit<Self>, _: ())
        {
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
