use flowio::runtime::buffer::pool::{IoBuffPool, IoBuffPoolConfig, IoBuffPoolConfigError};
use flowio::runtime::buffer::{IoBuffError, IoBuffReadOnly};

macro_rules! seg {
    ($chain:expr, $index:expr) => {
        $chain
            .get($index)
            .expect("valid pool chain segment in test")
    };
}

fn new_pool(config: IoBuffPoolConfig) -> IoBuffPool
{
    IoBuffPool::new(config).expect("pool config invalid")
}

// ============================================================================
// IoBuffPool — basic allocation
// ============================================================================

#[test]
fn pool_alloc_basic()
{
    println!("--- Pool alloc basic ---");
    let mut pool = new_pool(IoBuffPoolConfig {
        headroom: 16,
        payload: 256,
        tailroom: 8,
        objs_per_slab: 32,
    });
    pool.init();

    let buf = pool.alloc().unwrap();
    println!("  headroom_capacity:  {}", buf.headroom_capacity());
    println!("  payload_capacity:   {}", buf.payload_capacity());
    println!("  tailroom_capacity:  {}", buf.tailroom_capacity());
    println!("  headroom_remaining: {}", buf.headroom_remaining());
    println!("  payload_remaining:  {}", buf.payload_remaining());
    println!("  tailroom_remaining: {}", buf.tailroom_remaining());

    assert_eq!(buf.headroom_capacity(), 16);
    assert_eq!(buf.payload_capacity(), 256);
    assert_eq!(buf.tailroom_capacity(), 8);
    assert_eq!(buf.headroom_remaining(), 16);
    assert_eq!(buf.payload_remaining(), 256);
    assert_eq!(buf.tailroom_remaining(), 8);
    assert!(buf.is_empty());
    assert_eq!(buf.len(), 0);
}

#[test]
fn pool_new_zero_objs_per_slab_returns_error()
{
    let result = IoBuffPool::new(IoBuffPoolConfig {
        headroom: 0,
        payload: 64,
        tailroom: 0,
        objs_per_slab: 0,
    });
    assert!(matches!(
        result,
        Err(IoBuffPoolConfigError::ObjsPerSlabZero)
    ));
}

#[test]
fn pool_new_layout_overflow_returns_error()
{
    let result = IoBuffPool::new(IoBuffPoolConfig {
        headroom: usize::MAX,
        payload: 1,
        tailroom: 0,
        objs_per_slab: 1,
    });

    assert!(matches!(result, Err(IoBuffPoolConfigError::LayoutOverflow)));
}

#[test]
fn pool_alloc_before_init_returns_error()
{
    let mut pool = new_pool(IoBuffPoolConfig {
        headroom: 0,
        payload: 64,
        tailroom: 0,
        objs_per_slab: 1,
    });

    assert!(matches!(pool.alloc(), Err(IoBuffError::PoolNotInitialized)));
}

#[test]
fn pool_alloc_write_read()
{
    println!("--- Pool alloc + write + read ---");
    let mut pool = new_pool(IoBuffPoolConfig {
        headroom: 8,
        payload: 64,
        tailroom: 4,
        objs_per_slab: 16,
    });
    pool.init();

    let mut buf = pool.alloc().unwrap();
    buf.payload_append(b"pool data").unwrap();
    buf.headroom_prepend(b"H:").unwrap();
    buf.tailroom_append(b":T").unwrap();
    println!("  bytes: {:?}", buf.bytes());

    assert_eq!(buf.bytes(), b"H:pool data:T");
    assert_eq!(buf.payload_bytes(), b"pool data");
    assert_eq!(buf.payload_len(), 9);
    assert_eq!(buf.len(), 13);
}

// ============================================================================
// IoBuffPool — reuse after drop
// ============================================================================

#[test]
fn pool_reuse_after_drop()
{
    println!("--- Pool reuse after drop ---");
    let mut pool = new_pool(IoBuffPoolConfig {
        headroom: 0,
        payload: 64,
        tailroom: 0,
        objs_per_slab: 8,
    });
    pool.init();

    let buf1 = pool.alloc().unwrap();
    let ptr1 = IoBuffReadOnly::as_ptr(&buf1);
    println!("  First alloc ptr:  {:?}", ptr1);
    drop(buf1);

    let buf2 = pool.alloc().unwrap();
    let ptr2 = IoBuffReadOnly::as_ptr(&buf2);
    println!("  Second alloc ptr: {:?}", ptr2);
    assert_eq!(ptr1, ptr2, "pool should reuse the returned buffer slot");
}

#[test]
fn pool_reuse_resets_state()
{
    println!("--- Pool reuse resets buffer state ---");
    let mut pool = new_pool(IoBuffPoolConfig {
        headroom: 8,
        payload: 64,
        tailroom: 8,
        objs_per_slab: 8,
    });
    pool.init();

    let mut buf = pool.alloc().unwrap();
    buf.payload_append(b"stale data").unwrap();
    buf.headroom_prepend(b"H:").unwrap();
    buf.tailroom_append(b":T").unwrap();
    println!(
        "  Before return: bytes={:?}, len={}",
        buf.bytes(),
        buf.len()
    );
    drop(buf);

    let reused = pool.alloc().unwrap();
    println!(
        "  After reuse: len={}, payload_len={}",
        reused.len(),
        reused.payload_len()
    );
    assert!(reused.is_empty(), "reused buffer must start empty");
    assert_eq!(reused.payload_len(), 0);
    assert_eq!(reused.headroom_remaining(), 8);
    assert_eq!(reused.payload_remaining(), 64);
    assert_eq!(reused.tailroom_remaining(), 8);
}

// ============================================================================
// IoBuffPool — multiple alloc/drop cycles
// ============================================================================

#[test]
fn pool_multiple_cycles()
{
    println!("--- Pool 1000 alloc/drop cycles ---");
    let mut pool = new_pool(IoBuffPoolConfig {
        headroom: 0,
        payload: 128,
        tailroom: 0,
        objs_per_slab: 16,
    });
    pool.init();

    for i in 0..1000
    {
        let mut buf = pool.alloc().unwrap();
        buf.payload_append(format!("cycle-{i}").as_bytes()).unwrap();
        assert_eq!(&buf.payload_bytes()[..6], b"cycle-");
        drop(buf);
    }
    println!("  1000 cycles completed");
}

#[test]
fn pool_concurrent_buffers()
{
    println!("--- Pool multiple buffers alive at once ---");
    let mut pool = new_pool(IoBuffPoolConfig {
        headroom: 0,
        payload: 32,
        tailroom: 0,
        objs_per_slab: 8,
    });
    pool.init();

    let mut buffers = Vec::new();
    for i in 0..20
    {
        let mut buf = pool.alloc().unwrap();
        buf.payload_append(format!("buf-{i}").as_bytes()).unwrap();
        buffers.push(buf);
    }

    println!("  {} buffers alive simultaneously", buffers.len());
    for (i, buf) in buffers.iter().enumerate()
    {
        let expected = format!("buf-{i}");
        assert_eq!(buf.payload_bytes(), expected.as_bytes());
    }

    // Drop all — should return to free list
    drop(buffers);
    println!("  All dropped, pool should have 20 free slots");

    // Re-allocate — should reuse
    for _ in 0..20
    {
        let buf = pool.alloc().unwrap();
        assert!(buf.is_empty());
        drop(buf);
    }
    println!("  Re-allocated 20 from free list");
}

// ============================================================================
// IoBuffPool — slab growth
// ============================================================================

#[test]
fn pool_grows_across_slabs()
{
    println!("--- Pool slab growth ---");
    let mut pool = new_pool(IoBuffPoolConfig {
        headroom: 0,
        payload: 64,
        tailroom: 0,
        objs_per_slab: 4, // very small slab to force multiple slab allocations
    });
    pool.init();

    let mut ptrs = Vec::new();
    for i in 0..20
    {
        let buf = pool.alloc().unwrap();
        ptrs.push(IoBuffReadOnly::as_ptr(&buf));
        println!("  alloc #{}: ptr={:?}", i, ptrs.last().unwrap());
        std::mem::forget(buf); // intentionally leak to keep all slots alive
    }

    // All pointers should be unique
    let unique_count = {
        let mut sorted = ptrs.clone();
        sorted.sort();
        sorted.dedup();
        sorted.len()
    };
    assert_eq!(
        unique_count, 20,
        "all allocations must be at unique addresses"
    );
    println!("  20 unique allocations across multiple slabs: OK");
}

// ============================================================================
// IoBuffPool — freeze + clone with pool-allocated buffers
// ============================================================================

#[test]
fn pool_freeze_clone_return()
{
    println!("--- Pool: alloc → freeze → clone → drop all → reuse ---");
    let mut pool = new_pool(IoBuffPoolConfig {
        headroom: 0,
        payload: 64,
        tailroom: 0,
        objs_per_slab: 8,
    });
    pool.init();

    let mut buf = pool.alloc().unwrap();
    buf.payload_append(b"shared").unwrap();
    let ptr = IoBuffReadOnly::as_ptr(&buf);
    println!("  Original ptr: {:?}", ptr);

    let frozen = buf.freeze();
    let c1 = frozen.clone();
    let c2 = frozen.clone();

    // All share same backing storage
    assert_eq!(frozen.bytes().as_ptr(), ptr);
    assert_eq!(c1.bytes().as_ptr(), ptr);
    assert_eq!(c2.bytes().as_ptr(), ptr);

    drop(c1);
    drop(frozen);
    // c2 is the last reference — dropping it returns to pool
    drop(c2);

    // Re-allocate — should get the same slot back
    let reused = pool.alloc().unwrap();
    let reused_ptr = IoBuffReadOnly::as_ptr(&reused);
    println!("  Reused ptr: {:?}", reused_ptr);
    assert_eq!(
        ptr, reused_ptr,
        "slot should be returned to pool after all refs drop"
    );
    assert!(reused.is_empty());
}

// ============================================================================
// IoBuffPool — flat (no headroom/tailroom)
// ============================================================================

#[test]
fn pool_flat_buffers()
{
    println!("--- Pool flat (0 headroom, 0 tailroom) ---");
    let mut pool = new_pool(IoBuffPoolConfig {
        headroom: 0,
        payload: 4096,
        tailroom: 0,
        objs_per_slab: 8,
    });
    pool.init();

    let mut buf = pool.alloc().unwrap();
    assert_eq!(buf.headroom_capacity(), 0);
    assert_eq!(buf.payload_capacity(), 4096);
    assert_eq!(buf.tailroom_capacity(), 0);

    buf.payload_append(&[0xAB; 4096]).unwrap();
    assert_eq!(buf.payload_len(), 4096);
    assert_eq!(buf.len(), 4096);
    println!("  Flat 4096-byte pool buffer: OK");
}

// ============================================================================
// IoBuffPool — edge: single object per slab
// ============================================================================

#[test]
fn pool_single_obj_per_slab()
{
    println!("--- Pool: 1 object per slab ---");
    let mut pool = new_pool(IoBuffPoolConfig {
        headroom: 0,
        payload: 64,
        tailroom: 0,
        objs_per_slab: 1,
    });
    pool.init();

    for i in 0..10
    {
        let mut buf = pool.alloc().unwrap();
        buf.payload_append(format!("s{i}").as_bytes()).unwrap();
        drop(buf);
    }
    println!("  10 cycles with 1 obj/slab: OK");
}

// ============================================================================
// IoBuffPool — pool drop cleans up
// ============================================================================

#[test]
fn pool_drop_cleans_up()
{
    println!("--- Pool drop releases all slab pages ---");
    let mut pool = new_pool(IoBuffPoolConfig {
        headroom: 0,
        payload: 256,
        tailroom: 0,
        objs_per_slab: 4,
    });
    pool.init();

    // Allocate and drop many buffers to create multiple slabs.
    for _ in 0..100
    {
        let buf = pool.alloc().unwrap();
        drop(buf);
    }

    // Explicit drop — now safe because slab tracking uses a singly-linked
    // list with no self-referential sentinel.
    drop(pool);
    println!("  Pool dropped: no leak");
}

// ============================================================================
// IoBuffPool — full protocol pattern
// ============================================================================

#[test]
fn pool_full_protocol_pattern()
{
    println!("--- Pool: full protocol frame (headroom + payload + tailroom) ---");
    let mut pool = new_pool(IoBuffPoolConfig {
        headroom: 16,
        payload: 4096,
        tailroom: 8,
        objs_per_slab: 8,
    });
    pool.init();

    let mut buf = pool.alloc().unwrap();

    // 1. Fill payload
    buf.payload_append(b"request body here").unwrap();
    println!("  Payload: {:?}", buf.payload_bytes());

    // 2. Prepend protocol header
    buf.headroom_prepend(b"HDR:").unwrap();
    println!("  After header: {:?}", buf.bytes());

    // 3. Append trailer
    buf.tailroom_append(b"--END").unwrap();
    println!("  After trailer: {:?}", buf.bytes());

    assert_eq!(buf.bytes(), b"HDR:request body here--END");
    assert_eq!(buf.payload_len(), 17);
    assert_eq!(buf.headroom_remaining(), 12);
    assert_eq!(buf.tailroom_remaining(), 3);

    // 4. Freeze and share
    let frozen = buf.freeze();
    let c1 = frozen.clone();
    let c2 = frozen.clone();
    assert_eq!(c1.bytes(), b"HDR:request body here--END");
    assert_eq!(c2.bytes(), b"HDR:request body here--END");

    // 5. Drop all — returns to pool
    drop(c1);
    drop(c2);
    drop(frozen);

    // 6. Re-alloc from pool — should reuse
    let reused = pool.alloc().unwrap();
    assert!(reused.is_empty());
    assert_eq!(reused.headroom_remaining(), 16);
    assert_eq!(reused.payload_remaining(), 4096);
    assert_eq!(reused.tailroom_remaining(), 8);
    println!("  Full protocol cycle + pool reuse: OK");
}

// ============================================================================
// IoBuffPool + IoBuffVec integration
// ============================================================================

#[test]
fn pool_with_vec_chain()
{
    use flowio::runtime::buffer::iobuffvec::IoBuffVecMut;

    println!("--- Pool + Vec chain integration ---");
    let mut pool = new_pool(IoBuffPoolConfig {
        headroom: 8,
        payload: 256,
        tailroom: 0,
        objs_per_slab: 16,
    });
    pool.init();

    // Build a 3-segment message from pool buffers
    let mut header = pool.alloc().unwrap();
    header.payload_append(b"DIAMETER").unwrap();
    header.headroom_prepend(b"\x01").unwrap();

    let mut payload = pool.alloc().unwrap();
    payload.payload_append(b"AVP-DATA-HERE").unwrap();

    let mut trailer = pool.alloc().unwrap();
    trailer.payload_append(b"CRC32").unwrap();

    let mut chain = IoBuffVecMut::<3>::new();
    chain.push(header).unwrap();
    chain.push(payload).unwrap();
    chain.push(trailer).unwrap();

    println!("  Chain segments: {}", chain.segments());
    println!("  Chain total len: {}", chain.len());

    // Freeze the chain
    let frozen = chain.freeze();
    println!("  Frozen segments: {}", frozen.segments());
    println!("  seg0: {:?}", seg!(frozen, 0).bytes());
    println!("  seg1: {:?}", seg!(frozen, 1).bytes());
    println!("  seg2: {:?}", seg!(frozen, 2).bytes());

    assert_eq!(seg!(frozen, 0).bytes(), b"\x01DIAMETER");
    assert_eq!(seg!(frozen, 1).bytes(), b"AVP-DATA-HERE");
    assert_eq!(seg!(frozen, 2).bytes(), b"CRC32");

    assert_eq!(frozen.segments(), 3);
    println!(
        "  segment lengths: {}, {}, {}",
        seg!(frozen, 0).len(),
        seg!(frozen, 1).len(),
        seg!(frozen, 2).len()
    );

    // Clone the chain
    let cloned = frozen.clone();
    assert_eq!(seg!(cloned, 0).bytes(), seg!(frozen, 0).bytes());

    // Drop everything — pool reclaims all 3 buffers
    drop(cloned);
    drop(frozen);

    // Verify pool reuse
    let reused = pool.alloc().unwrap();
    assert!(reused.is_empty());
    println!("  Pool + Vec chain: OK");
}

// ============================================================================
// IoBuffPool — simulated kernel readv
// ============================================================================

#[test]
fn pool_simulated_readv()
{
    use flowio::runtime::buffer::iobuffvec::IoBuffVecMut;

    println!("--- Pool: simulated readv (kernel fills multiple buffers) ---");
    let mut pool = new_pool(IoBuffPoolConfig {
        headroom: 0,
        payload: 128,
        tailroom: 0,
        objs_per_slab: 8,
    });
    pool.init();

    // Prepare 3 receive buffers
    let mut chain = IoBuffVecMut::<3>::new();
    chain.push(pool.alloc().unwrap()).unwrap();
    chain.push(pool.alloc().unwrap()).unwrap();
    chain.push(pool.alloc().unwrap()).unwrap();

    println!("  writable_len: {}", chain.writable_len());
    assert_eq!(chain.writable_len(), 384);

    // Simulate kernel readv writing 200 bytes across the chain
    unsafe { chain.distribute_written(200) };

    println!("  seg0 payload_len: {}", seg!(chain, 0).payload_len());
    println!("  seg1 payload_len: {}", seg!(chain, 1).payload_len());
    println!("  seg2 payload_len: {}", seg!(chain, 2).payload_len());

    assert_eq!(seg!(chain, 0).payload_len(), 128); // full
    assert_eq!(seg!(chain, 1).payload_len(), 72); // partial
    assert_eq!(seg!(chain, 2).payload_len(), 0); // untouched
    println!("  Simulated readv: OK");
}

// ============================================================================
// IoBuffPool — lifetime safety regressions
// ============================================================================

#[test]
fn pool_buffer_survives_pool_drop()
{
    println!("--- Pool buffer survives pool drop ---");
    let mut pool = new_pool(IoBuffPoolConfig {
        headroom: 4,
        payload: 64,
        tailroom: 4,
        objs_per_slab: 8,
    });
    pool.init();

    let mut buf = pool.alloc().unwrap();
    buf.payload_append(b"data").unwrap();
    buf.headroom_prepend(b"H:").unwrap();
    let ptr = IoBuffReadOnly::as_ptr(&buf);

    drop(pool);

    assert_eq!(buf.bytes(), b"H:data");
    assert_eq!(IoBuffReadOnly::as_ptr(&buf), ptr);
    drop(buf);
    println!("  Buffer remained valid after dropping pool owner");
}

#[test]
fn pool_frozen_clone_survives_pool_drop()
{
    println!("--- Pool frozen clone survives pool drop ---");
    let mut pool = new_pool(IoBuffPoolConfig {
        headroom: 0,
        payload: 64,
        tailroom: 0,
        objs_per_slab: 8,
    });
    pool.init();

    let mut buf = pool.alloc().unwrap();
    buf.payload_append(b"shared").unwrap();
    let ptr = IoBuffReadOnly::as_ptr(&buf);
    let frozen = buf.freeze();
    let clone = frozen.clone();

    drop(pool);

    assert_eq!(frozen.bytes(), b"shared");
    assert_eq!(clone.bytes(), b"shared");
    assert_eq!(frozen.bytes().as_ptr(), ptr);
    assert_eq!(clone.bytes().as_ptr(), ptr);

    drop(frozen);
    drop(clone);
    println!("  Frozen pool buffers remained valid after dropping pool owner");
}

#[test]
fn pool_move_with_live_buffer_is_safe()
{
    println!("--- Moving outer pool handle with live buffer ---");
    let mut pool = new_pool(IoBuffPoolConfig {
        headroom: 0,
        payload: 64,
        tailroom: 0,
        objs_per_slab: 8,
    });
    pool.init();

    let mut buf = pool.alloc().unwrap();
    buf.payload_append(b"move-safe").unwrap();
    let ptr = IoBuffReadOnly::as_ptr(&buf);

    let mut moved_pool = Some(pool);
    let mut pool = moved_pool.take().unwrap();

    assert_eq!(buf.bytes(), b"move-safe");
    assert_eq!(IoBuffReadOnly::as_ptr(&buf), ptr);
    drop(buf);

    let reused = pool.alloc().unwrap();
    assert!(reused.is_empty());
    drop(reused);
    println!("  Moving the outer pool handle did not invalidate the live buffer");
}
