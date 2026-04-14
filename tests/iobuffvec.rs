mod common;

use common::TestIoBuffMut as IoBuffMut;
use flowio::runtime::buffer::IoBuffError;
use flowio::runtime::buffer::iobuffvec::IoBuffVecMut;

macro_rules! seg {
    ($chain:expr, $index:expr) => {
        $chain.get($index).expect("valid vectored segment in test")
    };
}

macro_rules! seg_mut {
    ($chain:expr, $index:expr) => {
        $chain
            .get_mut($index)
            .expect("valid mutable vectored segment in test")
    };
}

// ============================================================================
// IoBuffVecMut — construction and push
// ============================================================================

#[test]
fn vec_mut_new_empty()
{
    println!("--- IoBuffVecMut::new empty ---");
    let chain = IoBuffVecMut::<4>::new();
    println!(
        "  segments: {}, capacity: {}, len: {}",
        chain.segments(),
        chain.capacity(),
        chain.len()
    );
    assert_eq!(chain.segments(), 0);
    assert_eq!(chain.capacity(), 4);
    assert_eq!(chain.len(), 0);
    assert!(chain.is_empty());
}

#[test]
fn vec_mut_default()
{
    println!("--- IoBuffVecMut::default ---");
    let chain = IoBuffVecMut::<2>::default();
    assert_eq!(chain.segments(), 0);
    assert_eq!(chain.capacity(), 2);
}

#[test]
fn vec_mut_from_array()
{
    println!("--- IoBuffVecMut::from_array ---");
    let mut b1 = IoBuffMut::new(0, 16, 0);
    b1.payload_append(b"one").unwrap();
    let mut b2 = IoBuffMut::new(0, 16, 0);
    b2.payload_append(b"two").unwrap();

    let chain = IoBuffVecMut::<2>::from_array([b1, b2]);
    assert_eq!(chain.segments(), 2);
    assert_eq!(seg!(chain, 0).payload_bytes(), b"one");
    assert_eq!(seg!(chain, 1).payload_bytes(), b"two");
}

#[test]
fn vec_mut_push_single()
{
    println!("--- Push single buffer ---");
    let mut buf = IoBuffMut::new(0, 64, 0);
    buf.payload_append(b"hello").unwrap();
    println!(
        "  Buffer: payload_len={}, len={}",
        buf.payload_len(),
        buf.len()
    );

    let mut chain = IoBuffVecMut::<4>::new();
    chain.push(buf).unwrap();
    println!(
        "  After push: segments={}, len={}",
        chain.segments(),
        chain.len()
    );

    assert_eq!(chain.segments(), 1);
    assert_eq!(chain.len(), 5);
    assert!(!chain.is_empty());
}

#[test]
fn vec_mut_push_multiple()
{
    println!("--- Push multiple buffers ---");
    let mut header = IoBuffMut::new(0, 16, 0);
    header.payload_append(b"HDR:").unwrap();

    let mut payload = IoBuffMut::new(0, 256, 0);
    payload.payload_append(b"payload data here").unwrap();

    let mut trailer = IoBuffMut::new(0, 8, 0);
    trailer.payload_append(b"--END").unwrap();

    let mut chain = IoBuffVecMut::<3>::new();
    chain.push(header).unwrap();
    chain.push(payload).unwrap();
    chain.push(trailer).unwrap();

    println!("  segments: {}", chain.segments());
    println!("  total len: {}", chain.len());
    println!("  segment 0: {:?}", seg!(chain, 0).payload_bytes());
    println!("  segment 1: {:?}", seg!(chain, 1).payload_bytes());
    println!("  segment 2: {:?}", seg!(chain, 2).payload_bytes());

    assert_eq!(chain.segments(), 3);
    assert_eq!(chain.len(), 4 + 17 + 5);
    assert_eq!(seg!(chain, 0).payload_bytes(), b"HDR:");
    assert_eq!(seg!(chain, 1).payload_bytes(), b"payload data here");
    assert_eq!(seg!(chain, 2).payload_bytes(), b"--END");
}

#[test]
fn vec_mut_push_at_capacity_returns_error()
{
    println!("--- Push at capacity ---");
    let mut chain = IoBuffVecMut::<2>::new();
    chain.push(IoBuffMut::new(0, 8, 0)).unwrap();
    chain.push(IoBuffMut::new(0, 8, 0)).unwrap();

    let result = chain.push(IoBuffMut::new(0, 8, 0));
    println!("  Push #3 on capacity=2: {:?}", result);
    assert_eq!(result, Err(IoBuffError::ChainFull));
    assert_eq!(chain.segments(), 2);
}

#[test]
fn vec_mut_push_empty_buffers()
{
    println!("--- Push empty buffers ---");
    let mut chain = IoBuffVecMut::<3>::new();
    chain.push(IoBuffMut::new(0, 64, 0)).unwrap();
    chain.push(IoBuffMut::new(0, 128, 0)).unwrap();
    chain.push(IoBuffMut::new(0, 256, 0)).unwrap();

    println!(
        "  segments: {}, len: {}, writable_len: {}",
        chain.segments(),
        chain.len(),
        chain.writable_len()
    );

    assert_eq!(chain.segments(), 3);
    assert_eq!(chain.len(), 0);
    assert!(chain.is_empty());
    assert_eq!(chain.writable_len(), 64 + 128 + 256);
}

// ============================================================================
// IoBuffVec — frozen chain construction
// ============================================================================

#[test]
fn vec_frozen_new_empty()
{
    println!("--- IoBuffVec::new empty ---");
    let chain = flowio::runtime::buffer::iobuffvec::IoBuffVec::<4>::new();
    assert_eq!(chain.segments(), 0);
    assert_eq!(chain.capacity(), 4);
    assert_eq!(chain.len(), 0);
    assert!(chain.is_empty());
}

#[test]
fn vec_frozen_push_multiple()
{
    println!("--- IoBuffVec::push multiple frozen segments ---");
    let mut b1 = IoBuffMut::new(0, 16, 0);
    b1.payload_append(b"hello").unwrap();
    let mut b2 = IoBuffMut::new(0, 16, 0);
    b2.payload_append(b"world").unwrap();

    let mut chain = flowio::runtime::buffer::iobuffvec::IoBuffVec::<2>::new();
    chain.push(b1.freeze()).unwrap();
    chain.push(b2.freeze()).unwrap();

    assert_eq!(chain.segments(), 2);
    assert_eq!(seg!(chain, 0).bytes(), b"hello");
    assert_eq!(seg!(chain, 1).bytes(), b"world");
}

#[test]
fn vec_frozen_from_array()
{
    println!("--- IoBuffVec::from_array ---");
    let mut b1 = IoBuffMut::new(0, 16, 0);
    b1.payload_append(b"seg0").unwrap();
    let mut b2 = IoBuffMut::new(0, 16, 0);
    b2.payload_append(b"seg1").unwrap();

    let chain = flowio::runtime::buffer::iobuffvec::IoBuffVec::<2>::from_array([
        b1.freeze(),
        b2.freeze(),
    ]);

    assert_eq!(chain.segments(), 2);
    assert_eq!(seg!(chain, 0).bytes(), b"seg0");
    assert_eq!(seg!(chain, 1).bytes(), b"seg1");
}

#[test]
fn vec_frozen_push_at_capacity_returns_error()
{
    println!("--- IoBuffVec::push at capacity ---");
    let mut b1 = IoBuffMut::new(0, 8, 0);
    b1.payload_append(b"a").unwrap();
    let mut b2 = IoBuffMut::new(0, 8, 0);
    b2.payload_append(b"b").unwrap();
    let mut b3 = IoBuffMut::new(0, 8, 0);
    b3.payload_append(b"c").unwrap();

    let mut chain = flowio::runtime::buffer::iobuffvec::IoBuffVec::<2>::new();
    chain.push(b1.freeze()).unwrap();
    chain.push(b2.freeze()).unwrap();
    let result = chain.push(b3.freeze());
    assert_eq!(result, Err(IoBuffError::ChainFull));
}

// ============================================================================
// IoBuffVecMut — get / get_mut
// ============================================================================

#[test]
fn vec_mut_get()
{
    println!("--- get ---");
    let mut chain = IoBuffVecMut::<2>::new();
    let mut buf = IoBuffMut::new(0, 32, 0);
    buf.payload_append(b"segment0").unwrap();
    chain.push(buf).unwrap();

    let seg = seg!(chain, 0);
    println!("  get(0): payload={:?}", seg.payload_bytes());
    assert_eq!(seg.payload_bytes(), b"segment0");
}

#[test]
fn vec_mut_get_mut_modify()
{
    println!("--- get_mut modify ---");
    let mut chain = IoBuffVecMut::<2>::new();
    chain.push(IoBuffMut::new(0, 32, 0)).unwrap();

    seg_mut!(chain, 0).payload_append(b"modified").unwrap();
    println!(
        "  After modify: payload={:?}",
        seg!(chain, 0).payload_bytes()
    );
    assert_eq!(seg!(chain, 0).payload_bytes(), b"modified");
}

#[test]
fn vec_mut_get_out_of_bounds_returns_error()
{
    let chain = IoBuffVecMut::<2>::new();
    assert!(matches!(chain.get(0), Err(IoBuffError::IndexOutOfBounds)));
}

#[test]
fn vec_mut_get_mut_out_of_bounds_returns_error()
{
    let mut chain = IoBuffVecMut::<2>::new();
    assert!(matches!(
        chain.get_mut(0),
        Err(IoBuffError::IndexOutOfBounds)
    ));
}

// ============================================================================
// IoBuffVecMut — writable_len
// ============================================================================

#[test]
fn vec_mut_writable_len()
{
    println!("--- writable_len ---");
    let mut chain = IoBuffVecMut::<2>::new();

    let mut buf1 = IoBuffMut::new(0, 100, 0);
    buf1.payload_append(b"partial").unwrap();
    chain.push(buf1).unwrap();

    chain.push(IoBuffMut::new(0, 200, 0)).unwrap();

    let writable = chain.writable_len();
    println!("  writable_len: {} (100-7 + 200 = 293)", writable);
    assert_eq!(writable, (100 - 7) + 200);
}

// ============================================================================
// IoBuffVecMut — distribute_written (kernel readv simulation)
// ============================================================================

#[test]
fn vec_mut_distribute_written_single_segment()
{
    println!("--- distribute_written: single segment ---");
    let mut chain = IoBuffVecMut::<1>::new();
    chain.push(IoBuffMut::new(0, 64, 0)).unwrap();

    unsafe { chain.distribute_written(20) };
    println!(
        "  After 20 bytes: payload_len={}",
        seg!(chain, 0).payload_len()
    );
    assert_eq!(seg!(chain, 0).payload_len(), 20);
    assert_eq!(chain.len(), 20);
}

#[test]
fn vec_mut_distribute_written_across_multiple()
{
    println!("--- distribute_written: across 3 segments ---");
    let mut chain = IoBuffVecMut::<3>::new();
    chain.push(IoBuffMut::new(0, 64, 0)).unwrap();
    chain.push(IoBuffMut::new(0, 4096, 0)).unwrap();
    chain.push(IoBuffMut::new(0, 64, 0)).unwrap();

    // Simulate kernel writing 2000 bytes: fills seg0 (64), seg1 (1936), seg2 (0)
    unsafe { chain.distribute_written(2000) };

    println!("  seg0: payload_len={}", seg!(chain, 0).payload_len());
    println!("  seg1: payload_len={}", seg!(chain, 1).payload_len());
    println!("  seg2: payload_len={}", seg!(chain, 2).payload_len());

    assert_eq!(seg!(chain, 0).payload_len(), 64);
    assert_eq!(seg!(chain, 1).payload_len(), 2000 - 64);
    assert_eq!(seg!(chain, 2).payload_len(), 0);
    assert_eq!(chain.len(), 2000);
}

#[test]
fn vec_mut_distribute_written_fills_all()
{
    println!("--- distribute_written: fills all segments exactly ---");
    let mut chain = IoBuffVecMut::<3>::new();
    chain.push(IoBuffMut::new(0, 10, 0)).unwrap();
    chain.push(IoBuffMut::new(0, 20, 0)).unwrap();
    chain.push(IoBuffMut::new(0, 30, 0)).unwrap();

    unsafe { chain.distribute_written(60) };

    assert_eq!(seg!(chain, 0).payload_len(), 10);
    assert_eq!(seg!(chain, 1).payload_len(), 20);
    assert_eq!(seg!(chain, 2).payload_len(), 30);
    assert_eq!(chain.len(), 60);
    println!("  All segments filled exactly: OK");
}

#[test]
fn vec_mut_distribute_written_zero()
{
    println!("--- distribute_written: zero bytes ---");
    let mut chain = IoBuffVecMut::<2>::new();
    chain.push(IoBuffMut::new(0, 64, 0)).unwrap();
    chain.push(IoBuffMut::new(0, 64, 0)).unwrap();

    unsafe { chain.distribute_written(0) };
    assert_eq!(seg!(chain, 0).payload_len(), 0);
    assert_eq!(seg!(chain, 1).payload_len(), 0);
    assert_eq!(chain.len(), 0);
    println!("  Zero bytes distributed: OK");
}

// ============================================================================
// IoBuffVecMut — freeze → IoBuffVec
// ============================================================================

#[test]
fn vec_mut_freeze()
{
    println!("--- freeze ---");
    let mut chain = IoBuffVecMut::<3>::new();

    let mut h = IoBuffMut::new(0, 16, 0);
    h.payload_append(b"HDR:").unwrap();
    chain.push(h).unwrap();

    let mut p = IoBuffMut::new(0, 64, 0);
    p.payload_append(b"data").unwrap();
    chain.push(p).unwrap();

    let frozen = chain.freeze();
    println!("  segments: {}", frozen.segments());
    println!("  total len: {}", frozen.len());
    println!("  seg0: {:?}", seg!(frozen, 0).bytes());
    println!("  seg1: {:?}", seg!(frozen, 1).bytes());

    assert_eq!(frozen.segments(), 2);
    assert_eq!(frozen.len(), 8);
    assert_eq!(seg!(frozen, 0).bytes(), b"HDR:");
    assert_eq!(seg!(frozen, 1).bytes(), b"data");
}

#[test]
fn vec_mut_freeze_empty()
{
    println!("--- freeze empty chain ---");
    let chain = IoBuffVecMut::<4>::new();
    let frozen = chain.freeze();
    assert_eq!(frozen.segments(), 0);
    assert_eq!(frozen.len(), 0);
    assert!(frozen.is_empty());
    println!("  Empty freeze: OK");
}

#[test]
fn vec_mut_freeze_with_headroom()
{
    println!("--- freeze buffers with headroom ---");
    let mut chain = IoBuffVecMut::<2>::new();

    let mut buf = IoBuffMut::new(8, 64, 0);
    buf.payload_append(b"payload").unwrap();
    buf.headroom_prepend(b"H:").unwrap();
    chain.push(buf).unwrap();

    let frozen = chain.freeze();
    println!("  seg0 bytes: {:?}", seg!(frozen, 0).bytes());
    assert_eq!(seg!(frozen, 0).bytes(), b"H:payload");
    assert_eq!(frozen.len(), 9);
}

// ============================================================================
// IoBuffVec — clone
// ============================================================================

#[test]
fn vec_frozen_clone()
{
    println!("--- IoBuffVec clone ---");
    let mut chain = IoBuffVecMut::<2>::new();
    let mut buf = IoBuffMut::new(0, 32, 0);
    buf.payload_append(b"shared").unwrap();
    chain.push(buf).unwrap();

    let frozen = chain.freeze();
    let cloned = frozen.clone();

    println!(
        "  original seg0: {:?} ptr={:?}",
        seg!(frozen, 0).bytes(),
        seg!(frozen, 0).bytes().as_ptr()
    );
    println!(
        "  cloned   seg0: {:?} ptr={:?}",
        seg!(cloned, 0).bytes(),
        seg!(cloned, 0).bytes().as_ptr()
    );

    assert_eq!(seg!(frozen, 0).bytes(), seg!(cloned, 0).bytes());
    // Same backing storage (refcount bump, not deep copy)
    assert_eq!(
        seg!(frozen, 0).bytes().as_ptr(),
        seg!(cloned, 0).bytes().as_ptr(),
        "clone must share backing storage"
    );
}

#[test]
fn vec_frozen_clone_independent_drop()
{
    println!("--- IoBuffVec clone + independent drop ---");
    let mut chain = IoBuffVecMut::<2>::new();
    let mut b1 = IoBuffMut::new(0, 32, 0);
    b1.payload_append(b"seg1").unwrap();
    let mut b2 = IoBuffMut::new(0, 32, 0);
    b2.payload_append(b"seg2").unwrap();
    chain.push(b1).unwrap();
    chain.push(b2).unwrap();

    let frozen = chain.freeze();
    let c1 = frozen.clone();
    let c2 = frozen.clone();

    drop(c1);
    assert_eq!(seg!(frozen, 0).bytes(), b"seg1");
    assert_eq!(seg!(frozen, 1).bytes(), b"seg2");

    drop(frozen);
    assert_eq!(seg!(c2, 0).bytes(), b"seg1");
    assert_eq!(seg!(c2, 1).bytes(), b"seg2");

    drop(c2);
    println!("  All dropped independently: OK");
}

// ============================================================================
// IoBuffVec — iter
// ============================================================================

#[test]
fn vec_frozen_iter()
{
    println!("--- IoBuffVec iter ---");
    let mut chain = IoBuffVecMut::<3>::new();
    for i in 0..3
    {
        let mut buf = IoBuffMut::new(0, 16, 0);
        buf.payload_append(format!("seg{i}").as_bytes()).unwrap();
        chain.push(buf).unwrap();
    }

    let frozen = chain.freeze();
    let collected: Vec<&[u8]> = frozen.iter().map(|b| b.bytes()).collect();
    println!("  iter: {:?}", collected);
    assert_eq!(collected, vec![b"seg0".as_slice(), b"seg1", b"seg2"]);
}

// ============================================================================
// Chain semantics
// ============================================================================

#[test]
fn vec_frozen_chain_reports_segments_and_len()
{
    println!("--- Frozen chain reports segments and len ---");
    let mut chain = IoBuffVecMut::<2>::new();
    let mut buf = IoBuffMut::new(0, 32, 0);
    buf.payload_append(b"test data").unwrap();
    chain.push(buf).unwrap();

    let frozen = chain.freeze();
    println!(
        "  len={}, segments={}, seg0={:?}",
        frozen.len(),
        frozen.segments(),
        seg!(frozen, 0).bytes()
    );
    assert_eq!(frozen.len(), 9);
    assert_eq!(frozen.segments(), 1);
    assert_eq!(seg!(frozen, 0).bytes(), b"test data");
}

#[test]
fn vec_mut_writable_len_and_distribute_written()
{
    println!("--- writable_len + distribute_written ---");
    let mut chain = IoBuffVecMut::<2>::new();
    chain.push(IoBuffMut::new(0, 64, 0)).unwrap();
    chain.push(IoBuffMut::new(0, 128, 0)).unwrap();

    let writable = chain.writable_len();
    let segs = chain.segments();
    println!("  writable_len={}, segments={}", writable, segs);
    assert_eq!(writable, 192);
    assert_eq!(segs, 2);

    // Simulate kernel readv
    unsafe { chain.distribute_written(100) };
    println!("  After set_written_len(100):");
    println!("    seg0 payload_len={}", seg!(chain, 0).payload_len());
    println!("    seg1 payload_len={}", seg!(chain, 1).payload_len());
    assert_eq!(seg!(chain, 0).payload_len(), 64);
    assert_eq!(seg!(chain, 1).payload_len(), 36);
}

// ============================================================================
// Edge cases
// ============================================================================

#[test]
fn vec_mut_single_capacity()
{
    println!("--- Single capacity chain ---");
    let mut chain = IoBuffVecMut::<1>::new();
    let mut buf = IoBuffMut::new(0, 64, 0);
    buf.payload_append(b"only one").unwrap();
    chain.push(buf).unwrap();

    let result = chain.push(IoBuffMut::new(0, 8, 0));
    assert_eq!(result, Err(IoBuffError::ChainFull));

    assert_eq!(chain.len(), 8);
    assert_eq!(seg!(chain, 0).payload_bytes(), b"only one");
    println!("  Single segment chain: OK");
}

#[test]
fn vec_mut_large_segment_count()
{
    println!("--- Large segment count (16) ---");
    let mut chain = IoBuffVecMut::<16>::new();
    for i in 0..16
    {
        let mut buf = IoBuffMut::new(0, 32, 0);
        buf.payload_append(&[i as u8; 4]).unwrap();
        chain.push(buf).unwrap();
    }

    assert_eq!(chain.segments(), 16);
    assert_eq!(chain.len(), 64);
    for i in 0..16
    {
        assert_eq!(seg!(chain, i).payload_bytes(), &[i as u8; 4]);
    }
    println!("  16 segments: OK");
}

#[test]
fn vec_mut_freeze_reflects_modified_buffers_without_rebuild()
{
    println!("--- freeze reflects modified buffers without rebuild ---");
    let mut chain = IoBuffVecMut::<2>::new();
    chain.push(IoBuffMut::new(0, 64, 0)).unwrap();
    chain.push(IoBuffMut::new(0, 64, 0)).unwrap();

    // Modify buffers after push
    seg_mut!(chain, 0).payload_append(b"first").unwrap();
    seg_mut!(chain, 1).payload_append(b"second").unwrap();

    // Freeze and verify the segments reflect the modified state directly.
    let frozen = chain.freeze();
    println!("  seg0: {:?}", seg!(frozen, 0).bytes());
    println!("  seg1: {:?}", seg!(frozen, 1).bytes());
    assert_eq!(frozen.segments(), 2);
    assert_eq!(seg!(frozen, 0).bytes(), b"first");
    assert_eq!(seg!(frozen, 1).bytes(), b"second");
}

#[test]
fn vec_mut_drop_cleans_up()
{
    println!("--- Drop cleans up all segments ---");
    let mut chain = IoBuffVecMut::<4>::new();
    for _ in 0..4
    {
        let mut buf = IoBuffMut::new(0, 1024, 0);
        buf.payload_append(&[0xAA; 512]).unwrap();
        chain.push(buf).unwrap();
    }
    drop(chain);
    println!("  Dropped 4-segment chain: no leak, no double-free");
}

#[test]
fn vec_frozen_drop_cleans_up()
{
    println!("--- Frozen drop cleans up all segments ---");
    let mut chain = IoBuffVecMut::<3>::new();
    for _ in 0..3
    {
        let mut buf = IoBuffMut::new(0, 512, 0);
        buf.payload_append(&[0xBB; 256]).unwrap();
        chain.push(buf).unwrap();
    }
    let frozen = chain.freeze();
    let c1 = frozen.clone();
    let c2 = frozen.clone();
    drop(c1);
    drop(frozen);
    drop(c2);
    println!("  Dropped frozen chain + 2 clones: no leak, no double-free");
}

#[test]
fn vec_mut_mixed_buffer_sizes()
{
    println!("--- Mixed buffer sizes ---");
    let mut chain = IoBuffVecMut::<4>::new();

    let sizes = [8, 4096, 64, 65536];
    for &sz in &sizes
    {
        let mut buf = IoBuffMut::new(0, sz, 0);
        buf.payload_append(&vec![0xCC; sz / 2]).unwrap();
        chain.push(buf).unwrap();
    }

    let total: usize = sizes.iter().map(|s| s / 2).sum();
    println!("  Total len: {} (expected {})", chain.len(), total);
    assert_eq!(chain.len(), total);

    for (i, &sz) in sizes.iter().enumerate()
    {
        assert_eq!(seg!(chain, i).payload_len(), sz / 2);
    }
    println!("  Mixed sizes: OK");
}

#[test]
fn vec_distribute_written_partial_first_segment()
{
    println!("--- distribute_written: partial first segment only ---");
    let mut chain = IoBuffVecMut::<3>::new();
    chain.push(IoBuffMut::new(0, 100, 0)).unwrap();
    chain.push(IoBuffMut::new(0, 200, 0)).unwrap();
    chain.push(IoBuffMut::new(0, 300, 0)).unwrap();

    unsafe { chain.distribute_written(50) };

    assert_eq!(seg!(chain, 0).payload_len(), 50);
    assert_eq!(seg!(chain, 1).payload_len(), 0);
    assert_eq!(seg!(chain, 2).payload_len(), 0);
    println!("  Partial first segment: OK");
}

// ============================================================================
// try_mut_all tests
// ============================================================================

/// try_mut_all succeeds when all segments are sole-owned.
#[test]
fn vec_try_mut_all_sole_owner()
{
    let mut chain = IoBuffVecMut::<3>::new();
    let mut b1 = IoBuffMut::new(0, 64, 0);
    b1.payload_append(b"aaa").unwrap();
    let mut b2 = IoBuffMut::new(0, 64, 0);
    b2.payload_append(b"bbb").unwrap();
    chain.push(b1).unwrap();
    chain.push(b2).unwrap();

    let frozen = chain.freeze();
    assert_eq!(frozen.segments(), 2);
    assert_eq!(frozen.len(), 6);

    let mut mutable = match frozen.try_mut_all()
    {
        Ok(m) => m,
        Err((e, _)) => panic!("try_mut_all failed: {:?}", e),
    };
    assert_eq!(mutable.segments(), 2);

    // Segments should have payload from the frozen view.
    assert_eq!(seg!(mutable, 0).payload_bytes(), b"aaa");
    assert_eq!(seg!(mutable, 1).payload_bytes(), b"bbb");

    // Can reset and refill.
    seg_mut!(mutable, 0).reset();
    seg_mut!(mutable, 0).payload_append(b"xxx").unwrap();
    seg_mut!(mutable, 1).reset();
    seg_mut!(mutable, 1).payload_append(b"yyy").unwrap();

    assert_eq!(seg!(mutable, 0).payload_bytes(), b"xxx");
    assert_eq!(seg!(mutable, 1).payload_bytes(), b"yyy");
}

/// try_mut_all fails when a segment is shared (cloned frozen chain).
#[test]
fn vec_try_mut_all_shared_fails()
{
    let mut chain = IoBuffVecMut::<2>::new();
    let mut b1 = IoBuffMut::new(0, 64, 0);
    b1.payload_append(b"data").unwrap();
    chain.push(b1).unwrap();

    let frozen = chain.freeze();
    let _clone = frozen.clone(); // refcount > 1

    let result = frozen.try_mut_all();
    let (err, recovered) = match result
    {
        Ok(_) => panic!("try_mut_all should have failed"),
        Err(pair) => pair,
    };
    assert!(matches!(err, IoBuffError::SharedBuffer));
    // Original chain returned intact.
    assert_eq!(recovered.segments(), 1);
    assert_eq!(seg!(recovered, 0).bytes(), b"data");
}

/// Round-trip: freeze → try_mut_all → reset → refill → freeze.
#[test]
fn vec_try_mut_all_round_trip()
{
    let fill_a = b"hello";
    let fill_b = b"world";

    let mut chain = IoBuffVecMut::<2>::new();
    let mut s1 = IoBuffMut::new(0, 64, 0);
    s1.payload_append(fill_a).unwrap();
    let mut s2 = IoBuffMut::new(0, 64, 0);
    s2.payload_append(fill_b).unwrap();
    chain.push(s1).unwrap();
    chain.push(s2).unwrap();

    // First freeze.
    let frozen1 = chain.freeze();
    assert_eq!(frozen1.len(), 10);

    // Unfreeze.
    let mut mutable = match frozen1.try_mut_all()
    {
        Ok(m) => m,
        Err((e, _)) => panic!("try_mut_all failed: {:?}", e),
    };

    // Reset and refill with different data.
    for i in 0..mutable.segments()
    {
        seg_mut!(mutable, i).reset();
    }
    seg_mut!(mutable, 0).payload_append(b"AAAAA").unwrap();
    seg_mut!(mutable, 1).payload_append(b"BBBBB").unwrap();
    // Second freeze.
    let frozen2 = mutable.freeze();
    assert_eq!(frozen2.len(), 10);
    assert_eq!(seg!(frozen2, 0).bytes(), b"AAAAA");
    assert_eq!(seg!(frozen2, 1).bytes(), b"BBBBB");
}

#[test]
fn vec_try_mut_all_with_structured_segments()
{
    let mut chain = IoBuffVecMut::<2>::new();

    let mut s1 = IoBuffMut::new(4, 64, 4);
    s1.payload_append(b"payload").unwrap();
    s1.headroom_prepend(b"H:").unwrap();
    s1.tailroom_append(b":T").unwrap();

    let mut s2 = IoBuffMut::new(2, 32, 2);
    s2.payload_append(b"data").unwrap();
    s2.headroom_prepend(b"[").unwrap();
    s2.tailroom_append(b"]").unwrap();

    chain.push(s1).unwrap();
    chain.push(s2).unwrap();

    let frozen = chain.freeze();
    let mutable = match frozen.try_mut_all()
    {
        Ok(m) => m,
        Err((e, _)) => panic!("try_mut_all failed: {:?}", e),
    };

    assert_eq!(seg!(mutable, 0).bytes(), b"H:payload:T");
    assert_eq!(seg!(mutable, 0).payload_bytes(), b"payload");
    assert_eq!(seg!(mutable, 1).bytes(), b"[data]");
    assert_eq!(seg!(mutable, 1).payload_bytes(), b"data");
}
