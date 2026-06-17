mod utils {
    pub use flowio::utils::*;
}

#[allow(dead_code)]
// RetainedPayloadPool is crate-internal; include the module source directly
// so this white-box test can exercise its pool and fallback behavior.
#[path = "../src/runtime/retained.rs"]
mod retained;

use retained::RetainedPayloadPool;
use std::cell::Cell;
use std::rc::Rc;

/// Payload fixture that counts its own drops.
///
/// Retained-payload tests use this to distinguish take(), which moves a value
/// out without dropping it, from drop_and_free(), which drops in place.
struct DropTracked {
    /// Inner value used to prove take() returns the original payload.
    value: usize,
    /// Shared drop counter bumped by Drop.
    drops: Rc<Cell<usize>>,
}

impl DropTracked {
    fn new(value: usize, drops: &Rc<Cell<usize>>) -> Self {
        Self {
            value,
            drops: Rc::clone(drops),
        }
    }
}

impl Drop for DropTracked {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

#[repr(align(128))]
// Alignment exceeds the retained pool's 64-byte max class alignment and
// forces the heap-fallback path.
struct OverAligned([u8; 64]);

#[test]
fn retained_payload_pool_allocates_from_size_class() {
    let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
    let payload = pool.alloc([7u8; 64]);
    assert_eq!(unsafe { payload.as_ref() }[0], 7);
    unsafe { payload.drop_and_free(&mut pool) };

    let stats = pool.stats();
    println!("size-class alloc stats: {stats:?}");
    assert_eq!(stats.pooled_allocs, 1);
    assert_eq!(stats.slab_allocs, 1);
    assert_eq!(stats.pooled_frees, 1);
    assert_eq!(stats.heap_fallbacks, 0);
}

#[test]
fn retained_payload_pool_reuses_returned_block() {
    let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
    let first = pool.alloc([1u8; 128]);
    let first_ptr = first.as_ptr();
    unsafe { first.drop_and_free(&mut pool) };

    let second = pool.alloc([2u8; 128]);
    assert_eq!(second.as_ptr(), first_ptr);
    unsafe { second.drop_and_free(&mut pool) };

    let stats = pool.stats();
    println!("reuse stats: {stats:?}");
    assert_eq!(stats.pooled_allocs, 2);
    assert_eq!(stats.pooled_reuses, 1);
    assert_eq!(stats.slab_allocs, 1);
    assert_eq!(stats.pooled_frees, 2);
}

/// take() moves the payload out of pooled storage without dropping it; the
/// block is still returned to the pool.
#[test]
fn retained_payload_take_moves_value_without_dropping_it() {
    let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
    let drops = Rc::new(Cell::new(0));
    let payload = pool.alloc(DropTracked::new(42, &drops));

    let value = unsafe { payload.take(&mut pool) };
    assert_eq!(value.value, 42);
    assert_eq!(drops.get(), 0);

    drop(value);
    assert_eq!(drops.get(), 1);

    let stats = pool.stats();
    println!("take pooled stats: {stats:?}");
    assert_eq!(stats.pooled_frees, 1);
}

/// drop_and_free() runs the payload's Drop exactly once and returns the block
/// to the pool, unlike take(), which moves the value out.
#[test]
fn retained_payload_drop_and_free_drops_value_once() {
    let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
    let drops = Rc::new(Cell::new(0));
    let payload = pool.alloc(DropTracked::new(7, &drops));

    unsafe { payload.drop_and_free(&mut pool) };

    let stats = pool.stats();
    println!("drop pooled stats: {stats:?}");
    assert_eq!(drops.get(), 1);
    assert_eq!(stats.pooled_frees, 1);
}

#[test]
fn retained_payload_pool_allocates_larger_payload_classes_on_demand() {
    let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
    let payload = pool.alloc([9u8; 8192]);
    assert_eq!(unsafe { payload.as_ref() }[0], 9);
    unsafe { payload.drop_and_free(&mut pool) };

    let stats = pool.stats();
    println!("large pooled payload stats: {stats:?}");
    assert_eq!(stats.pooled_allocs, 1);
    assert_eq!(stats.slab_allocs, 1);
    assert_eq!(stats.pooled_frees, 1);
    assert_eq!(stats.heap_fallbacks, 0);
}

#[test]
fn retained_payload_pool_uses_heap_for_oversized_payloads() {
    let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
    // One byte over the largest size class (65536) must use heap fallback.
    let payload = pool.alloc([9u8; 65537]);
    assert_eq!(unsafe { payload.as_ref() }[0], 9);
    unsafe { payload.drop_and_free(&mut pool) };

    let stats = pool.stats();
    println!("oversized heap fallback stats: {stats:?}");
    assert_eq!(stats.pooled_allocs, 0);
    assert_eq!(stats.heap_fallbacks, 1);
    assert_eq!(stats.heap_frees, 1);
}

#[test]
fn retained_payload_take_frees_heap_storage_for_oversized_payloads() {
    let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
    let payload = pool.alloc([3u8; 65537]);
    let value = unsafe { payload.take(&mut pool) };

    assert_eq!(value[0], 3);

    let stats = pool.stats();
    println!("large take heap fallback stats: {stats:?}");
    assert_eq!(stats.heap_fallbacks, 1);
    assert_eq!(stats.heap_frees, 1);
}

#[test]
fn retained_payload_pool_uses_heap_for_overaligned_payloads() {
    let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
    let payload = pool.alloc(OverAligned([5u8; 64]));
    assert_eq!(unsafe { payload.as_ref() }.0[0], 5);
    unsafe { payload.drop_and_free(&mut pool) };

    let stats = pool.stats();
    println!("overaligned heap fallback stats: {stats:?}");
    assert_eq!(stats.pooled_allocs, 0);
    assert_eq!(stats.heap_fallbacks, 1);
}

#[test]
fn retained_payload_pool_requests_new_slab_after_class_exhaustion() {
    let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
    // One more than a full 64KiB slab of 4096-byte blocks forces a second slab.
    let count = 64 * 1024 / 4096 + 1;
    let mut payloads = Vec::with_capacity(count);

    for _ in 0..count {
        payloads.push(pool.alloc([1u8; 4096]));
    }

    assert_eq!(pool.stats().slab_allocs, 2);

    for payload in payloads {
        unsafe { payload.drop_and_free(&mut pool) };
    }

    let stats = pool.stats();
    println!("class exhaustion stats: {stats:?}");
    assert_eq!(stats.pooled_frees, count);
}

#[test]
fn retained_iovec_scratch_uses_inline_for_small_counts() {
    let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
    // Counts up to the inline threshold (16) use embedded scratch, not a pool.
    let mut scratch = pool
        .alloc_iovec_scratch(16)
        .expect("inline scratch allocation failed");
    assert_eq!(scratch.as_uninit_slice().len(), 16);
    scratch.as_uninit_slice_mut()[0].write(libc::iovec {
        iov_base: std::ptr::null_mut(),
        iov_len: 1,
    });
    drop(scratch);

    let stats = pool.stats();
    println!("inline iovec scratch stats: {stats:?}");
    assert_eq!(stats.writev_scratch_inline_allocs, 1);
    assert_eq!(stats.writev_scratch_pooled_allocs, 0);
    assert_eq!(stats.writev_scratch_pooled_frees, 0);
}

#[test]
fn retained_iovec_scratch_uses_pool_for_512_iovecs() {
    let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
    {
        let scratch = pool
            .alloc_iovec_scratch(512)
            .expect("512 iovec scratch allocation failed");
        assert_eq!(scratch.as_uninit_slice().len(), 512);
    }

    let stats = pool.stats();
    println!("512 iovec scratch stats: {stats:?}");
    assert_eq!(stats.writev_scratch_inline_allocs, 0);
    assert_eq!(stats.writev_scratch_pooled_allocs, 1);
    assert_eq!(stats.writev_scratch_slab_allocs, 1);
    assert_eq!(stats.writev_scratch_pooled_frees, 1);
    assert_eq!(stats.heap_fallbacks, 0);
}

#[test]
fn retained_iovec_scratch_reuses_returned_sidecar_block() {
    let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
    let first = pool
        .alloc_iovec_scratch(512)
        .expect("first scratch allocation failed");
    let first_ptr = first.as_uninit_slice().as_ptr();
    drop(first);

    let second = pool
        .alloc_iovec_scratch(512)
        .expect("second scratch allocation failed");
    assert_eq!(second.as_uninit_slice().as_ptr(), first_ptr);
    drop(second);

    let stats = pool.stats();
    println!("512 iovec scratch reuse stats: {stats:?}");
    assert_eq!(stats.writev_scratch_pooled_allocs, 2);
    assert_eq!(stats.writev_scratch_pooled_reuses, 1);
    assert_eq!(stats.writev_scratch_slab_allocs, 1);
    assert_eq!(stats.writev_scratch_pooled_frees, 2);
}

#[test]
fn retained_iovec_scratch_rejects_oversized_count() {
    let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
    // 1025 exceeds the retained iovec cap (1024) and is rejected up front.
    let err = match pool.alloc_iovec_scratch(1025) {
        Ok(_) => panic!("oversized scratch request should fail"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

    let stats = pool.stats();
    println!("oversized iovec scratch stats: {stats:?}");
    assert_eq!(stats.writev_scratch_oversize_rejections, 1);
    assert_eq!(stats.writev_scratch_pooled_allocs, 0);
}
