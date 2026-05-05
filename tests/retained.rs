mod utils {
    pub use flowio::utils::*;
}

#[allow(dead_code)]
#[path = "../src/runtime/retained.rs"]
mod retained;

use retained::RetainedPayloadPool;
use std::cell::Cell;
use std::rc::Rc;

struct DropTracked {
    value: usize,
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
fn retained_payload_pool_uses_heap_for_large_payloads() {
    let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
    let payload = pool.alloc([9u8; 4097]);
    assert_eq!(unsafe { payload.as_ref() }[0], 9);
    unsafe { payload.drop_and_free(&mut pool) };

    let stats = pool.stats();
    println!("large heap fallback stats: {stats:?}");
    assert_eq!(stats.pooled_allocs, 0);
    assert_eq!(stats.heap_fallbacks, 1);
    assert_eq!(stats.heap_frees, 1);
}

#[test]
fn retained_payload_take_frees_heap_storage_for_large_payloads() {
    let mut pool = RetainedPayloadPool::new().expect("retained pool init failed");
    let payload = pool.alloc([3u8; 4097]);
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
