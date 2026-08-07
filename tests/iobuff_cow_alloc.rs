#[path = "common/counting_allocator.rs"]
mod counting_allocator;

use counting_allocator::{
    CountingAllocator, ThreadLocalAllocationSnapshot, assert_allocation_failure_consumed,
    fail_next_allocation,
};
use flowio::runtime::buffer::pool::{IoBuffPool, IoBuffPoolConfig};
use flowio::runtime::buffer::{IoBuff, IoBuffError, IoBuffMut};

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

const HEADROOM: usize = 4;
const PAYLOAD: usize = 8;
const TAILROOM: usize = 4;
const ACTIVE: &[u8] = b"cdefghTAIL";
const ACTIVE_PAYLOAD: &[u8] = b"cdefgh";

fn advanced_frozen(mut buffer: IoBuffMut) -> IoBuff {
    buffer
        .payload_append(b"abcdefgh")
        .expect("payload should fill its region");
    buffer
        .headroom_prepend(b"HEAD")
        .expect("header should fill its region");
    buffer
        .tailroom_append(b"TAIL")
        .expect("trailer should fill its region");
    buffer
        .advance(HEADROOM + 2)
        .expect("advance should consume the header and two payload bytes");

    let frozen = buffer.freeze();
    assert_eq!(frozen.bytes(), ACTIVE);
    assert_eq!(frozen.headroom_len(), 0);
    assert_eq!(frozen.payload_bytes(), ACTIVE_PAYLOAD);
    assert_eq!(frozen.tailroom_len(), TAILROOM);
    frozen
}

fn assert_advanced_shape(buffer: &mut IoBuffMut) {
    assert_eq!(buffer.bytes(), ACTIVE);
    assert_eq!(buffer.headroom_capacity(), HEADROOM);
    assert_eq!(buffer.headroom_remaining(), 0);
    assert_eq!(buffer.payload_capacity(), PAYLOAD);
    assert_eq!(buffer.payload_bytes(), ACTIVE_PAYLOAD);
    assert_eq!(buffer.payload_remaining(), 0);
    assert_eq!(buffer.tailroom_capacity(), TAILROOM);
    assert_eq!(buffer.tailroom_remaining(), 0);
    assert_eq!(
        buffer.headroom_prepend(b"!"),
        Err(IoBuffError::HeadroomFull)
    );
    assert_eq!(buffer.payload_append(b"!"), Err(IoBuffError::PayloadSealed));
    assert_eq!(buffer.tailroom_append(b"!"), Err(IoBuffError::TailroomFull));
}

fn assert_heap_shared_cow_success() {
    let frozen = advanced_frozen(
        IoBuffMut::new(HEADROOM, PAYLOAD, TAILROOM).expect("heap source allocation should succeed"),
    );
    let survivor = frozen.clone();
    let original_ptr = survivor.bytes().as_ptr();

    let before = ThreadLocalAllocationSnapshot::current();
    let mut copied = frozen
        .make_mut()
        .expect("shared heap source should copy successfully");
    ThreadLocalAllocationSnapshot::current().assert_delta_since(before, 1, 0);
    assert_advanced_shape(&mut copied);
    assert_ne!(copied.bytes().as_ptr(), original_ptr);

    copied
        .advance(1)
        .expect("copied buffer should remain mutable");
    assert_eq!(copied.bytes(), &ACTIVE[1..]);
    assert_eq!(survivor.bytes(), ACTIVE);
    drop(copied);
    ThreadLocalAllocationSnapshot::current().assert_delta_since(before, 1, 1);

    let recovered = survivor
        .try_mut()
        .expect("consumed COW source should leave one original reference");
    assert_eq!(recovered.bytes().as_ptr(), original_ptr);
    let before_original_drop = ThreadLocalAllocationSnapshot::current();
    drop(recovered);
    ThreadLocalAllocationSnapshot::current().assert_delta_since(before_original_drop, 0, 1);
}

fn assert_heap_shared_cow_failure() {
    let frozen = advanced_frozen(
        IoBuffMut::new(HEADROOM, PAYLOAD, TAILROOM)
            .expect("heap failure source allocation should succeed"),
    );
    let survivor = frozen.clone();
    let original_ptr = survivor.bytes().as_ptr();

    let before = ThreadLocalAllocationSnapshot::current();
    fail_next_allocation();
    let err = match frozen.make_mut() {
        Ok(_) => panic!("forced shared COW allocation unexpectedly succeeded"),
        Err(err) => err,
    };
    assert_allocation_failure_consumed();
    assert_eq!(err, IoBuffError::AllocFailed);
    ThreadLocalAllocationSnapshot::current().assert_unchanged_since(before);
    assert_eq!(survivor.bytes(), ACTIVE);

    let recovered = survivor
        .try_mut()
        .expect("failed COW should leave one valid original reference");
    assert_eq!(recovered.bytes().as_ptr(), original_ptr);
    drop(recovered);
    ThreadLocalAllocationSnapshot::current().assert_delta_since(before, 0, 1);
}

fn assert_pool_shared_cow_success_and_failure() {
    let mut pool = IoBuffPool::new(IoBuffPoolConfig {
        headroom: HEADROOM,
        payload: PAYLOAD,
        tailroom: TAILROOM,
        objs_per_slab: 1,
    })
    .expect("pool configuration should be valid");
    pool.init();

    let frozen = advanced_frozen(pool.alloc().expect("pool source allocation should succeed"));
    let survivor = frozen.clone();
    assert_eq!(pool.live_slots_for_test(), 1);

    let before = ThreadLocalAllocationSnapshot::current();
    let mut copied = frozen
        .make_mut()
        .expect("shared pool source should copy successfully");
    ThreadLocalAllocationSnapshot::current().assert_delta_since(before, 1, 0);
    assert_advanced_shape(&mut copied);
    copied
        .advance(1)
        .expect("copied buffer should remain mutable");
    assert_eq!(survivor.bytes(), ACTIVE);
    assert_eq!(pool.live_slots_for_test(), 1);
    drop(copied);
    ThreadLocalAllocationSnapshot::current().assert_delta_since(before, 1, 1);

    drop(
        survivor
            .try_mut()
            .expect("successful COW should leave one pool reference"),
    );
    assert_eq!(pool.live_slots_for_test(), 0);

    let frozen = advanced_frozen(pool.alloc().expect("pool slot should be reusable"));
    let survivor = frozen.clone();
    assert_eq!(pool.live_slots_for_test(), 1);

    let before = ThreadLocalAllocationSnapshot::current();
    fail_next_allocation();
    let err = match frozen.make_mut() {
        Ok(_) => panic!("forced pool-backed COW allocation unexpectedly succeeded"),
        Err(err) => err,
    };
    assert_allocation_failure_consumed();
    assert_eq!(err, IoBuffError::AllocFailed);
    ThreadLocalAllocationSnapshot::current().assert_unchanged_since(before);
    assert_eq!(survivor.bytes(), ACTIVE);
    assert_eq!(pool.live_slots_for_test(), 1);

    drop(
        survivor
            .try_mut()
            .expect("failed COW should leave one valid pool reference"),
    );
    assert_eq!(pool.live_slots_for_test(), 0);
    ThreadLocalAllocationSnapshot::current().assert_unchanged_since(before);

    let reuse_before = ThreadLocalAllocationSnapshot::current();
    let reused = pool
        .alloc()
        .expect("failed COW should return the original slot for reuse");
    ThreadLocalAllocationSnapshot::current().assert_unchanged_since(reuse_before);
    assert_eq!(pool.live_slots_for_test(), 1);
    drop(reused);
    assert_eq!(pool.live_slots_for_test(), 0);
    ThreadLocalAllocationSnapshot::current().assert_unchanged_since(reuse_before);
}

#[test]
fn shared_iobuff_make_mut_has_exact_allocation_and_failure_ownership() {
    assert_heap_shared_cow_success();
    assert_heap_shared_cow_failure();
    assert_pool_shared_cow_success_and_failure();
}
