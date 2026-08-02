mod common;

use common::TestIoBuffMut as IoBuffMut;
use flowio::runtime::buffer::iobuffvec::{IoBuffVec, IoBuffVecMut};
use flowio::runtime::buffer::{
    IoBuff, IoBuffError, IoBuffMut as RealIoBuffMut, IoBuffOwnedView, IoBuffReadOnly,
    IoBuffReadWrite, IoBuffView,
};
use static_assertions::assert_not_impl_any;
use std::mem::MaybeUninit;

fn expect_view(view: Result<IoBuffView, IoBuffError>) -> IoBuffView {
    view.expect("valid IoBuff slice in test")
}

fn expect_owned_view(view: Result<IoBuffOwnedView, (IoBuff, IoBuffError)>) -> IoBuffOwnedView {
    view.expect("valid IoBuff owned view in test")
}

fn assert_active_window_within_allocation(buf: &RealIoBuffMut, base: *const u8, total: usize) {
    let active = IoBuffReadOnly::as_ptr(buf);
    let offset = unsafe { active.offset_from(base) };
    assert!(offset >= 0, "active window moved before allocation base");
    let end = offset as usize + buf.len();
    assert!(
        end <= total,
        "active window end {end} exceeds allocation capacity {total}"
    );
    let _ = buf.bytes();
}

fn initialize_spare_prefix(spare: &mut [MaybeUninit<u8>], bytes: &[u8]) {
    assert!(
        bytes.len() <= spare.len(),
        "test payload exceeds spare capacity"
    );
    for (slot, byte) in spare.iter_mut().zip(bytes) {
        slot.write(*byte);
    }
}

#[cfg(not(debug_assertions))]
struct DefaultInitializedWritable {
    storage: Box<[u8]>,
}

#[cfg(not(debug_assertions))]
impl DefaultInitializedWritable {
    const WRITABLE: usize = 4;
    const OVERSIZED: usize = 8;
    const SENTINEL: u8 = 0xA5;

    fn new() -> Self {
        let backing_len = if cfg!(miri) {
            Self::WRITABLE
        } else {
            Self::OVERSIZED
        };
        Self {
            storage: vec![Self::SENTINEL; backing_len].into_boxed_slice(),
        }
    }
}

// SAFETY: the fixed boxed allocation is pointer-stable and contains the
// complete advertised writable window. It models a downstream implementation
// that inherits the trait's default userspace initializer.
#[cfg(not(debug_assertions))]
unsafe impl IoBuffReadWrite for DefaultInitializedWritable {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.storage.as_mut_ptr()
    }

    fn writable_len(&self) -> usize {
        Self::WRITABLE
    }

    unsafe fn set_written_len(&mut self, _len: usize) {
        // This fixed-size test buffer has no separate logical length.
    }
}

#[cfg(not(debug_assertions))]
#[derive(Default)]
struct NullEmptyInitializedWritable {
    pointer_calls: usize,
}

// SAFETY: this buffer exposes no writable bytes, so the trait contract permits
// its pointer to be null. Its zero-length window is stable.
#[cfg(not(debug_assertions))]
unsafe impl IoBuffReadWrite for NullEmptyInitializedWritable {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.pointer_calls += 1;
        std::ptr::null_mut()
    }

    fn writable_len(&self) -> usize {
        0
    }

    unsafe fn set_written_len(&mut self, _len: usize) {
        // This empty test buffer has no logical length.
    }
}

// ============================================================================
// IoBuffMut — basic construction and payload operations
// ============================================================================

#[test]
fn buffer_mut_new_flat() {
    println!("--- Creating flat buffer (0 headroom, 64 payload, 0 tailroom) ---");
    let buf = IoBuffMut::new(0, 64, 0);
    println!("  headroom_capacity: {}", buf.headroom_capacity());
    println!("  payload_capacity:  {}", buf.payload_capacity());
    println!("  tailroom_capacity: {}", buf.tailroom_capacity());
    println!("  payload_len:       {}", buf.payload_len());
    println!("  len (active):      {}", buf.len());

    assert_eq!(buf.headroom_capacity(), 0);
    assert_eq!(buf.payload_capacity(), 64);
    assert_eq!(buf.tailroom_capacity(), 0);
    assert_eq!(buf.payload_len(), 0);
    assert_eq!(buf.payload_remaining(), 64);
    assert!(buf.payload_is_empty());
    assert!(buf.is_empty());
    assert_eq!(buf.len(), 0);
    assert_eq!(buf.headroom_remaining(), 0);
}

#[test]
fn buffer_mut_new_with_all_regions() {
    println!("--- Creating buffer (32 headroom, 4096 payload, 64 tailroom) ---");
    let buf = IoBuffMut::new(32, 4096, 64);
    println!("  headroom_capacity:  {}", buf.headroom_capacity());
    println!("  headroom_remaining: {}", buf.headroom_remaining());
    println!("  payload_capacity:   {}", buf.payload_capacity());
    println!("  payload_remaining:  {}", buf.payload_remaining());
    println!("  tailroom_capacity:  {}", buf.tailroom_capacity());
    println!("  tailroom_remaining: {}", buf.tailroom_remaining());

    assert_eq!(buf.headroom_capacity(), 32);
    assert_eq!(buf.headroom_remaining(), 32);
    assert_eq!(buf.payload_capacity(), 4096);
    assert_eq!(buf.payload_remaining(), 4096);
    assert_eq!(buf.tailroom_capacity(), 64);
    assert_eq!(buf.tailroom_remaining(), 64);
    assert_eq!(buf.len(), 0);
}

#[test]
fn buffer_mut_new_layout_overflow_returns_error() {
    let result = RealIoBuffMut::new(usize::MAX, 1, 0);
    assert!(matches!(result, Err(IoBuffError::LayoutOverflow)));
}

#[test]
fn buffer_mut_payload_append() {
    println!("--- Payload append ---");
    let mut buf = IoBuffMut::new(0, 64, 0);

    buf.payload_append(b"hello").unwrap();
    println!(
        "  After append 'hello': payload_len={}, payload={:?}",
        buf.payload_len(),
        buf.payload_bytes()
    );

    assert_eq!(buf.payload_len(), 5);
    assert_eq!(buf.payload_bytes(), b"hello");
    assert_eq!(buf.payload_remaining(), 59);
    assert_eq!(buf.len(), 5);
    assert_eq!(buf.bytes(), b"hello");

    buf.payload_append(b" world").unwrap();
    println!(
        "  After append ' world': payload_len={}, payload={:?}",
        buf.payload_len(),
        buf.payload_bytes()
    );

    assert_eq!(buf.payload_len(), 11);
    assert_eq!(buf.payload_bytes(), b"hello world");
    assert_eq!(buf.bytes(), b"hello world");
}

#[test]
fn buffer_mut_payload_append_overflow_returns_error() {
    println!("--- Payload append overflow ---");
    let mut buf = IoBuffMut::new(0, 4, 0);

    buf.payload_append(b"abcd").unwrap();
    println!(
        "  After filling 4/4 bytes: payload_len={}",
        buf.payload_len()
    );

    let result = buf.payload_append(b"x");
    println!("  Appending 1 more byte: {:?}", result);
    assert_eq!(result, Err(IoBuffError::PayloadFull));

    // Payload data should be unchanged after failed append
    assert_eq!(buf.payload_bytes(), b"abcd");
}

#[test]
fn buffer_mut_payload_set_len() {
    println!("--- Payload set_len (absolute) ---");
    let mut buf = IoBuffMut::new(0, 64, 0);

    // Write directly into the unwritten region
    let spare = buf.payload_unwritten_mut();
    initialize_spare_prefix(spare, b"direct");
    println!("  Wrote 'direct' into payload_unwritten_mut");

    // SAFETY: all six newly published payload bytes were initialized above.
    unsafe { buf.payload_set_len_initialized(6).unwrap() };
    println!(
        "  After payload_set_len_initialized(6): payload={:?}",
        buf.payload_bytes()
    );
    assert_eq!(buf.payload_bytes(), b"direct");
    assert_eq!(buf.payload_len(), 6);

    // Set to a smaller length (truncate)
    buf.payload_set_len(3).unwrap();
    println!(
        "  After payload_set_len(3): payload={:?}",
        buf.payload_bytes()
    );
    assert_eq!(buf.payload_bytes(), b"dir");

    // Set to zero
    buf.payload_set_len(0).unwrap();
    assert!(buf.payload_is_empty());
}

#[test]
fn buffer_mut_payload_set_len_overflow_returns_error() {
    println!("--- Payload set_len overflow ---");
    let mut buf = IoBuffMut::new(0, 8, 0);
    let result = buf.payload_set_len(9);
    println!("  payload_set_len(9) on capacity=8: {:?}", result);
    assert_eq!(result, Err(IoBuffError::PayloadFull));
}

#[test]
fn buffer_spare_capacity_api_signatures_are_initialization_safe() {
    let unwritten: for<'a> fn(&'a mut RealIoBuffMut) -> &'a mut [MaybeUninit<u8>] =
        RealIoBuffMut::payload_unwritten_mut;
    let publish: for<'a> unsafe fn(&'a mut RealIoBuffMut, usize) -> Result<(), IoBuffError> =
        RealIoBuffMut::payload_set_len_initialized;
    let userspace: for<'a> unsafe fn(&'a mut RealIoBuffMut, usize) -> &'a mut [u8] =
        <RealIoBuffMut as IoBuffReadWrite>::initialized_writable_slice;
    let write_base: for<'a> fn(&'a RealIoBuffMut) -> usize =
        <RealIoBuffMut as IoBuffReadWrite>::write_base_len;

    let mut buf = IoBuffMut::new(0, 4, 0);
    assert_eq!(write_base(&buf), 0);
    assert_eq!(unwritten(&mut buf).len(), 4);
    // SAFETY: one byte is within the validated writable capacity.
    assert_eq!(unsafe { userspace(&mut buf, 1) }, &[0]);
    // SAFETY: publishing zero bytes exposes no new storage.
    unsafe { publish(&mut buf, 0).unwrap() };
}

#[cfg(not(debug_assertions))]
#[test]
fn default_initialized_writable_slice_clamps_oversized_request() {
    let mut buffer = DefaultInitializedWritable::new();

    // SAFETY: this deliberately exceeds the documented caller bound to prove
    // the default implementation's release-mode defensive seam. Native runs
    // retain initialized guard bytes so the pre-fix implementation fails
    // deterministically without leaving the allocation; release Miri uses an
    // exact allocation and therefore also proves the raw write stays bounded.
    let initialized =
        unsafe { buffer.initialized_writable_slice(DefaultInitializedWritable::OVERSIZED) };
    assert_eq!(initialized.len(), DefaultInitializedWritable::WRITABLE);
    assert_eq!(initialized, &[0; DefaultInitializedWritable::WRITABLE]);

    #[cfg(not(miri))]
    assert_eq!(
        &buffer.storage[DefaultInitializedWritable::WRITABLE..],
        &[DefaultInitializedWritable::SENTINEL; 4]
    );
}

#[cfg(not(debug_assertions))]
#[test]
fn default_initialized_writable_slice_clamps_to_empty_without_pointer_access() {
    let mut buffer = NullEmptyInitializedWritable::default();

    // SAFETY: this deliberately exceeds the documented caller bound to prove
    // that the default implementation's release-mode defensive clamp handles
    // a permitted null empty window without consulting its pointer.
    let initialized = unsafe { buffer.initialized_writable_slice(1) };
    assert!(initialized.is_empty());
    assert_eq!(buffer.pointer_calls, 0);
}

#[test]
fn buffer_safe_growth_requires_initialized_payload() {
    let mut buf = IoBuffMut::new(0, 8, 0);

    assert_eq!(
        buf.payload_set_len(1),
        Err(IoBuffError::PayloadUninitialized)
    );
    assert_eq!(buf.payload_len(), 0);
    assert_eq!(buf.payload_remaining(), 8);
    assert!(buf.payload_bytes().is_empty());
}

#[test]
fn buffer_partial_initialization_publishes_only_initialized_prefix() {
    let mut buf = IoBuffMut::new(0, 8, 0);
    initialize_spare_prefix(buf.payload_unwritten_mut(), b"abc");

    // SAFETY: the first three payload bytes were initialized above.
    unsafe { buf.payload_set_len_initialized(3).unwrap() };
    assert_eq!(buf.payload_bytes(), b"abc");
    assert_eq!(
        buf.payload_set_len(4),
        Err(IoBuffError::PayloadUninitialized)
    );
    assert_eq!(buf.payload_bytes(), b"abc");
}

#[test]
fn buffer_safe_shrink_and_regrow_stay_within_initialized_frontier() {
    let mut buf = IoBuffMut::new(0, 8, 0);
    initialize_spare_prefix(buf.payload_unwritten_mut(), b"abcdef");
    // SAFETY: the first six payload bytes were initialized above.
    unsafe { buf.payload_set_len_initialized(6).unwrap() };

    buf.payload_set_len(3).unwrap();
    assert_eq!(buf.payload_bytes(), b"abc");
    buf.payload_set_len(6).unwrap();
    assert_eq!(buf.payload_bytes(), b"abcdef");

    buf.payload_set_len(4).unwrap();
    assert_eq!(
        buf.payload_set_len(7),
        Err(IoBuffError::PayloadUninitialized)
    );
    assert_eq!(buf.payload_bytes(), b"abcd");
}

#[test]
fn buffer_spare_borrow_discards_hidden_initialization_knowledge() {
    let mut buf = IoBuffMut::new(0, 8, 0);
    initialize_spare_prefix(buf.payload_unwritten_mut(), b"abcd");
    // SAFETY: the first four payload bytes were initialized above.
    unsafe { buf.payload_set_len_initialized(4).unwrap() };
    buf.payload_set_len(2).unwrap();

    {
        let spare = buf.payload_unwritten_mut();
        spare[0] = MaybeUninit::uninit();
    }

    assert_eq!(
        buf.payload_set_len(3),
        Err(IoBuffError::PayloadUninitialized),
        "a safe spare borrow may de-initialize hidden bytes"
    );
    assert_eq!(buf.payload_bytes(), b"ab");
}

#[test]
fn buffer_initialized_publication_errors_are_atomic() {
    let mut capacity_limited = IoBuffMut::new(0, 4, 0);
    initialize_spare_prefix(capacity_limited.payload_unwritten_mut(), b"ab");
    // SAFETY: the first two payload bytes were initialized above.
    unsafe { capacity_limited.payload_set_len_initialized(2).unwrap() };

    // SAFETY: this call cannot publish because the requested length is beyond
    // capacity; the error must leave both visible length and frontier intact.
    let result = unsafe { capacity_limited.payload_set_len_initialized(5) };
    assert_eq!(result, Err(IoBuffError::PayloadFull));
    assert_eq!(capacity_limited.payload_bytes(), b"ab");
    assert_eq!(
        capacity_limited.payload_set_len(3),
        Err(IoBuffError::PayloadUninitialized)
    );

    let mut sealed = IoBuffMut::new(0, 4, 2);
    sealed.payload_append(b"ab").unwrap();
    sealed.tailroom_append(b"T").unwrap();
    // SAFETY: this call cannot publish while tailroom is active; the error
    // must leave the existing payload and trailer unchanged.
    let result = unsafe { sealed.payload_set_len_initialized(3) };
    assert_eq!(result, Err(IoBuffError::PayloadSealed));
    assert_eq!(sealed.payload_len(), 2);
    assert_eq!(sealed.bytes(), b"abT");
}

#[test]
fn buffer_initialized_frontier_tracks_advance_and_reset() {
    let mut buf = IoBuffMut::new(0, 8, 0);
    initialize_spare_prefix(buf.payload_unwritten_mut(), b"abcdef");
    // SAFETY: the first six payload bytes were initialized above.
    unsafe { buf.payload_set_len_initialized(6).unwrap() };

    buf.advance(2).unwrap();
    assert_eq!(buf.payload_bytes(), b"cdef");
    assert_eq!(
        buf.payload_set_len(5),
        Err(IoBuffError::PayloadUninitialized)
    );
    initialize_spare_prefix(buf.payload_unwritten_mut(), b"g");
    // SAFETY: four bytes remain initialized after advance and the next byte
    // was initialized above.
    unsafe { buf.payload_set_len_initialized(5).unwrap() };
    assert_eq!(buf.payload_bytes(), b"cdefg");

    buf.reset();
    assert!(buf.payload_bytes().is_empty());
    assert_eq!(
        buf.payload_set_len(1),
        Err(IoBuffError::PayloadUninitialized)
    );
}

#[test]
fn buffer_advance_into_tailroom_does_not_publish_stale_trailer_bytes() {
    let mut buf = IoBuffMut::new(0, 2, 2);
    buf.payload_append(b"ab").unwrap();
    buf.tailroom_append(b"XY").unwrap();

    buf.advance(3).unwrap();
    assert_eq!(buf.bytes(), b"Y");
    assert!(buf.payload_unwritten_mut().is_empty());

    buf.advance(1).unwrap();
    assert!(buf.bytes().is_empty());
    assert!(buf.payload_unwritten_mut().is_empty());
    assert_eq!(buf.payload_set_len(1), Err(IoBuffError::PayloadFull));
}

#[test]
fn buffer_tailroom_extension_after_advance_keeps_new_payload_uninitialized() {
    let mut buf = IoBuffMut::new(0, 2, 4);
    buf.payload_append(b"ab").unwrap();
    buf.tailroom_append(b"WXYZ").unwrap();

    buf.advance(3).unwrap();
    assert_eq!(buf.bytes(), b"XYZ");
    buf.payload_extend_from_tailroom(4).unwrap();

    assert!(buf.is_empty());
    assert_eq!(buf.payload_remaining(), 3);
    assert_eq!(
        buf.payload_set_len(1),
        Err(IoBuffError::PayloadUninitialized),
        "discarded trailer bytes must not become safely publishable payload"
    );

    initialize_spare_prefix(buf.payload_unwritten_mut(), b"N");
    // SAFETY: the first byte in the newly available payload region was
    // initialized above.
    unsafe { buf.payload_set_len_initialized(1).unwrap() };
    assert_eq!(buf.payload_bytes(), b"N");
}

#[test]
fn buffer_freeze_thaw_and_shared_cow_discard_unpublished_frontier() {
    let mut sole = IoBuffMut::new(0, 8, 0);
    initialize_spare_prefix(sole.payload_unwritten_mut(), b"abcdef");
    // SAFETY: the first six payload bytes were initialized above.
    unsafe { sole.payload_set_len_initialized(6).unwrap() };
    sole.payload_set_len(3).unwrap();

    let frozen = sole.freeze();
    assert_eq!(frozen.bytes(), b"abc");
    let mut thawed = frozen.try_mut().expect("sole frozen owner must thaw");
    assert_eq!(
        thawed.payload_set_len(4),
        Err(IoBuffError::PayloadUninitialized)
    );
    initialize_spare_prefix(thawed.payload_unwritten_mut(), b"d");
    // SAFETY: the next unpublished byte was initialized above.
    unsafe { thawed.payload_set_len_initialized(4).unwrap() };
    assert_eq!(thawed.payload_bytes(), b"abcd");

    let mut shared_source = IoBuffMut::new(0, 8, 0);
    initialize_spare_prefix(shared_source.payload_unwritten_mut(), b"wxyzQR");
    // SAFETY: the first six payload bytes were initialized above.
    unsafe { shared_source.payload_set_len_initialized(6).unwrap() };
    shared_source.payload_set_len(4).unwrap();
    let frozen = shared_source.freeze();
    let shared = frozen.clone();
    let mut copied = frozen.make_mut().unwrap();

    assert_eq!(copied.payload_bytes(), b"wxyz");
    assert_eq!(
        copied.payload_set_len(5),
        Err(IoBuffError::PayloadUninitialized)
    );
    initialize_spare_prefix(copied.payload_unwritten_mut(), b"!");
    // SAFETY: the next unpublished byte was initialized above.
    unsafe { copied.payload_set_len_initialized(5).unwrap() };
    assert_eq!(copied.payload_bytes(), b"wxyz!");
    assert_eq!(shared.bytes(), b"wxyz");
}

// ============================================================================
// IoBuffMut — headroom operations
// ============================================================================

#[test]
fn buffer_mut_headroom_prepend() {
    println!("--- Headroom prepend ---");
    let mut buf = IoBuffMut::new(16, 64, 0);

    buf.payload_append(b"payload").unwrap();
    println!("  After payload: bytes={:?}", buf.bytes());
    assert_eq!(buf.bytes(), b"payload");

    buf.headroom_prepend(b"HDR:").unwrap();
    println!("  After prepend 'HDR:': bytes={:?}", buf.bytes());
    println!("  headroom_remaining: {}", buf.headroom_remaining());
    assert_eq!(buf.bytes(), b"HDR:payload");
    assert_eq!(buf.headroom_remaining(), 12);
    assert_eq!(buf.len(), 11);

    // Payload is still intact
    assert_eq!(buf.payload_bytes(), b"payload");
    assert_eq!(buf.payload_len(), 7);
}

#[test]
fn buffer_mut_headroom_prepend_multiple() {
    println!("--- Multiple headroom prepends ---");
    let mut buf = IoBuffMut::new(32, 64, 0);

    buf.payload_append(b"DATA").unwrap();
    buf.headroom_prepend(b"L2:").unwrap();
    println!("  After L2 prepend: bytes={:?}", buf.bytes());
    assert_eq!(buf.bytes(), b"L2:DATA");

    buf.headroom_prepend(b"L1:").unwrap();
    println!("  After L1 prepend: bytes={:?}", buf.bytes());
    assert_eq!(buf.bytes(), b"L1:L2:DATA");
    assert_eq!(buf.headroom_remaining(), 26);
}

#[test]
fn buffer_mut_headroom_prepend_overflow_returns_error() {
    println!("--- Headroom prepend overflow ---");
    let mut buf = IoBuffMut::new(4, 64, 0);

    buf.headroom_prepend(b"1234").unwrap();
    println!(
        "  Filled headroom (4/4): headroom_remaining={}",
        buf.headroom_remaining()
    );

    let result = buf.headroom_prepend(b"x");
    println!("  Prepend 1 more: {:?}", result);
    assert_eq!(result, Err(IoBuffError::HeadroomFull));
}

#[test]
fn buffer_mut_headroom_prepend_on_zero_headroom_returns_error() {
    println!("--- Prepend on zero headroom ---");
    let mut buf = IoBuffMut::new(0, 64, 0);
    let result = buf.headroom_prepend(b"x");
    println!("  Result: {:?}", result);
    assert_eq!(result, Err(IoBuffError::HeadroomFull));
}

// ============================================================================
// IoBuffMut — tailroom operations
// ============================================================================

#[test]
fn buffer_mut_tailroom_append() {
    println!("--- Tailroom append ---");
    let mut buf = IoBuffMut::new(0, 64, 16);

    buf.payload_append(b"body").unwrap();
    buf.tailroom_append(b"--END").unwrap();
    println!("  After payload + tailroom: bytes={:?}", buf.bytes());
    assert_eq!(buf.bytes(), b"body--END");
    assert_eq!(buf.tailroom_remaining(), 11);
    assert_eq!(buf.len(), 9);
    assert_eq!(buf.payload_len(), 4);
}

#[test]
fn buffer_mut_tailroom_append_overflow_returns_error() {
    println!("--- Tailroom append overflow ---");
    let mut buf = IoBuffMut::new(0, 64, 2);

    buf.tailroom_append(b"OK").unwrap();
    let result = buf.tailroom_append(b"x");
    println!("  Overflow result: {:?}", result);
    assert_eq!(result, Err(IoBuffError::TailroomFull));
}

#[test]
fn buffer_mut_tailroom_append_on_zero_tailroom_returns_error() {
    println!("--- Append on zero tailroom ---");
    let mut buf = IoBuffMut::new(0, 64, 0);
    let result = buf.tailroom_append(b"x");
    println!("  Result: {:?}", result);
    assert_eq!(result, Err(IoBuffError::TailroomFull));
}

#[test]
fn buffer_mut_tailroom_seals_payload_growth() {
    println!("--- Tailroom seals payload growth ---");
    let mut buf = IoBuffMut::new(0, 8, 4);

    buf.payload_append(b"data").unwrap();
    buf.tailroom_append(b"TR").unwrap();

    assert_eq!(buf.payload_remaining(), 0);
    assert!(buf.payload_unwritten_mut().is_empty());
    assert_eq!(IoBuffReadWrite::writable_len(&buf), 0);

    let append_result = buf.payload_append(b"x");
    println!(
        "  payload_append after tailroom_append: {:?}",
        append_result
    );
    assert_eq!(append_result, Err(IoBuffError::PayloadSealed));

    let set_len_result = buf.payload_set_len(5);
    println!(
        "  payload_set_len growth after tailroom_append: {:?}",
        set_len_result
    );
    assert_eq!(set_len_result, Err(IoBuffError::PayloadSealed));

    buf.payload_set_len(4).unwrap();
    assert_eq!(buf.bytes(), b"dataTR");
}

// ============================================================================
// IoBuffMut — all three regions combined
// ============================================================================

#[test]
fn buffer_mut_full_protocol_frame() {
    println!("--- Full protocol frame: headroom + payload + tailroom ---");
    let mut buf = IoBuffMut::new(8, 256, 4);

    buf.payload_append(b"Hello, World!").unwrap();
    println!("  Payload: {:?}", buf.payload_bytes());

    buf.headroom_prepend(b"\x00\x11").unwrap();
    println!("  After header prepend: bytes={:?}", buf.bytes());

    buf.tailroom_append(b"\xAA\xBB").unwrap();
    println!("  After trailer append: bytes={:?}", buf.bytes());

    let expected_len = 2 + 13 + 2; // header + payload + trailer
    println!("  Total len: {} (expected {})", buf.len(), expected_len);
    assert_eq!(buf.len(), expected_len);
    assert_eq!(buf.payload_len(), 13);
    assert_eq!(buf.headroom_remaining(), 6);
    assert_eq!(buf.tailroom_remaining(), 2);

    // The contiguous bytes() should be: header + payload + trailer
    let bytes = buf.bytes();
    assert_eq!(&bytes[0..2], b"\x00\x11");
    assert_eq!(&bytes[2..15], b"Hello, World!");
    assert_eq!(&bytes[15..17], b"\xAA\xBB");
}

// ============================================================================
// IoBuffMut — payload_extend_from_tailroom
// ============================================================================

#[test]
fn buffer_mut_payload_extend_from_tailroom() {
    println!("--- Extend payload from tailroom ---");
    let mut buf = IoBuffMut::new(0, 16, 32);
    println!(
        "  Initial: payload_capacity={}, tailroom_capacity={}",
        buf.payload_capacity(),
        buf.tailroom_capacity()
    );

    buf.payload_extend_from_tailroom(16).unwrap();
    println!(
        "  After extend(16): payload_capacity={}, tailroom_capacity={}",
        buf.payload_capacity(),
        buf.tailroom_capacity()
    );
    assert_eq!(buf.payload_capacity(), 32);
    assert_eq!(buf.tailroom_capacity(), 16);

    // Can now write 32 bytes into payload
    buf.payload_append(&[0xAA; 32]).unwrap();
    assert_eq!(buf.payload_len(), 32);
}

#[test]
fn buffer_mut_payload_extend_from_tailroom_overflow_returns_error() {
    println!("--- Extend payload from tailroom overflow ---");
    let mut buf = IoBuffMut::new(0, 16, 8);
    let result = buf.payload_extend_from_tailroom(9);
    println!("  Extend(9) from tailroom(8): {:?}", result);
    assert_eq!(result, Err(IoBuffError::TailroomInsufficient));
}

#[test]
fn buffer_mut_payload_extend_from_tailroom_discards_tailroom_data() {
    println!("--- Extend from tailroom discards tailroom data ---");
    let mut buf = IoBuffMut::new(0, 16, 16);

    buf.tailroom_append(b"trailer").unwrap();
    println!("  Tailroom has {} bytes written", buf.tailroom_remaining());

    buf.payload_extend_from_tailroom(8).unwrap();
    println!(
        "  After extend: tailroom_remaining={}",
        buf.tailroom_remaining()
    );
    // tailroom_len is reset to 0 by extend
    assert_eq!(buf.tailroom_remaining(), buf.tailroom_capacity());
}

// ============================================================================
// IoBuffMut — advance (cursor)
// ============================================================================

#[test]
fn buffer_mut_advance_payload() {
    println!("--- Advance through payload ---");
    let mut buf = IoBuffMut::new(0, 64, 0);
    buf.payload_append(b"HEADER:PAYLOAD").unwrap();
    println!(
        "  Before advance: bytes={:?}, len={}",
        buf.bytes(),
        buf.len()
    );

    buf.advance(7).unwrap();
    println!(
        "  After advance(7): bytes={:?}, len={}",
        buf.bytes(),
        buf.len()
    );
    assert_eq!(buf.bytes(), b"PAYLOAD");
    assert_eq!(buf.payload_len(), 7);
}

#[test]
fn buffer_mut_advance_payload_accessors_follow_active_payload() {
    let mut buf = IoBuffMut::new(0, 10, 0);
    buf.payload_append(b"abcdef").unwrap();

    buf.advance(2).unwrap();
    assert_eq!(buf.bytes(), b"cdef");
    assert_eq!(buf.payload_bytes(), b"cdef");
    assert_eq!(buf.payload_remaining(), 4);

    buf.payload_bytes_mut()[0] = b'C';
    assert_eq!(buf.payload_bytes(), b"Cdef");

    let spare = buf.payload_unwritten_mut();
    assert_eq!(spare.len(), 4);
    initialize_spare_prefix(spare, b"gh");
    // SAFETY: the two newly published payload bytes were initialized above.
    unsafe { buf.payload_set_len_initialized(6).unwrap() };
    assert_eq!(buf.payload_bytes(), b"Cdefgh");
    assert_eq!(buf.bytes(), b"Cdefgh");

    let ptr = IoBuffReadWrite::as_mut_ptr(&mut buf);
    unsafe {
        std::ptr::copy_nonoverlapping(b"ij".as_ptr(), ptr, 2);
        IoBuffReadWrite::set_written_len(&mut buf, 8);
    }
    assert_eq!(buf.payload_bytes(), b"Cdefghij");
    assert_eq!(buf.payload_remaining(), 0);
    assert_eq!(buf.payload_append(b"k"), Err(IoBuffError::PayloadFull));

    let frozen = buf.freeze();
    assert_eq!(frozen.payload_bytes(), b"Cdefghij");
}

#[test]
fn buffer_mut_advance_into_payload_closes_headroom_prepend() {
    let mut buf = IoBuffMut::new(2, 8, 0);
    buf.payload_append(b"abc").unwrap();
    buf.headroom_prepend(b"H").unwrap();

    buf.advance(2).unwrap();

    assert_eq!(buf.bytes(), b"bc");
    assert_eq!(buf.payload_bytes(), b"bc");
    assert_eq!(buf.headroom_remaining(), 0);
    assert_eq!(buf.headroom_prepend(b"!"), Err(IoBuffError::HeadroomFull));
}

#[test]
fn buffer_mut_tailroom_append_after_advance_stays_after_payload() {
    let mut buf = IoBuffMut::new(0, 8, 4);
    buf.payload_append(b"abcd").unwrap();

    buf.advance(2).unwrap();
    buf.tailroom_append(b":t").unwrap();

    assert_eq!(buf.payload_bytes(), b"cd");
    assert_eq!(buf.bytes(), b"cd:t");
}

#[test]
fn buffer_mut_tailroom_advance_consumed_capacity_is_not_reused() {
    let mut buf = IoBuffMut::new(0, 8, 4);

    buf.payload_append(b"abcdefgh").unwrap();
    buf.tailroom_append(b"WXYZ").unwrap();
    buf.advance(10).unwrap();

    assert_eq!(buf.bytes(), b"YZ");
    assert_eq!(buf.tailroom_remaining(), 0);
    assert_eq!(buf.tailroom_append(b"!!"), Err(IoBuffError::TailroomFull));
}

#[test]
fn buffer_mut_headroom_advance_into_payload_keeps_prepend_closed() {
    let mut buf = IoBuffMut::new(4, 8, 0);

    buf.payload_append(b"abcdefgh").unwrap();
    buf.headroom_prepend(b"WXYZ").unwrap();
    buf.advance(6).unwrap();

    assert_eq!(buf.bytes(), b"cdefgh");
    assert_eq!(buf.headroom_remaining(), 0);
    assert_eq!(buf.headroom_prepend(b"!"), Err(IoBuffError::HeadroomFull));
}

#[test]
fn buffer_mut_tailroom_append_advance_interleavings_stay_in_bounds() {
    const HEAD: &[u8; 3] = b"HDR";
    const PAYLOAD: &[u8; 8] = b"abcdefgh";
    const TAIL: &[u8; 4] = b"WXYZ";

    for headroom in [0, HEAD.len()] {
        let total = headroom + PAYLOAD.len() + TAIL.len();
        for head_len in 0..=headroom {
            for payload_len in 0..=PAYLOAD.len() {
                for first_tail_len in 0..=TAIL.len() {
                    let initial_len = head_len + payload_len + first_tail_len;
                    for advance in 0..=initial_len {
                        let mut buf = IoBuffMut::new(headroom, PAYLOAD.len(), TAIL.len());
                        // SAFETY: a new buffer's active pointer starts exactly
                        // `headroom` bytes after the backing allocation base.
                        let base = unsafe { IoBuffReadOnly::as_ptr(&buf).sub(headroom) };

                        buf.payload_append(&PAYLOAD[..payload_len]).unwrap();
                        if head_len != 0 {
                            buf.headroom_prepend(&HEAD[..head_len]).unwrap();
                        }
                        buf.tailroom_append(&TAIL[..first_tail_len]).unwrap();
                        assert_active_window_within_allocation(&buf, base, total);

                        buf.advance(advance).unwrap();
                        assert_active_window_within_allocation(&buf, base, total);

                        let remaining = buf.tailroom_remaining();
                        buf.tailroom_append(&TAIL[..remaining]).unwrap();
                        assert_eq!(buf.tailroom_remaining(), 0);
                        assert_active_window_within_allocation(&buf, base, total);
                    }
                }
            }
        }
    }
}

#[test]
fn buffer_mut_advance_through_headroom_into_payload() {
    println!("--- Advance through headroom into payload ---");
    let mut buf = IoBuffMut::new(8, 64, 0);
    buf.payload_append(b"data").unwrap();
    buf.headroom_prepend(b"HDR:").unwrap();
    println!(
        "  Before advance: bytes={:?}, len={}",
        buf.bytes(),
        buf.len()
    );
    println!("  headroom_remaining={}", buf.headroom_remaining());

    // Advance past the header (4 bytes headroom) + 2 bytes of payload
    buf.advance(6).unwrap();
    println!(
        "  After advance(6): bytes={:?}, len={}",
        buf.bytes(),
        buf.len()
    );
    assert_eq!(buf.bytes(), b"ta");
    assert_eq!(buf.payload_len(), 2);
}

#[test]
fn buffer_mut_advance_overflow_returns_error() {
    println!("--- Advance overflow ---");
    let mut buf = IoBuffMut::new(0, 8, 0);
    buf.payload_append(b"tiny").unwrap();
    let result = buf.advance(5);
    println!("  advance(5) on len=4: {:?}", result);
    assert_eq!(result, Err(IoBuffError::AdvanceOutOfBounds));
    // Buffer should be unchanged after failed advance
    assert_eq!(buf.bytes(), b"tiny");
}

#[test]
fn buffer_mut_advance_into_tailroom() {
    let mut buf = IoBuffMut::new(4, 64, 4);
    buf.payload_append(b"DATA").unwrap();
    buf.headroom_prepend(b"H:").unwrap();
    buf.tailroom_append(b":T").unwrap();

    buf.advance(7).unwrap();
    assert_eq!(buf.bytes(), b"T");
    assert_eq!(buf.payload_len(), 0);
    assert_eq!(buf.tailroom_remaining(), 3);
}

#[test]
fn buffer_mut_advance_entire_active_window_with_tailroom() {
    let mut buf = IoBuffMut::new(4, 64, 4);
    buf.payload_append(b"DATA").unwrap();
    buf.headroom_prepend(b"H:").unwrap();
    buf.tailroom_append(b":T").unwrap();

    let total = buf.len();
    buf.advance(total).unwrap();
    assert!(buf.is_empty());
    assert_eq!(buf.len(), 0);
    assert_eq!(buf.payload_len(), 0);
    assert_eq!(buf.tailroom_remaining(), buf.tailroom_capacity());
}

#[test]
fn buffer_mut_advance_overflow_with_tailroom_leaves_state_unchanged() {
    let mut buf = IoBuffMut::new(4, 64, 4);
    buf.payload_append(b"DATA").unwrap();
    buf.headroom_prepend(b"H:").unwrap();
    buf.tailroom_append(b":T").unwrap();

    let before = buf.bytes().to_vec();
    let result = buf.advance(buf.len() + 1);
    assert_eq!(result, Err(IoBuffError::AdvanceOutOfBounds));
    assert_eq!(buf.bytes(), before.as_slice());
    assert_eq!(buf.payload_len(), 4);
    assert_eq!(buf.tailroom_remaining(), 2);
}

// ============================================================================
// IoBuffMut — reset
// ============================================================================

#[test]
fn buffer_mut_reset() {
    println!("--- Reset ---");
    let mut buf = IoBuffMut::new(16, 64, 8);
    buf.payload_append(b"data").unwrap();
    buf.headroom_prepend(b"H:").unwrap();
    buf.tailroom_append(b"T").unwrap();
    println!("  Before reset: bytes={:?}, len={}", buf.bytes(), buf.len());

    buf.reset();
    println!(
        "  After reset: len={}, payload_len={}",
        buf.len(),
        buf.payload_len()
    );
    assert_eq!(buf.len(), 0);
    assert_eq!(buf.payload_len(), 0);
    assert!(buf.is_empty());
    assert_eq!(buf.headroom_remaining(), 16);
    assert_eq!(buf.tailroom_remaining(), 8);
    assert_eq!(buf.payload_remaining(), 64);
}

#[test]
fn buffer_mut_reset_preserves_transferred_region_geometry() {
    let mut buf = IoBuffMut::new(4, 8, 4);
    buf.payload_extend_from_tailroom(3).unwrap();
    buf.payload_append(b"data").unwrap();
    buf.headroom_prepend(b"H").unwrap();
    buf.tailroom_append(b"T").unwrap();

    buf.reset();

    assert!(buf.is_empty());
    assert_eq!(buf.headroom_remaining(), 4);
    assert_eq!(buf.payload_capacity(), 11);
    assert_eq!(buf.payload_remaining(), 11);
    assert_eq!(buf.tailroom_capacity(), 1);
    assert_eq!(buf.tailroom_remaining(), 1);
}

// ============================================================================
// IoBuffMut — payload_bytes_mut
// ============================================================================

#[test]
fn buffer_mut_payload_bytes_mut_modify_in_place() {
    println!("--- Modify payload in place ---");
    let mut buf = IoBuffMut::new(0, 64, 0);
    buf.payload_append(b"hello").unwrap();
    println!("  Before: {:?}", buf.payload_bytes());

    buf.payload_bytes_mut()[0] = b'H';
    println!("  After modifying [0]='H': {:?}", buf.payload_bytes());
    assert_eq!(buf.payload_bytes(), b"Hello");
}

// ============================================================================
// IoBuffMut — Deref / AsRef
// ============================================================================

#[test]
fn buffer_mut_deref_returns_full_window() {
    println!("--- Deref returns full active window ---");
    let mut buf = IoBuffMut::new(4, 64, 4);
    buf.payload_append(b"mid").unwrap();
    buf.headroom_prepend(b"H").unwrap();
    buf.tailroom_append(b"T").unwrap();

    let slice: &[u8] = &buf;
    println!("  Deref: {:?}", slice);
    assert_eq!(slice, b"HmidT");

    let as_ref: &[u8] = buf.as_ref();
    assert_eq!(as_ref, b"HmidT");
}

// ============================================================================
// IoBuffMut — freeze → IoBuff
// ============================================================================

#[test]
fn buffer_freeze_zero_copy() {
    println!("--- Freeze (zero-copy) ---");
    let mut buf = IoBuffMut::new(4, 64, 0);
    buf.payload_append(b"frozen_data").unwrap();
    buf.headroom_prepend(b"H:").unwrap();

    let ptr_before = buf.bytes().as_ptr();
    println!(
        "  Before freeze: ptr={:?}, bytes={:?}",
        ptr_before,
        buf.bytes()
    );

    let frozen = buf.freeze();
    let ptr_after = frozen.bytes().as_ptr();
    println!(
        "  After freeze:  ptr={:?}, bytes={:?}",
        ptr_after,
        frozen.bytes()
    );

    assert_eq!(ptr_before, ptr_after, "freeze must be zero-copy");
    assert_eq!(frozen.bytes(), b"H:frozen_data");
    assert_eq!(frozen.len(), 13);
}

// ============================================================================
// IoBuff — clone, refcount, sharing
// ============================================================================

#[test]
fn buffer_frozen_clone_shares_data() {
    println!("--- Frozen clone shares backing storage ---");
    let mut buf = IoBuffMut::new(0, 64, 0);
    buf.payload_append(b"shared").unwrap();
    let frozen = buf.freeze();

    let clone1 = frozen.clone();
    let clone2 = frozen.clone();

    println!("  original ptr: {:?}", frozen.bytes().as_ptr());
    println!("  clone1 ptr:   {:?}", clone1.bytes().as_ptr());
    println!("  clone2 ptr:   {:?}", clone2.bytes().as_ptr());

    assert_eq!(frozen.bytes().as_ptr(), clone1.bytes().as_ptr());
    assert_eq!(frozen.bytes().as_ptr(), clone2.bytes().as_ptr());
    assert_eq!(frozen.bytes(), b"shared");
    assert_eq!(clone1.bytes(), b"shared");
    assert_eq!(clone2.bytes(), b"shared");
}

// ============================================================================
// IoBuff — slice (zero-copy views)
// ============================================================================

#[test]
fn buffer_frozen_slice() {
    println!("--- Frozen slice ---");
    let mut buf = IoBuffMut::new(0, 64, 0);
    buf.payload_append(b"Hello, World!").unwrap();
    let frozen = buf.freeze();

    let hello = expect_view(frozen.slice(0..5));
    let world = expect_view(frozen.slice(7..12));
    println!("  hello: {:?}", hello.bytes());
    println!("  world: {:?}", world.bytes());

    assert_eq!(hello.bytes(), b"Hello");
    assert_eq!(world.bytes(), b"World");
    assert_eq!(hello.len(), 5);
    assert_eq!(world.len(), 5);
}

#[test]
fn buffer_frozen_slice_returns_view() {
    let mut buf = IoBuffMut::new(0, 16, 0);
    buf.payload_append(b"abcdef").unwrap();
    let frozen = buf.freeze();
    let view: IoBuffView = expect_view(frozen.slice(1..4));
    assert_eq!(view.bytes(), b"bcd");
}

#[test]
fn buffer_frozen_slice_full_range() {
    println!("--- Frozen slice full range ---");
    let mut buf = IoBuffMut::new(0, 16, 0);
    buf.payload_append(b"all").unwrap();
    let frozen = buf.freeze();

    let full = expect_view(frozen.slice(..));
    assert_eq!(full.bytes(), b"all");

    let from_start = expect_view(frozen.slice(..2));
    assert_eq!(from_start.bytes(), b"al");

    let to_end = expect_view(frozen.slice(1..));
    assert_eq!(to_end.bytes(), b"ll");
}

#[test]
fn buffer_frozen_slice_out_of_bounds_returns_error() {
    println!("--- Frozen slice out of bounds ---");
    let mut buf = IoBuffMut::new(0, 8, 0);
    buf.payload_append(b"abc").unwrap();
    let frozen = buf.freeze();
    assert!(matches!(
        frozen.slice(0..4),
        Err(IoBuffError::SliceOutOfBounds)
    ));
}

#[test]
fn buffer_frozen_slice_empty() {
    println!("--- Frozen slice empty range ---");
    let mut buf = IoBuffMut::new(0, 8, 0);
    buf.payload_append(b"abc").unwrap();
    let frozen = buf.freeze();

    let empty = expect_view(frozen.slice(1..1));
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
}

// ============================================================================
// IoBuff — owned views
// ============================================================================

#[test]
fn buffer_frozen_owned_view_full_range() {
    let mut buf = IoBuffMut::new(4, 16, 4);
    buf.payload_append(b"payload").unwrap();
    buf.headroom_prepend(b"H:").unwrap();
    buf.tailroom_append(b":T").unwrap();
    let frozen = buf.freeze();
    let expected_ptr = frozen.bytes().as_ptr();

    let view = expect_owned_view(frozen.into_owned_view(..));

    assert_eq!(view.bytes(), b"H:payload:T");
    assert_eq!(view.len(), 11);
    assert!(!view.is_empty());
    assert_eq!(view.bytes().as_ptr(), expected_ptr);
}

#[test]
fn buffer_frozen_owned_view_middle_range() {
    let mut buf = IoBuffMut::new(4, 16, 4);
    buf.payload_append(b"payload").unwrap();
    buf.headroom_prepend(b"H:").unwrap();
    buf.tailroom_append(b":T").unwrap();
    let frozen = buf.freeze();
    let expected_ptr = frozen.bytes()[2..9].as_ptr();

    let view = expect_owned_view(frozen.into_owned_view(2..9));

    let as_ref: &[u8] = view.as_ref();
    assert_eq!(view.bytes(), b"payload");
    assert_eq!(as_ref, b"payload");
    assert_eq!(&*view, b"payload");
    assert_eq!(view.len(), 7);
    assert_eq!(view.bytes().as_ptr(), expected_ptr);
}

#[test]
fn buffer_frozen_owned_view_empty_range() {
    let mut buf = IoBuffMut::new(0, 8, 0);
    buf.payload_append(b"abc").unwrap();
    let frozen = buf.freeze();

    let view = expect_owned_view(frozen.into_owned_view(1..1));

    assert!(view.is_empty());
    assert_eq!(view.len(), 0);
    assert_eq!(view.bytes(), b"");
}

#[test]
fn buffer_frozen_owned_view_unbounded_and_inclusive_ranges() {
    let mut buf = IoBuffMut::new(0, 16, 0);
    buf.payload_append(b"abcdef").unwrap();
    let frozen = buf.freeze();

    let inclusive = expect_owned_view(frozen.into_owned_view(..=2));
    assert_eq!(inclusive.bytes(), b"abc");

    let frozen = inclusive.into_inner();
    let to_end = expect_owned_view(frozen.into_owned_view(2..));
    assert_eq!(to_end.bytes(), b"cdef");
}

/// On out-of-bounds, into_owned_view returns the original buffer in the error
/// tuple without retaining or dropping it, so unique ownership is preserved.
#[test]
fn buffer_frozen_owned_view_out_of_bounds_returns_original() {
    let mut buf = IoBuffMut::new(0, 8, 0);
    buf.payload_append(b"abc").unwrap();
    let frozen = buf.freeze();
    let expected_ptr = frozen.bytes().as_ptr();

    let result = frozen.into_owned_view(0..4);
    let (original, err) = result.expect_err("out-of-bounds owned view should fail");

    assert_eq!(err, IoBuffError::SliceOutOfBounds);
    assert_eq!(original.bytes(), b"abc");
    assert_eq!(original.bytes().as_ptr(), expected_ptr);
    assert!(
        original.try_mut().is_ok(),
        "failed owned view must not retain or lose ownership"
    );
}

/// Sole-owner into_owned_view does not bump the refcount; after into_inner,
/// try_mut recovers the original allocation in place.
#[test]
fn buffer_frozen_owned_view_from_sole_owner_does_not_retain() {
    let mut buf = IoBuffMut::new(0, 16, 0);
    buf.payload_append(b"exclusive").unwrap();
    let frozen = buf.freeze();
    let expected_ptr = frozen.bytes().as_ptr();

    let view = expect_owned_view(frozen.into_owned_view(1..5));
    assert_eq!(view.bytes(), b"xclu");

    let original = view.into_inner();
    let unfrozen = original
        .try_mut()
        .expect("owned view must not add a backing refcount");
    assert_eq!(unfrozen.bytes().as_ptr(), expected_ptr);
    assert_eq!(unfrozen.bytes(), b"exclusive");
}

#[test]
fn buffer_frozen_owned_view_respects_existing_clone_refcount() {
    let mut buf = IoBuffMut::new(0, 16, 0);
    buf.payload_append(b"shared").unwrap();
    let frozen = buf.freeze();
    let clone = frozen.clone();
    let expected_ptr = frozen.bytes().as_ptr();

    let view = expect_owned_view(frozen.into_owned_view(1..4));
    assert_eq!(view.bytes(), b"har");

    let original = view.into_inner();
    let result = original.try_mut();
    assert!(
        result.is_err(),
        "existing clone must still prevent mutable recovery"
    );

    let original = result
        .err()
        .expect("failed try_mut returns original buffer");
    drop(clone);

    let unfrozen = original
        .try_mut()
        .expect("dropping the clone should restore sole ownership");
    assert_eq!(unfrozen.bytes().as_ptr(), expected_ptr);
    assert_eq!(unfrozen.bytes(), b"shared");
}

// ============================================================================
// IoBuff — try_mut / make_mut (copy-on-write)
// ============================================================================

fn advanced_structured_frozen(advance: usize) -> IoBuff {
    let mut buf = IoBuffMut::new(4, 8, 4);
    buf.payload_append(b"abcdefgh").unwrap();
    buf.headroom_prepend(b"WXYZ").unwrap();
    buf.tailroom_append(b"IJKL").unwrap();
    buf.advance(advance).unwrap();
    buf.freeze()
}

#[test]
fn buffer_frozen_try_mut_sole_owner() {
    println!("--- try_mut on sole owner (zero-copy) ---");
    let mut buf = IoBuffMut::new(0, 64, 0);
    buf.payload_append(b"exclusive").unwrap();
    let frozen = buf.freeze();

    let ptr = frozen.bytes().as_ptr();
    let result = frozen.try_mut();
    assert!(result.is_ok());

    let mut unfrozen = result.unwrap();
    println!(
        "  ptr before: {:?}, ptr after: {:?}",
        ptr,
        unfrozen.bytes().as_ptr()
    );
    assert_eq!(
        ptr,
        unfrozen.bytes().as_ptr(),
        "try_mut sole owner must be zero-copy"
    );
    assert_eq!(unfrozen.bytes(), b"exclusive");

    // Can mutate again
    unfrozen.payload_append(b"!").unwrap();
    assert_eq!(unfrozen.bytes(), b"exclusive!");
}

#[test]
fn buffer_frozen_try_mut_sole_owner_with_headroom_and_tailroom() {
    let mut buf = IoBuffMut::new(4, 64, 4);
    buf.payload_append(b"payload").unwrap();
    buf.headroom_prepend(b"H:").unwrap();
    buf.tailroom_append(b":T").unwrap();

    let frozen = buf.freeze();
    let unfrozen = frozen.try_mut().unwrap();

    assert_eq!(unfrozen.bytes(), b"H:payload:T");
    assert_eq!(unfrozen.payload_bytes(), b"payload");
    assert_eq!(unfrozen.headroom_remaining(), 2);
    assert_eq!(unfrozen.tailroom_remaining(), 2);
}

#[test]
fn buffer_frozen_try_mut_shared_fails() {
    println!("--- try_mut on shared buffer fails ---");
    let mut buf = IoBuffMut::new(0, 64, 0);
    buf.payload_append(b"shared").unwrap();
    let frozen = buf.freeze();
    let _clone = frozen.clone();

    let result = frozen.try_mut();
    println!("  Result: {:?}", result.is_err());
    assert!(result.is_err(), "try_mut must fail when refcount > 1");
}

#[test]
fn buffer_frozen_make_mut_copies_when_shared() {
    println!("--- make_mut copies when shared ---");
    let mut buf = IoBuffMut::new(0, 64, 0);
    buf.payload_append(b"original").unwrap();
    let frozen = buf.freeze();

    let clone = frozen.clone();
    let original_ptr = frozen.bytes().as_ptr();

    let exclusive = frozen.make_mut().unwrap();
    println!("  original ptr: {:?}", original_ptr);
    println!("  copy ptr:     {:?}", exclusive.bytes().as_ptr());
    assert_ne!(
        original_ptr,
        exclusive.bytes().as_ptr(),
        "make_mut must copy when shared"
    );

    // Both are independently valid
    assert_eq!(exclusive.bytes(), b"original");
    assert_eq!(clone.bytes(), b"original");

    // IoBuff::make_mut preserves the original headroom/payload/tailroom
    // capacities. The key invariant here is independence from the clone.
    drop(exclusive);
    assert_eq!(clone.bytes(), b"original");
}

#[test]
fn buffer_frozen_make_mut_preserves_shape() {
    let mut buf = IoBuffMut::new(4, 64, 4);
    buf.payload_append(b"payload").unwrap();
    buf.headroom_prepend(b"H:").unwrap();
    buf.tailroom_append(b":T").unwrap();
    let frozen = buf.freeze();
    let _clone = frozen.clone();

    let copied = frozen.make_mut().unwrap();
    assert_eq!(copied.bytes(), b"H:payload:T");
    assert_eq!(copied.payload_bytes(), b"payload");
    assert_eq!(copied.headroom_remaining(), 2);
    assert_eq!(copied.tailroom_remaining(), 2);
}

#[test]
fn buffer_frozen_shared_make_mut_matches_unique_advanced_state() {
    let cases = [
        (
            0,
            b"WXYZabcdefghIJKL".as_slice(),
            b"abcdefgh".as_slice(),
            (4, 8, 4),
        ),
        (
            2,
            b"YZabcdefghIJKL".as_slice(),
            b"abcdefgh".as_slice(),
            (2, 8, 4),
        ),
        (6, b"cdefghIJKL".as_slice(), b"cdefgh".as_slice(), (0, 6, 4)),
        (14, b"KL".as_slice(), b"".as_slice(), (0, 0, 2)),
        (16, b"".as_slice(), b"".as_slice(), (0, 0, 0)),
    ];

    for (advance, expected_bytes, expected_payload, expected_regions) in cases {
        let unique = advanced_structured_frozen(advance)
            .try_mut()
            .expect("sole frozen buffer must thaw");

        let frozen = advanced_structured_frozen(advance);
        let alias = frozen.clone();
        let shared = frozen.make_mut().expect("shared COW allocation failed");

        assert_eq!(unique.bytes(), expected_bytes, "unique advance {advance}");
        assert_eq!(shared.bytes(), expected_bytes, "shared advance {advance}");
        assert_eq!(alias.bytes(), expected_bytes, "alias advance {advance}");
        assert_eq!(unique.payload_bytes(), expected_payload);
        assert_eq!(shared.payload_bytes(), expected_payload);

        assert_eq!(unique.headroom_capacity(), 4);
        assert_eq!(shared.headroom_capacity(), 4);
        assert_eq!(unique.payload_capacity(), 8);
        assert_eq!(shared.payload_capacity(), 8);
        assert_eq!(unique.tailroom_capacity(), 4);
        assert_eq!(shared.tailroom_capacity(), 4);
        assert_eq!(unique.headroom_remaining(), shared.headroom_remaining());
        assert_eq!(unique.payload_remaining(), shared.payload_remaining());
        assert_eq!(unique.tailroom_remaining(), shared.tailroom_remaining());

        let unique = unique.freeze();
        let shared = shared.freeze();
        let unique_regions = (
            unique.headroom_len(),
            unique.payload_len(),
            unique.tailroom_len(),
        );
        let shared_regions = (
            shared.headroom_len(),
            shared.payload_len(),
            shared.tailroom_len(),
        );
        assert_eq!(unique_regions, expected_regions, "unique advance {advance}");
        assert_eq!(shared_regions, expected_regions, "shared advance {advance}");
    }
}

#[test]
fn buffer_frozen_shared_make_mut_preserves_consumed_capacity_and_reset() {
    fn source() -> IoBuff {
        let mut buf = IoBuffMut::new(4, 8, 0);
        buf.payload_append(b"abcdefgh").unwrap();
        buf.headroom_prepend(b"WXYZ").unwrap();
        buf.advance(6).unwrap();
        buf.freeze()
    }

    let mut unique = source().try_mut().expect("sole frozen buffer must thaw");
    let frozen = source();
    let alias = frozen.clone();
    let mut shared = frozen.make_mut().expect("shared COW allocation failed");

    for buf in [&mut unique, &mut shared] {
        assert_eq!(buf.bytes(), b"cdefgh");
        assert_eq!(buf.payload_bytes(), b"cdefgh");
        assert_eq!(buf.headroom_remaining(), 0);
        assert_eq!(buf.payload_remaining(), 0);
        assert_eq!(buf.headroom_prepend(b"!"), Err(IoBuffError::HeadroomFull));
        assert_eq!(buf.payload_append(b"!"), Err(IoBuffError::PayloadFull));

        buf.reset();
        assert_eq!(buf.headroom_remaining(), 4);
        assert_eq!(buf.payload_remaining(), 8);
        buf.payload_append(b"xy").unwrap();
        buf.headroom_prepend(b"H").unwrap();
        assert_eq!(buf.bytes(), b"Hxy");
    }

    assert_eq!(alias.bytes(), b"cdefgh");
    assert_eq!(unique.bytes(), shared.bytes());
}

#[test]
fn buffer_frozen_shared_make_mut_preserves_frontier_and_clone_independence() {
    fn source() -> IoBuff {
        let mut buf = IoBuffMut::new(4, 8, 0);
        buf.payload_append(b"abcdef").unwrap();
        buf.headroom_prepend(b"WXYZ").unwrap();
        buf.advance(6).unwrap();
        buf.freeze()
    }

    let mut unique = source().try_mut().expect("sole frozen buffer must thaw");
    let frozen = source();
    let alias = frozen.clone();
    let mut shared = frozen.make_mut().expect("shared COW allocation failed");

    for buf in [&mut unique, &mut shared] {
        assert_eq!(buf.payload_bytes(), b"cdef");
        assert_eq!(buf.payload_remaining(), 2);
        assert_eq!(
            buf.payload_set_len(5),
            Err(IoBuffError::PayloadUninitialized)
        );
        buf.payload_append(b"!").unwrap();
        assert_eq!(buf.payload_bytes(), b"cdef!");
    }

    assert_eq!(alias.bytes(), b"cdef");
    assert_eq!(unique.bytes(), shared.bytes());
}

#[test]
fn buffer_view_make_mut_is_tight_payload_only() {
    let mut buf = IoBuffMut::new(4, 64, 4);
    buf.payload_append(b"payload").unwrap();
    buf.headroom_prepend(b"H:").unwrap();
    buf.tailroom_append(b":T").unwrap();
    let frozen = buf.freeze();
    let view = expect_view(frozen.slice(..));

    let copied = view.make_mut().unwrap();
    assert_eq!(copied.bytes(), b"H:payload:T");
    assert_eq!(copied.payload_bytes(), b"H:payload:T");
    assert_eq!(copied.headroom_capacity(), 0);
    assert_eq!(copied.tailroom_capacity(), 0);
}

// ============================================================================
// Trait integration — IoBuffReadOnly / IoBuffReadWrite
// ============================================================================

#[test]
fn buffer_trait_read_only_on_iobuff_mut() {
    println!("--- IoBuffReadOnly on IoBuffMut ---");
    let mut buf = IoBuffMut::new(4, 64, 4);
    buf.payload_append(b"payload").unwrap();
    buf.headroom_prepend(b"H:").unwrap();
    buf.tailroom_append(b":T").unwrap();

    let ptr = IoBuffReadOnly::as_ptr(&buf);
    let len = IoBuffReadOnly::len(&buf);
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    println!("  Trait as_ptr/len: {:?}", slice);
    assert_eq!(slice, b"H:payload:T");
}

#[test]
fn buffer_trait_read_write_on_iobuff_mut() {
    println!("--- IoBuffReadWrite on IoBuffMut ---");
    let mut buf = IoBuffMut::new(0, 64, 0);

    let writable_ptr = IoBuffReadWrite::as_mut_ptr(&mut buf);
    let writable_len = IoBuffReadWrite::writable_len(&buf);
    let write_base_len = IoBuffReadWrite::write_base_len(&buf);
    println!(
        "  writable_ptr: {:?}, writable_len: {}",
        writable_ptr, writable_len
    );
    assert_eq!(writable_len, 64);
    assert_eq!(write_base_len, 0);

    // Simulate kernel write
    unsafe {
        std::ptr::copy_nonoverlapping(b"kernel_data".as_ptr(), writable_ptr, 11);
        IoBuffReadWrite::set_written_len(&mut buf, 11);
    }
    println!(
        "  After set_written_len(11): payload={:?}",
        buf.payload_bytes()
    );
    assert_eq!(buf.payload_bytes(), b"kernel_data");
    assert_eq!(buf.payload_len(), 11);
}

#[test]
fn buffer_trait_iobuff_write_base_tracks_payload_not_active_window() {
    let mut buf = IoBuffMut::new(4, 8, 0);
    buf.headroom_prepend(b"H:").unwrap();
    buf.payload_append(b"HEAD").unwrap();

    assert_eq!(IoBuffReadWrite::write_base_len(&buf), 4);
    assert_eq!(IoBuffReadWrite::writable_len(&buf), 4);
    assert_eq!(IoBuffReadOnly::len(&buf), 6);
}

#[test]
fn buffer_trait_read_only_on_iobuff() {
    println!("--- IoBuffReadOnly on IoBuff ---");
    let mut buf = IoBuffMut::new(0, 64, 0);
    buf.payload_append(b"frozen").unwrap();
    let frozen = buf.freeze();

    let ptr = IoBuffReadOnly::as_ptr(&frozen);
    let len = IoBuffReadOnly::len(&frozen);
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    println!("  Trait as_ptr/len: {:?}", slice);
    assert_eq!(slice, b"frozen");
}

// ============================================================================
// Edge cases
// ============================================================================

#[test]
fn buffer_mut_zero_capacity_all_regions() {
    println!("--- Zero capacity in all regions ---");
    let buf = IoBuffMut::new(0, 0, 0);
    assert_eq!(buf.len(), 0);
    assert!(buf.is_empty());
    assert_eq!(buf.payload_remaining(), 0);
    assert_eq!(buf.headroom_remaining(), 0);
    assert_eq!(buf.tailroom_remaining(), 0);
}

#[test]
fn buffer_mut_zero_payload_with_headroom_tailroom() {
    println!("--- Zero payload but non-zero headroom/tailroom ---");
    let mut buf = IoBuffMut::new(8, 0, 8);
    assert_eq!(buf.payload_remaining(), 0);

    // Cannot append payload
    let result = buf.payload_append(b"x");
    assert_eq!(result, Err(IoBuffError::PayloadFull));

    // But can prepend headroom and append tailroom
    buf.headroom_prepend(b"H").unwrap();
    buf.tailroom_append(b"T").unwrap();
    assert_eq!(buf.bytes(), b"HT");
}

#[test]
fn buffer_mut_large_buffer() {
    println!("--- Large buffer (1MB payload) ---");
    let mut buf = IoBuffMut::new(0, 1024 * 1024, 0);
    let data = vec![0xABu8; 1024 * 1024];
    buf.payload_append(&data).unwrap();
    assert_eq!(buf.payload_len(), 1024 * 1024);
    assert_eq!(buf.payload_bytes()[0], 0xAB);
    assert_eq!(buf.payload_bytes()[1024 * 1024 - 1], 0xAB);
    println!("  1MB buffer OK: payload_len={}", buf.payload_len());
}

#[test]
fn buffer_mut_advance_entire_active_window() {
    println!("--- Advance entire active window ---");
    let mut buf = IoBuffMut::new(4, 64, 0);
    buf.payload_append(b"data").unwrap();
    buf.headroom_prepend(b"H:").unwrap();

    let total = buf.len();
    buf.advance(total).unwrap();
    println!("  After advancing all {} bytes: len={}", total, buf.len());
    assert_eq!(buf.len(), 0);
    assert!(buf.is_empty());
}

#[test]
fn buffer_mut_payload_append_exact_fit() {
    println!("--- Payload append exact fit ---");
    let mut buf = IoBuffMut::new(0, 5, 0);
    buf.payload_append(b"exact").unwrap();
    assert_eq!(buf.payload_len(), 5);
    assert_eq!(buf.payload_remaining(), 0);
    assert_eq!(buf.payload_bytes(), b"exact");
    println!("  Exact fit OK");
}

#[test]
fn buffer_frozen_drop_all_clones() {
    println!("--- Drop all frozen clones (refcount reaches 0) ---");
    let mut buf = IoBuffMut::new(0, 64, 0);
    buf.payload_append(b"data").unwrap();
    let frozen = buf.freeze();
    let c1 = frozen.clone();
    let c2 = frozen.clone();
    let c3 = frozen.clone();

    println!("  Created 4 handles (1 original + 3 clones)");
    drop(c1);
    drop(c2);
    drop(c3);
    drop(frozen);
    println!("  All dropped — no leak, no double-free");
}

#[test]
fn buffer_mut_payload_unwritten_mut_partial_fill() {
    println!("--- Partial fill via payload_unwritten_mut ---");
    let mut buf = IoBuffMut::new(0, 64, 0);

    // First write 3 bytes via append
    buf.payload_append(b"abc").unwrap();

    // Then fill 4 more bytes directly
    let spare = buf.payload_unwritten_mut();
    println!("  Spare capacity after 3 bytes: {}", spare.len());
    initialize_spare_prefix(spare, b"defg");
    // SAFETY: the four newly published payload bytes were initialized above.
    unsafe { buf.payload_set_len_initialized(7).unwrap() };

    println!("  After partial fill: payload={:?}", buf.payload_bytes());
    assert_eq!(buf.payload_bytes(), b"abcdefg");
}

#[test]
fn buffer_frozen_empty_buffer() {
    println!("--- Freeze empty buffer ---");
    let buf = IoBuffMut::new(8, 64, 8);
    let frozen = buf.freeze();
    assert!(frozen.is_empty());
    assert_eq!(frozen.len(), 0);
    assert_eq!(frozen.bytes(), b"");
    println!("  Empty frozen buffer OK");
}

#[test]
fn buffer_mut_headroom_prepend_then_advance_restores() {
    println!("--- Prepend then advance (undo prepend) ---");
    let mut buf = IoBuffMut::new(8, 64, 0);
    buf.payload_append(b"data").unwrap();
    println!("  Before prepend: bytes={:?}", buf.bytes());

    buf.headroom_prepend(b"HDR:").unwrap();
    println!("  After prepend: bytes={:?}", buf.bytes());
    assert_eq!(buf.bytes(), b"HDR:data");

    buf.advance(4).unwrap();
    println!("  After advance(4): bytes={:?}", buf.bytes());
    assert_eq!(buf.bytes(), b"data");
    assert_eq!(buf.payload_len(), 4);
}

// ============================================================================
// Trait impls for standard types
// ============================================================================

#[test]
fn trait_vec_u8_read_only() {
    println!("--- IoBuffReadOnly for Vec<u8> ---");
    let v = vec![1u8, 2, 3, 4, 5];
    let ptr = IoBuffReadOnly::as_ptr(&v);
    let len = IoBuffReadOnly::len(&v);
    println!("  ptr={:?}, len={}", ptr, len);
    assert_eq!(len, 5);
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    assert_eq!(slice, &[1, 2, 3, 4, 5]);
}

#[test]
fn trait_vec_u8_read_write() {
    println!("--- IoBuffReadWrite for Vec<u8> ---");
    let mut v = Vec::with_capacity(64);
    let ptr = IoBuffReadWrite::as_mut_ptr(&mut v);
    let writable = IoBuffReadWrite::writable_len(&v);
    println!("  writable_len={}", writable);
    assert_eq!(writable, 64);

    // Simulate kernel write
    unsafe {
        std::ptr::copy_nonoverlapping(b"kernel".as_ptr(), ptr, 6);
        IoBuffReadWrite::set_written_len(&mut v, 6);
    }
    assert_eq!(&v[..], b"kernel");
    println!("  After set_written_len(6): {:?}", &v[..]);
}

#[test]
fn trait_vec_u8_read_write_prefilled_vec_is_fixed_scratch_from_zero() {
    let mut v = b"prefix".to_vec();
    v.reserve(8);

    let writable = IoBuffReadWrite::writable_len(&v);
    assert_eq!(writable, v.capacity());
    assert_eq!(IoBuffReadWrite::write_base_len(&v), 0);

    let ptr = IoBuffReadWrite::as_mut_ptr(&mut v);
    unsafe {
        std::ptr::copy_nonoverlapping(b"io".as_ptr(), ptr, 2);
        IoBuffReadWrite::set_written_len(&mut v, 2);
    }

    assert_eq!(&v[..], b"io");
}

#[test]
fn trait_box_slice_read_only() {
    println!("--- IoBuffReadOnly for Box<[u8]> ---");
    let b: Box<[u8]> = vec![10u8, 20, 30].into_boxed_slice();
    let ptr = IoBuffReadOnly::as_ptr(&b);
    let len = IoBuffReadOnly::len(&b);
    println!("  ptr={:?}, len={}", ptr, len);
    assert_eq!(len, 3);
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    assert_eq!(slice, &[10, 20, 30]);
}

#[test]
fn trait_static_slice_read_only() {
    println!("--- IoBuffReadOnly for &'static [u8] ---");
    let s: &'static [u8] = b"static data";
    let ptr = IoBuffReadOnly::as_ptr(&s);
    let len = IoBuffReadOnly::len(&s);
    println!("  ptr={:?}, len={}", ptr, len);
    assert_eq!(len, 11);
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    assert_eq!(slice, b"static data");
}

// ============================================================================
// Threading contract
// ============================================================================

#[test]
fn buffer_types_are_not_send() {
    assert_not_impl_any!(RealIoBuffMut: Send);
    assert_not_impl_any!(IoBuff: Send);
    assert_not_impl_any!(IoBuffView: Send);
    assert_not_impl_any!(IoBuffOwnedView: Send);
    assert_not_impl_any!(IoBuffVecMut<1>: Send);
    assert_not_impl_any!(IoBuffVec<1>: Send);
}

// ============================================================================
// IoBuffMut — simulated kernel read (transport pattern)
// ============================================================================

#[test]
fn buffer_mut_simulated_kernel_read() {
    println!("--- Simulated kernel read via IoBuffReadWrite trait ---");
    let mut buf = IoBuffMut::new(0, 4096, 0);

    // Step 1: runtime gets writable pointer and length
    let ptr = IoBuffReadWrite::as_mut_ptr(&mut buf);
    let writable = IoBuffReadWrite::writable_len(&buf);
    println!("  writable_ptr={:?}, writable_len={}", ptr, writable);
    assert_eq!(writable, 4096);

    // Step 2: simulate kernel writing 100 bytes
    unsafe {
        std::ptr::write_bytes(ptr, 0xAB, 100);
        IoBuffReadWrite::set_written_len(&mut buf, 100);
    }

    println!(
        "  After kernel write: payload_len={}, len={}",
        buf.payload_len(),
        buf.len()
    );
    assert_eq!(buf.payload_len(), 100);
    assert_eq!(buf.len(), 100);
    assert_eq!(buf.payload_bytes()[0], 0xAB);
    assert_eq!(buf.payload_bytes()[99], 0xAB);

    // Step 3: user prepends a protocol header
    buf.headroom_prepend(b"").unwrap(); // no-op, just verify it works with 0 headroom
    println!("  Protocol frame simulation: OK");
}

#[test]
fn buffer_mut_simulated_kernel_read_with_headroom() {
    println!("--- Simulated kernel read + headroom prepend ---");
    let mut buf = IoBuffMut::new(16, 4096, 0);

    // Kernel fills the payload region
    let ptr = IoBuffReadWrite::as_mut_ptr(&mut buf);
    unsafe {
        std::ptr::copy_nonoverlapping(b"response payload".as_ptr(), ptr, 16);
        IoBuffReadWrite::set_written_len(&mut buf, 16);
    }
    assert_eq!(buf.payload_bytes(), b"response payload");

    // Application prepends a protocol header
    buf.headroom_prepend(b"HDR:").unwrap();
    println!("  Full frame: {:?}", buf.bytes());
    assert_eq!(buf.bytes(), b"HDR:response payload");
    assert_eq!(buf.len(), 20);
    assert_eq!(buf.payload_len(), 16);
}

// ============================================================================
// IoBuffMut — reset after advance
// ============================================================================

#[test]
fn buffer_mut_reset_after_advance() {
    println!("--- Reset after advance ---");
    let mut buf = IoBuffMut::new(8, 64, 4);
    buf.payload_append(b"data").unwrap();
    buf.headroom_prepend(b"H:").unwrap();
    println!("  Before advance: bytes={:?}", buf.bytes());

    buf.advance(3).unwrap();
    println!(
        "  After advance(3): bytes={:?}, headroom_remaining={}",
        buf.bytes(),
        buf.headroom_remaining()
    );

    buf.reset();
    println!(
        "  After reset: len={}, headroom_remaining={}, payload_remaining={}",
        buf.len(),
        buf.headroom_remaining(),
        buf.payload_remaining()
    );
    assert_eq!(buf.len(), 0);
    assert_eq!(buf.headroom_remaining(), 8);
    assert_eq!(buf.payload_remaining(), 64);
    assert_eq!(buf.tailroom_remaining(), 4);
}

// ============================================================================
// IoBuff — multiple slice chains
// ============================================================================

#[test]
fn buffer_frozen_slice_of_slice() {
    println!("--- Slice of a slice ---");
    let mut buf = IoBuffMut::new(0, 64, 0);
    buf.payload_append(b"ABCDEFGHIJ").unwrap();
    let frozen = buf.freeze();

    let mid = expect_view(frozen.slice(2..8)); // "CDEFGH"
    let inner = expect_view(mid.slice(1..4)); // "DEF"
    println!("  frozen: {:?}", frozen.bytes());
    println!("  mid:    {:?}", mid.bytes());
    println!("  inner:  {:?}", inner.bytes());
    assert_eq!(inner.bytes(), b"DEF");

    // All share same backing storage
    assert!(frozen.bytes().as_ptr() < mid.bytes().as_ptr());
    assert!(
        mid.bytes().as_ptr() < inner.bytes().as_ptr()
            || mid.bytes().as_ptr() == inner.bytes().as_ptr()
    );
}

#[test]
fn buffer_frozen_many_clones_and_slices() {
    println!("--- Many clones and slices ---");
    let mut buf = IoBuffMut::new(0, 128, 0);
    buf.payload_append(b"the quick brown fox jumps over the lazy dog")
        .unwrap();
    let frozen = buf.freeze();

    let mut handles = Vec::new();
    let mut views = Vec::new();
    for i in 0..10 {
        handles.push(frozen.clone());
        if i < frozen.len() {
            views.push(expect_view(frozen.slice(i..frozen.len())));
        }
    }
    println!(
        "  Created {} handles and {} views",
        handles.len(),
        views.len()
    );
    assert!(handles.len() >= 10);
    assert!(views.len() >= 5);

    // Drop them all — refcount must reach zero
    drop(views);
    drop(handles);
    drop(frozen);
    println!("  All dropped: OK");
}
