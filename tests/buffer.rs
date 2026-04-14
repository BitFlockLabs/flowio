mod common;

use common::TestIoBuffMut as IoBuffMut;
use flowio::runtime::buffer::{
    IoBuffError, IoBuffMut as RealIoBuffMut, IoBuffReadOnly, IoBuffReadWrite, IoBuffView,
};
use static_assertions::assert_not_impl_any;

fn expect_view(view: Result<IoBuffView, IoBuffError>) -> IoBuffView
{
    view.expect("valid IoBuff slice in test")
}

// ============================================================================
// IoBuffMut — basic construction and payload operations
// ============================================================================

#[test]
fn buffer_mut_new_flat()
{
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
fn buffer_mut_new_with_all_regions()
{
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
fn buffer_mut_new_layout_overflow_returns_error()
{
    let result = RealIoBuffMut::new(usize::MAX, 1, 0);
    assert!(matches!(result, Err(IoBuffError::LayoutOverflow)));
}

#[test]
fn buffer_mut_payload_append()
{
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
fn buffer_mut_payload_append_overflow_returns_error()
{
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
fn buffer_mut_payload_set_len()
{
    println!("--- Payload set_len (absolute) ---");
    let mut buf = IoBuffMut::new(0, 64, 0);

    // Write directly into the unwritten region
    let spare = buf.payload_unwritten_mut();
    spare[..6].copy_from_slice(b"direct");
    println!("  Wrote 'direct' into payload_unwritten_mut");

    buf.payload_set_len(6).unwrap();
    println!(
        "  After payload_set_len(6): payload={:?}",
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
fn buffer_mut_payload_set_len_overflow_returns_error()
{
    println!("--- Payload set_len overflow ---");
    let mut buf = IoBuffMut::new(0, 8, 0);
    let result = buf.payload_set_len(9);
    println!("  payload_set_len(9) on capacity=8: {:?}", result);
    assert_eq!(result, Err(IoBuffError::PayloadFull));
}

// ============================================================================
// IoBuffMut — headroom operations
// ============================================================================

#[test]
fn buffer_mut_headroom_prepend()
{
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
fn buffer_mut_headroom_prepend_multiple()
{
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
fn buffer_mut_headroom_prepend_overflow_returns_error()
{
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
fn buffer_mut_headroom_prepend_on_zero_headroom_returns_error()
{
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
fn buffer_mut_tailroom_append()
{
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
fn buffer_mut_tailroom_append_overflow_returns_error()
{
    println!("--- Tailroom append overflow ---");
    let mut buf = IoBuffMut::new(0, 64, 2);

    buf.tailroom_append(b"OK").unwrap();
    let result = buf.tailroom_append(b"x");
    println!("  Overflow result: {:?}", result);
    assert_eq!(result, Err(IoBuffError::TailroomFull));
}

#[test]
fn buffer_mut_tailroom_append_on_zero_tailroom_returns_error()
{
    println!("--- Append on zero tailroom ---");
    let mut buf = IoBuffMut::new(0, 64, 0);
    let result = buf.tailroom_append(b"x");
    println!("  Result: {:?}", result);
    assert_eq!(result, Err(IoBuffError::TailroomFull));
}

#[test]
fn buffer_mut_tailroom_seals_payload_growth()
{
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
fn buffer_mut_full_protocol_frame()
{
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
fn buffer_mut_payload_extend_from_tailroom()
{
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
fn buffer_mut_payload_extend_from_tailroom_overflow_returns_error()
{
    println!("--- Extend payload from tailroom overflow ---");
    let mut buf = IoBuffMut::new(0, 16, 8);
    let result = buf.payload_extend_from_tailroom(9);
    println!("  Extend(9) from tailroom(8): {:?}", result);
    assert_eq!(result, Err(IoBuffError::TailroomInsufficient));
}

#[test]
fn buffer_mut_payload_extend_from_tailroom_discards_tailroom_data()
{
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
fn buffer_mut_advance_payload()
{
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
fn buffer_mut_advance_through_headroom_into_payload()
{
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
fn buffer_mut_advance_overflow_returns_error()
{
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
fn buffer_mut_advance_into_tailroom()
{
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
fn buffer_mut_advance_entire_active_window_with_tailroom()
{
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
fn buffer_mut_advance_overflow_with_tailroom_leaves_state_unchanged()
{
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
fn buffer_mut_reset()
{
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

// ============================================================================
// IoBuffMut — payload_bytes_mut
// ============================================================================

#[test]
fn buffer_mut_payload_bytes_mut_modify_in_place()
{
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
fn buffer_mut_deref_returns_full_window()
{
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
fn buffer_freeze_zero_copy()
{
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
fn buffer_frozen_clone_shares_data()
{
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
fn buffer_frozen_slice()
{
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
fn buffer_frozen_slice_returns_view()
{
    let mut buf = IoBuffMut::new(0, 16, 0);
    buf.payload_append(b"abcdef").unwrap();
    let frozen = buf.freeze();
    let view: IoBuffView = expect_view(frozen.slice(1..4));
    assert_eq!(view.bytes(), b"bcd");
}

#[test]
fn buffer_frozen_slice_full_range()
{
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
fn buffer_frozen_slice_out_of_bounds_returns_error()
{
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
fn buffer_frozen_slice_empty()
{
    println!("--- Frozen slice empty range ---");
    let mut buf = IoBuffMut::new(0, 8, 0);
    buf.payload_append(b"abc").unwrap();
    let frozen = buf.freeze();

    let empty = expect_view(frozen.slice(1..1));
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
}

// ============================================================================
// IoBuff — try_mut / make_mut (copy-on-write)
// ============================================================================

#[test]
fn buffer_frozen_try_mut_sole_owner()
{
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
fn buffer_frozen_try_mut_sole_owner_with_headroom_and_tailroom()
{
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
fn buffer_frozen_try_mut_shared_fails()
{
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
fn buffer_frozen_make_mut_copies_when_shared()
{
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

    // Note: make_mut creates a tight copy (payload_capacity = len), so we
    // can't append without creating a new buffer.  The important invariant is
    // that the copy is independent from the clone.
    drop(exclusive);
    assert_eq!(clone.bytes(), b"original");
}

#[test]
fn buffer_frozen_make_mut_preserves_shape()
{
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
fn buffer_view_make_mut_is_tight_payload_only()
{
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
fn buffer_trait_read_only_on_iobuff_mut()
{
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
fn buffer_trait_read_write_on_iobuff_mut()
{
    println!("--- IoBuffReadWrite on IoBuffMut ---");
    let mut buf = IoBuffMut::new(0, 64, 0);

    let writable_ptr = IoBuffReadWrite::as_mut_ptr(&mut buf);
    let writable_len = IoBuffReadWrite::writable_len(&buf);
    println!(
        "  writable_ptr: {:?}, writable_len: {}",
        writable_ptr, writable_len
    );
    assert_eq!(writable_len, 64);

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
fn buffer_trait_read_only_on_iobuff()
{
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
fn buffer_mut_zero_capacity_all_regions()
{
    println!("--- Zero capacity in all regions ---");
    let buf = IoBuffMut::new(0, 0, 0);
    assert_eq!(buf.len(), 0);
    assert!(buf.is_empty());
    assert_eq!(buf.payload_remaining(), 0);
    assert_eq!(buf.headroom_remaining(), 0);
    assert_eq!(buf.tailroom_remaining(), 0);
}

#[test]
fn buffer_mut_zero_payload_with_headroom_tailroom()
{
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
fn buffer_mut_large_buffer()
{
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
fn buffer_mut_advance_entire_active_window()
{
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
fn buffer_mut_payload_append_exact_fit()
{
    println!("--- Payload append exact fit ---");
    let mut buf = IoBuffMut::new(0, 5, 0);
    buf.payload_append(b"exact").unwrap();
    assert_eq!(buf.payload_len(), 5);
    assert_eq!(buf.payload_remaining(), 0);
    assert_eq!(buf.payload_bytes(), b"exact");
    println!("  Exact fit OK");
}

#[test]
fn buffer_frozen_drop_all_clones()
{
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
fn buffer_mut_payload_unwritten_mut_partial_fill()
{
    println!("--- Partial fill via payload_unwritten_mut ---");
    let mut buf = IoBuffMut::new(0, 64, 0);

    // First write 3 bytes via append
    buf.payload_append(b"abc").unwrap();

    // Then fill 4 more bytes directly
    let spare = buf.payload_unwritten_mut();
    println!("  Spare capacity after 3 bytes: {}", spare.len());
    spare[..4].copy_from_slice(b"defg");
    buf.payload_set_len(7).unwrap();

    println!("  After partial fill: payload={:?}", buf.payload_bytes());
    assert_eq!(buf.payload_bytes(), b"abcdefg");
}

#[test]
fn buffer_frozen_empty_buffer()
{
    println!("--- Freeze empty buffer ---");
    let buf = IoBuffMut::new(8, 64, 8);
    let frozen = buf.freeze();
    assert!(frozen.is_empty());
    assert_eq!(frozen.len(), 0);
    assert_eq!(frozen.bytes(), b"");
    println!("  Empty frozen buffer OK");
}

#[test]
fn buffer_mut_headroom_prepend_then_advance_restores()
{
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
fn trait_vec_u8_read_only()
{
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
fn trait_vec_u8_read_write()
{
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
fn trait_box_slice_read_only()
{
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
fn trait_static_slice_read_only()
{
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
fn buffer_types_are_not_send()
{
    assert_not_impl_any!(RealIoBuffMut: Send);
    assert_not_impl_any!(flowio::runtime::buffer::IoBuff: Send);
}

// ============================================================================
// IoBuffMut — simulated kernel read (transport pattern)
// ============================================================================

#[test]
fn buffer_mut_simulated_kernel_read()
{
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
fn buffer_mut_simulated_kernel_read_with_headroom()
{
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
fn buffer_mut_reset_after_advance()
{
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
fn buffer_frozen_slice_of_slice()
{
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
fn buffer_frozen_many_clones_and_slices()
{
    println!("--- Many clones and slices ---");
    let mut buf = IoBuffMut::new(0, 128, 0);
    buf.payload_append(b"the quick brown fox jumps over the lazy dog")
        .unwrap();
    let frozen = buf.freeze();

    let mut handles = Vec::new();
    let mut views = Vec::new();
    for i in 0..10
    {
        handles.push(frozen.clone());
        if i < frozen.len()
        {
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
