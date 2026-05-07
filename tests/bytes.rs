use flowio::runtime::buffer::IoBuffMut;
use flowio::runtime::buffer::bytes::*;

macro_rules! primitive_case {
    (
        $name:ident,
        $ty:ty,
        $value:expr,
        $le:expr,
        $be:expr,
        $read:ident, $read_le:ident, $read_be:ident,
        $write:ident, $write_le:ident, $write_be:ident
    ) => {
        #[test]
        fn $name() {
            let value: $ty = $value;
            let expected_le: &[u8] = &$le;
            let expected_be: &[u8] = &$be;
            let width = expected_le.len();
            assert_eq!(expected_be.len(), width);

            let mut buf = [0xA5u8; 40];
            let offset = 3;

            $write(&mut buf, offset, value).expect("native write should fit");
            assert_eq!(&buf[offset..offset + width], &value.to_ne_bytes());
            assert_eq!($read(&buf, offset).expect("native read should fit"), value);

            $write_le(&mut buf, offset, value).expect("little-endian write should fit");
            assert_eq!(&buf[offset..offset + width], expected_le);
            assert_eq!(
                $read_le(&buf, offset).expect("little-endian read should fit"),
                value
            );

            $write_be(&mut buf, offset, value).expect("big-endian write should fit");
            assert_eq!(&buf[offset..offset + width], expected_be);
            assert_eq!(
                $read_be(&buf, offset).expect("big-endian read should fit"),
                value
            );
        }
    };
}

primitive_case!(
    primitive_u8_all_endian_families,
    u8,
    0xABu8,
    [0xAB],
    [0xAB],
    read_u8_at,
    read_u8_le_at,
    read_u8_be_at,
    write_u8_at,
    write_u8_le_at,
    write_u8_be_at
);

primitive_case!(
    primitive_i8_all_endian_families,
    i8,
    -7i8,
    [0xF9],
    [0xF9],
    read_i8_at,
    read_i8_le_at,
    read_i8_be_at,
    write_i8_at,
    write_i8_le_at,
    write_i8_be_at
);

primitive_case!(
    primitive_u16_all_endian_families,
    u16,
    0x1234u16,
    [0x34, 0x12],
    [0x12, 0x34],
    read_u16_at,
    read_u16_le_at,
    read_u16_be_at,
    write_u16_at,
    write_u16_le_at,
    write_u16_be_at
);

primitive_case!(
    primitive_i16_all_endian_families,
    i16,
    i16::from_be_bytes([0x89, 0xAB]),
    [0xAB, 0x89],
    [0x89, 0xAB],
    read_i16_at,
    read_i16_le_at,
    read_i16_be_at,
    write_i16_at,
    write_i16_le_at,
    write_i16_be_at
);

primitive_case!(
    primitive_u32_all_endian_families,
    u32,
    0x1122_3344u32,
    [0x44, 0x33, 0x22, 0x11],
    [0x11, 0x22, 0x33, 0x44],
    read_u32_at,
    read_u32_le_at,
    read_u32_be_at,
    write_u32_at,
    write_u32_le_at,
    write_u32_be_at
);

primitive_case!(
    primitive_i32_all_endian_families,
    i32,
    i32::from_be_bytes([0x89, 0xAB, 0xCD, 0xEF]),
    [0xEF, 0xCD, 0xAB, 0x89],
    [0x89, 0xAB, 0xCD, 0xEF],
    read_i32_at,
    read_i32_le_at,
    read_i32_be_at,
    write_i32_at,
    write_i32_le_at,
    write_i32_be_at
);

primitive_case!(
    primitive_u64_all_endian_families,
    u64,
    0x1122_3344_5566_7788u64,
    [0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11],
    [0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88],
    read_u64_at,
    read_u64_le_at,
    read_u64_be_at,
    write_u64_at,
    write_u64_le_at,
    write_u64_be_at
);

primitive_case!(
    primitive_i64_all_endian_families,
    i64,
    i64::from_be_bytes([0x89, 0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67]),
    [0x67, 0x45, 0x23, 0x01, 0xEF, 0xCD, 0xAB, 0x89],
    [0x89, 0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67],
    read_i64_at,
    read_i64_le_at,
    read_i64_be_at,
    write_i64_at,
    write_i64_le_at,
    write_i64_be_at
);

primitive_case!(
    primitive_u128_all_endian_families,
    u128,
    0x0011_2233_4455_6677_8899_AABB_CCDD_EEFFu128,
    [
        0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,
        0x00
    ],
    [
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
        0xFF
    ],
    read_u128_at,
    read_u128_le_at,
    read_u128_be_at,
    write_u128_at,
    write_u128_le_at,
    write_u128_be_at
);

primitive_case!(
    primitive_i128_all_endian_families,
    i128,
    i128::from_be_bytes([
        0x89, 0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x10, 0x32, 0x54, 0x76, 0x98, 0xBA, 0xDC,
        0xFE
    ]),
    [
        0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10, 0x67, 0x45, 0x23, 0x01, 0xEF, 0xCD, 0xAB,
        0x89
    ],
    [
        0x89, 0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x10, 0x32, 0x54, 0x76, 0x98, 0xBA, 0xDC,
        0xFE
    ],
    read_i128_at,
    read_i128_le_at,
    read_i128_be_at,
    write_i128_at,
    write_i128_le_at,
    write_i128_be_at
);

primitive_case!(
    primitive_f32_all_endian_families,
    f32,
    f32::from_bits(0x1122_3344),
    [0x44, 0x33, 0x22, 0x11],
    [0x11, 0x22, 0x33, 0x44],
    read_f32_at,
    read_f32_le_at,
    read_f32_be_at,
    write_f32_at,
    write_f32_le_at,
    write_f32_be_at
);

primitive_case!(
    primitive_f64_all_endian_families,
    f64,
    f64::from_bits(0x1122_3344_5566_7788),
    [0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11],
    [0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88],
    read_f64_at,
    read_f64_le_at,
    read_f64_be_at,
    write_f64_at,
    write_f64_le_at,
    write_f64_be_at
);

#[test]
fn one_byte_endian_variants_are_aliases() {
    let mut u = [0u8; 3];
    write_u8_at(&mut u, 0, 0xA5).expect("u8 native write failed");
    write_u8_le_at(&mut u, 1, 0xA5).expect("u8 little-endian write failed");
    write_u8_be_at(&mut u, 2, 0xA5).expect("u8 big-endian write failed");
    assert_eq!(u, [0xA5, 0xA5, 0xA5]);
    assert_eq!(read_u8_at(&u, 0), read_u8_le_at(&u, 1));
    assert_eq!(read_u8_at(&u, 0), read_u8_be_at(&u, 2));

    let mut i = [0u8; 3];
    write_i8_at(&mut i, 0, -7).expect("i8 native write failed");
    write_i8_le_at(&mut i, 1, -7).expect("i8 little-endian write failed");
    write_i8_be_at(&mut i, 2, -7).expect("i8 big-endian write failed");
    assert_eq!(i, [0xF9, 0xF9, 0xF9]);
    assert_eq!(read_i8_at(&i, 0), read_i8_le_at(&i, 1));
    assert_eq!(read_i8_at(&i, 0), read_i8_be_at(&i, 2));
}

#[test]
fn bounds_errors_are_checked_and_writes_are_atomic() {
    let mut exact = [0u8; 4];
    write_u32_le_at(&mut exact, 0, 0x1122_3344).expect("exact fit write failed");
    assert_eq!(read_u32_le_at(&exact, 0), Ok(0x1122_3344));

    let short = [0u8; 3];
    assert_eq!(
        read_u32_le_at(&short, 0),
        Err(BufferRangeError {
            offset: 0,
            width: 4,
            len: 3
        })
    );

    let mut dst = [0xCCu8; 3];
    assert_eq!(
        write_u32_le_at(&mut dst, 0, 0x1122_3344),
        Err(BufferRangeError {
            offset: 0,
            width: 4,
            len: 3
        })
    );
    assert_eq!(dst, [0xCC; 3]);

    assert_eq!(
        read_u16_at(&exact, usize::MAX),
        Err(BufferRangeError {
            offset: usize::MAX,
            width: 2,
            len: 4
        })
    );
    assert_eq!(
        write_u16_at(&mut exact, usize::MAX, 0xABCD),
        Err(BufferRangeError {
            offset: usize::MAX,
            width: 2,
            len: 4
        })
    );
}

macro_rules! cursor_method_case {
    ($put:ident, $get:ident, $value:expr) => {{
        let value = $value;
        let width = std::mem::size_of_val(&value);
        let mut storage = [0xCCu8; 16];

        let mut out = BufferCursorMut::new(&mut storage[..width]);
        assert_eq!(out.position(), 0);
        assert_eq!(out.remaining(), width);
        out.$put(value).expect("cursor write should fit");
        assert_eq!(out.position(), width);
        assert_eq!(out.remaining(), 0);

        let before = storage;
        let mut out = BufferCursorMut::new(&mut storage[..width]);
        out.set_position(width)
            .expect("cursor set_position to end should fit");
        assert_eq!(
            out.$put(value),
            Err(BufferRangeError {
                offset: width,
                width,
                len: width
            })
        );
        assert_eq!(storage, before);

        let mut input = BufferCursor::new(&storage[..width]);
        assert_eq!(input.$get().expect("cursor read should fit"), value);
        assert_eq!(input.position(), width);
        assert_eq!(input.remaining(), 0);
        assert_eq!(
            input.$get(),
            Err(BufferRangeError {
                offset: width,
                width,
                len: width
            })
        );
        assert_eq!(input.position(), width);
    }};
}

#[test]
fn cursor_methods_cover_all_primitives_and_endian_families() {
    cursor_method_case!(put_u8, get_u8, 0xA5u8);
    cursor_method_case!(put_u8_le, get_u8_le, 0xA5u8);
    cursor_method_case!(put_u8_be, get_u8_be, 0xA5u8);
    cursor_method_case!(put_i8, get_i8, -7i8);
    cursor_method_case!(put_i8_le, get_i8_le, -7i8);
    cursor_method_case!(put_i8_be, get_i8_be, -7i8);
    cursor_method_case!(put_u16, get_u16, 0x1234u16);
    cursor_method_case!(put_u16_le, get_u16_le, 0x1234u16);
    cursor_method_case!(put_u16_be, get_u16_be, 0x1234u16);
    cursor_method_case!(put_i16, get_i16, -0x1234i16);
    cursor_method_case!(put_i16_le, get_i16_le, -0x1234i16);
    cursor_method_case!(put_i16_be, get_i16_be, -0x1234i16);
    cursor_method_case!(put_u32, get_u32, 0x1122_3344u32);
    cursor_method_case!(put_u32_le, get_u32_le, 0x1122_3344u32);
    cursor_method_case!(put_u32_be, get_u32_be, 0x1122_3344u32);
    cursor_method_case!(put_i32, get_i32, -0x1122_3344i32);
    cursor_method_case!(put_i32_le, get_i32_le, -0x1122_3344i32);
    cursor_method_case!(put_i32_be, get_i32_be, -0x1122_3344i32);
    cursor_method_case!(put_u64, get_u64, 0x1122_3344_5566_7788u64);
    cursor_method_case!(put_u64_le, get_u64_le, 0x1122_3344_5566_7788u64);
    cursor_method_case!(put_u64_be, get_u64_be, 0x1122_3344_5566_7788u64);
    cursor_method_case!(put_i64, get_i64, -0x0112_2334_4556_6778i64);
    cursor_method_case!(put_i64_le, get_i64_le, -0x0112_2334_4556_6778i64);
    cursor_method_case!(put_i64_be, get_i64_be, -0x0112_2334_4556_6778i64);
    cursor_method_case!(
        put_u128,
        get_u128,
        0x0011_2233_4455_6677_8899_AABB_CCDD_EEFFu128
    );
    cursor_method_case!(
        put_u128_le,
        get_u128_le,
        0x0011_2233_4455_6677_8899_AABB_CCDD_EEFFu128
    );
    cursor_method_case!(
        put_u128_be,
        get_u128_be,
        0x0011_2233_4455_6677_8899_AABB_CCDD_EEFFu128
    );
    cursor_method_case!(put_i128, get_i128, -0x1122_3344_5566_7788i128);
    cursor_method_case!(put_i128_le, get_i128_le, -0x1122_3344_5566_7788i128);
    cursor_method_case!(put_i128_be, get_i128_be, -0x1122_3344_5566_7788i128);
    cursor_method_case!(put_f32, get_f32, f32::from_bits(0x1122_3344));
    cursor_method_case!(put_f32_le, get_f32_le, f32::from_bits(0x1122_3344));
    cursor_method_case!(put_f32_be, get_f32_be, f32::from_bits(0x1122_3344));
    cursor_method_case!(put_f64, get_f64, f64::from_bits(0x1122_3344_5566_7788));
    cursor_method_case!(
        put_f64_le,
        get_f64_le,
        f64::from_bits(0x1122_3344_5566_7788)
    );
    cursor_method_case!(
        put_f64_be,
        get_f64_be,
        f64::from_bits(0x1122_3344_5566_7788)
    );
}

#[test]
fn cursor_mixed_sequential_encode_decode_and_positioning() {
    let mut frame = [0u8; 32];
    let mut out = BufferCursorMut::new(&mut frame);
    out.put_u8(0xAB).expect("put_u8 failed");
    out.put_u16_be(0x1234).expect("put_u16_be failed");
    out.put_i32_le(-123456).expect("put_i32_le failed");
    out.put_f64_be(f64::from_bits(0x1122_3344_5566_7788))
        .expect("put_f64_be failed");
    assert_eq!(out.position(), 15);
    assert_eq!(out.remaining(), 17);

    assert_eq!(
        out.set_position(33),
        Err(BufferRangeError {
            offset: 33,
            width: 0,
            len: 32
        })
    );
    assert_eq!(out.position(), 15);

    let mut input = BufferCursor::new(&frame);
    assert_eq!(input.get_u8().expect("get_u8 failed"), 0xAB);
    assert_eq!(input.get_u16_be().expect("get_u16_be failed"), 0x1234);
    assert_eq!(input.get_i32_le().expect("get_i32_le failed"), -123456);
    assert_eq!(
        input.get_f64_be().expect("get_f64_be failed").to_bits(),
        0x1122_3344_5566_7788
    );
    assert_eq!(input.position(), 15);
    assert_eq!(input.remaining(), 17);

    assert_eq!(
        input.set_position(33),
        Err(BufferRangeError {
            offset: 33,
            width: 0,
            len: 32
        })
    );
    assert_eq!(input.position(), 15);
    assert_eq!(input.into_inner(), &frame);
}

#[test]
fn extension_traits_work_for_slices_and_flowio_buffers() {
    let mut raw = [0u8; 8];
    raw.write_u32_be_at(2, 0x1122_3344)
        .expect("slice extension write failed");
    assert_eq!(raw.read_u32_be_at(2), Ok(0x1122_3344));

    let mut buf = IoBuffMut::new(2, 8, 2).expect("IoBuffMut allocation failed");
    buf.payload_append(&[0u8; 8])
        .expect("payload append failed");
    buf.headroom_prepend(b"HH")
        .expect("headroom prepend failed");
    assert_eq!(buf.len(), 10);

    buf.write_u32_le_at(2, 0x1122_3344)
        .expect("IoBuffMut active-window write failed");
    assert_eq!(buf.read_u32_le_at(2), Ok(0x1122_3344));
    assert_eq!(&buf.bytes()[2..6], &[0x44, 0x33, 0x22, 0x11]);

    let frozen = buf.freeze();
    assert_eq!(frozen.read_u32_le_at(2), Ok(0x1122_3344));

    let view = frozen.slice(2..6).expect("IoBuffView slice failed");
    assert_eq!(view.read_u32_le_at(0), Ok(0x1122_3344));

    let owned = frozen
        .clone()
        .into_owned_view(2..6)
        .expect("IoBuffOwnedView construction failed");
    assert_eq!(owned.read_u32_le_at(0), Ok(0x1122_3344));

    assert_eq!(
        frozen.read_u64_le_at(4),
        Err(BufferRangeError {
            offset: 4,
            width: 8,
            len: 10
        })
    );
}
