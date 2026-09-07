use flowio::runtime::buffer::IoBuffMut;
use flowio::runtime::buffer::bytes::*;

macro_rules! primitive_case {
    (
        $name:ident,
        $ty:ty,
        $value:expr,
        $read:ident, $write:ident
        $(,
            $le:expr, $be:expr,
            $read_le:ident, $read_be:ident,
            $write_le:ident, $write_be:ident
        )?
    ) => {
        #[test]
        fn $name() {
            let value: $ty = $value;
            let width = std::mem::size_of::<$ty>();
            let mut buf = [0xA5u8; 40];
            let offset = 3;

            $write(&mut buf, offset, value).expect("native write should fit");
            assert_eq!(&buf[offset..offset + width], &value.to_ne_bytes());
            assert_eq!($read(&buf, offset).expect("native read should fit"), value);

            $(
                let expected_le: &[u8] = &$le;
                let expected_be: &[u8] = &$be;
                assert_eq!(expected_le.len(), width);
                assert_eq!(expected_be.len(), width);
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
            )?
        }
    };
}

primitive_case!(primitive_u8_unsuffixed, u8, 0xABu8, read_u8_at, write_u8_at);

primitive_case!(primitive_i8_unsuffixed, i8, -7i8, read_i8_at, write_i8_at);

primitive_case!(
    primitive_u16_all_endian_families,
    u16,
    0x1234u16,
    read_u16_at,
    write_u16_at,
    [0x34, 0x12],
    [0x12, 0x34],
    read_u16_le_at,
    read_u16_be_at,
    write_u16_le_at,
    write_u16_be_at
);

primitive_case!(
    primitive_i16_all_endian_families,
    i16,
    i16::from_be_bytes([0x89, 0xAB]),
    read_i16_at,
    write_i16_at,
    [0xAB, 0x89],
    [0x89, 0xAB],
    read_i16_le_at,
    read_i16_be_at,
    write_i16_le_at,
    write_i16_be_at
);

primitive_case!(
    primitive_u32_all_endian_families,
    u32,
    0x1122_3344u32,
    read_u32_at,
    write_u32_at,
    [0x44, 0x33, 0x22, 0x11],
    [0x11, 0x22, 0x33, 0x44],
    read_u32_le_at,
    read_u32_be_at,
    write_u32_le_at,
    write_u32_be_at
);

primitive_case!(
    primitive_i32_all_endian_families,
    i32,
    i32::from_be_bytes([0x89, 0xAB, 0xCD, 0xEF]),
    read_i32_at,
    write_i32_at,
    [0xEF, 0xCD, 0xAB, 0x89],
    [0x89, 0xAB, 0xCD, 0xEF],
    read_i32_le_at,
    read_i32_be_at,
    write_i32_le_at,
    write_i32_be_at
);

primitive_case!(
    primitive_u64_all_endian_families,
    u64,
    0x1122_3344_5566_7788u64,
    read_u64_at,
    write_u64_at,
    [0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11],
    [0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88],
    read_u64_le_at,
    read_u64_be_at,
    write_u64_le_at,
    write_u64_be_at
);

primitive_case!(
    primitive_i64_all_endian_families,
    i64,
    i64::from_be_bytes([0x89, 0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67]),
    read_i64_at,
    write_i64_at,
    [0x67, 0x45, 0x23, 0x01, 0xEF, 0xCD, 0xAB, 0x89],
    [0x89, 0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67],
    read_i64_le_at,
    read_i64_be_at,
    write_i64_le_at,
    write_i64_be_at
);

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
    ($put:ident, $get:ident, $value:expr, $bytes:ident) => {{
        let value = $value;
        let width = std::mem::size_of_val(&value);
        let mut storage = [0xCCu8; 16];

        let mut out = BufferCursorMut::new(&mut storage[..width]);
        assert_eq!(out.position(), 0);
        assert_eq!(out.remaining(), width);
        out.$put(value).expect("cursor write should fit");
        assert_eq!(out.position(), width);
        assert_eq!(out.remaining(), 0);

        assert_eq!(&storage[..width], &value.$bytes());
        assert_eq!(&storage[width..], &[0xCCu8; 16][width..]);
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
        assert_eq!(out.position(), width);
        assert_eq!(out.remaining(), 0);
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
    cursor_method_case!(put_u8, get_u8, 0xA5u8, to_ne_bytes);
    cursor_method_case!(put_i8, get_i8, -7i8, to_ne_bytes);
    cursor_method_case!(put_u16, get_u16, 0x1234u16, to_ne_bytes);
    cursor_method_case!(put_u16_le, get_u16_le, 0x1234u16, to_le_bytes);
    cursor_method_case!(put_u16_be, get_u16_be, 0x1234u16, to_be_bytes);
    cursor_method_case!(put_i16, get_i16, -0x1234i16, to_ne_bytes);
    cursor_method_case!(put_i16_le, get_i16_le, -0x1234i16, to_le_bytes);
    cursor_method_case!(put_i16_be, get_i16_be, -0x1234i16, to_be_bytes);
    cursor_method_case!(put_u32, get_u32, 0x1122_3344u32, to_ne_bytes);
    cursor_method_case!(put_u32_le, get_u32_le, 0x1122_3344u32, to_le_bytes);
    cursor_method_case!(put_u32_be, get_u32_be, 0x1122_3344u32, to_be_bytes);
    cursor_method_case!(put_i32, get_i32, -0x1122_3344i32, to_ne_bytes);
    cursor_method_case!(put_i32_le, get_i32_le, -0x1122_3344i32, to_le_bytes);
    cursor_method_case!(put_i32_be, get_i32_be, -0x1122_3344i32, to_be_bytes);
    cursor_method_case!(put_u64, get_u64, 0x1122_3344_5566_7788u64, to_ne_bytes);
    cursor_method_case!(
        put_u64_le,
        get_u64_le,
        0x1122_3344_5566_7788u64,
        to_le_bytes
    );
    cursor_method_case!(
        put_u64_be,
        get_u64_be,
        0x1122_3344_5566_7788u64,
        to_be_bytes
    );
    cursor_method_case!(put_i64, get_i64, -0x0112_2334_4556_6778i64, to_ne_bytes);
    cursor_method_case!(
        put_i64_le,
        get_i64_le,
        -0x0112_2334_4556_6778i64,
        to_le_bytes
    );
    cursor_method_case!(
        put_i64_be,
        get_i64_be,
        -0x0112_2334_4556_6778i64,
        to_be_bytes
    );
}

#[test]
fn cursor_mixed_sequential_encode_decode_and_positioning() {
    let mut frame = [0u8; 32];
    let mut out = BufferCursorMut::new(&mut frame);
    out.put_u8(0xAB).expect("put_u8 failed");
    out.put_u16_be(0x1234).expect("put_u16_be failed");
    out.put_i32_le(-123456).expect("put_i32_le failed");
    out.put_u64_be(0x1122_3344_5566_7788)
        .expect("put_u64_be failed");
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

    assert_eq!(
        &frame[..15],
        &[
            0xAB, 0x12, 0x34, 0xC0, 0x1D, 0xFE, 0xFF, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
            0x88
        ]
    );
    assert_eq!(&frame[15..], &[0; 17]);
    let mut input = BufferCursor::new(&frame);
    assert_eq!(input.get_u8().expect("get_u8 failed"), 0xAB);
    assert_eq!(input.get_u16_be().expect("get_u16_be failed"), 0x1234);
    assert_eq!(input.get_i32_le().expect("get_i32_le failed"), -123456);
    assert_eq!(
        input.get_u64_be().expect("get_u64_be failed"),
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

macro_rules! extension_method_case {
    ($write:ident, $read:ident, $value:expr, $bytes:ident) => {{
        let value = $value;
        let expected = value.$bytes();
        let width = expected.len();
        let offset = 3;
        let mut raw = [0xA5u8; 16];
        raw.as_mut_slice()
            .$write(offset, value)
            .expect("slice extension write should fit");
        assert_eq!(&raw[offset..offset + width], &expected);
        assert_eq!(&raw[..offset], &[0xA5; 3]);
        assert_eq!(&raw[offset + width..], &[0xA5; 16][offset + width..]);
        assert_eq!(raw.as_slice().$read(offset), Ok(value));

        for (offset, len) in [(0, width - 1), (17 - width, 16), (usize::MAX, 16)] {
            let error = BufferRangeError { offset, width, len };
            let before = raw;
            assert_eq!($read(&raw[..len], offset), Err(error));
            assert_eq!($write(&mut raw[..len], offset, value), Err(error));
            assert_eq!(raw, before);
            assert_eq!(raw[..len].$read(offset), Err(error));
            assert_eq!(raw[..len].$write(offset, value), Err(error));
            assert_eq!(raw, before);
        }

        let mut buffer = IoBuffMut::new(2, 16, 2).expect("buffer allocation should succeed");
        buffer
            .payload_append(&[0xA5; 16])
            .expect("payload should fit");
        buffer.headroom_prepend(b"HH").expect("headroom should fit");
        buffer
            .$write(offset, value)
            .expect("active-window write should fit");
        assert_eq!(buffer.len(), 18);
        assert_eq!(buffer.$read(offset), Ok(value));
        assert_eq!(&buffer.bytes()[offset..offset + width], &expected);
        assert_eq!(&buffer.bytes()[..offset], &[b'H', b'H', 0xA5]);
        assert_eq!(
            &buffer.bytes()[offset + width..],
            &[0xA5; 18][offset + width..]
        );
        let before: [u8; 18] = buffer.bytes().try_into().expect("active length is 18");
        for bad_offset in [19 - width, usize::MAX] {
            let error = BufferRangeError {
                offset: bad_offset,
                width,
                len: 18,
            };
            assert_eq!(buffer.$read(bad_offset), Err(error));
            assert_eq!(buffer.$write(bad_offset, value), Err(error));
            assert_eq!(buffer.bytes(), before);
            assert_eq!(buffer.len(), 18);
        }
        let frozen = buffer.freeze();
        let pointer = frozen.bytes().as_ptr();
        assert_eq!(frozen.$read(offset), Ok(value));
        let view = frozen
            .slice(offset..offset + width)
            .expect("buffer view should fit");
        assert_eq!(view.$read(0), Ok(value));
        assert_eq!(view.bytes(), expected);
        assert_eq!(
            view.$read(1),
            Err(BufferRangeError {
                offset: 1,
                width,
                len: width
            })
        );
        let owned = frozen
            .clone()
            .into_owned_view(offset..offset + width)
            .expect("owned view should fit");
        assert_eq!(owned.$read(0), Ok(value));
        assert_eq!(owned.bytes(), expected);
        assert_eq!(
            owned.$read(1),
            Err(BufferRangeError {
                offset: 1,
                width,
                len: width
            })
        );
        drop(view);
        drop(owned);
        let recovered = frozen
            .try_mut()
            .expect("all views should release their owners");
        assert_eq!(recovered.bytes().as_ptr(), pointer);
        assert_eq!(recovered.bytes(), before);
    }};
}

#[test]
fn extension_methods_cover_all_retained_integer_operations() {
    extension_method_case!(write_u8_at, read_u8_at, 0xA5u8, to_ne_bytes);
    extension_method_case!(write_i8_at, read_i8_at, -7i8, to_ne_bytes);
    extension_method_case!(write_u16_at, read_u16_at, 0x1234u16, to_ne_bytes);
    extension_method_case!(write_u16_le_at, read_u16_le_at, 0x1234u16, to_le_bytes);
    extension_method_case!(write_u16_be_at, read_u16_be_at, 0x1234u16, to_be_bytes);
    extension_method_case!(write_i16_at, read_i16_at, -0x1234i16, to_ne_bytes);
    extension_method_case!(write_i16_le_at, read_i16_le_at, -0x1234i16, to_le_bytes);
    extension_method_case!(write_i16_be_at, read_i16_be_at, -0x1234i16, to_be_bytes);
    extension_method_case!(write_u32_at, read_u32_at, 0x1122_3344u32, to_ne_bytes);
    extension_method_case!(write_u32_le_at, read_u32_le_at, 0x1122_3344u32, to_le_bytes);
    extension_method_case!(write_u32_be_at, read_u32_be_at, 0x1122_3344u32, to_be_bytes);
    extension_method_case!(write_i32_at, read_i32_at, -0x1122_3344i32, to_ne_bytes);
    extension_method_case!(
        write_i32_le_at,
        read_i32_le_at,
        -0x1122_3344i32,
        to_le_bytes
    );
    extension_method_case!(
        write_i32_be_at,
        read_i32_be_at,
        -0x1122_3344i32,
        to_be_bytes
    );
    extension_method_case!(
        write_u64_at,
        read_u64_at,
        0x1122_3344_5566_7788u64,
        to_ne_bytes
    );
    extension_method_case!(
        write_u64_le_at,
        read_u64_le_at,
        0x1122_3344_5566_7788u64,
        to_le_bytes
    );
    extension_method_case!(
        write_u64_be_at,
        read_u64_be_at,
        0x1122_3344_5566_7788u64,
        to_be_bytes
    );
    extension_method_case!(
        write_i64_at,
        read_i64_at,
        -0x0112_2334_4556_6778i64,
        to_ne_bytes
    );
    extension_method_case!(
        write_i64_le_at,
        read_i64_le_at,
        -0x0112_2334_4556_6778i64,
        to_le_bytes
    );
    extension_method_case!(
        write_i64_be_at,
        read_i64_be_at,
        -0x0112_2334_4556_6778i64,
        to_be_bytes
    );
}
