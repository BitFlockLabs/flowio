//! Checked byte-order helpers for protocol and frame buffers.
//!
//! This module provides allocation-free, panic-free reads and writes for
//! primitive numeric values in byte slices. The core helpers operate on
//! `&[u8]` and `&mut [u8]`, so they work with stack arrays, `Vec<u8>`,
//! scratch buffers, FlowIO buffers, and non-FlowIO protocol code.
//!
//! Native-endian helpers without an endian suffix, such as [`read_u32_at`],
//! use the local machine byte order. They are intended only for local-memory
//! formats and must not be used for portable wire or file formats. Protocols
//! should use explicit little-endian (`_le`) or big-endian (`_be`) helpers.
//! Big-endian is the usual network byte order.
//!
//! For one-byte values (`u8` and `i8`), the `_le` and `_be` variants are
//! aliases for the native helper. They exist for API completeness.
//!
//! # Fast-Path Guidance
//!
//! These helpers are allocation-free and check bounds before every access. Use
//! the explicit `_be` / `_le` helpers in protocol parsers and encoders so wire
//! byte order is visible at the call site. Use [`BufferCursor`] and
//! [`BufferCursorMut`] for sequential frames when a cursor makes progress
//! tracking simpler than repeated offsets.
//!
//! Prefer not to use native-endian helpers for wire or file formats. Prefer
//! not to open-code indexing in protocol parsers when one of these checked
//! helpers expresses the same access.
//!
//! # Slice Example
//! ```
//! use flowio::runtime::buffer::bytes::{read_u32_le_at, write_u32_le_at};
//!
//! let mut frame = [0u8; 8];
//! write_u32_le_at(&mut frame, 2, 0x1122_3344).unwrap();
//! assert_eq!(&frame[2..6], &[0x44, 0x33, 0x22, 0x11]);
//! assert_eq!(read_u32_le_at(&frame, 2).unwrap(), 0x1122_3344);
//! ```
//!
//! # Cursor Example
//! ```
//! use flowio::runtime::buffer::bytes::{BufferCursor, BufferCursorMut};
//!
//! let mut frame = [0u8; 12];
//! let mut out = BufferCursorMut::new(&mut frame);
//! out.put_u32_be(0x1122_3344).unwrap();
//! out.put_u64_le(0x0102_0304_0506_0708).unwrap();
//!
//! let mut input = BufferCursor::new(&frame);
//! assert_eq!(input.get_u32_be().unwrap(), 0x1122_3344);
//! assert_eq!(input.get_u64_le().unwrap(), 0x0102_0304_0506_0708);
//! assert_eq!(input.remaining(), 0);
//! ```

use std::fmt;

/// Error returned when a checked byte read/write would exceed a buffer.
///
/// # Example
/// ```
/// use flowio::runtime::buffer::bytes::{BufferRangeError, ByteReadAt};
///
/// let err = [0u8; 2].read_u32_be_at(0).unwrap_err();
/// assert_eq!(
///     err,
///     BufferRangeError {
///         offset: 0,
///         width: 4,
///         len: 2,
///     }
/// );
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferRangeError {
    /// Requested byte offset.
    pub offset: usize,
    /// Number of bytes required by the operation.
    pub width: usize,
    /// Length of the source or destination buffer.
    pub len: usize,
}

impl BufferRangeError {
    #[inline]
    const fn new(offset: usize, width: usize, len: usize) -> Self {
        Self { offset, width, len }
    }
}

impl fmt::Display for BufferRangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "buffer range out of bounds: offset={}, width={}, len={}",
            self.offset, self.width, self.len
        )
    }
}

impl std::error::Error for BufferRangeError {}

#[inline]
fn checked_slice(src: &[u8], offset: usize, width: usize) -> Result<&[u8], BufferRangeError> {
    let err = BufferRangeError::new(offset, width, src.len());
    let end = offset.checked_add(width).ok_or(err)?;
    let bytes = src.get(offset..end).ok_or(err)?;
    if bytes.len() != width {
        return Err(err);
    }
    Ok(bytes)
}

#[inline]
fn checked_slice_mut(
    dst: &mut [u8],
    offset: usize,
    width: usize,
) -> Result<&mut [u8], BufferRangeError> {
    let len = dst.len();
    let err = BufferRangeError::new(offset, width, len);
    let end = offset.checked_add(width).ok_or(err)?;
    let bytes = dst.get_mut(offset..end).ok_or(err)?;
    if bytes.len() != width {
        return Err(err);
    }
    Ok(bytes)
}

#[inline]
fn read_array<const N: usize>(src: &[u8], offset: usize) -> Result<[u8; N], BufferRangeError> {
    let bytes = checked_slice(src, offset, N)?;
    let mut out = [0u8; N];
    out.copy_from_slice(bytes);
    Ok(out)
}

#[inline]
fn write_array<const N: usize>(
    dst: &mut [u8],
    offset: usize,
    bytes: [u8; N],
) -> Result<(), BufferRangeError> {
    checked_slice_mut(dst, offset, N)?.copy_from_slice(&bytes);
    Ok(())
}

macro_rules! define_byte_helpers {
    (
        $ty:ty,
        $read:ident, $read_le:ident, $read_be:ident,
        $write:ident, $write_le:ident, $write_be:ident
    ) => {
        #[doc = concat!("Reads a native-endian `", stringify!($ty), "` at `offset`.")]
        ///
        /// Native-endian helpers are for local-memory formats only. Use the
        /// explicit `_le` or `_be` variants for protocol wire/file formats.
        #[inline]
        pub fn $read(src: &[u8], offset: usize) -> Result<$ty, BufferRangeError> {
            Ok(<$ty>::from_ne_bytes(read_array::<
                { std::mem::size_of::<$ty>() },
            >(src, offset)?))
        }

        #[doc = concat!("Reads a little-endian `", stringify!($ty), "` at `offset`.")]
        #[inline]
        pub fn $read_le(src: &[u8], offset: usize) -> Result<$ty, BufferRangeError> {
            Ok(<$ty>::from_le_bytes(read_array::<
                { std::mem::size_of::<$ty>() },
            >(src, offset)?))
        }

        #[doc = concat!("Reads a big-endian `", stringify!($ty), "` at `offset`.")]
        ///
        /// Big-endian is the usual network byte order.
        #[inline]
        pub fn $read_be(src: &[u8], offset: usize) -> Result<$ty, BufferRangeError> {
            Ok(<$ty>::from_be_bytes(read_array::<
                { std::mem::size_of::<$ty>() },
            >(src, offset)?))
        }

        #[doc = concat!("Writes a native-endian `", stringify!($ty), "` at `offset`.")]
        ///
        /// Native-endian helpers are for local-memory formats only. Use the
        /// explicit `_le` or `_be` variants for protocol wire/file formats.
        #[inline]
        pub fn $write(dst: &mut [u8], offset: usize, value: $ty) -> Result<(), BufferRangeError> {
            write_array(dst, offset, value.to_ne_bytes())
        }

        #[doc = concat!("Writes a little-endian `", stringify!($ty), "` at `offset`.")]
        #[inline]
        pub fn $write_le(
            dst: &mut [u8],
            offset: usize,
            value: $ty,
        ) -> Result<(), BufferRangeError> {
            write_array(dst, offset, value.to_le_bytes())
        }

        #[doc = concat!("Writes a big-endian `", stringify!($ty), "` at `offset`.")]
        ///
        /// Big-endian is the usual network byte order.
        #[inline]
        pub fn $write_be(
            dst: &mut [u8],
            offset: usize,
            value: $ty,
        ) -> Result<(), BufferRangeError> {
            write_array(dst, offset, value.to_be_bytes())
        }
    };
}

define_byte_helpers!(
    u8,
    read_u8_at,
    read_u8_le_at,
    read_u8_be_at,
    write_u8_at,
    write_u8_le_at,
    write_u8_be_at
);
define_byte_helpers!(
    i8,
    read_i8_at,
    read_i8_le_at,
    read_i8_be_at,
    write_i8_at,
    write_i8_le_at,
    write_i8_be_at
);
define_byte_helpers!(
    u16,
    read_u16_at,
    read_u16_le_at,
    read_u16_be_at,
    write_u16_at,
    write_u16_le_at,
    write_u16_be_at
);
define_byte_helpers!(
    i16,
    read_i16_at,
    read_i16_le_at,
    read_i16_be_at,
    write_i16_at,
    write_i16_le_at,
    write_i16_be_at
);
define_byte_helpers!(
    u32,
    read_u32_at,
    read_u32_le_at,
    read_u32_be_at,
    write_u32_at,
    write_u32_le_at,
    write_u32_be_at
);
define_byte_helpers!(
    i32,
    read_i32_at,
    read_i32_le_at,
    read_i32_be_at,
    write_i32_at,
    write_i32_le_at,
    write_i32_be_at
);
define_byte_helpers!(
    u64,
    read_u64_at,
    read_u64_le_at,
    read_u64_be_at,
    write_u64_at,
    write_u64_le_at,
    write_u64_be_at
);
define_byte_helpers!(
    i64,
    read_i64_at,
    read_i64_le_at,
    read_i64_be_at,
    write_i64_at,
    write_i64_le_at,
    write_i64_be_at
);
define_byte_helpers!(
    u128,
    read_u128_at,
    read_u128_le_at,
    read_u128_be_at,
    write_u128_at,
    write_u128_le_at,
    write_u128_be_at
);
define_byte_helpers!(
    i128,
    read_i128_at,
    read_i128_le_at,
    read_i128_be_at,
    write_i128_at,
    write_i128_le_at,
    write_i128_be_at
);
define_byte_helpers!(
    f32,
    read_f32_at,
    read_f32_le_at,
    read_f32_be_at,
    write_f32_at,
    write_f32_le_at,
    write_f32_be_at
);
define_byte_helpers!(
    f64,
    read_f64_at,
    read_f64_le_at,
    read_f64_be_at,
    write_f64_at,
    write_f64_le_at,
    write_f64_be_at
);

macro_rules! cursor_getters {
    (
        $ty:ty,
        $get:ident, $get_le:ident, $get_be:ident,
        $read:ident, $read_le:ident, $read_be:ident
    ) => {
        #[doc = concat!("Reads a native-endian `", stringify!($ty), "` and advances the cursor.")]
        #[inline]
        pub fn $get(&mut self) -> Result<$ty, BufferRangeError> {
            let value = $read(self.src, self.position)?;
            self.advance(std::mem::size_of::<$ty>());
            Ok(value)
        }

        #[doc = concat!("Reads a little-endian `", stringify!($ty), "` and advances the cursor.")]
        #[inline]
        pub fn $get_le(&mut self) -> Result<$ty, BufferRangeError> {
            let value = $read_le(self.src, self.position)?;
            self.advance(std::mem::size_of::<$ty>());
            Ok(value)
        }

        #[doc = concat!("Reads a big-endian `", stringify!($ty), "` and advances the cursor.")]
        #[inline]
        pub fn $get_be(&mut self) -> Result<$ty, BufferRangeError> {
            let value = $read_be(self.src, self.position)?;
            self.advance(std::mem::size_of::<$ty>());
            Ok(value)
        }
    };
}

macro_rules! cursor_putters {
    (
        $ty:ty,
        $put:ident, $put_le:ident, $put_be:ident,
        $write:ident, $write_le:ident, $write_be:ident
    ) => {
        #[doc = concat!("Writes a native-endian `", stringify!($ty), "` and advances the cursor.")]
        #[inline]
        pub fn $put(&mut self, value: $ty) -> Result<(), BufferRangeError> {
            $write(self.dst, self.position, value)?;
            self.advance(std::mem::size_of::<$ty>());
            Ok(())
        }

        #[doc = concat!("Writes a little-endian `", stringify!($ty), "` and advances the cursor.")]
        #[inline]
        pub fn $put_le(&mut self, value: $ty) -> Result<(), BufferRangeError> {
            $write_le(self.dst, self.position, value)?;
            self.advance(std::mem::size_of::<$ty>());
            Ok(())
        }

        #[doc = concat!("Writes a big-endian `", stringify!($ty), "` and advances the cursor.")]
        #[inline]
        pub fn $put_be(&mut self, value: $ty) -> Result<(), BufferRangeError> {
            $write_be(self.dst, self.position, value)?;
            self.advance(std::mem::size_of::<$ty>());
            Ok(())
        }
    };
}

macro_rules! read_trait_methods {
    (
        $ty:ty,
        $read:ident, $read_le:ident, $read_be:ident
    ) => {
        #[doc = concat!("Reads a native-endian `", stringify!($ty), "` at `offset`.")]
        fn $read(&self, offset: usize) -> Result<$ty, BufferRangeError>;

        #[doc = concat!("Reads a little-endian `", stringify!($ty), "` at `offset`.")]
        fn $read_le(&self, offset: usize) -> Result<$ty, BufferRangeError>;

        #[doc = concat!("Reads a big-endian `", stringify!($ty), "` at `offset`.")]
        fn $read_be(&self, offset: usize) -> Result<$ty, BufferRangeError>;
    };
}

macro_rules! read_trait_impl {
    (
        $ty:ty,
        $method:ident, $method_le:ident, $method_be:ident,
        $read:ident, $read_le:ident, $read_be:ident
    ) => {
        #[inline]
        fn $method(&self, offset: usize) -> Result<$ty, BufferRangeError> {
            $read(self.as_ref(), offset)
        }

        #[inline]
        fn $method_le(&self, offset: usize) -> Result<$ty, BufferRangeError> {
            $read_le(self.as_ref(), offset)
        }

        #[inline]
        fn $method_be(&self, offset: usize) -> Result<$ty, BufferRangeError> {
            $read_be(self.as_ref(), offset)
        }
    };
}

macro_rules! write_trait_methods {
    (
        $ty:ty,
        $write:ident, $write_le:ident, $write_be:ident
    ) => {
        #[doc = concat!("Writes a native-endian `", stringify!($ty), "` at `offset`.")]
        fn $write(&mut self, offset: usize, value: $ty) -> Result<(), BufferRangeError>;

        #[doc = concat!("Writes a little-endian `", stringify!($ty), "` at `offset`.")]
        fn $write_le(&mut self, offset: usize, value: $ty) -> Result<(), BufferRangeError>;

        #[doc = concat!("Writes a big-endian `", stringify!($ty), "` at `offset`.")]
        fn $write_be(&mut self, offset: usize, value: $ty) -> Result<(), BufferRangeError>;
    };
}

macro_rules! write_trait_impl {
    (
        $ty:ty,
        $method:ident, $method_le:ident, $method_be:ident,
        $write:ident, $write_le:ident, $write_be:ident
    ) => {
        #[inline]
        fn $method(&mut self, offset: usize, value: $ty) -> Result<(), BufferRangeError> {
            $write(self.as_mut(), offset, value)
        }

        #[inline]
        fn $method_le(&mut self, offset: usize, value: $ty) -> Result<(), BufferRangeError> {
            $write_le(self.as_mut(), offset, value)
        }

        #[inline]
        fn $method_be(&mut self, offset: usize, value: $ty) -> Result<(), BufferRangeError> {
            $write_be(self.as_mut(), offset, value)
        }
    };
}

/// Sequential checked reader over a byte slice.
///
/// Reads advance the cursor only on success. Failed reads leave the position
/// unchanged.
///
/// # Example
/// ```
/// use flowio::runtime::buffer::bytes::BufferCursor;
///
/// let frame = [0x12, 0x34, 0x56];
/// let mut cursor = BufferCursor::new(&frame);
/// assert_eq!(cursor.get_u16_be().unwrap(), 0x1234);
/// assert_eq!(cursor.get_u8().unwrap(), 0x56);
/// assert_eq!(cursor.remaining(), 0);
/// ```
pub struct BufferCursor<'a> {
    /// Source bytes read by this cursor.
    src: &'a [u8],
    /// Current read offset into `src`.
    position: usize,
}

impl<'a> BufferCursor<'a> {
    /// Creates a cursor at position 0.
    #[inline]
    pub fn new(src: &'a [u8]) -> Self {
        Self { src, position: 0 }
    }

    /// Returns the current cursor position.
    #[inline]
    pub fn position(&self) -> usize {
        self.position
    }

    /// Returns the number of bytes remaining from the current position.
    #[inline]
    pub fn remaining(&self) -> usize {
        self.src.len() - self.position
    }

    /// Sets the cursor position.
    ///
    /// Returns an error and leaves the cursor unchanged if `position` is past
    /// the end of the slice. The returned [`BufferRangeError`] uses
    /// `width = 0`.
    #[inline]
    pub fn set_position(&mut self, position: usize) -> Result<(), BufferRangeError> {
        if position > self.src.len() {
            return Err(BufferRangeError::new(position, 0, self.src.len()));
        }
        self.position = position;
        Ok(())
    }

    /// Returns the original slice.
    #[inline]
    pub fn into_inner(self) -> &'a [u8] {
        self.src
    }

    #[inline]
    fn advance(&mut self, width: usize) {
        self.position += width;
    }

    cursor_getters!(
        u8,
        get_u8,
        get_u8_le,
        get_u8_be,
        read_u8_at,
        read_u8_le_at,
        read_u8_be_at
    );
    cursor_getters!(
        i8,
        get_i8,
        get_i8_le,
        get_i8_be,
        read_i8_at,
        read_i8_le_at,
        read_i8_be_at
    );
    cursor_getters!(
        u16,
        get_u16,
        get_u16_le,
        get_u16_be,
        read_u16_at,
        read_u16_le_at,
        read_u16_be_at
    );
    cursor_getters!(
        i16,
        get_i16,
        get_i16_le,
        get_i16_be,
        read_i16_at,
        read_i16_le_at,
        read_i16_be_at
    );
    cursor_getters!(
        u32,
        get_u32,
        get_u32_le,
        get_u32_be,
        read_u32_at,
        read_u32_le_at,
        read_u32_be_at
    );
    cursor_getters!(
        i32,
        get_i32,
        get_i32_le,
        get_i32_be,
        read_i32_at,
        read_i32_le_at,
        read_i32_be_at
    );
    cursor_getters!(
        u64,
        get_u64,
        get_u64_le,
        get_u64_be,
        read_u64_at,
        read_u64_le_at,
        read_u64_be_at
    );
    cursor_getters!(
        i64,
        get_i64,
        get_i64_le,
        get_i64_be,
        read_i64_at,
        read_i64_le_at,
        read_i64_be_at
    );
    cursor_getters!(
        u128,
        get_u128,
        get_u128_le,
        get_u128_be,
        read_u128_at,
        read_u128_le_at,
        read_u128_be_at
    );
    cursor_getters!(
        i128,
        get_i128,
        get_i128_le,
        get_i128_be,
        read_i128_at,
        read_i128_le_at,
        read_i128_be_at
    );
    cursor_getters!(
        f32,
        get_f32,
        get_f32_le,
        get_f32_be,
        read_f32_at,
        read_f32_le_at,
        read_f32_be_at
    );
    cursor_getters!(
        f64,
        get_f64,
        get_f64_le,
        get_f64_be,
        read_f64_at,
        read_f64_le_at,
        read_f64_be_at
    );
}

/// Sequential checked writer over a byte slice.
///
/// Writes advance the cursor only on success. Failed writes leave the
/// position and destination bytes unchanged.
///
/// # Example
/// ```
/// use flowio::runtime::buffer::bytes::BufferCursorMut;
///
/// let mut frame = [0u8; 3];
/// {
///     let mut cursor = BufferCursorMut::new(&mut frame);
///     cursor.put_u16_be(0x1234).unwrap();
///     cursor.put_u8(0x56).unwrap();
///     assert_eq!(cursor.position(), 3);
/// }
/// assert_eq!(frame, [0x12, 0x34, 0x56]);
/// ```
pub struct BufferCursorMut<'a> {
    /// Destination bytes written by this cursor.
    dst: &'a mut [u8],
    /// Current write offset into `dst`.
    position: usize,
}

impl<'a> BufferCursorMut<'a> {
    /// Creates a cursor at position 0.
    #[inline]
    pub fn new(dst: &'a mut [u8]) -> Self {
        Self { dst, position: 0 }
    }

    /// Returns the current cursor position.
    #[inline]
    pub fn position(&self) -> usize {
        self.position
    }

    /// Returns the number of bytes remaining from the current position.
    #[inline]
    pub fn remaining(&self) -> usize {
        self.dst.len() - self.position
    }

    /// Sets the cursor position.
    ///
    /// Returns an error and leaves the cursor unchanged if `position` is past
    /// the end of the slice. The returned [`BufferRangeError`] uses
    /// `width = 0`.
    #[inline]
    pub fn set_position(&mut self, position: usize) -> Result<(), BufferRangeError> {
        if position > self.dst.len() {
            return Err(BufferRangeError::new(position, 0, self.dst.len()));
        }
        self.position = position;
        Ok(())
    }

    /// Returns the original mutable slice.
    #[inline]
    pub fn into_inner(self) -> &'a mut [u8] {
        self.dst
    }

    #[inline]
    fn advance(&mut self, width: usize) {
        self.position += width;
    }

    cursor_putters!(
        u8,
        put_u8,
        put_u8_le,
        put_u8_be,
        write_u8_at,
        write_u8_le_at,
        write_u8_be_at
    );
    cursor_putters!(
        i8,
        put_i8,
        put_i8_le,
        put_i8_be,
        write_i8_at,
        write_i8_le_at,
        write_i8_be_at
    );
    cursor_putters!(
        u16,
        put_u16,
        put_u16_le,
        put_u16_be,
        write_u16_at,
        write_u16_le_at,
        write_u16_be_at
    );
    cursor_putters!(
        i16,
        put_i16,
        put_i16_le,
        put_i16_be,
        write_i16_at,
        write_i16_le_at,
        write_i16_be_at
    );
    cursor_putters!(
        u32,
        put_u32,
        put_u32_le,
        put_u32_be,
        write_u32_at,
        write_u32_le_at,
        write_u32_be_at
    );
    cursor_putters!(
        i32,
        put_i32,
        put_i32_le,
        put_i32_be,
        write_i32_at,
        write_i32_le_at,
        write_i32_be_at
    );
    cursor_putters!(
        u64,
        put_u64,
        put_u64_le,
        put_u64_be,
        write_u64_at,
        write_u64_le_at,
        write_u64_be_at
    );
    cursor_putters!(
        i64,
        put_i64,
        put_i64_le,
        put_i64_be,
        write_i64_at,
        write_i64_le_at,
        write_i64_be_at
    );
    cursor_putters!(
        u128,
        put_u128,
        put_u128_le,
        put_u128_be,
        write_u128_at,
        write_u128_le_at,
        write_u128_be_at
    );
    cursor_putters!(
        i128,
        put_i128,
        put_i128_le,
        put_i128_be,
        write_i128_at,
        write_i128_le_at,
        write_i128_be_at
    );
    cursor_putters!(
        f32,
        put_f32,
        put_f32_le,
        put_f32_be,
        write_f32_at,
        write_f32_le_at,
        write_f32_be_at
    );
    cursor_putters!(
        f64,
        put_f64,
        put_f64_le,
        put_f64_be,
        write_f64_at,
        write_f64_le_at,
        write_f64_be_at
    );
}

/// Checked read-at extension methods for byte-backed buffers.
///
/// This trait is blanket-implemented for every `T: AsRef<[u8]>`, including
/// slices, `Vec<u8>`, `Box<[u8]>`, `IoBuff`, `IoBuffMut`, `IoBuffView`, and
/// `IoBuffOwnedView`.
///
/// # Example
/// ```
/// use flowio::runtime::buffer::bytes::ByteReadAt;
///
/// let frame = [0x12, 0x34, 0x00, 0x2a];
/// assert_eq!(frame.read_u16_be_at(0).unwrap(), 0x1234);
/// assert_eq!(frame.read_u16_be_at(2).unwrap(), 42);
/// ```
pub trait ByteReadAt {
    read_trait_methods!(u8, read_u8_at, read_u8_le_at, read_u8_be_at);
    read_trait_methods!(i8, read_i8_at, read_i8_le_at, read_i8_be_at);
    read_trait_methods!(u16, read_u16_at, read_u16_le_at, read_u16_be_at);
    read_trait_methods!(i16, read_i16_at, read_i16_le_at, read_i16_be_at);
    read_trait_methods!(u32, read_u32_at, read_u32_le_at, read_u32_be_at);
    read_trait_methods!(i32, read_i32_at, read_i32_le_at, read_i32_be_at);
    read_trait_methods!(u64, read_u64_at, read_u64_le_at, read_u64_be_at);
    read_trait_methods!(i64, read_i64_at, read_i64_le_at, read_i64_be_at);
    read_trait_methods!(u128, read_u128_at, read_u128_le_at, read_u128_be_at);
    read_trait_methods!(i128, read_i128_at, read_i128_le_at, read_i128_be_at);
    read_trait_methods!(f32, read_f32_at, read_f32_le_at, read_f32_be_at);
    read_trait_methods!(f64, read_f64_at, read_f64_le_at, read_f64_be_at);
}

impl<T: AsRef<[u8]> + ?Sized> ByteReadAt for T {
    read_trait_impl!(
        u8,
        read_u8_at,
        read_u8_le_at,
        read_u8_be_at,
        read_u8_at,
        read_u8_le_at,
        read_u8_be_at
    );
    read_trait_impl!(
        i8,
        read_i8_at,
        read_i8_le_at,
        read_i8_be_at,
        read_i8_at,
        read_i8_le_at,
        read_i8_be_at
    );
    read_trait_impl!(
        u16,
        read_u16_at,
        read_u16_le_at,
        read_u16_be_at,
        read_u16_at,
        read_u16_le_at,
        read_u16_be_at
    );
    read_trait_impl!(
        i16,
        read_i16_at,
        read_i16_le_at,
        read_i16_be_at,
        read_i16_at,
        read_i16_le_at,
        read_i16_be_at
    );
    read_trait_impl!(
        u32,
        read_u32_at,
        read_u32_le_at,
        read_u32_be_at,
        read_u32_at,
        read_u32_le_at,
        read_u32_be_at
    );
    read_trait_impl!(
        i32,
        read_i32_at,
        read_i32_le_at,
        read_i32_be_at,
        read_i32_at,
        read_i32_le_at,
        read_i32_be_at
    );
    read_trait_impl!(
        u64,
        read_u64_at,
        read_u64_le_at,
        read_u64_be_at,
        read_u64_at,
        read_u64_le_at,
        read_u64_be_at
    );
    read_trait_impl!(
        i64,
        read_i64_at,
        read_i64_le_at,
        read_i64_be_at,
        read_i64_at,
        read_i64_le_at,
        read_i64_be_at
    );
    read_trait_impl!(
        u128,
        read_u128_at,
        read_u128_le_at,
        read_u128_be_at,
        read_u128_at,
        read_u128_le_at,
        read_u128_be_at
    );
    read_trait_impl!(
        i128,
        read_i128_at,
        read_i128_le_at,
        read_i128_be_at,
        read_i128_at,
        read_i128_le_at,
        read_i128_be_at
    );
    read_trait_impl!(
        f32,
        read_f32_at,
        read_f32_le_at,
        read_f32_be_at,
        read_f32_at,
        read_f32_le_at,
        read_f32_be_at
    );
    read_trait_impl!(
        f64,
        read_f64_at,
        read_f64_le_at,
        read_f64_be_at,
        read_f64_at,
        read_f64_le_at,
        read_f64_be_at
    );
}

/// Checked write-at extension methods for byte-backed mutable buffers.
///
/// This trait is blanket-implemented for every `T: AsMut<[u8]>`, including
/// mutable slices, `Vec<u8>`, `Box<[u8]>`, and `IoBuffMut`.
///
/// For `IoBuffMut`, these methods write into the active initialized byte
/// window exposed by `bytes_mut()` / `AsMut<[u8]>`. To encode into spare
/// unwritten payload capacity, write through `payload_unwritten_mut()` and
/// then commit the initialized length with `payload_set_len()`.
///
/// # Example
/// ```
/// use flowio::runtime::buffer::bytes::ByteWriteAt;
///
/// let mut frame = [0u8; 4];
/// frame.write_u16_be_at(0, 0x1234).unwrap();
/// frame.write_u16_be_at(2, 42).unwrap();
/// assert_eq!(frame, [0x12, 0x34, 0x00, 0x2a]);
/// ```
pub trait ByteWriteAt {
    write_trait_methods!(u8, write_u8_at, write_u8_le_at, write_u8_be_at);
    write_trait_methods!(i8, write_i8_at, write_i8_le_at, write_i8_be_at);
    write_trait_methods!(u16, write_u16_at, write_u16_le_at, write_u16_be_at);
    write_trait_methods!(i16, write_i16_at, write_i16_le_at, write_i16_be_at);
    write_trait_methods!(u32, write_u32_at, write_u32_le_at, write_u32_be_at);
    write_trait_methods!(i32, write_i32_at, write_i32_le_at, write_i32_be_at);
    write_trait_methods!(u64, write_u64_at, write_u64_le_at, write_u64_be_at);
    write_trait_methods!(i64, write_i64_at, write_i64_le_at, write_i64_be_at);
    write_trait_methods!(u128, write_u128_at, write_u128_le_at, write_u128_be_at);
    write_trait_methods!(i128, write_i128_at, write_i128_le_at, write_i128_be_at);
    write_trait_methods!(f32, write_f32_at, write_f32_le_at, write_f32_be_at);
    write_trait_methods!(f64, write_f64_at, write_f64_le_at, write_f64_be_at);
}

impl<T: AsMut<[u8]> + ?Sized> ByteWriteAt for T {
    write_trait_impl!(
        u8,
        write_u8_at,
        write_u8_le_at,
        write_u8_be_at,
        write_u8_at,
        write_u8_le_at,
        write_u8_be_at
    );
    write_trait_impl!(
        i8,
        write_i8_at,
        write_i8_le_at,
        write_i8_be_at,
        write_i8_at,
        write_i8_le_at,
        write_i8_be_at
    );
    write_trait_impl!(
        u16,
        write_u16_at,
        write_u16_le_at,
        write_u16_be_at,
        write_u16_at,
        write_u16_le_at,
        write_u16_be_at
    );
    write_trait_impl!(
        i16,
        write_i16_at,
        write_i16_le_at,
        write_i16_be_at,
        write_i16_at,
        write_i16_le_at,
        write_i16_be_at
    );
    write_trait_impl!(
        u32,
        write_u32_at,
        write_u32_le_at,
        write_u32_be_at,
        write_u32_at,
        write_u32_le_at,
        write_u32_be_at
    );
    write_trait_impl!(
        i32,
        write_i32_at,
        write_i32_le_at,
        write_i32_be_at,
        write_i32_at,
        write_i32_le_at,
        write_i32_be_at
    );
    write_trait_impl!(
        u64,
        write_u64_at,
        write_u64_le_at,
        write_u64_be_at,
        write_u64_at,
        write_u64_le_at,
        write_u64_be_at
    );
    write_trait_impl!(
        i64,
        write_i64_at,
        write_i64_le_at,
        write_i64_be_at,
        write_i64_at,
        write_i64_le_at,
        write_i64_be_at
    );
    write_trait_impl!(
        u128,
        write_u128_at,
        write_u128_le_at,
        write_u128_be_at,
        write_u128_at,
        write_u128_le_at,
        write_u128_be_at
    );
    write_trait_impl!(
        i128,
        write_i128_at,
        write_i128_le_at,
        write_i128_be_at,
        write_i128_at,
        write_i128_le_at,
        write_i128_be_at
    );
    write_trait_impl!(
        f32,
        write_f32_at,
        write_f32_le_at,
        write_f32_be_at,
        write_f32_at,
        write_f32_le_at,
        write_f32_be_at
    );
    write_trait_impl!(
        f64,
        write_f64_at,
        write_f64_le_at,
        write_f64_be_at,
        write_f64_at,
        write_f64_le_at,
        write_f64_be_at
    );
}
