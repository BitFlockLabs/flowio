//! Internal utility modules shared by the runtime and transport layers.
//!
//! These modules are intentionally low-level:
//! - [`list`] contains intrusive linked-list primitives
//! - [`memory`] contains slab and pool allocation helpers

pub mod list;
pub mod memory;

/// Error returned when `size_up` cannot round a size to the requested
/// alignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SizeUpError {
    /// The requested alignment was not a non-zero power of two.
    InvalidAlign,
    /// Rounding the requested size overflowed `usize`.
    SizeOverflow,
}

impl std::fmt::Display for SizeUpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidAlign => f.write_str("alignment must be a non-zero power of two"),
            Self::SizeOverflow => f.write_str("rounded size overflowed usize"),
        }
    }
}

impl std::error::Error for SizeUpError {}

#[inline(always)]
const fn is_pow2(x: usize) -> bool {
    x != 0 && (x & (x - 1)) == 0
}

#[inline(always)]
/// Rounds `addr` up to the next `align` boundary.
fn align_up(addr: usize, align: usize) -> Result<usize, SizeUpError> {
    if !is_pow2(align) {
        return Err(SizeUpError::InvalidAlign);
    }

    let rounded = addr
        .checked_add(align - 1)
        .ok_or(SizeUpError::SizeOverflow)?;
    Ok(rounded & !(align - 1))
}

#[inline(always)]
/// Rounds an object size up to a caller-specified alignment.
pub(crate) fn size_up(size: usize, align: usize) -> Result<usize, SizeUpError> {
    align_up(size, align)
}
