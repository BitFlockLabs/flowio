#![doc = include_str!("../README.md")]

pub mod net;
pub mod runtime;
pub(crate) mod utils;

/// Fuzzing-only re-exports of internal parsers. Enabled by the dev-only
/// `fuzzing` feature and consumed exclusively by the out-of-source `fuzz/`
/// crate; not part of the supported public API.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzzing;

/// Test-support-only re-exports of internal hooks. Enabled by the dev-only
/// `test-support` feature and consumed by out-of-source tests and benchmarks;
/// not part of the supported public API.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod test_support;
