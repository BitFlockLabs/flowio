#![doc = include_str!("../README.md")]

pub mod net;
pub mod runtime;
#[cfg(any(test, feature = "test-support"))]
#[path = "utils/test_child.rs"]
mod test_child;
pub(crate) mod utils;

/// Internal parser entry points for the cargo-fuzz crate in `fuzz/` and the
/// fixture integration tests. Enabled by the dev-only `fuzzing` feature; not
/// part of the supported public API. Fixture observation hooks additionally
/// require `test-support`.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzzing;

/// Test-support-only re-exports of internal hooks. Enabled by the dev-only
/// `test-support` feature and used by the crate's integration tests and
/// benchmark harnesses; not part of the supported public API.
#[cfg(feature = "test-support")]
#[doc(hidden)]
#[path = "utils/test_support.rs"]
pub mod test_support;
