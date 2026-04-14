/// Shared test-only helpers for `flowio` integration tests.
pub struct TestIoBuffMut;

impl TestIoBuffMut
{
    /// Creates a heap-backed `IoBuffMut` and unwraps the result for tests that
    /// intentionally exercise the infallible happy path.
    pub fn new(
        headroom: usize,
        payload: usize,
        tailroom: usize,
    ) -> flowio::runtime::buffer::IoBuffMut
    {
        flowio::runtime::buffer::IoBuffMut::new(headroom, payload, tailroom).unwrap()
    }
}
