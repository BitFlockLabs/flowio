use flowio::runtime::buffer::iobuffvec::{IoBuffVec, IoBuffVecMut};
use flowio::runtime::executor::Executor;
use std::future::Future;

/// Runs one integration-test future on a fresh executor.
#[allow(dead_code)]
pub fn run_test<F>(future: F)
where
    F: Future<Output = ()> + 'static,
{
    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor.run(future).expect("executor run failed");
}

/// Shared test-only helpers for `flowio` integration tests.
pub struct TestIoBuffMut;

impl TestIoBuffMut {
    /// Creates a heap-backed `IoBuffMut` and unwraps the result for tests that
    /// intentionally exercise the infallible happy path.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        headroom: usize,
        payload: usize,
        tailroom: usize,
    ) -> flowio::runtime::buffer::IoBuffMut {
        flowio::runtime::buffer::IoBuffMut::new(headroom, payload, tailroom).unwrap()
    }
}

/// Builds a frozen vectored chain from payload segments.
#[allow(dead_code)]
pub fn make_payload_chain<const N: usize>(segments: [&[u8]; N]) -> IoBuffVec<N> {
    let mut chain = IoBuffVecMut::<N>::new();
    for segment in segments {
        let mut buf = TestIoBuffMut::new(0, segment.len(), 0);
        buf.payload_append(segment).unwrap();
        chain.push(buf).unwrap();
    }
    chain.freeze()
}

/// Builds a writable vectored chain with one buffer per requested capacity.
#[allow(dead_code)]
pub fn make_read_chain<const N: usize>(capacities: [usize; N]) -> IoBuffVecMut<N> {
    let mut chain = IoBuffVecMut::<N>::new();
    for capacity in capacities {
        chain.push(TestIoBuffMut::new(0, capacity, 0)).unwrap();
    }
    chain
}
