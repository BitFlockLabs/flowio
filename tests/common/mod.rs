use flowio::net::{WritevPieces, WritevProjection};
use flowio::runtime::buffer::iobuffvec::{IoBuffReadOnlyVec, IoBuffVec, IoBuffVecMut};
use flowio::runtime::executor::Executor;
use std::future::Future;
use std::io;

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

/// Builds a generic read-only vectored chain from `Vec<u8>` payload segments.
#[allow(dead_code)]
pub fn make_read_only_chain<const N: usize>(segments: [&[u8]; N]) -> IoBuffReadOnlyVec<Vec<u8>, N> {
    let mut chain = IoBuffReadOnlyVec::<Vec<u8>, N>::new();
    for segment in segments {
        chain.push(segment.to_vec()).unwrap();
    }
    chain
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

/// Compact projected write source for stream integration tests.
#[allow(dead_code)]
pub struct TestProjected<const N: usize> {
    segments: [&'static [u8]; N],
}

impl<const N: usize> TestProjected<N> {
    #[allow(dead_code)]
    pub fn new(segments: [&'static [u8]; N]) -> Self {
        Self { segments }
    }

    #[allow(dead_code)]
    pub fn expected(&self) -> Vec<u8> {
        let mut expected = Vec::new();
        for segment in self.segments {
            expected.extend_from_slice(segment);
        }
        expected
    }
}

impl<const N: usize> WritevProjection for TestProjected<N> {
    fn writev_count_and_len(&self) -> (usize, usize) {
        let mut count = 0;
        let mut total = 0;
        for segment in self.segments {
            if !segment.is_empty() {
                count += 1;
                total += segment.len();
            }
        }
        (count, total)
    }

    fn project_writev<'a>(&'a self, pieces: &mut WritevPieces<'a>) -> io::Result<()> {
        for segment in self.segments {
            pieces.push(segment)?;
        }
        Ok(())
    }
}
