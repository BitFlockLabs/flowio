use flowio::net::{WritevPieces, WritevProjection};
use flowio::runtime::buffer::iobuffvec::{IoBuffReadOnlyVec, IoBuffVec, IoBuffVecMut};
use flowio::runtime::buffer::pool::IoBuffPool;
use flowio::runtime::buffer::{IoBuffReadOnly, IoBuffReadWrite};
use flowio::runtime::executor::Executor;
use flowio::runtime::timer::sleep;
use std::cell::Cell;
use std::future::{Future, poll_fn};
use std::io;
use std::rc::Rc;
use std::task::Poll;
use std::time::Duration;

/// Runs one integration-test future on a fresh executor.
#[allow(dead_code)]
pub fn run_test<F>(future: F)
where
    F: Future<Output = ()> + 'static,
{
    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor.run(future).expect("executor run failed");
}

#[allow(dead_code)]
pub async fn assert_poll_after_ready_parks<F>(future: F)
where
    F: Future,
{
    let mut future = Box::pin(future);
    poll_fn(|cx| match future.as_mut().poll(cx) {
        Poll::Ready(_) => Poll::Ready(()),
        Poll::Pending => Poll::Pending,
    })
    .await;

    poll_fn(|cx| match future.as_mut().poll(cx) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(_) => panic!("rental future completed again after Ready"),
    })
    .await;
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

/// Fake read-only buffer reporting a length above `u32::MAX` to exercise
/// oversize-send rejection without allocating that much memory.
#[allow(dead_code)]
pub struct HugeReadOnly;

// SAFETY: this fixture is only used on validation paths that must reject the
// oversized length before submitting kernel I/O or dereferencing the pointer.
unsafe impl IoBuffReadOnly for HugeReadOnly {
    fn as_ptr(&self) -> *const u8 {
        b"x".as_ptr()
    }

    fn len(&self) -> usize {
        u32::MAX as usize + 1
    }
}

/// Drop-tracking read-only buffer used by retained-payload tests.
#[allow(dead_code)]
pub struct DropTrackedReadOnly {
    /// Payload bytes exposed through `IoBuffReadOnly`.
    bytes: Vec<u8>,
    /// Shared drop counter bumped exactly once by `Drop`.
    drops: Rc<Cell<usize>>,
}

impl DropTrackedReadOnly {
    #[allow(dead_code)]
    pub fn new(bytes: Vec<u8>, drops: &Rc<Cell<usize>>) -> Self {
        Self {
            bytes,
            drops: Rc::clone(drops),
        }
    }

    #[allow(dead_code)]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for DropTrackedReadOnly {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

// SAFETY: `bytes` is a heap-allocated Vec, so `as_ptr()` stays valid for
// `len()` bytes for the lifetime of the value and is not invalidated by moves.
unsafe impl IoBuffReadOnly for DropTrackedReadOnly {
    fn as_ptr(&self) -> *const u8 {
        self.bytes.as_ptr()
    }

    fn len(&self) -> usize {
        self.bytes.len()
    }
}

/// Drop-tracking writable buffer used by retained read tests.
#[allow(dead_code)]
pub struct DropTrackedReadWrite {
    /// Writable backing bytes.
    bytes: Vec<u8>,
    /// Shared drop counter bumped exactly once by `Drop`.
    drops: Rc<Cell<usize>>,
}

impl DropTrackedReadWrite {
    #[allow(dead_code)]
    pub fn new(bytes: Vec<u8>, drops: &Rc<Cell<usize>>) -> Self {
        Self {
            bytes,
            drops: Rc::clone(drops),
        }
    }

    #[allow(dead_code)]
    pub fn zeroed(len: usize, drops: &Rc<Cell<usize>>) -> Self {
        Self::new(vec![0; len], drops)
    }
}

impl Drop for DropTrackedReadWrite {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

// SAFETY: `bytes` is a heap-allocated Vec, so `as_mut_ptr()` stays valid and
// writable for `writable_len()` bytes across moves. The runtime only calls
// `set_written_len(len)` with `len <= writable_len()`, so `set_len` stays
// within the allocation.
unsafe impl IoBuffReadWrite for DropTrackedReadWrite {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.bytes.as_mut_ptr()
    }

    fn writable_len(&self) -> usize {
        self.bytes.capacity()
    }

    unsafe fn set_written_len(&mut self, len: usize) {
        unsafe { self.bytes.set_len(len) };
    }
}

/// Waits until the original CQE retires and frees a retained payload exactly
/// `expected` times, bounded to roughly 500 ms.
#[allow(dead_code)]
pub async fn wait_for_drop_count(drops: &Rc<Cell<usize>>, expected: usize) {
    for _ in 0..100 {
        if drops.get() == expected {
            return;
        }
        sleep(Duration::from_millis(5))
            .await
            .expect("drop wait sleep failed");
    }

    assert_eq!(
        drops.get(),
        expected,
        "retained payload was not dropped exactly once after CQE retirement"
    );
}

/// Waits until pool-backed retained buffers are released after their original
/// CQEs retire.
#[allow(dead_code)]
pub async fn wait_for_live_slots(pool: &IoBuffPool, expected: usize) {
    for _ in 0..100 {
        if pool.live_slots_for_test() == expected {
            return;
        }
        sleep(Duration::from_millis(5))
            .await
            .expect("pool live-slot wait sleep failed");
    }

    assert_eq!(
        pool.live_slots_for_test(),
        expected,
        "retained pool-backed buffers were not released after CQE retirement"
    );
}

/// Compact projected write source for stream integration tests.
#[allow(dead_code)]
pub struct TestProjected<const N: usize> {
    /// Per-iovec payload pieces in send order.
    segments: [&'static [u8]; N],
}

impl<const N: usize> TestProjected<N> {
    #[allow(dead_code)]
    pub fn new(segments: [&'static [u8]; N]) -> Self {
        Self { segments }
    }

    /// Flattened wire image used to assert received bytes.
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
