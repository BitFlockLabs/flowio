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

/// Repeatedly invokes one nonblocking owned-buffer write until the socket
/// reports `WouldBlock`, retaining the source buffer between attempts.
#[allow(dead_code)]
pub fn fill_try_send_buffer(
    mut try_write: impl FnMut(Vec<u8>) -> (io::Result<usize>, Vec<u8>),
) -> (bool, Vec<u8>) {
    let mut payload = vec![0xA5; 1024 * 1024];
    let mut saw_partial = false;
    for _ in 0..256 {
        let requested = payload.len();
        let (res, returned) = try_write(payload);
        payload = returned;
        match res {
            Ok(n) if n == requested => {}
            Ok(n) => {
                assert!(n < requested, "short write must report partial progress");
                saw_partial = true;
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                return (saw_partial, payload);
            }
            Err(err) => panic!("try_write failed unexpectedly: {err}"),
        }
    }

    panic!("socket send buffer did not fill within bounded attempts");
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

/// Uninitialized writable buffer that records userspace initialization.
///
/// TLS tests use the counters to prove a userspace destination is initialized
/// once across repeated polls. Kernel-read tests prove the same hook remains
/// untouched when the kernel writes directly into raw capacity.
#[allow(dead_code)]
pub struct InitializationTrackedReadWrite {
    /// Writable allocation whose logical length starts at zero.
    bytes: Vec<u8>,
    /// Number of userspace-initialization hook calls.
    initialization_calls: Rc<Cell<usize>>,
    /// Total byte count requested through the initialization hook.
    initialized_bytes: Rc<Cell<usize>>,
}

impl InitializationTrackedReadWrite {
    #[allow(dead_code)]
    pub fn new(
        capacity: usize,
        initialization_calls: &Rc<Cell<usize>>,
        initialized_bytes: &Rc<Cell<usize>>,
    ) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity),
            initialization_calls: Rc::clone(initialization_calls),
            initialized_bytes: Rc::clone(initialized_bytes),
        }
    }

    #[allow(dead_code)]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

// SAFETY: `bytes` owns pointer-stable writable capacity across moves. Both
// publication and userspace initialization clamp defensively to that capacity;
// valid runtime callers already report a bounded length.
unsafe impl IoBuffReadWrite for InitializationTrackedReadWrite {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.bytes.as_mut_ptr()
    }

    fn writable_len(&self) -> usize {
        self.bytes.capacity()
    }

    unsafe fn initialized_writable_slice(&mut self, len: usize) -> &mut [u8] {
        let len = len.min(self.bytes.capacity());
        self.initialization_calls
            .set(self.initialization_calls.get() + 1);
        self.initialized_bytes
            .set(self.initialized_bytes.get() + len);
        let ptr = self.bytes.as_mut_ptr();
        unsafe {
            std::ptr::write_bytes(ptr, 0, len);
            std::slice::from_raw_parts_mut(ptr, len)
        }
    }

    unsafe fn set_written_len(&mut self, len: usize) {
        let len = len.min(self.bytes.capacity());
        unsafe { self.bytes.set_len(len) };
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

/// Declared-empty projection fixture with selectable validation behavior.
#[allow(dead_code)]
pub struct EmptyProjected {
    behavior: EmptyProjectionBehavior,
    projection_calls: Cell<usize>,
}

#[derive(Clone, Copy)]
enum EmptyProjectionBehavior {
    Valid,
    StaleNonempty,
    Error,
}

impl EmptyProjected {
    #[allow(dead_code)]
    pub fn valid() -> Self {
        Self::new(EmptyProjectionBehavior::Valid)
    }

    #[allow(dead_code)]
    pub fn stale_nonempty() -> Self {
        Self::new(EmptyProjectionBehavior::StaleNonempty)
    }

    #[allow(dead_code)]
    pub fn failing() -> Self {
        Self::new(EmptyProjectionBehavior::Error)
    }

    fn new(behavior: EmptyProjectionBehavior) -> Self {
        Self {
            behavior,
            projection_calls: Cell::new(0),
        }
    }

    #[allow(dead_code)]
    pub fn projection_calls(&self) -> usize {
        self.projection_calls.get()
    }
}

impl WritevProjection for EmptyProjected {
    fn writev_count_and_len(&self) -> (usize, usize) {
        (0, 0)
    }

    fn project_writev<'a>(&'a self, pieces: &mut WritevPieces<'a>) -> io::Result<()> {
        self.projection_calls
            .set(self.projection_calls.get().saturating_add(1));
        match self.behavior {
            EmptyProjectionBehavior::Valid => pieces.push(b""),
            EmptyProjectionBehavior::StaleNonempty => pieces.push(b"stale"),
            EmptyProjectionBehavior::Error => Err(io::Error::from(io::ErrorKind::PermissionDenied)),
        }
    }
}

/// Asserts one declared-empty projection result and exact source return.
#[allow(dead_code)]
pub fn assert_empty_projection(
    output: (io::Result<usize>, EmptyProjected),
    expected_error: Option<io::ErrorKind>,
) {
    let (result, source) = output;
    match expected_error {
        Some(kind) => assert_eq!(
            result
                .expect_err("malformed empty projection should fail")
                .kind(),
            kind
        ),
        None => assert_eq!(result.expect("valid empty projection should succeed"), 0),
    }
    assert_eq!(source.projection_calls(), 1);
}

/// Exercises declared-empty validation on the immediate projected-write API.
#[allow(unused_macros)]
macro_rules! assert_empty_projected_try_cases {
    ($stream:ident) => {{
        $crate::common::assert_empty_projection(
            $stream.try_writev_projected($crate::common::EmptyProjected::valid()),
            None,
        );
        $crate::common::assert_empty_projection(
            $stream.try_writev_projected($crate::common::EmptyProjected::stale_nonempty()),
            Some(std::io::ErrorKind::InvalidInput),
        );
        $crate::common::assert_empty_projection(
            $stream.try_writev_projected($crate::common::EmptyProjected::failing()),
            Some(std::io::ErrorKind::PermissionDenied),
        );
    }};
}
#[allow(unused_imports)]
pub(crate) use assert_empty_projected_try_cases;

/// Exercises declared-empty validation on one async projected-write API.
#[allow(unused_macros)]
macro_rules! assert_empty_projected_async_cases {
    ($stream:ident, $method:ident) => {{
        $crate::common::assert_empty_projection(
            $stream
                .$method($crate::common::EmptyProjected::valid())
                .await,
            None,
        );
        $crate::common::assert_empty_projection(
            $stream
                .$method($crate::common::EmptyProjected::stale_nonempty())
                .await,
            Some(std::io::ErrorKind::InvalidInput),
        );
        $crate::common::assert_empty_projection(
            $stream
                .$method($crate::common::EmptyProjected::failing())
                .await,
            Some(std::io::ErrorKind::PermissionDenied),
        );
    }};
}
#[allow(unused_imports)]
pub(crate) use assert_empty_projected_async_cases;

/// Projection fixture whose reported byte count disagrees with its pieces.
#[allow(dead_code)]
pub struct TryMismatchedProjected;

impl WritevProjection for TryMismatchedProjected {
    fn writev_count_and_len(&self) -> (usize, usize) {
        (1, 2)
    }

    fn project_writev<'a>(&'a self, pieces: &mut WritevPieces<'a>) -> io::Result<()> {
        pieces.push(b"x")
    }
}

/// Projection fixture whose reported piece count disagrees with its pieces.
#[allow(dead_code)]
pub struct TryCountMismatchedProjected;

impl WritevProjection for TryCountMismatchedProjected {
    fn writev_count_and_len(&self) -> (usize, usize) {
        (2, 2)
    }

    fn project_writev<'a>(&'a self, pieces: &mut WritevPieces<'a>) -> io::Result<()> {
        pieces.push(b"xx")
    }
}

/// Exercises piece-count and byte-total mismatch validation on one async API.
#[allow(unused_macros)]
macro_rules! assert_projected_async_mismatches {
    ($stream:ident, $method:ident) => {{
        let (result, source) = $stream
            .$method($crate::common::TryCountMismatchedProjected)
            .await;
        assert_eq!(
            result
                .expect_err("projected piece-count mismatch should fail")
                .kind(),
            std::io::ErrorKind::InvalidInput
        );
        let _source = source;

        let (result, source) = $stream
            .$method($crate::common::TryMismatchedProjected)
            .await;
        assert_eq!(
            result
                .expect_err("projected byte-total mismatch should fail")
                .kind(),
            std::io::ErrorKind::InvalidInput
        );
        let _source = source;
    }};
}
#[allow(unused_imports)]
pub(crate) use assert_projected_async_mismatches;

/// Projection fixture whose reported piece count exceeds FlowIO's iovec cap.
#[allow(dead_code)]
pub struct TryOversizedProjected;

impl WritevProjection for TryOversizedProjected {
    fn writev_count_and_len(&self) -> (usize, usize) {
        (1025, 1025)
    }

    fn project_writev<'a>(&'a self, _pieces: &mut WritevPieces<'a>) -> io::Result<()> {
        panic!("oversized try_writev_projected should fail before projection")
    }
}
