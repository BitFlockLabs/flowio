use flowio::net::{WritevPieces, WritevProjection};
use flowio::runtime::buffer::iobuffvec::{IoBuffReadOnlyVec, IoBuffVec, IoBuffVecMut};
use flowio::runtime::buffer::pool::IoBuffPool;
use flowio::runtime::buffer::{IoBuffReadOnly, IoBuffReadWrite};
use flowio::runtime::executor::Executor;
use flowio::runtime::timer::sleep;
use std::cell::Cell;
use std::future::{Future, poll_fn};
use std::io;
use std::os::fd::RawFd;
use std::process::{Command, Stdio};
use std::rc::Rc;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::Poll;
use std::time::{Duration, Instant};

/// Runs one integration-test future on a fresh executor.
#[allow(dead_code)]
pub fn run_test<F>(future: F)
where
    F: Future<Output = ()> + 'static,
{
    let mut executor = Executor::new().expect("failed to construct runtime executor");
    executor.run(future).expect("executor run failed");
}

/// Runs one integration-test future and returns its output after the executor
/// has fully drained.
#[allow(dead_code)]
pub fn run_test_output<F, T>(executor: &mut Executor, future: F) -> T
where
    F: Future<Output = T> + 'static,
    T: 'static,
{
    let output = Rc::new(Cell::new(None));
    let output_slot = Rc::clone(&output);
    executor
        .run(async move {
            output_slot.set(Some(future.await));
        })
        .expect("executor run failed");
    output
        .take()
        .expect("integration-test future did not return its output")
}

/// Polls one future exactly once, verifies that it submitted asynchronous work,
/// and then drops it while still pending.
#[allow(dead_code)]
pub async fn poll_once_pending<F: Future>(future: F) {
    let mut future = std::pin::pin!(future);
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

/// Returns whether one numeric descriptor currently names an open descriptor.
///
/// This is used only by isolated child-process ownership regressions, where no
/// sibling test can race descriptor creation with the observation.
#[allow(dead_code)]
pub fn raw_fd_is_open(fd: RawFd) -> bool {
    loop {
        // SAFETY: F_GETFD accepts any numeric descriptor and does not take
        // ownership of it.
        let rc = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if rc >= 0 {
            return true;
        }

        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        assert_eq!(
            err.raw_os_error(),
            Some(libc::EBADF),
            "fcntl(F_GETFD) failed unexpectedly for descriptor {fd}: {err}",
        );
        return false;
    }
}

/// Finds the lowest descriptor number a subsequent socket allocation must use.
///
/// Linux allocates the lowest available descriptor. Callers run in an exact
/// child test and create no descriptor between this probe and the socket under
/// test, making numeric reuse deterministic without mutating production code.
#[allow(dead_code)]
pub fn lowest_available_fd() -> RawFd {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is writable for the duration of getrlimit.
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };
    assert_eq!(
        rc,
        0,
        "getrlimit(RLIMIT_NOFILE) failed: {}",
        io::Error::last_os_error(),
    );
    let upper = if limit.rlim_cur == libc::RLIM_INFINITY {
        RawFd::MAX as libc::rlim_t
    } else {
        limit.rlim_cur.min(RawFd::MAX as libc::rlim_t)
    };

    for fd in 0..upper as RawFd {
        if !raw_fd_is_open(fd) {
            return fd;
        }
    }
    panic!("no descriptor is available below the process soft limit");
}

/// Default absolute deadline for one blocking standard-library TCP peer.
#[allow(dead_code)]
pub const TCP_PEER_TIMEOUT: Duration = Duration::from_secs(5);

/// Returns whether a trusted standard-library probe proved that IPv6 loopback
/// is unavailable on this host.
#[allow(dead_code)]
pub fn ipv6_loopback_capability_unavailable(err: &io::Error) -> bool {
    matches!(
        err.raw_os_error(),
        Some(libc::EAFNOSUPPORT) | Some(libc::EPFNOSUPPORT) | Some(libc::EADDRNOTAVAIL)
    )
}

/// One absolute deadline shared by a standard-library TCP peer's setup and I/O.
#[derive(Clone, Copy)]
#[allow(dead_code)]
pub struct TcpPeerDeadline {
    label: &'static str,
    expires_at: Instant,
}

#[allow(dead_code)]
impl TcpPeerDeadline {
    /// Starts the default bounded interval for a named TCP peer.
    pub fn new(label: &'static str) -> Self {
        Self::with_timeout(label, TCP_PEER_TIMEOUT)
    }

    /// Starts a caller-selected bounded interval for deterministic regressions.
    pub fn with_timeout(label: &'static str, timeout: Duration) -> Self {
        assert!(!timeout.is_zero(), "TCP peer timeout must be nonzero");
        Self {
            label,
            expires_at: Instant::now() + timeout,
        }
    }

    fn timeout_error(&self, operation: &str) -> io::Error {
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "bounded TCP peer '{}' exceeded its deadline during {operation}",
                self.label
            ),
        )
    }

    fn remaining(&self, operation: &str) -> io::Result<Duration> {
        self.expires_at
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| self.timeout_error(operation))
    }

    fn remaining_for_wait(&self) -> Duration {
        self.expires_at
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO)
    }

    /// Connects a standard TCP peer within the remaining absolute deadline.
    pub fn connect(&self, addr: std::net::SocketAddr) -> io::Result<BoundedTcpStream> {
        let remaining = self.remaining("connect")?;
        std::net::TcpStream::connect_timeout(&addr, remaining)
            .map(|stream| BoundedTcpStream::new(stream, *self))
            .map_err(|err| {
                if matches!(
                    err.kind(),
                    io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                ) {
                    self.timeout_error("connect")
                } else {
                    err
                }
            })
    }

    /// Accepts a standard TCP peer through bounded nonblocking polling.
    pub fn accept(
        &self,
        listener: &BoundedTcpListener,
    ) -> io::Result<(BoundedTcpStream, std::net::SocketAddr)> {
        listener.inner.set_nonblocking(true)?;
        let accepted = loop {
            if let Err(err) = self.remaining("accept") {
                break Err(err);
            }
            match listener.inner.accept() {
                Ok(pair) => break Ok(pair),
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    let remaining = match self.remaining("accept") {
                        Ok(remaining) => remaining,
                        Err(err) => break Err(err),
                    };
                    std::thread::sleep(remaining.min(Duration::from_millis(1)));
                }
                Err(err) => break Err(err),
            }
        };
        listener.inner.set_nonblocking(false)?;
        accepted.map(|(stream, addr)| (BoundedTcpStream::new(stream, *self), addr))
    }

    /// Receives a peer-coordination value within the remaining deadline.
    pub fn recv<T>(&self, receiver: &std::sync::mpsc::Receiver<T>) -> io::Result<T> {
        let remaining = self.remaining("channel receive")?;
        receiver.recv_timeout(remaining).map_err(|err| match err {
            std::sync::mpsc::RecvTimeoutError::Timeout => self.timeout_error("channel receive"),
            std::sync::mpsc::RecvTimeoutError::Disconnected => io::Error::new(
                io::ErrorKind::BrokenPipe,
                format!(
                    "bounded TCP peer '{}' disconnected during channel receive",
                    self.label
                ),
            ),
        })
    }
}

/// Standard TCP listener whose inner socket is only exposed to bounded helpers.
#[allow(dead_code)]
pub struct BoundedTcpListener {
    inner: std::net::TcpListener,
}

#[allow(dead_code)]
impl BoundedTcpListener {
    /// Binds a standard TCP listener for a bounded test peer.
    pub fn bind(addr: std::net::SocketAddr) -> io::Result<Self> {
        std::net::TcpListener::bind(addr).map(|inner| Self { inner })
    }

    /// Returns the listener's assigned local address.
    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.inner.local_addr()
    }
}

impl std::os::fd::AsRawFd for BoundedTcpListener {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        std::os::fd::AsRawFd::as_raw_fd(&self.inner)
    }
}

/// Standard TCP stream whose blocking operations share one absolute deadline.
#[allow(dead_code)]
pub struct BoundedTcpStream {
    inner: std::net::TcpStream,
    deadline: TcpPeerDeadline,
    nonblocking: bool,
    read_timeout_cap: Option<Duration>,
    write_timeout_cap: Option<Duration>,
}

#[allow(dead_code)]
impl BoundedTcpStream {
    fn new(inner: std::net::TcpStream, deadline: TcpPeerDeadline) -> Self {
        Self {
            inner,
            deadline,
            nonblocking: false,
            read_timeout_cap: None,
            write_timeout_cap: None,
        }
    }

    fn operation_timeout(&self, operation: &str, cap: Option<Duration>) -> io::Result<Duration> {
        let remaining = self.deadline.remaining(operation)?;
        Ok(cap.map_or(remaining, |cap| cap.min(remaining)))
    }

    fn read_once(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if !self.nonblocking {
            let timeout = self.operation_timeout("read", self.read_timeout_cap)?;
            self.inner.set_read_timeout(Some(timeout))?;
        }
        std::io::Read::read(&mut self.inner, buf).map_err(|err| {
            if !self.nonblocking
                && matches!(
                    err.kind(),
                    io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                )
            {
                self.deadline.timeout_error("read")
            } else {
                err
            }
        })
    }

    fn write_once(&mut self, buf: &[u8]) -> io::Result<usize> {
        if !self.nonblocking {
            let timeout = self.operation_timeout("write", self.write_timeout_cap)?;
            self.inner.set_write_timeout(Some(timeout))?;
        }
        std::io::Write::write(&mut self.inner, buf).map_err(|err| {
            if !self.nonblocking
                && matches!(
                    err.kind(),
                    io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                )
            {
                self.deadline.timeout_error("write")
            } else {
                err
            }
        })
    }

    /// Reads once while preserving nonblocking-probe behavior when requested.
    pub fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.read_once(buf)
    }

    /// Reads the complete buffer under this stream's one absolute deadline.
    pub fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        std::io::Read::read_exact(self, buf)
    }

    /// Writes once while preserving nonblocking-probe behavior when requested.
    pub fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_once(buf)
    }

    /// Writes the complete buffer under this stream's one absolute deadline.
    pub fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        std::io::Write::write_all(self, buf)
    }

    /// Applies a smaller read cap without weakening the absolute deadline.
    pub fn set_read_timeout(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        self.inner.set_read_timeout(timeout)?;
        self.read_timeout_cap = timeout;
        Ok(())
    }

    /// Applies a smaller write cap without weakening the absolute deadline.
    pub fn set_write_timeout(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        self.inner.set_write_timeout(timeout)?;
        self.write_timeout_cap = timeout;
        Ok(())
    }

    /// Switches between blocking bounded I/O and immediate nonblocking probes.
    pub fn set_nonblocking(&mut self, nonblocking: bool) -> io::Result<()> {
        self.inner.set_nonblocking(nonblocking)?;
        self.nonblocking = nonblocking;
        Ok(())
    }

    /// Shuts down part or all of the connected peer.
    pub fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()> {
        self.inner.shutdown(how)
    }

    /// Returns the peer stream's local address.
    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.inner.local_addr()
    }

    /// Returns the peer stream's remote address.
    pub fn peer_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.inner.peer_addr()
    }

    /// Transfers the standard stream out for an ownership-adoption test.
    pub fn into_inner(self) -> std::net::TcpStream {
        self.inner
    }
}

impl std::io::Read for BoundedTcpStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.read_once(buf)
    }
}

impl std::io::Write for BoundedTcpStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_once(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.nonblocking {
            let timeout = self.operation_timeout("flush", self.write_timeout_cap)?;
            self.inner.set_write_timeout(Some(timeout))?;
        }
        std::io::Write::flush(&mut self.inner)
    }
}

impl std::os::fd::AsRawFd for BoundedTcpStream {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        std::os::fd::AsRawFd::as_raw_fd(&self.inner)
    }
}

/// Connects one bounded standard TCP peer using the default deadline.
#[allow(dead_code)]
pub fn connect_bounded_tcp_peer(
    label: &'static str,
    addr: std::net::SocketAddr,
) -> io::Result<BoundedTcpStream> {
    TcpPeerDeadline::new(label).connect(addr)
}

/// Join handle for a bounded standard TCP peer thread.
#[allow(dead_code)]
pub struct BoundedTcpPeer<T> {
    label: &'static str,
    deadline: TcpPeerDeadline,
    outcome: std::sync::mpsc::Receiver<std::thread::Result<T>>,
    handle: Option<std::thread::JoinHandle<()>>,
}

#[allow(dead_code)]
impl<T> BoundedTcpPeer<T> {
    /// Waits only until the peer's absolute deadline, then propagates its
    /// original result or panic.
    pub fn finish(mut self) -> T {
        let outcome = match self.outcome.try_recv() {
            Ok(outcome) => outcome,
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                panic!(
                    "bounded TCP peer '{}' disconnected without an outcome",
                    self.label
                )
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => self
                .outcome
                .recv_timeout(self.deadline.remaining_for_wait())
                .unwrap_or_else(|err| {
                    panic!(
                        "bounded TCP peer '{}' did not finish before its deadline: {err}",
                        self.label
                    )
                }),
        };

        let handle = self
            .handle
            .take()
            .expect("bounded TCP peer handle was already consumed");
        while !handle.is_finished() {
            let remaining = self.deadline.remaining_for_wait();
            assert!(
                !remaining.is_zero(),
                "bounded TCP peer '{}' returned an outcome but did not exit before its deadline",
                self.label
            );
            std::thread::sleep(remaining.min(Duration::from_millis(1)));
        }
        if let Err(panic) = handle.join() {
            std::panic::resume_unwind(panic);
        }

        match outcome {
            Ok(output) => output,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }
}

/// Spawns a named standard TCP peer with one shared absolute deadline.
#[allow(dead_code)]
pub fn spawn_bounded_tcp_peer<F, T>(label: &'static str, peer: F) -> BoundedTcpPeer<T>
where
    F: FnOnce(TcpPeerDeadline) -> T + Send + 'static,
    T: Send + 'static,
{
    spawn_bounded_tcp_peer_with_timeout(label, TCP_PEER_TIMEOUT, peer)
}

/// Spawns a named peer with a caller-selected timeout for regression tests.
#[allow(dead_code)]
pub fn spawn_bounded_tcp_peer_with_timeout<F, T>(
    label: &'static str,
    timeout: Duration,
    peer: F,
) -> BoundedTcpPeer<T>
where
    F: FnOnce(TcpPeerDeadline) -> T + Send + 'static,
    T: Send + 'static,
{
    let deadline = TcpPeerDeadline::with_timeout(label, timeout);
    let (sender, outcome) = std::sync::mpsc::sync_channel(1);
    let handle = std::thread::spawn(move || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| peer(deadline)));
        let _ = sender.send(result);
    });
    BoundedTcpPeer {
        label,
        deadline,
        outcome,
        handle: Some(handle),
    }
}

/// Runs one exact integration test in a subprocess and kills/reaps it if the
/// bounded deadline expires.
#[allow(dead_code)]
pub fn run_exact_test_child_with_watchdog(test_name: &str, child_env: &str, timeout: Duration) {
    run_exact_test_child_with_watchdog_env(test_name, child_env, timeout, &[]);
}

/// Runs one exact integration test in a bounded subprocess with additional
/// environment entries applied only to that child.
#[allow(dead_code)]
pub fn run_exact_test_child_with_watchdog_env(
    test_name: &str,
    child_env: &str,
    timeout: Duration,
    extra_env: &[(&str, &str)],
) {
    let current_exe = std::env::current_exe().expect("current integration-test executable");
    let mut child = Command::new(current_exe)
        .args(["--exact", test_name, "--nocapture"])
        .env(child_env, "1")
        .envs(extra_env.iter().copied())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn integration-test watchdog child");
    let deadline = Instant::now() + timeout;

    loop {
        if child
            .try_wait()
            .expect("poll integration-test watchdog child")
            .is_some()
        {
            let output = child
                .wait_with_output()
                .expect("collect integration-test watchdog child");
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                output.status.success(),
                "watchdog child {test_name} failed: status={:?}, stdout={}, stderr={}",
                output.status,
                stdout,
                stderr
            );
            assert!(
                stdout.contains("1 passed;"),
                "watchdog child {test_name} did not execute exactly one test: stdout={stdout}, stderr={stderr}"
            );
            return;
        }

        if Instant::now() >= deadline {
            let _ = child.kill();
            let output = child
                .wait_with_output()
                .expect("reap timed-out integration-test watchdog child");
            panic!(
                "watchdog child {test_name} exceeded {timeout:?}; stdout={}, stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }

        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Enables a positive `SO_LINGER` interval on a socket for terminal-close
/// routing tests.
#[allow(dead_code)]
pub fn set_positive_linger(fd: std::os::fd::RawFd) {
    let linger = libc::linger {
        l_onoff: 1,
        l_linger: 1,
    };
    // SAFETY: `linger` is initialized and borrowed for the exact socket-option
    // byte count during this call.
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_LINGER,
            std::ptr::addr_of!(linger).cast(),
            std::mem::size_of::<libc::linger>() as libc::socklen_t,
        )
    };
    assert_eq!(rc, 0, "setsockopt(SO_LINGER) failed");
}

/// Enables nanosecond receive timestamps so tests can exercise ancillary
/// control handling on a public raw socket descriptor.
#[allow(dead_code)]
pub fn enable_socket_timestampns(fd: std::os::fd::RawFd) {
    let enabled: libc::c_int = 1;
    // SAFETY: `enabled` is initialized and borrowed for the exact socket-
    // option byte count during this call.
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_TIMESTAMPNS,
            std::ptr::addr_of!(enabled).cast(),
            std::mem::size_of_val(&enabled) as libc::socklen_t,
        )
    };
    assert_eq!(
        rc,
        0,
        "setsockopt(SO_TIMESTAMPNS) failed: {}",
        io::Error::last_os_error()
    );
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

/// Active-iovec count immediately above retained stream scratch capacity.
#[allow(dead_code)]
pub const OVERSIZED_VECTORED_IOVECS: usize = 1025;

/// Stack reserved by exact child processes that instantiate 1,025-entry
/// inline vectored futures.
#[allow(dead_code)]
pub const OVERSIZED_VECTORED_TEST_STACK_BYTES: &str = "33554432";

/// Exact allocation-free diagnostic for retained stream scratch overflow.
#[allow(dead_code)]
pub const OVERSIZED_VECTORED_ERROR: &str = "active iovec count exceeds retained scratch capacity";

/// Builds a genuine mutable chain with 1,025 active one-byte entries.
#[allow(dead_code)]
pub fn make_oversized_read_chain() -> IoBuffVecMut<OVERSIZED_VECTORED_IOVECS> {
    let mut chain = IoBuffVecMut::new();
    for _ in 0..OVERSIZED_VECTORED_IOVECS {
        chain
            .push(TestIoBuffMut::new(0, 1, 0))
            .expect("oversized read chain exceeded its const capacity");
    }
    chain
}

/// Builds a genuine frozen chain with 1,025 active one-byte entries.
#[allow(dead_code)]
pub fn make_oversized_write_chain() -> IoBuffVec<OVERSIZED_VECTORED_IOVECS> {
    let mut chain = IoBuffVecMut::new();
    for _ in 0..OVERSIZED_VECTORED_IOVECS {
        let mut segment = TestIoBuffMut::new(0, 1, 0);
        segment
            .payload_append(&[0x5A])
            .expect("oversized write segment initialization failed");
        chain
            .push(segment)
            .expect("oversized write chain exceeded its const capacity");
    }
    chain.freeze()
}

/// Returns stable endpoint pointers for an exact-owner mutable-chain oracle.
#[allow(dead_code)]
pub fn oversized_read_chain_endpoints(
    chain: &mut IoBuffVecMut<OVERSIZED_VECTORED_IOVECS>,
) -> (usize, usize) {
    let first = chain
        .get_mut(0)
        .expect("oversized read chain first segment missing")
        .as_mut_ptr() as usize;
    let last = chain
        .get_mut(OVERSIZED_VECTORED_IOVECS - 1)
        .expect("oversized read chain last segment missing")
        .as_mut_ptr() as usize;
    (first, last)
}

/// Returns stable endpoint pointers for an exact-owner frozen-chain oracle.
#[allow(dead_code)]
pub fn oversized_write_chain_endpoints(
    chain: &IoBuffVec<OVERSIZED_VECTORED_IOVECS>,
) -> (usize, usize) {
    let first = chain
        .get(0)
        .expect("oversized write chain first segment missing")
        .as_ptr() as usize;
    let last = chain
        .get(OVERSIZED_VECTORED_IOVECS - 1)
        .expect("oversized write chain last segment missing")
        .as_ptr() as usize;
    (first, last)
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

/// Exact readable length used by the valid sparse oversized-send fixture.
#[cfg(all(target_os = "linux", target_pointer_width = "64", not(miri)))]
const SPARSE_OVERSIZED_READ_ONLY_LEN: usize = u32::MAX as usize + 1;

/// Read-only sparse mapping larger than the io_uring 32-bit byte-count limit.
///
/// The mapping reserves virtual address space without reserving swap or
/// populating physical pages. Oversized-send tests must reject it before
/// consulting the pointer or submitting an SQE.
#[cfg(all(target_os = "linux", target_pointer_width = "64", not(miri)))]
#[allow(dead_code)]
pub struct SparseOversizedReadOnly {
    base: std::ptr::NonNull<u8>,
    as_ptr_calls: Cell<usize>,
}

#[cfg(all(target_os = "linux", target_pointer_width = "64", not(miri)))]
#[allow(dead_code)]
impl SparseOversizedReadOnly {
    /// Reserves one valid initialized read-only mapping of exactly 2^32 bytes.
    pub fn new() -> io::Result<Self> {
        let raw = unsafe {
            // SAFETY: this requests a new anonymous private mapping. No input
            // pointer or file descriptor is dereferenced, and success is
            // checked before the returned address is retained.
            libc::mmap(
                std::ptr::null_mut(),
                SPARSE_OVERSIZED_READ_ONLY_LEN,
                libc::PROT_READ,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                -1,
                0,
            )
        };
        if raw == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }

        let Some(base) = std::ptr::NonNull::new(raw.cast::<u8>()) else {
            // SAFETY: `raw` names the successful mapping above and the exact
            // mapping length is unchanged.
            let rc = unsafe { libc::munmap(raw, SPARSE_OVERSIZED_READ_ONLY_LEN) };
            assert_eq!(rc, 0, "failed to unmap an unexpected null mapping");
            return Err(io::Error::other("mmap returned a null address"));
        };

        Ok(Self {
            base,
            as_ptr_calls: Cell::new(0),
        })
    }

    /// Returns how often a transport consulted this mapping's data pointer.
    pub fn as_ptr_calls(&self) -> usize {
        self.as_ptr_calls.get()
    }

    /// Returns the stable mapping identity without counting a data-pointer use.
    pub fn mapping_base_addr(&self) -> usize {
        self.base.as_ptr() as usize
    }
}

// SAFETY: successful anonymous mappings are zero-initialized and page-aligned.
// This mapping is one non-null, immutable 2^32-byte range, which is below
// `isize::MAX` on the gated 64-bit target. Its address remains stable across
// moves and stays readable until the owning value unmaps it in `Drop`.
#[cfg(all(target_os = "linux", target_pointer_width = "64", not(miri)))]
unsafe impl IoBuffReadOnly for SparseOversizedReadOnly {
    fn as_ptr(&self) -> *const u8 {
        self.as_ptr_calls
            .set(self.as_ptr_calls.get().saturating_add(1));
        self.base.as_ptr()
    }

    fn len(&self) -> usize {
        SPARSE_OVERSIZED_READ_ONLY_LEN
    }
}

#[cfg(all(target_os = "linux", target_pointer_width = "64", not(miri)))]
impl Drop for SparseOversizedReadOnly {
    fn drop(&mut self) {
        // SAFETY: this value uniquely owns the successful mapping at `base`,
        // and Drop supplies the exact original length once.
        let rc = unsafe {
            libc::munmap(
                self.base.as_ptr().cast::<libc::c_void>(),
                SPARSE_OVERSIZED_READ_ONLY_LEN,
            )
        };
        assert_eq!(
            rc,
            0,
            "failed to release sparse oversized test mapping: {}",
            io::Error::last_os_error()
        );
    }
}

/// Verifies the common oversize-send result without touching mapped bytes.
#[cfg(all(target_os = "linux", target_pointer_width = "64", not(miri)))]
fn assert_oversized_send_owner_uninspected(buffer: &SparseOversizedReadOnly) {
    assert_eq!(IoBuffReadOnly::len(buffer), SPARSE_OVERSIZED_READ_ONLY_LEN);
    assert_eq!(
        buffer.as_ptr_calls(),
        0,
        "oversized send consulted the buffer pointer before rejecting its length"
    );
}

/// Verifies the detailed asynchronous oversize-send result.
#[cfg(all(target_os = "linux", target_pointer_width = "64", not(miri)))]
#[allow(dead_code)]
pub fn assert_oversized_send_rejected(result: io::Result<usize>, buffer: &SparseOversizedReadOnly) {
    let err = result.expect_err("oversized send should fail");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        err.to_string(),
        "length exceeds io_uring u32 byte-count limit"
    );
    assert_oversized_send_owner_uninspected(buffer);
}

/// Verifies the message-free immediate oversize-send result.
#[cfg(all(target_os = "linux", target_pointer_width = "64", not(miri)))]
#[allow(dead_code)]
pub fn assert_oversized_try_send_rejected(
    result: io::Result<usize>,
    buffer: &SparseOversizedReadOnly,
) {
    let err = result.expect_err("oversized try send should fail");
    assert_message_free_invalid_input(err);
    assert_oversized_send_owner_uninspected(buffer);
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

/// First projected-write size that requires FlowIO's dynamic scratch.
#[allow(dead_code)]
pub const DYNAMIC_PROJECTED_PIECES: usize = 17;

/// Independent identity/projection/drop observations for one projected owner.
#[allow(dead_code)]
#[derive(Clone)]
pub struct ProjectedSourceWitness {
    identity: Arc<()>,
    projection_calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

#[allow(dead_code)]
impl ProjectedSourceWitness {
    pub fn new() -> Self {
        Self {
            identity: Arc::new(()),
            projection_calls: Arc::new(AtomicUsize::new(0)),
            drops: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn projection_calls(&self) -> usize {
        self.projection_calls.load(Ordering::Relaxed)
    }

    pub fn drops(&self) -> usize {
        self.drops.load(Ordering::Relaxed)
    }
}

/// A 17-piece projected owner with exact identity and drop observations.
#[allow(dead_code)]
pub struct DropTrackedProjected17 {
    bytes: [u8; DYNAMIC_PROJECTED_PIECES],
    witness: ProjectedSourceWitness,
}

#[allow(dead_code)]
impl DropTrackedProjected17 {
    pub fn new(byte: u8) -> (Self, ProjectedSourceWitness) {
        let witness = ProjectedSourceWitness::new();
        (Self::from_witness(byte, &witness), witness)
    }

    pub fn from_witness(byte: u8, witness: &ProjectedSourceWitness) -> Self {
        Self {
            bytes: [byte; DYNAMIC_PROJECTED_PIECES],
            witness: witness.clone(),
        }
    }

    pub fn has_identity(&self, witness: &ProjectedSourceWitness) -> bool {
        Arc::ptr_eq(&self.witness.identity, &witness.identity)
    }

    pub fn bytes(&self) -> &[u8; DYNAMIC_PROJECTED_PIECES] {
        &self.bytes
    }
}

impl Drop for DropTrackedProjected17 {
    fn drop(&mut self) {
        self.witness.drops.fetch_add(1, Ordering::Relaxed);
    }
}

impl WritevProjection for DropTrackedProjected17 {
    fn writev_count_and_len(&self) -> (usize, usize) {
        (DYNAMIC_PROJECTED_PIECES, DYNAMIC_PROJECTED_PIECES)
    }

    fn project_writev<'a>(&'a self, pieces: &mut WritevPieces<'a>) -> io::Result<()> {
        self.witness
            .projection_calls
            .fetch_add(1, Ordering::Relaxed);
        for byte in &self.bytes {
            pieces.push(std::slice::from_ref(byte))?;
        }
        Ok(())
    }
}

/// A projected owner that performs one nested immediate projected write while
/// FlowIO still holds the outer dynamic-scratch borrow.
#[allow(dead_code)]
pub struct ReentrantProjected17<S> {
    outer: DropTrackedProjected17,
    inner_stream: std::cell::RefCell<S>,
    inner_source: std::cell::RefCell<Option<DropTrackedProjected17>>,
    inner_output: std::cell::RefCell<Option<(io::Result<usize>, DropTrackedProjected17)>>,
}

#[allow(dead_code)]
impl<S> ReentrantProjected17<S> {
    pub fn new(
        outer: DropTrackedProjected17,
        inner_stream: S,
        inner_source: DropTrackedProjected17,
    ) -> Self {
        Self {
            outer,
            inner_stream: std::cell::RefCell::new(inner_stream),
            inner_source: std::cell::RefCell::new(Some(inner_source)),
            inner_output: std::cell::RefCell::new(None),
        }
    }

    pub fn take_inner_output(&mut self) -> Option<(io::Result<usize>, DropTrackedProjected17)> {
        self.inner_output.get_mut().take()
    }

    pub fn outer_has_identity(&self, witness: &ProjectedSourceWitness) -> bool {
        self.outer.has_identity(witness)
    }

    pub fn outer_bytes(&self) -> &[u8; DYNAMIC_PROJECTED_PIECES] {
        self.outer.bytes()
    }
}

/// Test-only common surface for immediate projected TCP/Unix writes.
#[allow(dead_code)]
pub trait ProjectedTryStream {
    fn try_projected<T: WritevProjection>(&mut self, source: T) -> (io::Result<usize>, T);
}

impl ProjectedTryStream for flowio::net::tcp::TcpStream {
    fn try_projected<T: WritevProjection>(&mut self, source: T) -> (io::Result<usize>, T) {
        self.try_writev_projected(source)
    }
}

impl ProjectedTryStream for flowio::net::unix::UnixStream {
    fn try_projected<T: WritevProjection>(&mut self, source: T) -> (io::Result<usize>, T) {
        self.try_writev_projected(source)
    }
}

impl<S: ProjectedTryStream + 'static> WritevProjection for ReentrantProjected17<S> {
    fn writev_count_and_len(&self) -> (usize, usize) {
        self.outer.writev_count_and_len()
    }

    fn project_writev<'a>(&'a self, pieces: &mut WritevPieces<'a>) -> io::Result<()> {
        let inner_source = self
            .inner_source
            .try_borrow_mut()
            .map_err(|_| io::Error::other("inner projected source is already borrowed"))?
            .take()
            .ok_or_else(|| io::Error::other("inner projected source was already consumed"))?;
        let output = self
            .inner_stream
            .try_borrow_mut()
            .map_err(|_| io::Error::other("inner stream is already borrowed"))?
            .try_projected(inner_source);
        let mut output_slot = self
            .inner_output
            .try_borrow_mut()
            .map_err(|_| io::Error::other("inner projected output is already borrowed"))?;
        if output_slot.is_some() {
            return Err(io::Error::other(
                "inner projected output was already recorded",
            ));
        }
        *output_slot = Some(output);
        drop(output_slot);

        self.outer.project_writev(pieces)
    }
}

/// Proves successful nested dynamic projected writes through a concrete stream
/// public API, including exact bytes, source identity, and explicit drop time.
#[allow(dead_code)]
pub fn assert_reentrant_projected_try_success<S, OuterPeer, InnerPeer>(
    outer_stream: &mut S,
    outer_peer: &mut OuterPeer,
    inner_stream: S,
    inner_peer: &mut InnerPeer,
) where
    S: ProjectedTryStream + 'static,
    OuterPeer: std::io::Read,
    InnerPeer: std::io::Read,
{
    let (outer_source, outer_witness) = DropTrackedProjected17::new(b'O');
    let (inner_source, inner_witness) = DropTrackedProjected17::new(b'I');
    let source = ReentrantProjected17::new(outer_source, inner_stream, inner_source);

    let (outer_result, mut source) = outer_stream.try_projected(source);
    assert_eq!(
        outer_result.expect("outer reentrant projected write failed"),
        DYNAMIC_PROJECTED_PIECES
    );
    assert!(
        source.outer_has_identity(&outer_witness),
        "outer projected source identity changed while returning from re-entry"
    );
    assert_eq!(source.outer_bytes(), &[b'O'; DYNAMIC_PROJECTED_PIECES]);

    let (inner_result, inner_source) = source
        .take_inner_output()
        .expect("outer projection did not execute the nested projected write");
    assert_eq!(
        inner_result.expect("inner reentrant projected write failed"),
        DYNAMIC_PROJECTED_PIECES
    );
    assert!(
        inner_source.has_identity(&inner_witness),
        "inner projected source identity changed while returning from re-entry"
    );
    assert_eq!(inner_source.bytes(), &[b'I'; DYNAMIC_PROJECTED_PIECES]);

    assert_eq!(outer_witness.projection_calls(), 1);
    assert_eq!(inner_witness.projection_calls(), 1);
    assert_eq!(outer_witness.drops(), 0);
    assert_eq!(inner_witness.drops(), 0);

    let mut outer_bytes = [0; DYNAMIC_PROJECTED_PIECES];
    std::io::Read::read_exact(outer_peer, &mut outer_bytes)
        .expect("read outer reentrant projected bytes");
    assert_eq!(outer_bytes, [b'O'; DYNAMIC_PROJECTED_PIECES]);
    let mut inner_bytes = [0; DYNAMIC_PROJECTED_PIECES];
    std::io::Read::read_exact(inner_peer, &mut inner_bytes)
        .expect("read inner reentrant projected bytes");
    assert_eq!(inner_bytes, [b'I'; DYNAMIC_PROJECTED_PIECES]);

    drop(inner_source);
    assert_eq!(inner_witness.drops(), 1);
    assert_eq!(outer_witness.drops(), 0);
    drop(source);
    assert_eq!(outer_witness.drops(), 1);
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

/// Exercises the shared TCP/Unix one-shot empty read/write policy.
#[allow(unused_macros)]
macro_rules! assert_empty_stream_io_cases {
    ($stream:ident) => {{
        let mut read_buffer = Vec::with_capacity(8);
        read_buffer.extend_from_slice(b"HEAD");
        let read_ptr = read_buffer.as_ptr();
        let read_capacity = read_buffer.capacity();
        let (result, read_buffer) = $stream.read(read_buffer, 0).await;
        assert_eq!(result.expect("empty stream read failed"), 0);
        assert_eq!(read_buffer, b"HEAD");
        assert_eq!(read_buffer.as_ptr(), read_ptr);
        assert_eq!(read_buffer.capacity(), read_capacity);

        let write_buffer = Vec::with_capacity(1);
        let write_ptr = write_buffer.as_ptr();
        let write_capacity = write_buffer.capacity();
        let (result, write_buffer) = $stream.write(write_buffer).await;
        assert_eq!(result.expect("empty stream write failed"), 0);
        assert!(write_buffer.is_empty());
        assert_eq!(write_buffer.as_ptr(), write_ptr);
        assert_eq!(write_buffer.capacity(), write_capacity);
    }};
}
#[allow(unused_imports)]
pub(crate) use assert_empty_stream_io_cases;

/// Projection fixture whose reported shape is internally inconsistent.
#[allow(dead_code)]
pub struct MalformedReportedProjected {
    shape: (usize, usize),
    projection_calls: Cell<usize>,
}

impl MalformedReportedProjected {
    #[allow(dead_code)]
    pub fn bytes_without_pieces() -> Self {
        Self::new((0, 1))
    }

    #[allow(dead_code)]
    pub fn pieces_without_bytes() -> Self {
        Self::new((1, 0))
    }

    fn new(shape: (usize, usize)) -> Self {
        Self {
            shape,
            projection_calls: Cell::new(0),
        }
    }

    #[allow(dead_code)]
    pub fn reported_shape(&self) -> (usize, usize) {
        self.shape
    }

    #[allow(dead_code)]
    pub fn projection_calls(&self) -> usize {
        self.projection_calls.get()
    }
}

impl WritevProjection for MalformedReportedProjected {
    fn writev_count_and_len(&self) -> (usize, usize) {
        self.shape
    }

    fn project_writev<'a>(&'a self, _pieces: &mut WritevPieces<'a>) -> io::Result<()> {
        self.projection_calls
            .set(self.projection_calls.get().saturating_add(1));
        Ok(())
    }
}

/// Exercises malformed reported-shape validation on the immediate API.
#[allow(unused_macros)]
macro_rules! assert_reported_projected_try_cases {
    ($stream:ident) => {{
        for source in [
            $crate::common::MalformedReportedProjected::bytes_without_pieces(),
            $crate::common::MalformedReportedProjected::pieces_without_bytes(),
        ] {
            let expected_shape = source.reported_shape();
            let (result, source) = $stream.try_writev_projected(source);
            let error = result.expect_err("malformed reported shape should fail");
            $crate::common::assert_message_free_invalid_input(error);
            assert_eq!(source.reported_shape(), expected_shape);
            assert_eq!(source.projection_calls(), 0);
        }
    }};
}
#[allow(unused_imports)]
pub(crate) use assert_reported_projected_try_cases;

/// Exercises malformed reported-shape validation on one asynchronous API.
#[allow(unused_macros)]
macro_rules! assert_reported_projected_async_cases {
    ($stream:ident, $method:ident) => {{
        for (source, expected_message) in [
            (
                $crate::common::MalformedReportedProjected::bytes_without_pieces(),
                "projected writev reported bytes but no active pieces",
            ),
            (
                $crate::common::MalformedReportedProjected::pieces_without_bytes(),
                "projected writev reported active pieces but no bytes",
            ),
        ] {
            let expected_shape = source.reported_shape();
            let (result, source) = $stream.$method(source).await;
            let error = result.expect_err("malformed reported shape should fail");
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(error.to_string(), expected_message);
            assert_eq!(source.reported_shape(), expected_shape);
            assert_eq!(source.projection_calls(), 0);
        }
    }};
}
#[allow(unused_imports)]
pub(crate) use assert_reported_projected_async_cases;

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

/// Verifies an `InvalidInput` result that carries no custom diagnostic.
#[allow(dead_code)]
pub fn assert_message_free_invalid_input(err: io::Error) {
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert!(
        err.get_ref().is_none(),
        "error unexpectedly carried a custom diagnostic: {err}"
    );
}

/// Exercises piece-count and byte-total mismatch validation on one async API.
#[allow(unused_macros)]
macro_rules! assert_projected_async_mismatches {
    ($stream:ident, $method:ident) => {{
        let (result, source) = $stream
            .$method($crate::common::TryCountMismatchedProjected)
            .await;
        let error = result.expect_err("projected piece-count mismatch should fail");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(
            error.to_string(),
            "projected writev piece count did not match counted pieces"
        );
        let _source = source;

        let (result, source) = $stream
            .$method($crate::common::TryMismatchedProjected)
            .await;
        let error = result.expect_err("projected byte-total mismatch should fail");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(
            error.to_string(),
            "projected writev byte length did not match counted length"
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
