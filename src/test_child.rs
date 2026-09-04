//! Development-only bounded capture of test subprocess output.

use std::fmt;
use std::io::{self, Read};
use std::os::fd::{AsRawFd, RawFd};
use std::process::{Child, ChildStderr, ChildStdout, ExitStatus};
use std::time::{Duration, Instant};

const MAX_CAPTURED_BYTES_PER_STREAM: usize = 256 * 1024;
const READ_BUFFER_BYTES: usize = 1024;
const READS_PER_STREAM_TURN: usize = 64;
const IDLE_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Identifies one captured child output stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildOutputStream {
    /// The child's standard output pipe.
    Stdout,
    /// The child's standard error pipe.
    Stderr,
}

impl fmt::Display for ChildOutputStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stdout => formatter.write_str("stdout"),
            Self::Stderr => formatter.write_str("stderr"),
        }
    }
}

/// Classifies a bounded child-capture failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildCaptureErrorKind {
    /// The caller did not configure one required pipe.
    MissingPipe(ChildOutputStream),
    /// A child pipe could not be switched to nonblocking mode.
    SetNonblocking(ChildOutputStream),
    /// A nonblocking pipe read failed.
    Read(ChildOutputStream),
    /// Capturing another byte would exceed the fixed per-stream limit.
    OutputLimitExceeded {
        /// Stream whose output exceeded the limit.
        stream: ChildOutputStream,
        /// Maximum retained bytes for that stream.
        limit: usize,
    },
    /// Polling the child exit status failed.
    Poll,
    /// The child did not finish and close both pipes before its deadline.
    Timeout {
        /// Caller-provided watchdog duration.
        timeout: Duration,
    },
}

/// Bounded diagnostics returned after the mandatory terminate/reap cleanup
/// sequence has run.
#[derive(Debug)]
pub struct ChildCaptureError {
    kind: ChildCaptureErrorKind,
    source: Option<io::Error>,
    cleanup_error: Option<io::Error>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

impl ChildCaptureError {
    /// Returns the precise capture failure classification.
    pub fn kind(&self) -> ChildCaptureErrorKind {
        self.kind
    }
}

impl fmt::Display for ChildCaptureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            ChildCaptureErrorKind::MissingPipe(stream) => {
                write!(formatter, "child {stream} was not piped")?;
            }
            ChildCaptureErrorKind::SetNonblocking(stream) => {
                write!(formatter, "set child {stream} nonblocking")?;
            }
            ChildCaptureErrorKind::Read(stream) => {
                write!(formatter, "read child {stream}")?;
            }
            ChildCaptureErrorKind::OutputLimitExceeded { stream, limit } => {
                write!(
                    formatter,
                    "child {stream} exceeded the {limit}-byte capture limit"
                )?;
            }
            ChildCaptureErrorKind::Poll => formatter.write_str("poll child exit status")?,
            ChildCaptureErrorKind::Timeout { timeout } => {
                write!(formatter, "child exceeded its {timeout:?} watchdog")?;
            }
        }
        if let Some(source) = &self.source {
            write!(formatter, ": {source}")?;
        }
        if let Some(cleanup_error) = &self.cleanup_error {
            write!(formatter, "; child cleanup failed: {cleanup_error}")?;
        }
        write!(
            formatter,
            "; stdout={:?}, stderr={:?}",
            String::from_utf8_lossy(&self.stdout),
            String::from_utf8_lossy(&self.stderr)
        )
    }
}

impl std::error::Error for ChildCaptureError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

/// Exit status and bounded output collected from one captured child process.
#[derive(Debug)]
pub struct CapturedChildOutput {
    /// Reaped child exit status.
    pub status: ExitStatus,
    /// Complete child stdout, bounded to 256 KiB.
    pub stdout: Vec<u8>,
    /// Complete child stderr, bounded to 256 KiB.
    pub stderr: Vec<u8>,
}

#[derive(Debug)]
struct CaptureFailure {
    kind: ChildCaptureErrorKind,
    source: Option<io::Error>,
}

impl CaptureFailure {
    fn new(kind: ChildCaptureErrorKind) -> Self {
        Self { kind, source: None }
    }

    fn with_source(kind: ChildCaptureErrorKind, source: io::Error) -> Self {
        Self {
            kind,
            source: Some(source),
        }
    }
}

/// Captures both piped streams until the child exits or its watchdog expires.
///
/// The function owns `child`, retains at most 256 KiB from each stream, and
/// performs one terminate/reap cleanup sequence before returning any capture
/// error. A cleanup failure is retained in that error's diagnostic. Both
/// `stdout` and `stderr` must be configured with
/// [`std::process::Stdio::piped`] before the child is spawned.
pub fn capture_child_with_watchdog(
    child: Child,
    timeout: Duration,
) -> Result<CapturedChildOutput, ChildCaptureError> {
    capture_child_with_limit(child, timeout, MAX_CAPTURED_BYTES_PER_STREAM)
}

fn capture_child_with_limit(
    mut child: Child,
    timeout: Duration,
    per_stream_limit: usize,
) -> Result<CapturedChildOutput, ChildCaptureError> {
    let mut stdout = match child.stdout.take() {
        Some(stdout) => stdout,
        None => {
            return Err(finish_failure(
                child,
                CaptureFailure::new(ChildCaptureErrorKind::MissingPipe(
                    ChildOutputStream::Stdout,
                )),
                Vec::new(),
                Vec::new(),
            ));
        }
    };
    let mut stderr = match child.stderr.take() {
        Some(stderr) => stderr,
        None => {
            return Err(finish_failure(
                child,
                CaptureFailure::new(ChildCaptureErrorKind::MissingPipe(
                    ChildOutputStream::Stderr,
                )),
                Vec::new(),
                Vec::new(),
            ));
        }
    };

    if let Err(source) = set_nonblocking(stdout.as_raw_fd()) {
        return Err(finish_failure(
            child,
            CaptureFailure::with_source(
                ChildCaptureErrorKind::SetNonblocking(ChildOutputStream::Stdout),
                source,
            ),
            Vec::new(),
            Vec::new(),
        ));
    }
    if let Err(source) = set_nonblocking(stderr.as_raw_fd()) {
        return Err(finish_failure(
            child,
            CaptureFailure::with_source(
                ChildCaptureErrorKind::SetNonblocking(ChildOutputStream::Stderr),
                source,
            ),
            Vec::new(),
            Vec::new(),
        ));
    }

    let mut stdout_bytes = Vec::with_capacity(per_stream_limit);
    let mut stderr_bytes = Vec::with_capacity(per_stream_limit);
    let mut stdout_eof = false;
    let mut stderr_eof = false;
    let mut stdout_first = true;
    let mut status = None;
    let started = Instant::now();

    loop {
        let drained = drain_pipes(
            &mut stdout,
            &mut stderr,
            &mut stdout_bytes,
            &mut stderr_bytes,
            &mut stdout_eof,
            &mut stderr_eof,
            per_stream_limit,
            stdout_first,
        );
        stdout_first = !stdout_first;
        let made_progress = match drained {
            Ok(made_progress) => made_progress,
            Err(failure) => {
                return Err(finish_failure(child, failure, stdout_bytes, stderr_bytes));
            }
        };

        if status.is_none() {
            match child.try_wait() {
                Ok(observed) => status = observed,
                Err(source) => {
                    return Err(finish_failure(
                        child,
                        CaptureFailure::with_source(ChildCaptureErrorKind::Poll, source),
                        stdout_bytes,
                        stderr_bytes,
                    ));
                }
            }
        }

        if stdout_eof
            && stderr_eof
            && let Some(status) = status
        {
            return Ok(CapturedChildOutput {
                status,
                stdout: stdout_bytes,
                stderr: stderr_bytes,
            });
        }

        if started.elapsed() >= timeout {
            return Err(finish_failure(
                child,
                CaptureFailure::new(ChildCaptureErrorKind::Timeout { timeout }),
                stdout_bytes,
                stderr_bytes,
            ));
        }

        if !made_progress {
            std::thread::sleep(IDLE_POLL_INTERVAL);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn drain_pipes(
    stdout: &mut ChildStdout,
    stderr: &mut ChildStderr,
    stdout_bytes: &mut Vec<u8>,
    stderr_bytes: &mut Vec<u8>,
    stdout_eof: &mut bool,
    stderr_eof: &mut bool,
    limit: usize,
    stdout_first: bool,
) -> Result<bool, CaptureFailure> {
    let (first_progress, second_progress) = if stdout_first {
        (
            drain_stream(
                stdout,
                stdout_bytes,
                stdout_eof,
                limit,
                ChildOutputStream::Stdout,
            )?,
            drain_stream(
                stderr,
                stderr_bytes,
                stderr_eof,
                limit,
                ChildOutputStream::Stderr,
            )?,
        )
    } else {
        (
            drain_stream(
                stderr,
                stderr_bytes,
                stderr_eof,
                limit,
                ChildOutputStream::Stderr,
            )?,
            drain_stream(
                stdout,
                stdout_bytes,
                stdout_eof,
                limit,
                ChildOutputStream::Stdout,
            )?,
        )
    };
    Ok(first_progress || second_progress)
}

fn drain_stream(
    stream: &mut impl Read,
    bytes: &mut Vec<u8>,
    eof: &mut bool,
    limit: usize,
    stream_name: ChildOutputStream,
) -> Result<bool, CaptureFailure> {
    if *eof {
        return Ok(false);
    }

    let mut made_progress = false;
    let mut read_buffer = [0u8; READ_BUFFER_BYTES];
    for _ in 0..READS_PER_STREAM_TURN {
        match stream.read(&mut read_buffer) {
            Ok(0) => {
                *eof = true;
                return Ok(true);
            }
            Ok(read) => {
                made_progress = true;
                if read > limit.saturating_sub(bytes.len()) {
                    return Err(CaptureFailure::new(
                        ChildCaptureErrorKind::OutputLimitExceeded {
                            stream: stream_name,
                            limit,
                        },
                    ));
                }
                bytes.extend_from_slice(&read_buffer[..read]);
            }
            Err(source) if source.kind() == io::ErrorKind::Interrupted => continue,
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                return Ok(made_progress);
            }
            Err(source) => {
                return Err(CaptureFailure::with_source(
                    ChildCaptureErrorKind::Read(stream_name),
                    source,
                ));
            }
        }
    }
    Ok(made_progress)
}

fn set_nonblocking(fd: RawFd) -> io::Result<()> {
    let flags = loop {
        // SAFETY: F_GETFL observes the status flags of the live borrowed pipe
        // descriptor and does not take ownership of it.
        let result = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if result >= 0 {
            break result;
        }
        let source = io::Error::last_os_error();
        if source.kind() != io::ErrorKind::Interrupted {
            return Err(source);
        }
    };
    if flags & libc::O_NONBLOCK != 0 {
        return Ok(());
    }

    loop {
        // SAFETY: F_SETFL updates status flags on the live borrowed pipe
        // descriptor. The original flags are preserved and ownership does not
        // change.
        let result = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
        if result >= 0 {
            return Ok(());
        }
        let source = io::Error::last_os_error();
        if source.kind() != io::ErrorKind::Interrupted {
            return Err(source);
        }
    }
}

fn finish_failure(
    mut child: Child,
    failure: CaptureFailure,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
) -> ChildCaptureError {
    let cleanup_error = terminate_and_reap(&mut child).err();
    ChildCaptureError {
        kind: failure.kind,
        source: failure.source,
        cleanup_error,
        stdout,
        stderr,
    }
}

fn terminate_and_reap(child: &mut Child) -> io::Result<()> {
    if let Ok(Some(_)) = child.try_wait() {
        return Ok(());
    }

    let kill_error = child.kill().err();
    match child.wait() {
        Ok(_) => Ok(()),
        Err(wait_error) => match kill_error {
            Some(kill_error) => Err(io::Error::new(
                wait_error.kind(),
                format!("kill failed: {kill_error}; reap failed: {wait_error}"),
            )),
            None => Err(wait_error),
        },
    }
}

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;
    use std::io::Write;
    use std::process::{Command, Stdio};

    const FIXTURE_ENV: &str = "FLOWIO_BOUNDED_CHILD_CAPTURE_FIXTURE";
    const FIXTURE_TEST: &str = "test_child::tests::bounded_child_capture_fixture";
    const LARGE_STREAM_BYTES: usize = 160 * 1024;

    #[test]
    fn bounded_child_capture_fixture() {
        let Some(mode) = std::env::var_os(FIXTURE_ENV) else {
            return;
        };
        match mode.to_str().expect("child fixture mode must be UTF-8") {
            "success" => {
                std::io::stdout()
                    .write_all(b"bounded child success stdout\n")
                    .expect("write success stdout");
                std::io::stderr()
                    .write_all(b"bounded child success stderr\n")
                    .expect("write success stderr");
            }
            "large" => {
                let stdout_chunk = [b'o'; READ_BUFFER_BYTES];
                let stderr_chunk = [b'e'; READ_BUFFER_BYTES];
                let mut stdout = std::io::stdout().lock();
                for _ in 0..LARGE_STREAM_BYTES / stdout_chunk.len() {
                    stdout
                        .write_all(&stdout_chunk)
                        .expect("write large child stdout");
                }
                stdout.flush().expect("flush large child stdout");
                drop(stdout);
                let mut stderr = std::io::stderr().lock();
                for _ in 0..LARGE_STREAM_BYTES / stderr_chunk.len() {
                    stderr
                        .write_all(&stderr_chunk)
                        .expect("write large child stderr");
                }
                stderr.flush().expect("flush large child stderr");
            }
            "overflow" => {
                let chunk = [b'x'; READ_BUFFER_BYTES];
                let mut stdout = std::io::stdout().lock();
                for _ in 0..64 {
                    stdout
                        .write_all(&chunk)
                        .expect("write overflowing child stdout");
                }
            }
            "timeout" => std::thread::sleep(Duration::from_secs(60)),
            "failure" => std::process::exit(23),
            other => panic!("unknown child fixture mode {other}"),
        }
    }

    fn spawn_fixture(mode: &str) -> Child {
        Command::new(std::env::current_exe().expect("current unit-test executable"))
            .args(["--exact", FIXTURE_TEST, "--nocapture"])
            .env(FIXTURE_ENV, mode)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|source| panic!("spawn {mode} child fixture: {source}"))
    }

    fn assert_reaped(pid: u32, mode: &str) {
        let pid = libc::pid_t::try_from(pid).expect("child PID must fit pid_t");
        // SAFETY: signal zero only probes whether the numeric process exists;
        // it does not deliver a signal or assume ownership.
        let result = unsafe { libc::kill(pid, 0) };
        assert_eq!(result, -1, "{mode} child {pid} still exists after capture");
        assert_eq!(
            io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH),
            "{mode} child {pid} was not observably reaped"
        );
    }

    #[test]
    fn bounded_child_capture_covers_all_exit_and_output_paths() {
        let child = spawn_fixture("success");
        let pid = child.id();
        let output = capture_child_with_watchdog(child, Duration::from_secs(2))
            .expect("capture successful child");
        assert!(output.status.success());
        assert!(
            output
                .stdout
                .windows(14)
                .any(|bytes| bytes == b"success stdout")
        );
        assert!(
            output
                .stderr
                .windows(14)
                .any(|bytes| bytes == b"success stderr")
        );
        assert_reaped(pid, "success");

        let child = spawn_fixture("failure");
        let pid = child.id();
        let output = capture_child_with_watchdog(child, Duration::from_secs(2))
            .expect("capture ordinarily failing child");
        assert_eq!(output.status.code(), Some(23));
        assert_reaped(pid, "failure");

        let child = spawn_fixture("large");
        let pid = child.id();
        let output = capture_child_with_watchdog(child, Duration::from_secs(5))
            .expect("capture child output larger than a normal pipe");
        assert!(output.status.success());
        assert!(output.stdout.iter().filter(|byte| **byte == b'o').count() >= LARGE_STREAM_BYTES);
        assert!(output.stderr.iter().filter(|byte| **byte == b'e').count() >= LARGE_STREAM_BYTES);
        assert_reaped(pid, "large-output");

        let child = spawn_fixture("overflow");
        let pid = child.id();
        let overflow = capture_child_with_limit(child, Duration::from_secs(2), READ_BUFFER_BYTES)
            .expect_err("overflowing child capture must fail");
        assert_eq!(
            overflow.kind(),
            ChildCaptureErrorKind::OutputLimitExceeded {
                stream: ChildOutputStream::Stdout,
                limit: READ_BUFFER_BYTES,
            }
        );
        assert!(overflow.stdout.len() <= READ_BUFFER_BYTES);
        assert!(overflow.stderr.len() <= READ_BUFFER_BYTES);
        assert_reaped(pid, "overflow");

        let child = spawn_fixture("timeout");
        let pid = child.id();
        let timeout = Duration::from_millis(50);
        let timed_out = capture_child_with_watchdog(child, timeout)
            .expect_err("stalled child capture must time out");
        assert_eq!(timed_out.kind(), ChildCaptureErrorKind::Timeout { timeout });
        assert_reaped(pid, "timeout");
    }
}
