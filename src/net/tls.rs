//! Minimal rustls-backed TLS client stream for FlowIO TCP connections.
//!
//! This module intentionally exposes a narrow client-only wrapper:
//! - the caller supplies an already-connected [`TcpStream`]
//! - the caller supplies an [`Arc<rustls::ClientConfig>`]
//! - the caller supplies an owned [`rustls::pki_types::ServerName`]
//! - TLS handshake driving is explicit via [`TlsClientStream::handshake`]
//!
//! The wrapper keeps the socket ownership boundary clear and avoids adding an
//! extra plaintext buffering layer on top of rustls. The only wrapper-owned
//! buffers are reusable ciphertext scratch buffers used to move TLS records
//! between rustls and the underlying TCP stream.
//!
//! `rustls` still has its own internal protocol buffers. This wrapper exposes
//! the public `rustls` write-direction limit via [`TlsClientOptions`] so that
//! buffering stays explicit and caller-controlled instead of silently relying
//! on crate defaults.
//!
//! # Example
//! ```no_run
//! use flowio::net::tcp::TcpStream;
//! use flowio::net::tls::{TlsClientOptions, TlsClientStream};
//! use flowio::runtime::executor::Executor;
//! use rustls::pki_types::ServerName;
//! use rustls::{ClientConfig, RootCertStore};
//! use std::net::{Ipv4Addr, SocketAddr};
//! use std::sync::Arc;
//!
//! let mut roots = RootCertStore::empty();
//! // Populate `roots` before relying on certificate validation in production.
//! let config = Arc::new(
//!     ClientConfig::builder()
//!         .with_root_certificates(roots)
//!         .with_no_client_auth(),
//! );
//! let options = TlsClientOptions {
//!     rustls_buffer_limit: Some(16 * 1024),
//!     transport_read_buffer_size: 16 * 1024,
//!     transport_write_buffer_size: 16 * 1024,
//! };
//!
//! let mut executor = Executor::new()?;
//! executor.run(async move {
//!     let tcp = TcpStream::connect(SocketAddr::from((Ipv4Addr::LOCALHOST, 5432)))
//!         .unwrap()
//!         .await
//!         .unwrap();
//!     let server_name = ServerName::try_from("localhost").unwrap();
//!     let mut tls = TlsClientStream::new(tcp, config, server_name, options).unwrap();
//!
//!     tls.handshake().await.unwrap();
//!
//!     let (res, _send) = tls.write_all(b"hello".to_vec()).await;
//!     res.unwrap();
//!
//!     let (res, recv) = tls.read_exact(vec![0u8; 5], 5).await;
//!     res.unwrap();
//!     assert_eq!(&recv[..], b"world");
//!
//!     tls.shutdown().await.unwrap();
//! })?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use super::stream;
use super::tcp::TcpStream;
use super::{checked_read_len, opt_mut, opt_ref, opt_take};
use crate::runtime::buffer::{IoBuffReadOnly, IoBuffReadWrite};
use rustls::ClientConfig;
use rustls::client::ClientConnection;
use rustls::pki_types::ServerName;
use std::future::Future;
use std::io::{self, Cursor, Read, Write};
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::slice;
use std::sync::Arc;
use std::task::{Context, Poll};

type PendingTlsRead = stream::ReadFuture<'static, Vec<u8>, TlsTransportMarker>;
type PendingTlsWrite = stream::WriteAllFuture<'static, Vec<u8>, TlsTransportMarker>;

struct TlsTransportMarker;

#[inline]
fn tls_protocol_error(err: rustls::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}

#[inline]
fn tls_internal_error(message: &'static str) -> io::Error {
    io::Error::other(message)
}

/// Explicit TLS wrapper buffer and rustls buffering configuration.
///
/// This type intentionally does not implement `Default` so callers must make
/// the buffering decision explicitly.
///
/// `transport_read_buffer_size` is the scratch capacity used for each raw TLS
/// read attempt from the underlying socket. `transport_write_buffer_size` is
/// the initial capacity used when collecting TLS records emitted by rustls
/// before writing them to the socket; the buffer may still grow if rustls
/// emits more than the initial capacity in one flush cycle.
///
/// # Example
/// ```
/// use flowio::net::tls::TlsClientOptions;
///
/// let options = TlsClientOptions {
///     rustls_buffer_limit: Some(8 * 1024),
///     transport_read_buffer_size: 8 * 1024,
///     transport_write_buffer_size: 8 * 1024,
/// };
///
/// assert_eq!(options.transport_read_buffer_size, 8 * 1024);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TlsClientOptions {
    /// rustls limit for unsent plaintext-before-handshake and pending TLS
    /// records.  `None` means rustls may buffer without bound.
    pub rustls_buffer_limit: Option<usize>,
    /// Capacity of the reusable ciphertext receive scratch buffer used for
    /// `read_tls`.
    pub transport_read_buffer_size: usize,
    /// Initial capacity of the reusable ciphertext send scratch buffer used
    /// for `write_tls`.
    pub transport_write_buffer_size: usize,
}

/// FlowIO-native TLS client stream around an existing connected TCP stream.
///
/// The wrapper owns:
/// - the connected [`TcpStream`]
/// - a single `rustls::ClientConnection`
/// - one reusable ciphertext read scratch buffer
/// - one reusable ciphertext write scratch buffer
///
/// It does not add another plaintext staging layer beyond what rustls itself
/// requires internally.
///
/// # Cancellation semantics
/// Dropping a `handshake`, `read`, `write`, `flush`, or `shutdown` future does
/// not discard already-started raw transport work. Any in-flight TLS record
/// read/write remains owned by the stream and will be resumed or retired by
/// the next TLS operation. This keeps TLS record handling correct without
/// introducing background threads or a broader transport abstraction.
pub struct TlsClientStream {
    stream: TcpStream,
    connection: ClientConnection,
    transport_read_buffer_size: usize,
    transport_write_buffer_size: usize,
    read_tls_buffer: Option<Vec<u8>>,
    write_tls_buffer: Option<Vec<u8>>,
    pending_read_tls: Option<PendingTlsRead>,
    pending_write_tls: Option<PendingTlsWrite>,
    write_shutdown: bool,
    transport_write_shutdown: bool,
}

impl TlsClientStream {
    /// Creates a new TLS client wrapper around an already-connected TCP stream.
    ///
    /// This allocates the wrapper's reusable ciphertext scratch buffers up
    /// front using the capacities provided in [`TlsClientOptions`].
    ///
    /// # Errors
    /// Returns `InvalidInput` if either transport scratch buffer size is zero.
    /// Returns `InvalidData` if rustls rejects the supplied config or server
    /// name while constructing the client connection.
    ///
    /// # Example
    /// ```no_run
    /// use flowio::net::tcp::TcpStream;
    /// use flowio::net::tls::{TlsClientOptions, TlsClientStream};
    /// use flowio::runtime::executor::Executor;
    /// use rustls::pki_types::ServerName;
    /// use rustls::{ClientConfig, RootCertStore};
    /// use std::net::{Ipv4Addr, SocketAddr};
    /// use std::sync::Arc;
    ///
    /// let config = Arc::new(
    ///     ClientConfig::builder()
    ///         .with_root_certificates(RootCertStore::empty())
    ///         .with_no_client_auth(),
    /// );
    /// let tcp = TcpStream::connect(SocketAddr::from((Ipv4Addr::LOCALHOST, 5432)))?;
    /// let options = TlsClientOptions {
    ///     rustls_buffer_limit: Some(16 * 1024),
    ///     transport_read_buffer_size: 16 * 1024,
    ///     transport_write_buffer_size: 16 * 1024,
    /// };
    ///
    /// let mut executor = Executor::new()?;
    /// executor.run(async move {
    ///     let tcp = TcpStream::connect(SocketAddr::from((Ipv4Addr::LOCALHOST, 5432)))
    ///         .unwrap()
    ///         .await
    ///         .unwrap();
    ///     let server_name = ServerName::try_from("localhost").unwrap();
    ///     let _tls = TlsClientStream::new(tcp, config, server_name, options).unwrap();
    /// })?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn new(
        stream: TcpStream,
        config: Arc<ClientConfig>,
        server_name: ServerName<'static>,
        options: TlsClientOptions,
    ) -> io::Result<Self> {
        if options.transport_read_buffer_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "transport_read_buffer_size must be greater than zero",
            ));
        }
        if options.transport_write_buffer_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "transport_write_buffer_size must be greater than zero",
            ));
        }

        let mut connection =
            ClientConnection::new(config, server_name).map_err(tls_protocol_error)?;
        connection.set_buffer_limit(options.rustls_buffer_limit);

        Ok(Self {
            stream,
            connection,
            transport_read_buffer_size: options.transport_read_buffer_size,
            transport_write_buffer_size: options.transport_write_buffer_size,
            read_tls_buffer: Some(Vec::with_capacity(options.transport_read_buffer_size)),
            write_tls_buffer: Some(Vec::with_capacity(options.transport_write_buffer_size)),
            pending_read_tls: None,
            pending_write_tls: None,
            write_shutdown: false,
            transport_write_shutdown: false,
        })
    }

    /// Returns `true` while the TLS handshake is still in progress.
    pub fn is_handshaking(&self) -> bool {
        self.connection.is_handshaking()
    }

    /// Starts driving the TLS handshake.
    ///
    /// The handshake is not performed implicitly by `read`, `write`, or
    /// `flush`. Callers must drive it explicitly before application I/O.
    pub fn handshake(&mut self) -> TlsHandshakeFuture<'_> {
        TlsHandshakeFuture { stream: self }
    }

    /// Reads up to `len` decrypted plaintext bytes into `buffer`.
    ///
    /// Returns `NotConnected` if the TLS handshake has not completed yet.
    pub fn read<B: IoBuffReadWrite>(&mut self, buffer: B, len: usize) -> TlsReadFuture<'_, B> {
        TlsReadFuture::new(self, buffer, len)
    }

    /// Reads exactly `len` decrypted plaintext bytes into `buffer`.
    ///
    /// Returns `UnexpectedEof` if the TLS session reaches EOF before `len`
    /// plaintext bytes become available.
    pub fn read_exact<B: IoBuffReadWrite>(
        &mut self,
        buffer: B,
        len: usize,
    ) -> TlsReadExactFuture<'_, B> {
        TlsReadExactFuture::new(self, buffer, len)
    }

    /// Writes some plaintext from `buffer`, then drains the resulting TLS
    /// records to the socket before completing.
    ///
    /// The returned count may be short if rustls accepts only part of the
    /// supplied plaintext, for example because of the configured rustls buffer
    /// limit. Use [`Self::write_all`] when the full buffer must be accepted.
    pub fn write<B: IoBuffReadOnly>(&mut self, buffer: B) -> TlsWriteFuture<'_, B> {
        TlsWriteFuture::new(self, buffer)
    }

    /// Writes the entire plaintext buffer, draining TLS records as needed.
    ///
    /// Returns `NotConnected` if called before handshake completion and
    /// `BrokenPipe` if the TLS write side has already been shut down.
    pub fn write_all<B: IoBuffReadOnly>(&mut self, buffer: B) -> TlsWriteAllFuture<'_, B> {
        TlsWriteAllFuture::new(self, buffer)
    }

    /// Flushes any pending TLS records already queued inside rustls or staged
    /// for the underlying TCP stream.
    ///
    /// This does not advance the TLS handshake by itself beyond draining
    /// already-generated outbound records.
    pub fn flush(&mut self) -> TlsFlushFuture<'_> {
        TlsFlushFuture { stream: self }
    }

    /// Sends `close_notify`, flushes it, and then shuts down the TCP write side.
    ///
    /// After this completes, further plaintext writes return `BrokenPipe`.
    /// The read side remains available until the peer closes its direction.
    pub fn shutdown(&mut self) -> TlsShutdownFuture<'_> {
        TlsShutdownFuture { stream: self }
    }

    /// Returns the end-entity certificate DER after handshake completion.
    ///
    /// This is sufficient for a later `tls-server-end-point` channel-binding
    /// implementation in a protocol-specific consumer such as PostgreSQL.
    pub fn peer_end_entity_certificate_der(&self) -> Option<&[u8]> {
        self.connection
            .peer_certificates()
            .and_then(|certs| certs.first().map(|cert| cert.as_ref()))
    }

    fn ensure_handshake_complete(&self) -> io::Result<()> {
        if self.connection.is_handshaking() {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "tls handshake not complete",
            ));
        }

        Ok(())
    }

    fn ensure_writable(&self) -> io::Result<()> {
        if self.write_shutdown {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "tls write side already shut down",
            ));
        }

        Ok(())
    }

    fn take_read_tls_buffer(&mut self) -> Vec<u8> {
        self.read_tls_buffer
            .take()
            .unwrap_or_else(|| Vec::with_capacity(self.transport_read_buffer_size))
    }

    fn restore_read_tls_buffer(&mut self, mut buffer: Vec<u8>) {
        buffer.clear();
        if self.read_tls_buffer.is_none() {
            self.read_tls_buffer = Some(buffer);
        }
    }

    fn take_write_tls_buffer(&mut self) -> Vec<u8> {
        self.write_tls_buffer
            .take()
            .unwrap_or_else(|| Vec::with_capacity(self.transport_write_buffer_size))
    }

    fn restore_write_tls_buffer(&mut self, mut buffer: Vec<u8>) {
        buffer.clear();
        if self.write_tls_buffer.is_none() {
            self.write_tls_buffer = Some(buffer);
        }
    }

    fn feed_transport_bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
        let mut cursor = Cursor::new(bytes);
        while (cursor.position() as usize) < bytes.len() {
            let read = self.connection.read_tls(&mut cursor)?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "rustls made no progress reading non-empty TLS transport input",
                ));
            }
        }

        self.connection
            .process_new_packets()
            .map_err(tls_protocol_error)?;
        Ok(())
    }

    fn feed_transport_eof(&mut self) -> io::Result<()> {
        let mut eof = io::empty();
        let _ = self.connection.read_tls(&mut eof)?;
        self.connection
            .process_new_packets()
            .map_err(tls_protocol_error)?;
        Ok(())
    }

    // Drain already-generated TLS records to the socket. This is also the
    // place where staged raw write futures are resumed after a caller drops a
    // higher-level TLS operation future.
    fn poll_flush_pending_tls(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            if let Some(future) = self.pending_write_tls.as_mut() {
                match Pin::new(future).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready((result, buffer)) => {
                        self.pending_write_tls = None;
                        self.restore_write_tls_buffer(buffer);
                        result?;
                        continue;
                    }
                }
            }

            if !self.connection.wants_write() {
                return Poll::Ready(Ok(()));
            }

            if self.pending_read_tls.is_some() {
                return Poll::Ready(Err(tls_internal_error("tls transport read/write overlap")));
            }

            let mut buffer = self.take_write_tls_buffer();
            buffer.clear();

            let mut total = 0usize;
            while self.connection.wants_write() {
                let wrote = self.connection.write_tls(&mut buffer)?;
                if wrote == 0 {
                    self.restore_write_tls_buffer(buffer);
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "rustls produced zero TLS bytes while wants_write() was true",
                    )));
                }
                total += wrote;
            }

            debug_assert_eq!(total, buffer.len());
            self.pending_write_tls =
                Some(stream::WriteAllFuture::new(self.stream.as_raw_fd(), buffer));
        }
    }

    // Pull one chunk of ciphertext from the socket and feed it into rustls.
    // The pending raw read future is kept on the stream so later TLS calls can
    // resume it if the original higher-level future is dropped.
    fn poll_fill_tls_from_transport(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            if let Some(future) = self.pending_read_tls.as_mut() {
                match Pin::new(future).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready((result, buffer)) => {
                        self.pending_read_tls = None;
                        let read = result?;
                        if read == 0 {
                            self.feed_transport_eof()?;
                        } else {
                            self.feed_transport_bytes(&buffer[..read])?;
                        }
                        self.restore_read_tls_buffer(buffer);
                        return Poll::Ready(Ok(()));
                    }
                }
            }

            if !self.connection.wants_read() {
                return Poll::Ready(Ok(()));
            }

            if self.pending_write_tls.is_some() {
                return Poll::Ready(Err(tls_internal_error("tls transport write/read overlap")));
            }

            let buffer = self.take_read_tls_buffer();
            let len = buffer.capacity();
            self.pending_read_tls = Some(stream::ReadFuture::new(
                self.stream.as_raw_fd(),
                buffer,
                len,
            ));
        }
    }

    fn poll_read_plaintext(
        &mut self,
        cx: &mut Context<'_>,
        dst: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            match self.poll_flush_pending_tls(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Ready(Ok(())) => {}
            }

            match self.connection.reader().read(dst) {
                Ok(read) => return Poll::Ready(Ok(read)),
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
                Err(err) => return Poll::Ready(Err(err)),
            }

            if self.pending_read_tls.is_some() || self.connection.wants_read() {
                match self.poll_fill_tls_from_transport(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Ready(Ok(())) => continue,
                }
            }

            if self.connection.wants_write() {
                continue;
            }

            return Poll::Ready(Err(tls_internal_error(
                "tls reader reported WouldBlock without transport demand",
            )));
        }
    }
}

impl Drop for TlsClientStream {
    fn drop(&mut self) {
        self.pending_read_tls = None;
        self.pending_write_tls = None;
    }
}

/// Explicit TLS handshake future.
#[doc(hidden)]
pub struct TlsHandshakeFuture<'a> {
    stream: &'a mut TlsClientStream,
}

impl Future for TlsHandshakeFuture<'_> {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            match this.stream.poll_flush_pending_tls(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Ready(Ok(())) => {}
            }

            if !this.stream.connection.is_handshaking() {
                return Poll::Ready(Ok(()));
            }

            if this.stream.pending_read_tls.is_some() || this.stream.connection.wants_read() {
                match this.stream.poll_fill_tls_from_transport(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Ready(Ok(())) => continue,
                }
            }

            if this.stream.connection.wants_write() {
                continue;
            }

            return Poll::Ready(Err(tls_internal_error(
                "tls handshake stalled without transport demand",
            )));
        }
    }
}

/// TLS plaintext read future.
#[doc(hidden)]
pub struct TlsReadFuture<'a, B: IoBuffReadWrite> {
    stream: &'a mut TlsClientStream,
    buffer: Option<B>,
    target: usize,
    input_error: Option<io::Error>,
}

impl<'a, B: IoBuffReadWrite> TlsReadFuture<'a, B> {
    fn new(stream: &'a mut TlsClientStream, buffer: B, len: usize) -> Self {
        let mut input_error = None;
        let target = match checked_read_len("tls read", len, buffer.writable_len()) {
            Ok(target) => target as usize,
            Err(err) => {
                input_error = Some(err);
                0
            }
        };

        Self {
            stream,
            buffer: Some(buffer),
            target,
            input_error,
        }
    }
}

impl<B: IoBuffReadWrite> Future for TlsReadFuture<'_, B> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if let Some(err) = this.input_error.take() {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }

        if let Err(err) = this.stream.ensure_handshake_complete() {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }

        if this.target == 0 {
            let mut buffer = unsafe { opt_take(&mut this.buffer) };
            unsafe { buffer.set_written_len(0) };
            return Poll::Ready((Ok(0), buffer));
        }

        let buffer = unsafe { opt_mut(&mut this.buffer) };
        // SAFETY: `target` was validated against `writable_len()` above.
        let dst = unsafe { slice::from_raw_parts_mut(buffer.as_mut_ptr(), this.target) };
        match this.stream.poll_read_plaintext(cx, dst) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(read)) => {
                unsafe { buffer.set_written_len(read) };
                let buffer = unsafe { opt_take(&mut this.buffer) };
                Poll::Ready((Ok(read), buffer))
            }
            Poll::Ready(Err(err)) => {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                Poll::Ready((Err(err), buffer))
            }
        }
    }
}

/// TLS plaintext read-exact future.
#[doc(hidden)]
pub struct TlsReadExactFuture<'a, B: IoBuffReadWrite> {
    stream: &'a mut TlsClientStream,
    buffer: Option<B>,
    base_ptr: *mut u8,
    target: usize,
    filled: usize,
    input_error: Option<io::Error>,
}

impl<'a, B: IoBuffReadWrite> TlsReadExactFuture<'a, B> {
    fn new(stream: &'a mut TlsClientStream, mut buffer: B, len: usize) -> Self {
        let mut input_error = None;
        let target = match checked_read_len("tls read_exact", len, buffer.writable_len()) {
            Ok(target) => target as usize,
            Err(err) => {
                input_error = Some(err);
                0
            }
        };
        let base_ptr = buffer.as_mut_ptr();

        Self {
            stream,
            buffer: Some(buffer),
            base_ptr,
            target,
            filled: 0,
            input_error,
        }
    }
}

impl<B: IoBuffReadWrite> Future for TlsReadExactFuture<'_, B> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if let Some(err) = this.input_error.take() {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }

        if let Err(err) = this.stream.ensure_handshake_complete() {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }

        if this.target == 0 {
            let mut buffer = unsafe { opt_take(&mut this.buffer) };
            unsafe { buffer.set_written_len(0) };
            return Poll::Ready((Ok(0), buffer));
        }

        loop {
            let remaining = this.target - this.filled;
            // SAFETY: `base_ptr` is stable for the entire buffer lifetime and
            // `remaining` stays within the validated writable region.
            let dst =
                unsafe { slice::from_raw_parts_mut(this.base_ptr.add(this.filled), remaining) };
            match this.stream.poll_read_plaintext(cx, dst) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(0)) => {
                    let mut buffer = unsafe { opt_take(&mut this.buffer) };
                    unsafe { buffer.set_written_len(this.filled) };
                    return Poll::Ready((
                        Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
                        buffer,
                    ));
                }
                Poll::Ready(Ok(read)) => {
                    this.filled += read;
                    if this.filled == this.target {
                        let mut buffer = unsafe { opt_take(&mut this.buffer) };
                        unsafe { buffer.set_written_len(this.target) };
                        return Poll::Ready((Ok(this.target), buffer));
                    }
                }
                Poll::Ready(Err(err)) => {
                    let mut buffer = unsafe { opt_take(&mut this.buffer) };
                    unsafe { buffer.set_written_len(this.filled) };
                    return Poll::Ready((Err(err), buffer));
                }
            }
        }
    }
}

/// TLS plaintext write future.
#[doc(hidden)]
pub struct TlsWriteFuture<'a, B: IoBuffReadOnly> {
    stream: &'a mut TlsClientStream,
    buffer: Option<B>,
    written: Option<usize>,
}

impl<'a, B: IoBuffReadOnly> TlsWriteFuture<'a, B> {
    fn new(stream: &'a mut TlsClientStream, buffer: B) -> Self {
        Self {
            stream,
            buffer: Some(buffer),
            written: None,
        }
    }
}

impl<B: IoBuffReadOnly> Future for TlsWriteFuture<'_, B> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.written.is_none() {
            if let Err(err) = this.stream.ensure_handshake_complete() {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(err), buffer));
            }
            if let Err(err) = this.stream.ensure_writable() {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Err(err), buffer));
            }

            match this.stream.poll_flush_pending_tls(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(err), buffer));
                }
                Poll::Ready(Ok(())) => {}
            }

            let buffer = unsafe { opt_ref(&this.buffer) };
            let src = unsafe { slice::from_raw_parts(buffer.as_ptr(), buffer.len()) };
            let written = match this.stream.connection.writer().write(src) {
                Ok(written) => written,
                Err(err) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(err), buffer));
                }
            };
            if written == 0 && !src.is_empty() {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((
                    Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "rustls accepted zero plaintext bytes",
                    )),
                    buffer,
                ));
            }
            this.written = Some(written);
        }

        match this.stream.poll_flush_pending_tls(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(err)) => {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                Poll::Ready((Err(err), buffer))
            }
            Poll::Ready(Ok(())) => {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                Poll::Ready((Ok(this.written.unwrap_or(0)), buffer))
            }
        }
    }
}

/// TLS plaintext write-all future.
#[doc(hidden)]
pub struct TlsWriteAllFuture<'a, B: IoBuffReadOnly> {
    stream: &'a mut TlsClientStream,
    buffer: Option<B>,
    base_ptr: *const u8,
    total: usize,
    offset: usize,
}

impl<'a, B: IoBuffReadOnly> TlsWriteAllFuture<'a, B> {
    fn new(stream: &'a mut TlsClientStream, buffer: B) -> Self {
        let base_ptr = buffer.as_ptr();
        let total = buffer.len();
        Self {
            stream,
            buffer: Some(buffer),
            base_ptr,
            total,
            offset: 0,
        }
    }
}

impl<B: IoBuffReadOnly> Future for TlsWriteAllFuture<'_, B> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if let Err(err) = this.stream.ensure_handshake_complete() {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }
        if let Err(err) = this.stream.ensure_writable() {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }

        loop {
            match this.stream.poll_flush_pending_tls(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(err), buffer));
                }
                Poll::Ready(Ok(())) => {}
            }

            if this.offset == this.total {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((Ok(this.total), buffer));
            }

            // SAFETY: `offset <= total` and `base_ptr` is stable for the
            // lifetime of the caller-provided buffer.
            let src = unsafe {
                slice::from_raw_parts(this.base_ptr.add(this.offset), this.total - this.offset)
            };
            let written = match this.stream.connection.writer().write(src) {
                Ok(written) => written,
                Err(err) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready((Err(err), buffer));
                }
            };
            if written == 0 {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                return Poll::Ready((
                    Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "rustls write_all made no plaintext progress",
                    )),
                    buffer,
                ));
            }
            this.offset += written;
        }
    }
}

/// Flush future for pending TLS records.
#[doc(hidden)]
pub struct TlsFlushFuture<'a> {
    stream: &'a mut TlsClientStream,
}

impl Future for TlsFlushFuture<'_> {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        this.stream.poll_flush_pending_tls(cx)
    }
}

/// Shutdown future for the TLS write side.
#[doc(hidden)]
pub struct TlsShutdownFuture<'a> {
    stream: &'a mut TlsClientStream,
}

impl Future for TlsShutdownFuture<'_> {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if !this.stream.write_shutdown {
            this.stream.connection.send_close_notify();
            this.stream.write_shutdown = true;
        }

        match this.stream.poll_flush_pending_tls(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Ready(Ok(())) => {}
        }

        if !this.stream.transport_write_shutdown {
            this.stream.stream.shutdown(std::net::Shutdown::Write)?;
            this.stream.transport_write_shutdown = true;
        }

        Poll::Ready(Ok(()))
    }
}
