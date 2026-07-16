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
//! # Fast-Path Guidance
//!
//! Preferred on the established-connection fast path:
//! - Keep one [`TlsClientStream`] alive for the lifetime of the connection.
//!   Build it once around an already-connected [`TcpStream`], then drive
//!   [`TlsClientStream::handshake`] exactly once before application I/O.
//! - Reuse a caller-owned [`Arc<rustls::ClientConfig>`] across connections.
//! - After the handshake, prefer [`TlsClientStream::read`] /
//!   [`TlsClientStream::write`] when the caller can handle partial plaintext
//!   progress. Those APIs do not loop to fill or consume the caller's complete
//!   plaintext buffer, although TLS record processing can still require
//!   multiple raw TCP operations.
//!
//! Avoid on the established-connection fast path:
//! - Avoid [`TlsClientStream::read_exact`] /
//!   [`TlsClientStream::write_all`] unless complete-buffer semantics are
//!   required. Use [`TlsClientStream::read`] / [`TlsClientStream::write`]
//!   instead when the caller can track progress explicitly.
//! - Avoid putting DNS resolution, TCP connection establishment, wrapper
//!   construction, or [`TlsClientStream::handshake`] inside a per-message
//!   loop. Reuse the established stream.
//! - Do not assume the TLS data path is allocator-free: rustls owns protocol
//!   buffers, and the wrapper's write scratch may grow beyond its configured
//!   initial capacity.
//!
//! The example below uses `write_all` / `read_exact` because it makes the
//! framing obvious. On the hot path, prefer `write` / `read` when the caller
//! can handle partial plaintext progress explicitly.
//!
//! # Example
//! ```no_run
//! use flowio::net::tcp::TcpConnector;
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
//! let mut connector = TcpConnector::new();
//! executor.run(async move {
//!     let tcp = connector
//!         .connect(SocketAddr::from((Ipv4Addr::LOCALHOST, 5432)))
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
//!     // Application reply bytes come from the peer; this is only the receive shape.
//!     let (res, recv) = tls.read_exact(vec![0u8; 5], 5).await;
//!     res.unwrap();
//!     let _reply = recv;
//!
//!     tls.shutdown().await.unwrap();
//! })?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use super::stream;
use super::tcp::TcpStream;
use super::{checked_read_len, opt_mut, opt_ref, opt_take};
use crate::net::complete_read_with_progress;
use crate::runtime::buffer::{IoBuffReadOnly, IoBuffReadWrite, readable_slice};
use rustls::ClientConfig;
use rustls::client::ClientConnection;
use rustls::pki_types::ServerName;
use rustls::pki_types::alg_id::{
    ECDSA_SHA256, ECDSA_SHA384, ECDSA_SHA512, ED448, ED25519, RSA_PKCS1_SHA256, RSA_PKCS1_SHA384,
    RSA_PKCS1_SHA512, RSA_PSS_SHA256, RSA_PSS_SHA384, RSA_PSS_SHA512,
};
use sha2::{Digest, Sha256, Sha384, Sha512};
use std::future::Future;
use std::io::{self, Cursor, Read, Write};
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::slice;
use std::sync::Arc;
use std::task::{Context, Poll};

// 1.2.840.113549.1.1.4 md5WithRSAEncryption
const RSA_PKCS1_MD5: &[u8] = &[
    0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x04, 0x05, 0x00,
];
// 1.2.840.113549.1.1.5 sha1WithRSAEncryption
const RSA_PKCS1_SHA1: &[u8] = &[
    0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x05, 0x05, 0x00,
];
// 1.2.840.10040.4.3 dsa-with-SHA1
const DSA_SHA1: &[u8] = &[0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x38, 0x04, 0x03];
// 1.2.840.10045.4.1 ecdsa-with-SHA1
const ECDSA_SHA1: &[u8] = &[0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x01];

type PendingTlsRead = stream::ReadFuture<'static, Vec<u8>, TlsTransportMarker>;
type PendingTlsWrite = stream::WriteAllFuture<'static, Vec<u8>, TlsTransportMarker>;

/// rustls deframer `MAX_WIRE_SIZE`; keep each raw read to at most one TLS
/// record so ciphertext and plaintext staging are both drained between feeds.
const TLS_MAX_WIRE_READ_SIZE: usize = 18_437;

/// Marker type used when the shared stream futures are driving raw TLS record
/// I/O for the wrapper instead of borrowing a public transport object.
struct TlsTransportMarker;

#[inline]
/// Maps rustls protocol errors into I/O-facing invalid-data failures.
fn tls_protocol_error(err: rustls::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}

#[inline]
/// Builds an internal-invariant error for unexpected wrapper state.
fn tls_internal_error(message: &'static str) -> io::Error {
    io::Error::other(message)
}

#[derive(Clone, Copy)]
enum TlsScratchKind {
    Read,
    Write,
}

impl TlsScratchKind {
    const fn zero_size_message(self) -> &'static str {
        match self {
            Self::Read => "transport_read_buffer_size must be greater than zero",
            Self::Write => "transport_write_buffer_size must be greater than zero",
        }
    }

    const fn impossible_size_message(self) -> &'static str {
        match self {
            Self::Read => "transport_read_buffer_size exceeds the maximum allocation size",
            Self::Write => "transport_write_buffer_size exceeds the maximum allocation size",
        }
    }
}

fn validate_tls_scratch_size(kind: TlsScratchKind, capacity: usize) -> io::Result<()> {
    if capacity == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            kind.zero_size_message(),
        ));
    }
    if capacity > isize::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            kind.impossible_size_message(),
        ));
    }

    Ok(())
}

fn reserve_valid_tls_scratch(capacity: usize) -> io::Result<Vec<u8>> {
    let mut buffer = Vec::new();
    buffer
        .try_reserve_exact(capacity)
        .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
    Ok(buffer)
}

fn allocate_tls_scratch(kind: TlsScratchKind, capacity: usize) -> io::Result<Vec<u8>> {
    validate_tls_scratch_size(kind, capacity)?;
    reserve_valid_tls_scratch(capacity)
}

fn take_or_reserve_tls_scratch(
    available: &mut Option<Vec<u8>>,
    kind: TlsScratchKind,
    capacity: usize,
) -> io::Result<Vec<u8>> {
    match available.take() {
        Some(buffer) => Ok(buffer),
        None => allocate_tls_scratch(kind, capacity),
    }
}

/// Returns one userspace-safe mutable destination for a TLS plaintext read.
///
/// The first call initializes the complete destination through the buffer's
/// userspace hook. Later polls reacquire the buffer's current raw pointer and
/// reuse that initialized range without repeating the initialization pass.
///
/// # Safety
///
/// `len` must be nonzero and validated against `buffer.writable_len()`. When
/// `initialized` is true, the same buffer and `len` must have completed an
/// earlier call, with no intervening logical-length or writable-base change.
#[inline(always)]
unsafe fn tls_userspace_destination<'a, B: IoBuffReadWrite>(
    buffer: &'a mut B,
    initialized: &mut bool,
    len: usize,
) -> &'a mut [u8] {
    if !*initialized {
        let dst = unsafe { buffer.initialized_writable_slice(len) };
        *initialized = true;
        return dst;
    }
    unsafe { slice::from_raw_parts_mut(buffer.as_mut_ptr(), len) }
}

/// Explicit TLS wrapper buffer and rustls buffering configuration.
///
/// This type intentionally does not implement `Default` so callers must make
/// the buffering decision explicitly.
///
/// `transport_read_buffer_size` is the nonzero reusable ciphertext scratch
/// capacity;
/// each submitted raw read is bounded by rustls' maximum wire-record size so
/// internal rustls ciphertext and plaintext buffers are drained between records.
/// `transport_write_buffer_size` is the nonzero initial capacity used when
/// collecting TLS records emitted by rustls before writing them to the socket;
/// the buffer may still grow if rustls emits more than the initial capacity in
/// one flush cycle. Both capacities must fit in `isize` and are reserved
/// fallibly when the wrapper is created.
///
/// For steady-state use, pick values once per connection profile and reuse
/// them. Recomputing or reallocating these choices per operation is not the
/// intended fast path.
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
    /// Nonzero capacity of the reusable ciphertext receive scratch buffer used
    /// for `read_tls`. The value must not exceed `isize::MAX`.
    pub transport_read_buffer_size: usize,
    /// Nonzero initial capacity of the reusable ciphertext send scratch buffer
    /// used for `write_tls`. The value must not exceed `isize::MAX`.
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
/// Preferred on the established-connection fast path:
/// - Use one long-lived stream per connection, call [`TlsClientStream::handshake`]
///   once, then prefer [`TlsClientStream::read`] / [`TlsClientStream::write`]
///   for steady-state plaintext I/O.
///
/// Avoid on the established-connection fast path:
/// - Avoid [`TlsClientStream::read_exact`] /
///   [`TlsClientStream::write_all`] unless the protocol truly requires
///   complete-buffer semantics.
/// - Do not reconstruct or re-handshake the wrapper per message. rustls owns
///   protocol buffers, and the reusable ciphertext buffers belong to this
///   connection.
///
/// # Cancellation semantics
/// Dropping a `handshake`, `read`, `write`, `flush`, or `shutdown` future does
/// not discard already-started raw transport work. Any in-flight TLS record
/// read/write remains owned by the stream and will be resumed or retired by
/// the next TLS operation. This keeps TLS record handling correct without
/// introducing background threads or a broader transport abstraction.
///
/// If a raw transport write fails after rustls has emitted TLS records, the
/// stream is marked failed. Retrying the same plaintext on that stream could
/// duplicate records that the kernel already accepted, so later TLS write,
/// flush, and shutdown operations return `BrokenPipe`. The read side may still
/// drain already-decrypted plaintext and continue reading the transport until
/// EOF; if a read requires a TLS transport write to make progress after the
/// write latch is set, that read returns `BrokenPipe`.
///
/// Ordinary TLS operations reuse the two reserved ciphertext buffers. If an
/// earlier exceptional path leaves one unavailable, the operation that needs
/// it recreates it fallibly and can return `OutOfMemory`.
pub struct TlsClientStream {
    /// Underlying connected TCP transport owned by this TLS wrapper.
    stream: TcpStream,
    /// rustls client connection holding TLS protocol state and plaintext
    /// buffers.
    connection: ClientConnection,
    /// Capacity used when creating new raw TLS receive buffers.
    transport_read_buffer_size: usize,
    /// Initial capacity used when collecting rustls-emitted TLS records.
    transport_write_buffer_size: usize,
    /// Available reusable ciphertext receive buffer. This is `None` while a
    /// pending raw read owns the buffer.
    read_tls_buffer: Option<Vec<u8>>,
    /// Available reusable ciphertext send buffer. This is `None` while a
    /// pending raw write owns the buffer.
    write_tls_buffer: Option<Vec<u8>>,
    /// In-flight raw transport read, if a TLS record read has already been
    /// submitted.
    pending_read_tls: Option<PendingTlsRead>,
    /// In-flight raw transport write, if emitted TLS records are still being
    /// flushed.
    pending_write_tls: Option<PendingTlsWrite>,
    /// True after the TCP read side has returned EOF and rustls has been
    /// notified.
    transport_read_eof: bool,
    /// True after a raw transport write failure made queued TLS records
    /// non-retryable without risking record duplication.
    transport_write_failed: bool,
    /// True after rustls has started TLS-level close-notify shutdown.
    write_shutdown: bool,
    /// True after the underlying TCP stream has been shutdown for writes.
    transport_write_shutdown: bool,
}

fn matches_signature_algorithm(signature_algorithm: &[u8], candidates: &[&[u8]]) -> bool {
    candidates.contains(&signature_algorithm)
}

/// Derives RFC 5929 `tls-server-end-point` channel-binding bytes from an
/// end-entity certificate DER blob.
///
/// Returns `None` when the certificate DER is malformed or its signature
/// algorithm is unsupported for this derivation. Unsupported cases include
/// algorithms without a defined binding digest, such as Ed25519 and Ed448.
///
/// This allocates the returned channel-binding bytes. Call it after the TLS
/// handshake when a protocol needs the binding value; it is not steady-state
/// I/O fast-path work.
///
/// # Example
/// ```
/// use flowio::net::tls::tls_server_end_point;
///
/// assert!(tls_server_end_point(&[]).is_none());
/// ```
pub fn tls_server_end_point(certificate_der: &[u8]) -> Option<Vec<u8>> {
    let signature_algorithm = extract_certificate_signature_algorithm(certificate_der)?;

    if signature_algorithm == ED25519.as_ref() || signature_algorithm == ED448.as_ref() {
        return None;
    }

    if matches_signature_algorithm(
        signature_algorithm,
        &[
            RSA_PKCS1_MD5,
            RSA_PKCS1_SHA1,
            DSA_SHA1,
            ECDSA_SHA1,
            RSA_PKCS1_SHA256.as_ref(),
            ECDSA_SHA256.as_ref(),
            RSA_PSS_SHA256.as_ref(),
        ],
    ) {
        return Some(Sha256::digest(certificate_der).to_vec());
    }

    if matches_signature_algorithm(
        signature_algorithm,
        &[
            RSA_PKCS1_SHA384.as_ref(),
            ECDSA_SHA384.as_ref(),
            RSA_PSS_SHA384.as_ref(),
        ],
    ) {
        return Some(Sha384::digest(certificate_der).to_vec());
    }

    if signature_algorithm == RSA_PKCS1_SHA512.as_ref()
        || signature_algorithm == ECDSA_SHA512.as_ref()
        || signature_algorithm == RSA_PSS_SHA512.as_ref()
    {
        return Some(Sha512::digest(certificate_der).to_vec());
    }

    None
}

/// Extracts the certificate signature-algorithm identifier from the outer
/// certificate sequence.
fn extract_certificate_signature_algorithm(certificate_der: &[u8]) -> Option<&[u8]> {
    let (tag, header_len, body_len) = read_tlv(certificate_der, 0)?;
    if tag != 0x30 {
        return None;
    }

    let body_start = header_len;
    let body_end = body_start.checked_add(body_len)?;
    if body_end != certificate_der.len() {
        return None;
    }

    let (_, first_header_len, first_body_len) = read_tlv(certificate_der, body_start)?;
    let first_end = body_start
        .checked_add(first_header_len)?
        .checked_add(first_body_len)?;
    let (second_tag, second_header_len, second_body_len) = read_tlv(certificate_der, first_end)?;
    if second_tag != 0x30 {
        return None;
    }

    let second_body_start = first_end.checked_add(second_header_len)?;
    let second_end = first_end
        .checked_add(second_header_len)?
        .checked_add(second_body_len)?;
    certificate_der.get(second_body_start..second_end)
}

/// Reads one DER tag-length-value header and returns its tag, header length,
/// and body length.
fn read_tlv(bytes: &[u8], offset: usize) -> Option<(u8, usize, usize)> {
    let tag = *bytes.get(offset)?;
    let first_len = *bytes.get(offset + 1)?;

    if first_len & 0x80 == 0 {
        let body_len = first_len as usize;
        let header_len = 2usize;
        let end = offset.checked_add(header_len)?.checked_add(body_len)?;
        if end > bytes.len() {
            return None;
        }
        return Some((tag, header_len, body_len));
    }

    let len_octets = (first_len & 0x7f) as usize;
    if len_octets == 0 || len_octets > std::mem::size_of::<usize>() {
        return None;
    }

    let mut body_len = 0usize;
    let len_start = offset.checked_add(2)?;
    let len_end = len_start.checked_add(len_octets)?;
    let len_bytes = bytes.get(len_start..len_end)?;

    for &octet in len_bytes {
        body_len = body_len.checked_shl(8)? | octet as usize;
    }

    let header_len = 2usize.checked_add(len_octets)?;
    let end = offset.checked_add(header_len)?.checked_add(body_len)?;
    if end > bytes.len() {
        return None;
    }

    Some((tag, header_len, body_len))
}

impl TlsClientStream {
    /// Creates a new TLS client wrapper around an already-connected TCP stream.
    ///
    /// This allocates the wrapper's reusable ciphertext scratch buffers up
    /// front using the capacities provided in [`TlsClientOptions`].
    ///
    /// This is connection-setup work. The intended fast path is to construct
    /// the wrapper once per connection and reuse it for the session lifetime.
    ///
    /// # Errors
    /// Returns `InvalidInput` if either transport scratch buffer size is zero
    /// or cannot be represented by a `Vec<u8>` allocation. Returns
    /// `OutOfMemory` if either scratch reservation fails.
    /// Returns `InvalidData` if rustls rejects the supplied config or server
    /// name while constructing the client connection.
    ///
    /// # Example
    /// ```no_run
    /// use flowio::net::tcp::TcpConnector;
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
    /// let options = TlsClientOptions {
    ///     rustls_buffer_limit: Some(16 * 1024),
    ///     transport_read_buffer_size: 16 * 1024,
    ///     transport_write_buffer_size: 16 * 1024,
    /// };
    ///
    /// let mut executor = Executor::new()?;
    /// let mut connector = TcpConnector::new();
    /// executor.run(async move {
    ///     let tcp = connector
    ///         .connect(SocketAddr::from((Ipv4Addr::LOCALHOST, 5432)))
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
        validate_tls_scratch_size(TlsScratchKind::Read, options.transport_read_buffer_size)?;
        validate_tls_scratch_size(TlsScratchKind::Write, options.transport_write_buffer_size)?;

        let read_tls_buffer = reserve_valid_tls_scratch(options.transport_read_buffer_size)?;
        let write_tls_buffer = reserve_valid_tls_scratch(options.transport_write_buffer_size)?;

        let mut connection =
            ClientConnection::new(config, server_name).map_err(tls_protocol_error)?;
        connection.set_buffer_limit(options.rustls_buffer_limit);

        Ok(Self {
            stream,
            connection,
            transport_read_buffer_size: options.transport_read_buffer_size,
            transport_write_buffer_size: options.transport_write_buffer_size,
            read_tls_buffer: Some(read_tls_buffer),
            write_tls_buffer: Some(write_tls_buffer),
            pending_read_tls: None,
            pending_write_tls: None,
            transport_read_eof: false,
            transport_write_failed: false,
            write_shutdown: false,
            transport_write_shutdown: false,
        })
    }

    /// Returns `true` while the TLS handshake is still in progress.
    ///
    /// This is a handshake-status/control-plane query, not a steady-state
    /// data-path call.
    pub fn is_handshaking(&self) -> bool {
        self.connection.is_handshaking()
    }

    /// Starts driving the TLS handshake.
    ///
    /// The handshake is not performed implicitly by `read`, `write`, or
    /// `flush`. Callers must drive it explicitly before application I/O.
    ///
    /// This is a connection-setup API, not a steady-state data-path API.
    pub fn handshake(&mut self) -> TlsHandshakeFuture<'_> {
        TlsHandshakeFuture { stream: self }
    }

    /// Reads up to `len` decrypted plaintext bytes into `buffer`.
    ///
    /// Positive progress appends to an `IoBuffMut` payload; buffers that keep
    /// the provided zero write base publish from their beginning. A clean
    /// zero-byte close or an error before plaintext progress preserves existing
    /// logical contents. The returned count is relative to this read.
    ///
    /// # Errors
    /// Returns `NotConnected` if the TLS handshake has not completed yet.
    /// A clean TLS close returns `Ok(0)`.
    /// After a raw transport write failure, already-decrypted plaintext may
    /// still be returned; if a read needs to emit TLS records to make progress
    /// after that latch is set, it returns `BrokenPipe`.
    ///
    /// Preferred when the caller tracks plaintext framing. This returns after
    /// one plaintext read instead of looping to fill `len`; obtaining that
    /// plaintext may still require processing multiple TLS records or raw TCP
    /// operations.
    pub fn read<B: IoBuffReadWrite>(&mut self, buffer: B, len: usize) -> TlsReadFuture<'_, B> {
        TlsReadFuture::new(self, buffer, len)
    }

    /// Reads exactly `len` decrypted plaintext bytes into `buffer`.
    ///
    /// Positive progress follows the same relative-publication contract as
    /// [`TlsClientStream::read`]. A terminal error publishes only plaintext
    /// completed by this operation and preserves the earlier prefix.
    ///
    /// # Errors
    /// Returns `NotConnected` if the TLS handshake has not completed yet.
    /// Returns `UnexpectedEof` if the TLS session reaches EOF before `len`
    /// plaintext bytes become available; any partial plaintext read into the
    /// caller buffer remains published in that returned buffer.
    /// After a raw transport write failure, already-decrypted plaintext may
    /// still be returned; if a read needs to emit TLS records to make progress
    /// after that latch is set, it returns `BrokenPipe`.
    ///
    /// This complete-buffer API loops until the requested plaintext length is
    /// filled. Avoid that loop when exact-length semantics are unnecessary;
    /// use [`TlsClientStream::read`] and track progress in the caller.
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
    ///
    /// # Errors
    /// Returns `NotConnected` if called before handshake completion and
    /// `BrokenPipe` if the TLS write side has already been shut down or a
    /// prior raw transport write failed.
    /// If rustls accepts plaintext but the following TLS-record flush fails,
    /// this future returns an error without a progress count; the stream is
    /// failed and callers must not retry the same plaintext on it.
    ///
    /// Preferred when the caller tracks plaintext progress. This offers the
    /// plaintext to rustls once and returns the accepted count after flushing
    /// the resulting records; that flush may require multiple raw TCP writes.
    pub fn write<B: IoBuffReadOnly>(&mut self, buffer: B) -> TlsWriteFuture<'_, B> {
        TlsWriteFuture::new(self, buffer)
    }

    /// Writes the entire plaintext buffer, draining TLS records as needed.
    ///
    /// # Errors
    /// Returns `NotConnected` if called before handshake completion and
    /// `BrokenPipe` if the TLS write side has already been shut down or a
    /// prior raw transport write failed.
    /// If rustls accepts plaintext but a later TLS-record flush fails, this
    /// future returns an error even though some plaintext may already be queued
    /// as TLS records; the stream is failed and callers must not retry the
    /// same plaintext on it.
    ///
    /// This complete-buffer API repeatedly offers plaintext to rustls until
    /// the full input is accepted. Avoid that loop when complete-buffer
    /// semantics are unnecessary; use [`TlsClientStream::write`] and track
    /// progress in the caller.
    pub fn write_all<B: IoBuffReadOnly>(&mut self, buffer: B) -> TlsWriteAllFuture<'_, B> {
        TlsWriteAllFuture::new(self, buffer)
    }

    /// Flushes any pending TLS records already queued inside rustls or staged
    /// for the underlying TCP stream.
    ///
    /// This does not advance the TLS handshake by itself beyond draining
    /// already-generated outbound records.
    ///
    /// # Errors
    /// Returns `BrokenPipe` if a prior raw transport write failed.
    ///
    /// This is primarily a control-path API for callers that need an explicit
    /// flush boundary.
    pub fn flush(&mut self) -> TlsFlushFuture<'_> {
        TlsFlushFuture { stream: self }
    }

    /// Sends `close_notify`, flushes it, and then shuts down the TCP write side.
    ///
    /// After this completes, further plaintext writes return `BrokenPipe`.
    /// The read side remains available until the peer closes its direction.
    ///
    /// # Errors
    /// If the transport write side has previously failed, shutdown returns
    /// `BrokenPipe` while reads can still drain plaintext that does not require
    /// emitting more TLS records.
    /// This is a shutdown-path API rather than a steady-state fast-path API.
    pub fn shutdown(&mut self) -> TlsShutdownFuture<'_> {
        TlsShutdownFuture { stream: self }
    }

    /// Returns the end-entity certificate DER after handshake completion.
    ///
    /// Callers can pass this to [`tls_server_end_point`] when a
    /// protocol-specific consumer, such as PostgreSQL, needs that channel
    /// binding value.
    /// Accessing this once after the handshake is the intended usage; it is
    /// not part of the steady-state I/O fast path.
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
        if self.transport_write_failed {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "tls transport write failed",
            ));
        }
        if self.write_shutdown {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "tls write side already shut down",
            ));
        }

        Ok(())
    }

    fn take_read_tls_buffer(&mut self) -> io::Result<Vec<u8>> {
        take_or_reserve_tls_scratch(
            &mut self.read_tls_buffer,
            TlsScratchKind::Read,
            self.transport_read_buffer_size,
        )
    }

    // Returns a read scratch buffer to the stream after clearing any bytes
    // that were filled by the last transport read attempt.
    fn restore_read_tls_buffer(&mut self, mut buffer: Vec<u8>) {
        buffer.clear();
        if self.read_tls_buffer.is_none() {
            self.read_tls_buffer = Some(buffer);
        }
    }

    fn take_write_tls_buffer(&mut self) -> io::Result<Vec<u8>> {
        take_or_reserve_tls_scratch(
            &mut self.write_tls_buffer,
            TlsScratchKind::Write,
            self.transport_write_buffer_size,
        )
    }

    // Returns a write scratch buffer to the stream after clearing any emitted
    // TLS records from the previous flush cycle.
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
            self.connection
                .process_new_packets()
                .map_err(tls_protocol_error)?;
        }
        Ok(())
    }

    fn feed_transport_eof(&mut self) -> io::Result<()> {
        self.transport_read_eof = true;
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
        if self.transport_write_failed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "tls transport write failed",
            )));
        }

        loop {
            if let Some(future) = self.pending_write_tls.as_mut() {
                match Pin::new(future).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready((result, buffer)) => {
                        self.pending_write_tls = None;
                        match result {
                            Ok(_) => {
                                self.restore_write_tls_buffer(buffer);
                            }
                            Err(err) => {
                                self.transport_write_failed = true;
                                self.restore_write_tls_buffer(buffer);
                                return Poll::Ready(Err(err));
                            }
                        }
                        continue;
                    }
                }
            }

            if !self.connection.wants_write() {
                return Poll::Ready(Ok(()));
            }

            let mut buffer = self.take_write_tls_buffer()?;
            buffer.clear();

            let mut total = 0usize;
            while self.connection.wants_write() {
                let wrote = self.connection.write_tls(&mut buffer)?;
                if wrote == 0 {
                    self.transport_write_failed = true;
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
                        let result = match result {
                            Ok(0) => self.feed_transport_eof(),
                            Ok(read) => self.feed_transport_bytes(&buffer[..read]),
                            Err(err) => Err(err),
                        };
                        self.restore_read_tls_buffer(buffer);
                        if let Err(err) = result {
                            return Poll::Ready(Err(err));
                        } else {
                            return Poll::Ready(Ok(()));
                        }
                    }
                }
            }

            if !self.connection.wants_read() {
                return Poll::Ready(Ok(()));
            }

            if self.transport_read_eof {
                if self.connection.is_handshaking() {
                    return Poll::Ready(Err(io::Error::from(io::ErrorKind::UnexpectedEof)));
                }
                return Poll::Ready(Ok(()));
            }

            if self.pending_write_tls.is_some() {
                return Poll::Ready(Err(tls_internal_error("tls transport write/read overlap")));
            }

            let buffer = self.take_read_tls_buffer()?;
            let len = buffer.capacity().min(TLS_MAX_WIRE_READ_SIZE);
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
            match self.connection.reader().read(dst) {
                Ok(read) => return Poll::Ready(Ok(read)),
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
                Err(err) => return Poll::Ready(Err(err)),
            }

            if self.pending_write_tls.is_some() {
                match self.poll_flush_pending_tls(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Ready(Ok(())) => continue,
                }
            }

            if self.pending_read_tls.is_some() || self.connection.wants_read() {
                match self.poll_fill_tls_from_transport(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Ready(Ok(())) => continue,
                }
            }

            if self.connection.wants_write() {
                match self.poll_flush_pending_tls(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Ready(Ok(())) => continue,
                }
            }

            return Poll::Ready(Err(tls_internal_error(
                "tls reader reported WouldBlock without transport demand",
            )));
        }
    }
}

impl Drop for TlsClientStream {
    fn drop(&mut self) {
        // Dropping the staged raw futures invokes their normal io_uring
        // cancellation/orphan cleanup before the owned TCP descriptor drops.
        self.pending_read_tls = None;
        self.pending_write_tls = None;
    }
}

/// Explicit TLS handshake future.
#[doc(hidden)]
pub struct TlsHandshakeFuture<'a> {
    /// TLS stream whose handshake state is being driven.
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
    /// TLS stream providing decrypted plaintext.
    stream: &'a mut TlsClientStream,
    /// Caller-owned destination buffer returned on completion.
    buffer: Option<B>,
    /// Whether the complete destination was initialized on an earlier poll.
    destination_initialized: bool,
    /// Logical readable length present before this read started.
    write_base_len: usize,
    /// Maximum plaintext bytes requested from the TLS reader.
    target: usize,
    /// Deferred validation error returned before any TLS work starts.
    input_error: Option<io::Error>,
}

impl<'a, B: IoBuffReadWrite> TlsReadFuture<'a, B> {
    fn new(stream: &'a mut TlsClientStream, buffer: B, len: usize) -> Self {
        let mut input_error = None;
        let write_base_len = buffer.write_base_len();
        let target = match checked_read_len(len, buffer.writable_len()) {
            Ok(target) => target as usize,
            Err(err) => {
                input_error = Some(err);
                0
            }
        };

        Self {
            stream,
            buffer: Some(buffer),
            destination_initialized: false,
            write_base_len,
            target,
            input_error,
        }
    }
}

impl<B: IoBuffReadWrite> Future for TlsReadFuture<'_, B> {
    type Output = (io::Result<usize>, B);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.buffer.is_none() {
            return Poll::Pending;
        }

        if let Some(err) = this.input_error.take() {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }

        if let Err(err) = this.stream.ensure_handshake_complete() {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }

        if this.target == 0 {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready(unsafe {
                complete_read_with_progress(buffer, this.write_base_len, 0, Ok(0))
            });
        }

        let buffer = unsafe { opt_mut(&mut this.buffer) };
        // SAFETY: `target` is nonzero and was validated against
        // `writable_len()` above. This future does not change the buffer's
        // writable base or logical length before returning it.
        let dst = unsafe {
            tls_userspace_destination(buffer, &mut this.destination_initialized, this.target)
        };
        match this.stream.poll_read_plaintext(cx, dst) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(read)) => {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                Poll::Ready(unsafe {
                    complete_read_with_progress(buffer, this.write_base_len, read, Ok(read))
                })
            }
            Poll::Ready(Err(err)) => {
                let buffer = unsafe { opt_take(&mut this.buffer) };
                Poll::Ready(unsafe {
                    complete_read_with_progress(buffer, this.write_base_len, 0, Err(err))
                })
            }
        }
    }
}

/// TLS plaintext read-exact future.
#[doc(hidden)]
pub struct TlsReadExactFuture<'a, B: IoBuffReadWrite> {
    /// TLS stream providing decrypted plaintext.
    stream: &'a mut TlsClientStream,
    /// Caller-owned destination buffer returned on completion.
    buffer: Option<B>,
    /// Whether the complete destination was initialized on an earlier poll.
    destination_initialized: bool,
    /// Logical readable length present before this read started.
    write_base_len: usize,
    /// Total plaintext bytes required before completion.
    target: usize,
    /// Plaintext bytes already written into the caller buffer.
    filled: usize,
    /// Deferred validation error returned before any TLS work starts.
    input_error: Option<io::Error>,
}

impl<'a, B: IoBuffReadWrite> TlsReadExactFuture<'a, B> {
    fn new(stream: &'a mut TlsClientStream, buffer: B, len: usize) -> Self {
        let mut input_error = None;
        let write_base_len = buffer.write_base_len();
        let target = match checked_read_len(len, buffer.writable_len()) {
            Ok(target) => target as usize,
            Err(err) => {
                input_error = Some(err);
                0
            }
        };
        Self {
            stream,
            buffer: Some(buffer),
            destination_initialized: false,
            write_base_len,
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

        if this.buffer.is_none() {
            return Poll::Pending;
        }

        if let Some(err) = this.input_error.take() {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }

        if let Err(err) = this.stream.ensure_handshake_complete() {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
        }

        if this.target == 0 {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready(unsafe {
                complete_read_with_progress(buffer, this.write_base_len, 0, Ok(0))
            });
        }

        loop {
            let remaining = this.target - this.filled;
            let buffer = unsafe { opt_mut(&mut this.buffer) };
            // SAFETY: `target` is nonzero and was validated against
            // `writable_len()` above. Neither the buffer's writable base nor
            // logical length changes until completion publishes `filled`
            // bytes.
            let dst = unsafe {
                tls_userspace_destination(buffer, &mut this.destination_initialized, this.target)
            };
            let dst = &mut dst[this.filled..this.filled + remaining];
            match this.stream.poll_read_plaintext(cx, dst) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(0)) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready(unsafe {
                        complete_read_with_progress(
                            buffer,
                            this.write_base_len,
                            this.filled,
                            Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
                        )
                    });
                }
                Poll::Ready(Ok(read)) => {
                    this.filled += read;
                    if this.filled == this.target {
                        let buffer = unsafe { opt_take(&mut this.buffer) };
                        return Poll::Ready(unsafe {
                            complete_read_with_progress(
                                buffer,
                                this.write_base_len,
                                this.target,
                                Ok(this.target),
                            )
                        });
                    }
                }
                Poll::Ready(Err(err)) => {
                    let buffer = unsafe { opt_take(&mut this.buffer) };
                    return Poll::Ready(unsafe {
                        complete_read_with_progress(
                            buffer,
                            this.write_base_len,
                            this.filled,
                            Err(err),
                        )
                    });
                }
            }
        }
    }
}

/// TLS plaintext write future.
#[doc(hidden)]
pub struct TlsWriteFuture<'a, B: IoBuffReadOnly> {
    /// TLS stream that will accept plaintext and flush resulting records.
    stream: &'a mut TlsClientStream,
    /// Caller-owned plaintext buffer returned on completion.
    buffer: Option<B>,
    /// Amount of plaintext accepted by rustls once the first write occurs.
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

        if this.buffer.is_none() {
            return Poll::Pending;
        }

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
            let src = readable_slice(buffer);
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
    /// TLS stream that will accept plaintext and flush resulting records.
    stream: &'a mut TlsClientStream,
    /// Caller-owned plaintext buffer returned on completion.
    buffer: Option<B>,
    /// Stable base pointer captured once for incremental plaintext writes.
    base_ptr: *const u8,
    /// Total plaintext byte count that must be accepted by rustls.
    total: usize,
    /// Plaintext bytes already accepted by rustls.
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

        if this.buffer.is_none() {
            return Poll::Pending;
        }

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
    /// TLS stream whose queued outbound records are being drained.
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
    /// TLS stream whose write side is being closed.
    stream: &'a mut TlsClientStream,
}

impl Future for TlsShutdownFuture<'_> {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.stream.transport_write_failed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "tls transport write failed",
            )));
        }

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

#[cfg(test)]
mod tests {
    #[cfg(not(miri))]
    use super::*;
    use super::{
        TlsScratchKind, allocate_tls_scratch, take_or_reserve_tls_scratch,
        tls_userspace_destination,
    };
    #[cfg(all(debug_assertions, feature = "test-support", not(miri)))]
    use crate::net::tls_test_peer;
    use crate::runtime::buffer::{IoBuffError, IoBuffMut, IoBuffReadWrite};
    #[cfg(not(miri))]
    use crate::runtime::executor::Executor;
    #[cfg(not(miri))]
    use rustls::RootCertStore;
    #[cfg(not(miri))]
    use std::net::{Ipv4Addr, SocketAddr};

    #[test]
    fn tls_scratch_sizes_reject_zero_and_impossible_geometry() {
        for (kind, field) in [
            (TlsScratchKind::Read, "transport_read_buffer_size"),
            (TlsScratchKind::Write, "transport_write_buffer_size"),
        ] {
            for capacity in [0, usize::MAX] {
                let err = allocate_tls_scratch(kind, capacity)
                    .expect_err("invalid TLS scratch geometry should fail");
                assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
                assert!(err.to_string().contains(field));
            }
        }
    }

    #[test]
    fn tls_scratch_reserves_valid_small_common_and_large_profiles() {
        for kind in [TlsScratchKind::Read, TlsScratchKind::Write] {
            for capacity in [1, 16 * 1024, 64 * 1024] {
                let scratch = allocate_tls_scratch(kind, capacity)
                    .expect("valid TLS scratch reservation should succeed");
                assert!(scratch.is_empty());
                assert!(scratch.capacity() >= capacity);
            }
        }
    }

    #[test]
    fn missing_tls_scratch_uses_the_same_fallible_reservation_path() {
        let mut read = None;
        let scratch = take_or_reserve_tls_scratch(&mut read, TlsScratchKind::Read, 1024)
            .expect("valid fallback reservation should succeed");
        assert!(scratch.is_empty());
        assert!(scratch.capacity() >= 1024);

        let mut write = None;
        let err = take_or_reserve_tls_scratch(&mut write, TlsScratchKind::Write, usize::MAX)
            .expect_err("impossible fallback reservation should fail");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    struct DefaultUninitializedBuffer {
        storage: Box<[std::mem::MaybeUninit<u8>]>,
        written: usize,
    }

    impl DefaultUninitializedBuffer {
        fn new(capacity: usize) -> Self {
            Self {
                storage: Box::new_uninit_slice(capacity),
                written: 0,
            }
        }
    }

    // SAFETY: the boxed allocation is pointer-stable across moves and contains
    // `storage.len()` writable bytes. The provided userspace initializer is
    // intentionally inherited to model a downstream custom implementation.
    unsafe impl IoBuffReadWrite for DefaultUninitializedBuffer {
        fn as_mut_ptr(&mut self) -> *mut u8 {
            self.storage.as_mut_ptr().cast()
        }

        fn writable_len(&self) -> usize {
            self.storage.len()
        }

        unsafe fn set_written_len(&mut self, len: usize) {
            self.written = len.min(self.storage.len());
        }
    }

    struct CountingUninitializedBuffer {
        storage: Box<[std::mem::MaybeUninit<u8>]>,
        initialization_calls: usize,
    }

    impl CountingUninitializedBuffer {
        fn new(capacity: usize) -> Self {
            Self {
                storage: Box::new_uninit_slice(capacity),
                initialization_calls: 0,
            }
        }
    }

    // SAFETY: the boxed allocation is pointer-stable and writable for the
    // reported capacity. The override initializes exactly the requested
    // prefix without changing its base or logical length.
    unsafe impl IoBuffReadWrite for CountingUninitializedBuffer {
        fn as_mut_ptr(&mut self) -> *mut u8 {
            self.storage.as_mut_ptr().cast()
        }

        fn writable_len(&self) -> usize {
            self.storage.len()
        }

        unsafe fn initialized_writable_slice(&mut self, len: usize) -> &mut [u8] {
            self.initialization_calls += 1;
            let ptr = self.as_mut_ptr();
            unsafe {
                std::ptr::write_bytes(ptr, 0, len);
                std::slice::from_raw_parts_mut(ptr, len)
            }
        }

        unsafe fn set_written_len(&mut self, _len: usize) {}
    }

    #[test]
    fn tls_userspace_destination_initializes_fresh_vec_prefix() {
        let mut buffer = vec![0xA5; 8];
        buffer.clear();
        let mut initialized = false;
        let dst = unsafe { tls_userspace_destination(&mut buffer, &mut initialized, 5) };
        assert_eq!(dst, &[0; 5]);
        dst.copy_from_slice(b"hello");
        // SAFETY: this test initialized all eight allocated bytes before
        // clearing the Vec's logical length. The userspace hook must not touch
        // bytes beyond its requested five-byte prefix.
        assert_eq!(unsafe { buffer.as_ptr().add(5).read() }, 0xA5);
        unsafe { buffer.set_written_len(5) };
        assert_eq!(buffer, b"hello");
    }

    #[test]
    fn tls_userspace_destination_initializes_fresh_iobuff() {
        let mut buffer = IoBuffMut::new(0, 8, 0).unwrap();
        let mut initialized = false;
        let dst = unsafe { tls_userspace_destination(&mut buffer, &mut initialized, 5) };
        assert_eq!(dst, &[0; 5]);
        dst[..3].copy_from_slice(b"abc");
        unsafe { buffer.set_written_len(3) };
        assert_eq!(buffer.payload_bytes(), b"abc");

        buffer.payload_set_len(5).unwrap();
        assert_eq!(buffer.payload_bytes(), b"abc\0\0");
        assert_eq!(
            buffer.payload_set_len(6),
            Err(IoBuffError::PayloadUninitialized)
        );
    }

    #[test]
    fn tls_userspace_destination_preserves_iobuff_frontier() {
        let mut buffer = IoBuffMut::new(0, 8, 0).unwrap();
        buffer.payload_append(b"ABCDEFG").unwrap();
        buffer.payload_set_len(2).unwrap();

        let mut initialized = false;
        let dst = unsafe { tls_userspace_destination(&mut buffer, &mut initialized, 5) };
        assert_eq!(dst, b"CDEFG");
        assert_eq!(buffer.payload_bytes(), b"AB");
    }

    #[test]
    fn tls_userspace_destination_initializes_default_custom_buffer() {
        let mut buffer = DefaultUninitializedBuffer::new(8);
        buffer.storage[5].write(0xA5);
        let mut initialized = false;
        let dst = unsafe { tls_userspace_destination(&mut buffer, &mut initialized, 5) };
        assert_eq!(dst, &[0; 5]);
        assert_eq!(unsafe { buffer.storage[5].assume_init() }, 0xA5);
    }

    #[test]
    fn tls_userspace_destination_reuses_prepared_range() {
        let mut buffer = CountingUninitializedBuffer::new(8);
        let mut initialized = false;
        let dst = unsafe { tls_userspace_destination(&mut buffer, &mut initialized, 5) };
        dst[..3].copy_from_slice(b"abc");

        let dst = unsafe { tls_userspace_destination(&mut buffer, &mut initialized, 5) };
        assert_eq!(&dst[..3], b"abc");
        assert_eq!(buffer.initialization_calls, 1);
    }

    #[test]
    fn tls_userspace_destination_preserves_initialized_box_prefix() {
        let mut buffer = vec![0x5A; 8].into_boxed_slice();
        let mut initialized = false;
        let dst = unsafe { tls_userspace_destination(&mut buffer, &mut initialized, 5) };
        assert_eq!(dst, &[0x5A; 5]);
    }

    #[cfg(not(miri))]
    fn drain_pending_rustls_writes(connection: &mut ClientConnection) {
        let mut sink = io::sink();
        while connection.wants_write() {
            let written = connection
                .write_tls(&mut sink)
                .expect("test rustls write drain failed");
            assert!(written > 0, "rustls wanted write but emitted no bytes");
        }
    }

    #[test]
    #[cfg(not(miri))]
    fn shutdown_after_transport_write_failure_does_not_queue_close_notify() {
        let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .expect("std bind failed");
        let addr = listener.local_addr().expect("local_addr failed");
        let server = std::thread::spawn(move || {
            let (_tcp, _) = listener.accept().expect("std accept failed");
        });

        let config = Arc::new(
            ClientConfig::builder()
                .with_root_certificates(RootCertStore::empty())
                .with_no_client_auth(),
        );
        let options = TlsClientOptions {
            rustls_buffer_limit: Some(1024),
            transport_read_buffer_size: 128,
            transport_write_buffer_size: 128,
        };

        let mut executor = Executor::new().expect("failed to construct runtime executor");
        executor
            .run(async move {
                let tcp = TcpStream::connect(addr)
                    .expect("connect init failed")
                    .await
                    .expect("connect failed");
                let mut tls = TlsClientStream::new(
                    tcp,
                    config,
                    ServerName::try_from("localhost").expect("invalid test server name"),
                    options,
                )
                .expect("tls stream init failed");

                drain_pending_rustls_writes(&mut tls.connection);
                assert!(
                    !tls.connection.wants_write(),
                    "test setup should leave rustls with no pending writes"
                );

                tls.transport_write_failed = true;
                let err = tls
                    .shutdown()
                    .await
                    .expect_err("failed transport write should reject shutdown");

                assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
                assert!(
                    !tls.write_shutdown,
                    "shutdown must not mark close_notify queued after failed transport write"
                );
                assert!(
                    !tls.connection.wants_write(),
                    "shutdown must not queue close_notify after failed transport write"
                );
            })
            .expect("executor run failed");

        server.join().expect("server thread panicked");
    }

    #[test]
    #[cfg(all(debug_assertions, feature = "test-support", not(miri)))]
    fn handshake_read_error_restores_transport_read_scratch() {
        let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .expect("std bind failed");
        let addr = listener.local_addr().expect("local_addr failed");
        let server = std::thread::spawn(move || {
            let (tcp, _) = listener.accept().expect("std accept failed");
            tls_test_peer::force_reset_on_drop(&tcp);
            tls_test_peer::drain_available_client_hello(tcp);
        });

        let config = Arc::new(
            ClientConfig::builder()
                .with_root_certificates(RootCertStore::empty())
                .with_no_client_auth(),
        );
        let options = TlsClientOptions {
            rustls_buffer_limit: Some(1024),
            transport_read_buffer_size: 128,
            transport_write_buffer_size: 128,
        };

        let mut executor = Executor::new().expect("failed to construct runtime executor");
        executor
            .run(async move {
                let tcp = TcpStream::connect(addr)
                    .expect("connect init failed")
                    .await
                    .expect("connect failed");
                let mut tls = TlsClientStream::new(
                    tcp,
                    config,
                    ServerName::try_from("localhost").expect("invalid test server name"),
                    options,
                )
                .expect("tls stream init failed");

                let err = tls
                    .handshake()
                    .await
                    .expect_err("handshake should fail after server reset");
                assert!(
                    matches!(
                        err.kind(),
                        io::ErrorKind::ConnectionReset
                            | io::ErrorKind::BrokenPipe
                            | io::ErrorKind::UnexpectedEof
                            | io::ErrorKind::InvalidData
                    ),
                    "unexpected handshake reset error: {err}"
                );

                let scratch = tls
                    .read_tls_buffer
                    .as_ref()
                    .expect("read scratch not restored after handshake error");
                assert_eq!(scratch.len(), 0);
                assert_eq!(scratch.capacity(), 128);
            })
            .expect("executor run failed");

        server.join().expect("server thread panicked");
    }
}
