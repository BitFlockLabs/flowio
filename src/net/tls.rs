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
//!   buffers. The wrapper's write scratch is a hard per-chunk bound and does
//!   not grow during ordinary ciphertext draining.
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
use crate::runtime::executor::validate_local_io_result;
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

const DER_SEQUENCE_TAG: u8 = 0x30;
const DER_BIT_STRING_TAG: u8 = 0x03;

/// rustls 0.23.42 deframer `MAX_WIRE_SIZE`; keep the reusable read scratch and
/// each raw read to at most one TLS record so ciphertext and plaintext staging
/// are both drained between feeds. Recheck this value when updating rustls.
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

/// Converts the plaintext progress recorded before the final TLS flush into
/// the public write result without masking an impossible missing count.
#[inline(always)]
fn tls_write_progress_result(written: Option<usize>) -> io::Result<usize> {
    match written {
        Some(written) => Ok(written),
        None => Err(tls_internal_error(
            "tls write completed without an accepted plaintext count",
        )),
    }
}

/// Validates one live public TLS future poll before any local completion,
/// staged raw-operation poll, or rustls-state mutation.
#[inline(always)]
fn validate_tls_poll_context(cx: &Context<'_>) -> io::Result<()> {
    validate_local_io_result(cx, Ok(()))
}

#[derive(Clone, Copy)]
enum TlsScratchKind {
    Read,
    Write,
}

impl TlsScratchKind {
    fn effective_capacity(self, requested: usize) -> usize {
        match self {
            Self::Read => requested.min(TLS_MAX_WIRE_READ_SIZE),
            Self::Write => requested,
        }
    }

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
    reserve_valid_tls_scratch(kind.effective_capacity(capacity))
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

#[inline]
fn tls_read_submission_len(buffer_capacity: usize, configured_bound: usize) -> usize {
    buffer_capacity.min(configured_bound)
}

/// Fixed-capacity append adapter for one reusable TLS ciphertext chunk.
///
/// The configured limit, rather than the allocator-provided `Vec` capacity,
/// defines the visible write boundary. The capacity term also makes an
/// invariant violation fail with zero progress instead of reallocating.
struct TlsWriteScratch<'a> {
    buffer: &'a mut Vec<u8>,
    limit: usize,
}

impl<'a> TlsWriteScratch<'a> {
    fn new(buffer: &'a mut Vec<u8>, limit: usize) -> Self {
        debug_assert!(buffer.len() <= limit);
        Self { buffer, limit }
    }

    #[inline(always)]
    fn remaining(&self) -> usize {
        self.limit
            .saturating_sub(self.buffer.len())
            .min(self.buffer.capacity().saturating_sub(self.buffer.len()))
    }
}

impl Write for TlsWriteScratch<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = bytes.len().min(self.remaining());
        self.buffer.extend_from_slice(&bytes[..written]);
        Ok(written)
    }

    fn write_vectored(&mut self, buffers: &[io::IoSlice<'_>]) -> io::Result<usize> {
        let mut remaining = self.remaining();
        let mut written = 0usize;

        for buffer in buffers {
            if remaining == 0 {
                break;
            }
            let take = buffer.len().min(remaining);
            self.buffer.extend_from_slice(&buffer[..take]);
            written += take;
            remaining -= take;
        }

        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Drains at most one configured ciphertext chunk from rustls.
fn drain_tls_write_scratch(
    connection: &mut ClientConnection,
    buffer: &mut Vec<u8>,
    limit: usize,
) -> io::Result<usize> {
    let initial_len = buffer.len();

    while connection.wants_write() && buffer.len() < limit {
        let written = {
            let mut scratch = TlsWriteScratch::new(buffer, limit);
            connection.write_tls(&mut scratch)?
        };
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "rustls produced zero TLS bytes while wants_write() was true",
            ));
        }
    }

    Ok(buffer.len() - initial_len)
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
/// `transport_read_buffer_size` is the nonzero requested reusable ciphertext
/// scratch bound. The stored effective raw-read bound and reservation request
/// are the smaller of the requested value and 18,437 bytes (the maximum TLS
/// wire-record byte bound), so internal rustls ciphertext and plaintext
/// staging is drained between bounded transport feeds. The allocator may
/// provide a larger `Vec` capacity, but that spare capacity never enlarges a
/// raw read.
/// `transport_write_buffer_size` is the nonzero hard bound for each reusable
/// ciphertext chunk collected from rustls before writing it to the socket.
/// Rustls output beyond that bound is drained only after the current owned
/// chunk is fully submitted. Both capacities must fit in `isize` and are
/// reserved fallibly when the wrapper is created.
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
    /// Nonzero requested bound for the reusable ciphertext receive scratch
    /// buffer used for `read_tls`. The stored effective raw-read bound and
    /// reservation request are capped at 18,437 bytes, the maximum TLS
    /// wire-record byte bound. Allocator-provided spare capacity does not
    /// enlarge a raw read. The requested value must not exceed `isize::MAX`.
    pub transport_read_buffer_size: usize,
    /// Nonzero hard capacity of each reusable ciphertext send chunk used for
    /// `write_tls`. The value must not exceed `isize::MAX`.
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
/// Every live TLS future poll validates that its waker belongs to the active
/// FlowIO executor before it completes locally or touches rustls or staged raw
/// I/O. A rejected poll returns `NotConnected` and leaves any staged raw
/// read/write attached for the next valid TLS operation. If an earlier valid
/// write poll already gave plaintext to rustls, the rejected future can return
/// its source while ciphertext remains staged; source return does not prove
/// that nothing was or will be transmitted, so callers must not retry it.
///
/// If draining outbound TLS records fails, the stream is marked failed. A
/// scratch-drain failure may have already consumed ciphertext from rustls, and
/// a raw transport failure may mean the kernel accepted bytes. Retrying the
/// same plaintext could therefore omit or duplicate records, so later TLS
/// write, flush, and shutdown operations return `BrokenPipe`. The read side may
/// still drain already-decrypted plaintext and continue reading the transport
/// until EOF; if a read requires a TLS transport write to make progress after
/// the write latch is set, that read returns `BrokenPipe`.
///
/// After the underlying TCP write side has completed shutdown, a read, flush,
/// or repeated shutdown that encounters staged or newly requested TLS output
/// returns `BrokenPipe` without polling or consuming that output. Reads of
/// already-decrypted plaintext and quiescent repeated flush or shutdown calls
/// remain available.
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
    /// Effective configured bound for raw TLS reads and receive-buffer
    /// reservation requests.
    transport_read_buffer_size: usize,
    /// Hard per-chunk bound used when collecting rustls-emitted TLS records.
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
    /// True after an outbound ciphertext drain failure made queued TLS records
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
/// The outer certificate sequence must consume the complete input and contain
/// exactly a `TBSCertificate` sequence, a `signatureAlgorithm` sequence, and a
/// `signatureValue` bit string, in that order. Those four parsed TLV headers
/// must use canonical DER length encoding: short form below 128 bytes and a
/// minimally encoded, non-zero-prefixed long form otherwise. The
/// `signatureValue` BIT STRING body must contain an unused-bit-count octet of
/// zero followed by at least one signature payload octet; a missing or
/// nonzero count, or an empty payload (`03 01 00`), returns `None`. This does
/// not recursively validate `TBSCertificate` or the signature bytes.
/// Returns `None` when the parsed structure is malformed or the signature
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
    if tag != DER_SEQUENCE_TAG {
        return None;
    }

    let body_start = header_len;
    let body_end = body_start.checked_add(body_len)?;
    if body_end != certificate_der.len() {
        return None;
    }

    let (first_tag, first_header_len, first_body_len) = read_tlv(certificate_der, body_start)?;
    if first_tag != DER_SEQUENCE_TAG {
        return None;
    }
    let first_end = body_start
        .checked_add(first_header_len)?
        .checked_add(first_body_len)?;
    let (second_tag, second_header_len, second_body_len) = read_tlv(certificate_der, first_end)?;
    if second_tag != DER_SEQUENCE_TAG {
        return None;
    }

    let second_body_start = first_end.checked_add(second_header_len)?;
    let second_end = second_body_start.checked_add(second_body_len)?;

    let (third_tag, third_header_len, third_body_len) = read_tlv(certificate_der, second_end)?;
    if third_tag != DER_BIT_STRING_TAG {
        return None;
    }
    let third_body_start = second_end.checked_add(third_header_len)?;
    let third_end = third_body_start.checked_add(third_body_len)?;
    if third_end != body_end || third_body_len == 0 {
        return None;
    }
    if certificate_der.get(third_body_start).copied()? != 0 {
        return None;
    }
    if third_body_len < 2 {
        return None;
    }

    certificate_der.get(second_body_start..second_end)
}

/// Reads a one-octet tag plus a definite, canonically encoded DER length and
/// returns the tag, header length, and body length after proving the complete
/// body is within `bytes`. Indefinite length and nonminimal or zero-prefixed
/// long forms are rejected. This validates only that header shape and body
/// bound; callers remain responsible for tag-specific body structure.
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
    if len_bytes.first().copied() == Some(0) {
        return None;
    }

    for &octet in len_bytes {
        body_len = body_len.checked_shl(8)? | octet as usize;
    }
    if body_len < 128 {
        return None;
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
    /// front. The read reservation request and every raw read honor the stored
    /// configured bound, which is capped at the maximum TLS wire-record byte
    /// bound; write scratch uses the capacity provided in
    /// [`TlsClientOptions`].
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

        let transport_read_buffer_size =
            TlsScratchKind::Read.effective_capacity(options.transport_read_buffer_size);
        let read_tls_buffer = reserve_valid_tls_scratch(transport_read_buffer_size)?;
        let write_tls_buffer = reserve_valid_tls_scratch(options.transport_write_buffer_size)?;

        let mut connection =
            ClientConnection::new(config, server_name).map_err(tls_protocol_error)?;
        connection.set_buffer_limit(options.rustls_buffer_limit);

        Ok(Self {
            stream,
            connection,
            transport_read_buffer_size,
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
    /// # Errors
    /// Returns `NotConnected` when polled without its active FlowIO executor
    /// task context.
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
    /// Returns `NotConnected` if the TLS handshake has not completed or the
    /// future is polled without its active FlowIO executor task context.
    /// A clean TLS close returns `Ok(0)`.
    /// After an outbound TLS write failure, already-decrypted plaintext may
    /// still be returned; if a read needs to emit TLS records to make progress
    /// after that latch is set or after the transport write side has completed
    /// shutdown, it returns `BrokenPipe`.
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
    /// Returns `NotConnected` if the TLS handshake has not completed or the
    /// future is polled without its active FlowIO executor task context.
    /// Returns `UnexpectedEof` if the TLS session reaches EOF before `len`
    /// plaintext bytes become available; any partial plaintext read into the
    /// caller buffer remains published in that returned buffer.
    /// After an outbound TLS write failure, already-decrypted plaintext may
    /// still be returned; if a read needs to emit TLS records to make progress
    /// after that latch is set or after the transport write side has completed
    /// shutdown, it returns `BrokenPipe`.
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
    /// Returns `NotConnected` if called before handshake completion or polled
    /// without its active FlowIO executor task context, and `BrokenPipe` if
    /// logical TLS write shutdown has begun, physical transport-write shutdown
    /// has completed, or a prior outbound ciphertext drain failed. Within
    /// these write-state checks, a prior transport-write failure retains
    /// precedence over either shutdown state. Shutdown rejection returns the
    /// exact source owner before plaintext admission or scratch mutation.
    /// If rustls accepts plaintext but the following TLS-record flush fails,
    /// this future returns an error without a progress count; the stream is
    /// failed and callers must not retry the same plaintext on it.
    /// An invalid-context repoll after an earlier valid poll can instead return
    /// the source while its ciphertext remains staged; callers must not retry
    /// that source because a later valid TLS operation resumes the raw write.
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
    /// Returns `NotConnected` if called before handshake completion or polled
    /// without its active FlowIO executor task context, and `BrokenPipe` if
    /// logical TLS write shutdown has begun, physical transport-write shutdown
    /// has completed, or a prior outbound ciphertext drain failed. Within
    /// these write-state checks, a prior transport-write failure retains
    /// precedence over either shutdown state. Shutdown rejection returns the
    /// exact source owner before plaintext admission or scratch mutation.
    /// If rustls accepts plaintext but a later TLS-record flush fails, this
    /// future returns an error even though some plaintext may already be queued
    /// as TLS records; the stream is failed and callers must not retry the
    /// same plaintext on it.
    /// An invalid-context repoll after an earlier valid poll can instead return
    /// the source while its ciphertext remains staged; callers must not retry
    /// that source because a later valid TLS operation resumes the raw write.
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
    /// Returns `NotConnected` when polled without its active FlowIO executor
    /// task context and `BrokenPipe` if a prior outbound ciphertext drain
    /// failed, or if TLS output is pending after the transport write side has
    /// completed shutdown.
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
    /// Returns `NotConnected` when polled without its active FlowIO executor
    /// task context. If the transport write side has previously failed,
    /// shutdown returns `BrokenPipe` while reads can still drain plaintext
    /// that does not require emitting more TLS records. After transport write
    /// shutdown completes, a repeated shutdown returns `BrokenPipe` if rustls
    /// has newly requested output, and otherwise remains successful.
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
        if self.write_shutdown || self.transport_write_shutdown {
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
        tls_transport_eof_result(self.connection.wants_read())
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
        if self.transport_write_shutdown
            && (self.pending_write_tls.is_some() || self.connection.wants_write())
        {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "tls transport write side already shut down",
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

            // The shared raw write future accepts a u32 byte count. Keep the
            // configured scratch size as an upper bound while never consuming
            // more ciphertext from rustls than one submission can represent.
            let chunk_limit = self.transport_write_buffer_size.min(u32::MAX as usize);
            let written =
                match drain_tls_write_scratch(&mut self.connection, &mut buffer, chunk_limit) {
                    Ok(written) => written,
                    Err(err) => {
                        self.transport_write_failed = true;
                        self.restore_write_tls_buffer(buffer);
                        return Poll::Ready(Err(err));
                    }
                };

            debug_assert!(written > 0);
            debug_assert!(buffer.len() <= chunk_limit);
            self.pending_write_tls = Some(stream::WriteAllFuture::new(
                self.stream.raw_fd_for_internal_io(),
                buffer,
            ));
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
                // The preceding wants_read() check established that rustls
                // still needs transport input. EOF cannot satisfy that demand,
                // so reporting a successful fill would expose no progress and
                // make the caller rediscover the terminal state. A clean
                // close-notify reaches the success branch above because
                // rustls no longer wants input.
                return Poll::Ready(tls_transport_eof_result(true));
            }

            if self.pending_write_tls.is_some() {
                return Poll::Ready(Err(tls_internal_error("tls transport write/read overlap")));
            }

            let buffer = self.take_read_tls_buffer()?;
            let len = tls_read_submission_len(buffer.capacity(), self.transport_read_buffer_size);
            self.pending_read_tls = Some(stream::ReadFuture::new(
                self.stream.raw_fd_for_internal_io(),
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

#[inline(always)]
fn tls_transport_eof_result(wants_read: bool) -> io::Result<()> {
    if wants_read {
        return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
    }
    Ok(())
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

        if let Err(err) = validate_tls_poll_context(cx) {
            return Poll::Ready(Err(err));
        }

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

        if let Err(err) = validate_tls_poll_context(cx) {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
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

        if let Err(err) = validate_tls_poll_context(cx) {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready(unsafe {
                complete_read_with_progress(buffer, this.write_base_len, this.filled, Err(err))
            });
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

        if let Err(err) = validate_tls_poll_context(cx) {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
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
                Poll::Ready((tls_write_progress_result(this.written), buffer))
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

        if let Err(err) = validate_tls_poll_context(cx) {
            let buffer = unsafe { opt_take(&mut this.buffer) };
            return Poll::Ready((Err(err), buffer));
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
        if let Err(err) = validate_tls_poll_context(cx) {
            return Poll::Ready(Err(err));
        }
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

        if let Err(err) = validate_tls_poll_context(cx) {
            return Poll::Ready(Err(err));
        }

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
    use super::drain_tls_write_scratch;
    #[cfg(not(miri))]
    use super::*;
    use super::{
        TLS_MAX_WIRE_READ_SIZE, TlsScratchKind, TlsWriteScratch, allocate_tls_scratch,
        take_or_reserve_tls_scratch, tls_read_submission_len, tls_transport_eof_result,
        tls_userspace_destination, tls_write_progress_result,
    };
    #[cfg(all(debug_assertions, feature = "test-support", not(miri)))]
    use crate::net::tls_test_peer;
    use crate::runtime::buffer::{IoBuffError, IoBuffMut, IoBuffReadWrite};
    #[cfg(not(miri))]
    use crate::runtime::executor::Executor;
    #[cfg(not(miri))]
    use rcgen::generate_simple_self_signed;
    #[cfg(not(miri))]
    use rustls::pki_types::PrivatePkcs8KeyDer;
    #[cfg(not(miri))]
    use rustls::{RootCertStore, ServerConfig, ServerConnection};
    use std::io::{self, Write};
    #[cfg(not(miri))]
    use std::net::{Ipv4Addr, SocketAddr};
    #[cfg(not(miri))]
    use std::task::Waker;

    #[cfg(not(miri))]
    fn handshaken_tls_for_shutdown_tests() -> (TlsClientStream, std::net::TcpStream) {
        let certified = generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("failed to generate self-signed test certificate");
        let certificate = certified.cert.der().clone();
        let private_key = PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der());

        let mut roots = RootCertStore::empty();
        roots
            .add(certificate.clone())
            .expect("failed to install test root certificate");
        let client_config = Arc::new(
            ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );
        let server_config = Arc::new(
            ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(vec![certificate], private_key.into())
                .expect("failed to build test server configuration"),
        );

        let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .expect("std bind failed");
        let client = std::net::TcpStream::connect(
            listener.local_addr().expect("listener local_addr failed"),
        )
        .expect("std connect failed");
        let (peer, _) = listener.accept().expect("std accept failed");
        let mut tls = TlsClientStream::new(
            TcpStream::from_owned_fd(client.into()),
            client_config,
            ServerName::try_from("localhost").expect("invalid test server name"),
            TlsClientOptions {
                rustls_buffer_limit: Some(1024),
                transport_read_buffer_size: 128,
                transport_write_buffer_size: 128,
            },
        )
        .expect("tls stream init failed");
        let mut server =
            ServerConnection::new(server_config).expect("test server connection failed");

        for _ in 0..64 {
            if tls.connection.wants_write() {
                let mut wire = Vec::new();
                while tls.connection.wants_write() {
                    let written = tls
                        .connection
                        .write_tls(&mut wire)
                        .expect("client handshake write failed");
                    assert!(written > 0, "client handshake write made no progress");
                }
                let mut cursor = Cursor::new(wire.as_slice());
                while (cursor.position() as usize) < wire.len() {
                    let read = server
                        .read_tls(&mut cursor)
                        .expect("server handshake read failed");
                    assert!(read > 0, "server handshake read made no progress");
                }
                server
                    .process_new_packets()
                    .expect("server handshake packet processing failed");
            }

            if server.wants_write() {
                let mut wire = Vec::new();
                while server.wants_write() {
                    let written = server
                        .write_tls(&mut wire)
                        .expect("server handshake write failed");
                    assert!(written > 0, "server handshake write made no progress");
                }
                let mut cursor = Cursor::new(wire.as_slice());
                while (cursor.position() as usize) < wire.len() {
                    let read = tls
                        .connection
                        .read_tls(&mut cursor)
                        .expect("client handshake read failed");
                    assert!(read > 0, "client handshake read made no progress");
                }
                tls.connection
                    .process_new_packets()
                    .expect("client handshake packet processing failed");
            }

            if !tls.connection.is_handshaking()
                && !server.is_handshaking()
                && !tls.connection.wants_write()
                && !server.wants_write()
            {
                assert_eq!(
                    tls.connection.protocol_version(),
                    Some(rustls::ProtocolVersion::TLSv1_3)
                );
                return (tls, peer);
            }
        }

        panic!("in-memory TLS handshake did not converge");
    }

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
    fn tls_write_progress_requires_an_explicit_accepted_count() {
        assert_eq!(tls_write_progress_result(Some(0)).unwrap(), 0);
        assert_eq!(tls_write_progress_result(Some(37)).unwrap(), 37);

        let err = tls_write_progress_result(None)
            .expect_err("missing TLS write progress unexpectedly became success");
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert_eq!(
            err.to_string(),
            "tls write completed without an accepted plaintext count"
        );
    }

    #[test]
    fn tls_scratch_reserves_valid_small_common_and_large_profiles() {
        for kind in [TlsScratchKind::Read, TlsScratchKind::Write] {
            for capacity in [1, 16 * 1024, 64 * 1024] {
                let scratch = allocate_tls_scratch(kind, capacity)
                    .expect("valid TLS scratch reservation should succeed");
                assert!(scratch.is_empty());
                assert!(scratch.capacity() >= kind.effective_capacity(capacity));
            }
        }
    }

    #[test]
    fn tls_read_scratch_capacity_clamps_only_above_one_wire_record() {
        for (requested, expected) in [
            (TLS_MAX_WIRE_READ_SIZE - 1, TLS_MAX_WIRE_READ_SIZE - 1),
            (TLS_MAX_WIRE_READ_SIZE, TLS_MAX_WIRE_READ_SIZE),
            (TLS_MAX_WIRE_READ_SIZE + 1, TLS_MAX_WIRE_READ_SIZE),
            (64 * 1024, TLS_MAX_WIRE_READ_SIZE),
        ] {
            assert_eq!(TlsScratchKind::Read.effective_capacity(requested), expected);
        }
        assert_eq!(
            TlsScratchKind::Write.effective_capacity(64 * 1024),
            64 * 1024
        );
    }

    #[test]
    fn tls_read_submission_length_honors_the_configured_bound() {
        for requested in [
            7,
            TLS_MAX_WIRE_READ_SIZE - 1,
            TLS_MAX_WIRE_READ_SIZE,
            TLS_MAX_WIRE_READ_SIZE + 1,
        ] {
            let configured = TlsScratchKind::Read.effective_capacity(requested);
            let scratch = Vec::<u8>::with_capacity(configured + 97);

            assert!(scratch.capacity() > configured);
            assert_eq!(
                tls_read_submission_len(scratch.capacity(), configured),
                configured
            );
        }

        assert_eq!(tls_read_submission_len(127, 128), 127);
    }

    #[test]
    fn tls_transport_eof_classifies_remaining_read_demand_as_unexpected_eof() {
        assert!(
            tls_transport_eof_result(false).is_ok(),
            "clean TLS close should not become an error"
        );
        let err = tls_transport_eof_result(true)
            .expect_err("remaining TLS read demand cannot make progress after transport EOF");
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn missing_tls_scratch_uses_the_same_fallible_reservation_path() {
        let mut read = None;
        let requested = 64 * 1024;
        let scratch = take_or_reserve_tls_scratch(&mut read, TlsScratchKind::Read, requested)
            .expect("valid fallback reservation should succeed");
        assert!(scratch.is_empty());
        assert_eq!(scratch.capacity(), TLS_MAX_WIRE_READ_SIZE);

        let mut write = None;
        let err = take_or_reserve_tls_scratch(&mut write, TlsScratchKind::Write, usize::MAX)
            .expect_err("impossible fallback reservation should fail");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn tls_write_scratch_scalar_stops_at_configured_bound_without_reallocation() {
        let mut buffer = Vec::with_capacity(32);
        let allocation = buffer.as_ptr();
        let capacity = buffer.capacity();

        let mut scratch = TlsWriteScratch::new(&mut buffer, 7);
        assert_eq!(scratch.write(b"abcd").unwrap(), 4);
        assert_eq!(scratch.write(b"efgh").unwrap(), 3);
        assert_eq!(scratch.write(b"ignored").unwrap(), 0);
        scratch.flush().unwrap();

        assert_eq!(buffer, b"abcdefg");
        assert_eq!(buffer.as_ptr(), allocation);
        assert_eq!(buffer.capacity(), capacity);
    }

    #[test]
    fn tls_write_scratch_vectored_preserves_order_and_partial_tail() {
        let mut buffer = Vec::with_capacity(32);
        let allocation = buffer.as_ptr();
        let capacity = buffer.capacity();
        let buffers = [
            io::IoSlice::new(b"ab"),
            io::IoSlice::new(b""),
            io::IoSlice::new(b"cdef"),
            io::IoSlice::new(b"ghij"),
        ];

        let mut scratch = TlsWriteScratch::new(&mut buffer, 8);
        assert_eq!(scratch.write_vectored(&buffers).unwrap(), 8);
        assert_eq!(scratch.write_vectored(&buffers).unwrap(), 0);

        assert_eq!(buffer, b"abcdefgh");
        assert_eq!(buffer.as_ptr(), allocation);
        assert_eq!(buffer.capacity(), capacity);
    }

    #[test]
    #[cfg(not(miri))]
    fn tls_write_scratch_drains_rustls_in_multiple_bounded_chunks() {
        const LIMIT: usize = 13;

        let config = Arc::new(
            ClientConfig::builder()
                .with_root_certificates(RootCertStore::empty())
                .with_no_client_auth(),
        );
        let mut connection = ClientConnection::new(
            config,
            ServerName::try_from("localhost").expect("invalid test server name"),
        )
        .expect("test TLS connection construction failed");
        connection.set_buffer_limit(None);

        let mut buffer = allocate_tls_scratch(TlsScratchKind::Write, LIMIT)
            .expect("bounded write scratch allocation failed");
        let allocation = buffer.as_ptr();
        let capacity = buffer.capacity();
        let mut chunks = 0usize;
        let mut total = 0usize;

        while connection.wants_write() {
            assert!(chunks < 1024, "rustls bounded drain did not converge");
            buffer.clear();
            let written = drain_tls_write_scratch(&mut connection, &mut buffer, LIMIT)
                .expect("bounded rustls drain failed");
            assert_eq!(written, buffer.len());
            assert!(written > 0);
            assert!(written <= LIMIT);
            assert_eq!(buffer.as_ptr(), allocation);
            assert_eq!(buffer.capacity(), capacity);
            chunks += 1;
            total += written;
        }

        assert!(chunks > 1, "test output fit in one bounded chunk");
        assert!(total > LIMIT);
    }

    #[test]
    #[cfg(not(miri))]
    fn tls_write_scratch_zero_progress_latches_and_restores_scratch() {
        let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .expect("std bind failed");
        let addr = listener.local_addr().expect("local_addr failed");
        let client = std::net::TcpStream::connect(addr).expect("std connect failed");
        let (_server, _) = listener.accept().expect("std accept failed");

        let config = Arc::new(
            ClientConfig::builder()
                .with_root_certificates(RootCertStore::empty())
                .with_no_client_auth(),
        );
        let mut tls = TlsClientStream::new(
            TcpStream::from_owned_fd(client.into()),
            config,
            ServerName::try_from("localhost").expect("invalid test server name"),
            TlsClientOptions {
                rustls_buffer_limit: Some(1024),
                transport_read_buffer_size: 128,
                transport_write_buffer_size: 128,
            },
        )
        .expect("tls stream init failed");

        assert!(
            tls.connection.wants_write(),
            "new client connection should have a ClientHello queued"
        );
        let mut constrained_scratch = Box::<[u8]>::from([0]).into_vec();
        constrained_scratch.clear();
        let scratch_allocation = constrained_scratch.as_ptr();
        tls.write_tls_buffer = Some(constrained_scratch);

        let mut cx = Context::from_waker(Waker::noop());
        let err = match tls.poll_flush_pending_tls(&mut cx) {
            Poll::Ready(Err(err)) => err,
            Poll::Ready(Ok(())) => panic!("capacity-exhausted scratch unexpectedly flushed"),
            Poll::Pending => panic!("zero-progress scratch unexpectedly submitted a raw write"),
        };

        assert_eq!(err.kind(), io::ErrorKind::WriteZero);
        assert_eq!(
            err.to_string(),
            "rustls produced zero TLS bytes while wants_write() was true"
        );
        assert!(
            tls.transport_write_failed,
            "scratch drain failure must latch the TLS write side"
        );
        assert!(
            tls.pending_write_tls.is_none(),
            "scratch drain failure must not create a raw write future"
        );
        assert!(
            tls.connection.wants_write(),
            "failed scratch drain must leave queued ciphertext protected by the latch"
        );
        let restored = tls
            .write_tls_buffer
            .as_ref()
            .expect("scratch drain failure did not restore the reusable buffer");
        assert!(restored.is_empty());
        assert_eq!(restored.capacity(), 1);
        assert_eq!(restored.as_ptr(), scratch_allocation);

        let err = match tls.poll_flush_pending_tls(&mut cx) {
            Poll::Ready(Err(err)) => err,
            Poll::Ready(Ok(())) => panic!("latched TLS write unexpectedly recovered"),
            Poll::Pending => panic!("latched TLS write unexpectedly remained pending"),
        };
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(
            tls.write_tls_buffer
                .as_ref()
                .expect("latched TLS write lost its reusable buffer")
                .as_ptr(),
            scratch_allocation
        );
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

    #[test]
    #[cfg(not(miri))]
    fn tls_read_exact_context_rejection_publishes_prior_progress_once() {
        let listener =
            std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
        let client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (_peer, _) = listener.accept().unwrap();
        let stream = TcpStream::from_owned_fd(client.into());
        let config = Arc::new(
            ClientConfig::builder()
                .with_root_certificates(RootCertStore::empty())
                .with_no_client_auth(),
        );
        let mut tls = TlsClientStream::new(
            stream,
            config,
            ServerName::try_from("localhost").unwrap(),
            TlsClientOptions {
                rustls_buffer_limit: Some(128),
                transport_read_buffer_size: 128,
                transport_write_buffer_size: 128,
            },
        )
        .unwrap();
        let mut future = Box::pin(TlsReadExactFuture::new(&mut tls, Vec::with_capacity(2), 2));

        let this = unsafe { future.as_mut().get_unchecked_mut() };
        let buffer = unsafe { opt_mut(&mut this.buffer) };
        let destination = unsafe {
            tls_userspace_destination(buffer, &mut this.destination_initialized, this.target)
        };
        destination[0] = b'a';
        this.filled = 1;

        let mut cx = Context::from_waker(Waker::noop());
        match future.as_mut().poll(&mut cx) {
            Poll::Ready((Err(err), buffer)) => {
                assert_eq!(err.kind(), io::ErrorKind::NotConnected);
                assert_eq!(buffer, b"a");
            }
            Poll::Ready((Ok(_), _)) => panic!("inactive exact read unexpectedly succeeded"),
            Poll::Pending => panic!("inactive exact read remained pending"),
        }
        assert!(
            future.as_mut().poll(&mut cx).is_pending(),
            "completed exact read did not park"
        );
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
    fn tls_shutdown_drains_logical_close_and_resumes_after_future_drop() {
        let (mut tls, _peer) = handshaken_tls_for_shutdown_tests();
        let mut executor = Executor::new().expect("failed to construct runtime executor");

        executor
            .run(async move {
                let mut first_shutdown = Box::pin(tls.shutdown());
                std::future::poll_fn(|cx| {
                    assert!(
                        first_shutdown.as_mut().poll(cx).is_pending(),
                        "initial shutdown poll should stage close_notify"
                    );
                    Poll::Ready(())
                })
                .await;
                drop(first_shutdown);

                assert!(tls.write_shutdown, "close_notify was not queued");
                assert!(
                    !tls.transport_write_shutdown,
                    "transport write side shut down before close_notify completed"
                );
                assert!(
                    tls.pending_write_tls.is_some() || tls.connection.wants_write(),
                    "logical shutdown did not retain close_notify output"
                );

                tls.shutdown()
                    .await
                    .expect("resumed shutdown failed to drain close_notify");
                assert!(tls.transport_write_shutdown);
                tls.shutdown()
                    .await
                    .expect("quiescent repeated shutdown should succeed");
                tls.flush()
                    .await
                    .expect("quiescent post-shutdown flush should succeed");
            })
            .expect("executor run failed");
    }

    #[test]
    #[cfg(not(miri))]
    fn tls_read_after_physical_shutdown_preserves_an_already_staged_raw_write() {
        let (mut tls, _peer) = handshaken_tls_for_shutdown_tests();
        tls.stream
            .shutdown(std::net::Shutdown::Write)
            .expect("transport write shutdown failed");
        tls.write_shutdown = true;
        tls.transport_write_shutdown = true;

        let staged = b"staged tls ciphertext".to_vec();
        tls.write_tls_buffer = None;
        tls.pending_write_tls = Some(stream::WriteAllFuture::new(
            tls.stream.raw_fd_for_internal_io(),
            staged,
        ));
        let mut executor = Executor::new().expect("failed to construct runtime executor");

        executor
            .run(async move {
                let pending_address = tls
                    .pending_write_tls
                    .as_ref()
                    .map(std::ptr::from_ref)
                    .expect("staged write was not installed");
                let destination = Vec::with_capacity(1);
                let (result, destination) = tls.read(destination, 1).await;
                let err = result.expect_err("read unexpectedly flushed after physical shutdown");

                assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
                assert_eq!(
                    err.to_string(),
                    "tls transport write side already shut down"
                );
                assert!(destination.is_empty());
                assert_eq!(destination.capacity(), 1);
                assert_eq!(
                    tls.pending_write_tls.as_ref().map(std::ptr::from_ref),
                    Some(pending_address),
                    "read rejection detached or replaced the staged raw write"
                );
                assert!(tls.write_tls_buffer.is_none());
                assert!(!tls.transport_write_failed);
            })
            .expect("executor run failed");
    }

    #[test]
    #[cfg(not(miri))]
    fn tls_physical_shutdown_rejects_key_update_without_draining_state() {
        let (mut tls, _peer) = handshaken_tls_for_shutdown_tests();
        let mut executor = Executor::new().expect("failed to construct runtime executor");

        executor
            .run(async move {
                tls.shutdown().await.expect("initial shutdown failed");
                assert!(tls.write_shutdown);
                assert!(tls.transport_write_shutdown);
                tls.flush()
                    .await
                    .expect("quiescent post-shutdown flush should succeed");

                tls.connection
                    .refresh_traffic_keys()
                    .expect("TLS 1.3 traffic-key refresh failed");
                assert!(tls.connection.wants_write());
                let scratch = tls
                    .write_tls_buffer
                    .as_ref()
                    .expect("reusable write scratch is missing");
                let scratch_state = (scratch.as_ptr(), scratch.len(), scratch.capacity());

                let err = tls
                    .flush()
                    .await
                    .expect_err("post-shutdown KeyUpdate unexpectedly flushed");
                assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
                assert_eq!(
                    err.to_string(),
                    "tls transport write side already shut down"
                );
                assert!(
                    tls.connection.wants_write(),
                    "rejection consumed queued KeyUpdate output"
                );
                let scratch = tls
                    .write_tls_buffer
                    .as_ref()
                    .expect("rejection lost the reusable write scratch");
                assert_eq!(
                    (scratch.as_ptr(), scratch.len(), scratch.capacity()),
                    scratch_state
                );
                assert!(tls.pending_write_tls.is_none());
                assert!(!tls.transport_write_failed);
            })
            .expect("executor run failed");
    }

    #[test]
    #[cfg(not(miri))]
    fn tls_physical_only_shutdown_rejects_plaintext_before_state_mutation() {
        let (mut tls, _peer) = handshaken_tls_for_shutdown_tests();
        assert!(!tls.write_shutdown);
        assert!(!tls.transport_write_shutdown);
        assert!(!tls.connection.wants_write());
        assert!(tls.pending_write_tls.is_none());

        tls.transport_write_shutdown = true;
        let scratch = tls
            .write_tls_buffer
            .as_ref()
            .expect("reusable write scratch is missing");
        let scratch_state = (scratch.as_ptr(), scratch.len(), scratch.capacity());
        let mut executor = Executor::new().expect("failed to construct runtime executor");

        executor
            .run(async move {
                let partial_source = b"partial physical-only shutdown".to_vec();
                let partial_owner = (
                    partial_source.as_ptr(),
                    partial_source.len(),
                    partial_source.capacity(),
                );
                let (result, partial_source) = tls.write(partial_source).await;
                let err = result.expect_err("partial write crossed physical-only shutdown");
                assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
                assert_eq!(err.to_string(), "tls write side already shut down");
                assert_eq!(
                    (
                        partial_source.as_ptr(),
                        partial_source.len(),
                        partial_source.capacity(),
                    ),
                    partial_owner
                );
                assert_eq!(partial_source, b"partial physical-only shutdown");
                assert!(!tls.connection.wants_write());

                let complete_source = b"complete physical-only shutdown".to_vec();
                let complete_owner = (
                    complete_source.as_ptr(),
                    complete_source.len(),
                    complete_source.capacity(),
                );
                let (result, complete_source) = tls.write_all(complete_source).await;
                let err = result.expect_err("write_all crossed physical-only shutdown");
                assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
                assert_eq!(err.to_string(), "tls write side already shut down");
                assert_eq!(
                    (
                        complete_source.as_ptr(),
                        complete_source.len(),
                        complete_source.capacity(),
                    ),
                    complete_owner
                );
                assert_eq!(complete_source, b"complete physical-only shutdown");

                assert!(!tls.connection.wants_write());
                assert!(tls.pending_write_tls.is_none());
                let scratch = tls
                    .write_tls_buffer
                    .as_ref()
                    .expect("rejection lost the reusable write scratch");
                assert_eq!(
                    (scratch.as_ptr(), scratch.len(), scratch.capacity()),
                    scratch_state
                );
                assert!(!tls.transport_write_failed);
                assert!(!tls.write_shutdown);
                assert!(tls.transport_write_shutdown);

                tls.transport_write_failed = true;
                let err = tls
                    .ensure_writable()
                    .expect_err("transport failure should retain precedence");
                assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
                assert_eq!(err.to_string(), "tls transport write failed");
            })
            .expect("executor run failed");
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
