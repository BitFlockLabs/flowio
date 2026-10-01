# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Alpha prereleases carry no compatibility guarantee.

## [Unreleased]

### Added

- `UdpSocket::bind_reuse_port` shares an explicit local port using
  `SO_REUSEPORT` without `SO_REUSEADDR`; port zero returns `InvalidInput`.
  Group members use the same address, port, and effective UID, and unconnected
  members receive datagrams distributed by flow hash.
- `SctpListener::bind_reuse_port` and `bind_reuse_port_with_config` create
  listener groups using `SO_REUSEADDR` and `SO_REUSEPORT`, with each
  listener's own accepted-stream configuration. Listening members require the
  same effective UID; the first may bind port zero and later members use its
  `local_addr()`.
- `TcpStream::keepalive` reads the live `SO_KEEPALIVE` setting.
- `IoBuffVec`, `IoBuffVecMut`, and `IoBuffReadOnlyVec` add `checked_len`,
  which returns `None` on total readable-length overflow; their `len()`
  methods saturate at `usize::MAX`. `IoBuffVecMut` also adds
  `checked_writable_len` and makes `writable_len()` saturate on overflow.
- `IoBuffPoolConfig` implements `Clone`, `Copy`, `Debug`, `PartialEq`, and
  `Eq`.
- Unsafe `IoBuffReadWrite::initialized_writable_slice` provides an initialized
  writable prefix for userspace producers. `write_base_len` defaults to zero
  for overwrite-style buffers and reports the current payload length for
  `IoBuffMut` append reads.
- `DnsResolver::set_total_query_timeout` configures the aggregate upstream
  query budget; `set_query_timeout` sets each response attempt's limit.
- `DnsResolver::nameservers` exposes the effective nameserver list, and
  `system_nameservers_were_truncated` reports omitted system entries.
- `flowio::net` exports `ReadFuture`, `ReadExactFuture`, `ReadvFuture`,
  `ReadvExactFuture`, `WriteFuture`, `WriteAllFuture`, `WritevFuture`,
  `WritevAllFuture`, `WritevProjectedFuture`, and `WritevAllProjectedFuture`
  so TCP/Unix callers can name these futures without boxing.
- `flowio::net::tcp` and `flowio::net::sctp` document `AcceptFuture`,
  `ConnectFuture`, and `ConnectTimeoutFuture` as supported public types; TCP
  also documents `OwnedConnectFuture` and `OwnedConnectTimeoutFuture`.
- `TcpStream`, `UnixStream`, and `SctpStream` add safe `from_owned_fd`
  constructors that consume an `OwnedFd`.
- `TcpListener::is_terminal` and `SctpListener::is_terminal` report whether
  FlowIO has permanently marked accept readiness unusable.
- `IoBuffError` and `PushError<T>` implement `Display` and
  `std::error::Error`; `PushError::source` exposes the underlying
  `IoBuffError`.
- `JoinError::Cancelled` represents a spawned task that cannot produce its
  output. `JoinError` implements `Clone`, `Copy`, `Debug`, `PartialEq`, `Eq`,
  `Display`, and `std::error::Error`.
- `TimeoutError` distinguishes deadline expiry with `Elapsed` from timer
  failure with `Runtime(io::Error)`. It implements `Debug`, `Display`, and
  `std::error::Error`, whose `source()` exposes the runtime error.
- `IoBuffError::PayloadUninitialized` reports payload growth beyond
  initialized bytes, and unsafe `IoBuffMut::payload_set_len_initialized`
  publishes a total length after the caller initializes the spare bytes.
- `SctpResetStreams::all_incoming`, `all_outgoing`, and `all_bidirectional`
  request resets of every stream in the selected directions.
- `SctpNotification::Authentication` exposes `flags`, `key_number`,
  `alternate_key_number`, `indication`, and `assoc_id` from authentication
  events. `SctpNotificationKind::Authentication` identifies this notification
  kind.
- `SctpNotification::SendFailed::flags` exposes raw notification flags,
  including unknown values, from both Linux send-failure layouts.
- The development-only `diagnostic-counters` feature adds executor-local
  counters through `test-support`; it is disabled by default and is outside
  the supported application API.

### Changed

- **Breaking:** A zero-byte receive completion, including EOF and an empty
  datagram, leaves a `Vec<u8>` buffer's length and contents unchanged instead
  of truncating it to zero. Use the returned byte count, or clear the buffer
  before each receive.
- **Breaking (custom buffer implementations):** `IoBuffReadOnly` and
  `IoBuffReadWrite` require stable readable ranges and writable bases while
  FlowIO owns the buffer, initialized readable bytes, and ranges no larger
  than `isize::MAX`. Positive ranges must be non-null and within one
  allocation; empty windows may use null pointers. Audit `unsafe impl`s
  against the trait documentation, including initialized-prefix guarantees
  and `set_written_len(write_base_len() + n)` after positive progress only;
  zero-byte completions do not call `set_written_len`.
- **Breaking:** `resolve_host` returns `InvalidData` when its unique result
  exceeds 64 socket addresses or `/etc/hosts` exceeds 4 MiB;
  `DnsResolver::from_system` returns `InvalidData` for `/etc/resolv.conf`
  larger than 64 KiB. Results are never truncated. Keep configuration files
  within these limits and use another resolver for larger result sets.
- Requires rustls 0.23.42 or newer.
- **Breaking:** `UdpSocket::bind` no longer enables `SO_REUSEADDR`, keeping
  local endpoints exclusive even when the kernel assigns the port; a
  conflicting bind returns `EADDRINUSE`. Use `bind_reuse_port` with an
  explicit port for intentional sharing.
- **Breaking:** `SctpConnector::with_local_addr` no longer enables
  `SO_REUSEADDR`, so an occupied local endpoint returns `EADDRINUSE` during
  connect preparation. Use a distinct local endpoint for each live
  association, or port zero to let the kernel choose.
- **Breaking:** `TcpStream`, `UnixStream`, and `UdpSocket` are explicitly
  `!Send + !Sync` and no longer implement `RefUnwindSafe`. Keep each socket
  on its owner OS thread, and review borrowed socket captures at `catch_unwind`
  boundaries before using `AssertUnwindSafe`. An idle socket can
  move between FlowIO executors on that thread, but queued I/O stays bound to
  its originating executor until FlowIO observes completion.
- **Breaking:** `UdpSocket::local_addr` returns `io::Result<SocketAddr>` and
  queries the live socket, including kernel-assigned ports and address changes
  after connect or reconnect. Callers must handle the result.
- **Breaking:** Awaiting `JoinHandle<T>` returns `Result<T, JoinError>` so
  a cancelled task's handle completes with `JoinError::Cancelled` instead of
  staying pending. Handle cancellation after executor shutdown or a task's
  `Future::poll` panic; `Executor::run` re-raises the original panic.
- **Breaking:** TCP, Unix, and SCTP `from_raw_fd` constructors require an
  unsafe sole-ownership proof. Prefer `from_owned_fd`, which consumes
  `OwnedFd`.
- **Breaking (`test-support` only):** `MemoryProvider` implementations require
  `unsafe impl` and the documented alignment, provenance, size, uniqueness,
  lifetime, and exact-free guarantees. Audit implementations against that
  contract and obtain `Slab` values from allocator methods instead of
  constructing or changing their now-private fields.
- **Breaking:** `IoBuffMut::payload_unwritten_mut` returns
  `&mut [MaybeUninit<u8>]`, and safe `payload_set_len` rejects growth beyond
  initialized bytes with the new `IoBuffError::PayloadUninitialized`. Add this
  variant to exhaustive matches; initialize with `payload_append`, or fill the
  spare slice and call unsafe `payload_set_len_initialized` with the total
  payload length before exposing the bytes.
- **Breaking:** Awaiting `timeout` or `timeout_at` yields
  `Result<F::Output, TimeoutError>`, with
  `TimeoutError::{Elapsed, Runtime(io::Error)}` replacing the unit `Elapsed`
  error. Match `TimeoutError::Elapsed` (for example with `matches!`) separately
  from runtime failure; the replacement does not implement `Clone`, `Copy`,
  `Default`, `PartialEq`, `Eq`, `UnwindSafe`, or `RefUnwindSafe`. Review
  retained errors at unwind boundaries. TCP and SCTP connect-timeout helpers
  map only expiry to `TimedOut`.
- **Breaking:** Upstream DNS requires an exact two-byte nonblocking kernel
  random value for each A and AAAA transaction ID before opening a socket,
  allowing at most three `EINTR` retries per ID. Handle short-read or
  random-source failures as lookup errors instead of relying on fallback IDs;
  literal-IP, localhost, and hosts-file lookups do not need transaction IDs.
- **Breaking:** `SctpResetStreams` no longer supports struct literals; use
  `incoming`, `outgoing`, or `bidirectional` for listed streams, or
  `all_incoming`, `all_outgoing`, or `all_bidirectional` for all streams.
  Include `..` in struct patterns. Public fields remain configurable, but
  listed requests must be nonempty and all-stream requests must remain empty,
  or the operation returns
  `InvalidInput` before a socket-option syscall.
- **Breaking:** `SctpStream::peer_addr_params` accepts only the exact 152-byte
  legacy or 156-byte modern Linux response layout. Handle `InvalidData` for
  intermediate or other unsupported lengths instead of reading a partial
  result.
- **Breaking:** SCTP `send`, `send_msg`, and `send_msg_vectored` reject empty
  buffers with `InvalidInput` and return the buffer. Skip the send when there
  are no readable bytes, including for vectored sends.
- **Breaking:** SCTP authentication notifications decode as
  `SctpNotification::Authentication`; exhaustive matches on that enum or
  `SctpNotificationKind` need a new arm. Authentication records shorter than
  20 bytes return `InvalidData`, and raw indication values are preserved.
- **Breaking:** `SctpNotification::SendFailed` includes raw `flags: u16` from
  both Linux notification layouts, including unknown values. Field-exhaustive
  patterns must bind `flags` or add `..`; constructors must supply `flags`.
- **Breaking:** UDP receive requests and SCTP receive windows of zero bytes
  return `InvalidInput` before consuming data or submitting I/O. Use a
  positive receive length even for an empty UDP datagram; a successful
  zero-byte data-only SCTP receive denotes peer EOF.
- FlowIO supports x86-64 Linux 5.11 or newer; other architectures are outside
  the support and validation contract.
- Socket construction and adoption allocate one reference-counted descriptor
  record; submitted I/O keeps the descriptor alive until FlowIO observes
  completion. Allocation failure invokes the global allocation-error handler,
  normally aborting the process, and allocation or final deallocation may
  block in the allocator.
- If executor shutdown abandons its ring with unfinished I/O, those operations
  stay pending and retain their descriptors, buffers, and kernel-visible
  storage until process exit to prevent premature reuse. Descriptor retention
  is bounded by `ring_entries` per abandoned ring and accumulates across rings.
- **Breaking:** Upstream DNS work has a default five-second aggregate budget
  across address families, nameserver failover, and CNAME follow-up. Use
  `set_total_query_timeout` when a longer budget is needed; local resolution
  is outside the budget, and a completed address wins over a later family
  timeout.
- TLS read-buffer reservations are capped at 18,437 bytes per connection after
  validating the configured size, avoiding larger unused reservations. Extra
  allocator capacity cannot enlarge the configured raw-read bound.
- SCTP message sends and receives copy less temporary data, and receive
  operations avoid parsing the same notification twice.
- DNS parsing avoids allocating discarded Authority and Additional names,
  and their record counts no longer reserve unused address-result capacity.
  UDP header and question validation rejects malformed responses with the
  expected transaction ID without allocating an error.
- **Breaking:** TCP and SCTP listeners become permanently unusable after
  `POLLHUP` or `POLLNVAL` without a queued connection; later accepts return
  `ConnectionAborted` without submitting I/O. Check `is_terminal()` and
  rebuild the listener; a confirmed `EBADF` is preserved on the detecting
  call, and `EMFILE`/`ENFILE` do not mark the listener terminal.
- **Breaking (error text):** TCP and Unix async vectored reads and writes,
  including projected writes, report more than 1,024 active segments as
  `InvalidInput` with the message "too many iovec segments for this
  operation". Match `io::ErrorKind` instead of the old display text; the
  active-segment check does not apply to a zero-length `readv_exact` request.
  Oversized active-segment requests fail before operation-slot allocation, so
  slot pressure cannot mask the input error as `WouldBlock`.
- **Breaking:** SCTP `send_msg_vectored` and `recv_msg_vectored` reject chains
  with more than 1,024 active segments with `InvalidInput` and return the
  chain before submitting I/O. Coalesce buffers to fit that limit; empty
  segments and unused chain capacity do not count.
- **Breaking:** After a dropped in-flight metadata receive or an incomplete
  SCTP record, data-only `SctpStream::recv` returns `InvalidInput` while
  record recovery is pending. Continue with `recv_msg` or `recv_msg_vectored`
  until recovery completes instead of switching to the data-only receive path.
- **Breaking:** `DnsResolver::new` removes duplicate nameservers in first-seen
  order and returns `InvalidInput` for more than eight unique addresses. Pass
  at most eight unique nameservers and manage extra retry attempts explicitly
  instead of repeating an address in the list.
- Each `Executor` starts one OS thread for socket closes that may block; `new`
  and `new_with_config` return the OS error if thread creation fails. Inside
  an executor, positive or unknown `SO_LINGER` uses a nonblocking queue of at
  most `ring_entries` descriptors plus one active close, and shutdown waits
  for these closes; if the queue is full or disconnected, FlowIO tries to
  disable linger before closing directly, which can still block if that fails.

### Removed

- **Breaking:** TCP and Unix `try_read_append`, `read_exact_append`, and
  `ReadExactAppendFuture` are removed. Use `try_read` or `read_exact` with
  `IoBuffMut` to append, and `ReadExactFuture<'a, IoBuffMut, S>` when naming
  the future type.
- **Breaking:** Checked byte accessors remove all `u128`, `i128`, `f32`, and
  `f64` operations and one-byte endian aliases from free functions, extension
  traits, and cursors. Use unsuffixed `u8`/`i8` accessors and native,
  little-endian, or big-endian 16/32/64-bit integer accessors; encode wider
  integers and floats with their standard-library byte conversions.
- **Breaking:** `PushError::value_mut` is removed. Save the reason with
  `error()` and recover the value with `into_value()`, or use `into_parts()`,
  before mutating a rejected value.
- **Breaking:** `flowio::runtime::timer::Elapsed` is removed. Use
  `TimeoutError::Elapsed` and match expiry separately from runtime errors.

### Fixed

- Buffer and task reference-count overflow terminates the process instead of
  wrapping and risking premature memory reclamation.
- I/O submission returns `WouldBlock` when a full submission queue makes no
  progress instead of spinning.
- TCP/Unix vectored I/O and SCTP metadata vectored I/O reject overflowing
  byte totals and inconsistent segment counts or lengths with `InvalidInput`
  before kernel submission.
- TCP and SCTP accepts rearm bare `POLLERR` once before returning `WouldBlock`;
  descriptor-pressure errors preserve readiness for a direct retry.
- TCP and SCTP connects reported as already connected (`EISCONN`) by the
  kernel complete successfully instead of failing.
- SCTP metadata receives accept `SCTP_RCVINFO` after other ancillary records,
  such as socket timestamps, and return default receive-info fields when
  receive-info is not requested, instead of `InvalidData`.
- `SctpStream::local_addrs` decodes Linux's local-address reply correctly
  instead of returning `InvalidData`. It and `peer_addrs` retry `ENOMEM` with
  larger buffers up to 1,024 `sockaddr_storage` units.
- `IoBuff::make_mut` preserves the consumed offset when copying a shared
  buffer, keeping `payload_remaining()` consistent with the unique-owner path.
- `Executor::run` returns `InvalidInput` when another run is active on the
  same thread, preventing nested runs from replacing the active runtime.
- TCP, Unix, UDP, SCTP, and TLS reads append new bytes to `IoBuffMut`.
  Received bytes remain visible after exact-read EOF or errors, including
  datagram truncation and metadata errors.
- TCP and Unix zero-length reads and writes return `Ok(0)` after runtime
  validation without allocating or submitting I/O; a
  zero-length read does not indicate peer EOF. Projected writes also validate
  empty projections, returning projection errors instead of bypassing them.
- TCP and Unix `try_writev_projected` can run during thread-local destruction
  or re-entry without panicking. If temporary `iovec` storage cannot be
  allocated, it returns `WouldBlock` with the source value and copies no
  payload bytes.
- Moving or dropping an `Executor` no longer invalidates the `iovec` storage
  held by vectored and projected I/O with more than 16 active segments.
- `IoBuffMut::payload_extend_from_tailroom(0)` preserves active trailer bytes
  and the restriction on changing payload length while a trailer is present.
- TLS accepts empty custom buffers with null pointers and initializes
  plaintext destinations before constructing mutable byte slices.
- `IoBuffPool::new` rejects configurations that exceed Rust allocation-layout
  limits with `IoBuffPoolConfigError::LayoutOverflow` before allocating a slab.
- With `test-support`, `SlabAllocator` rejects acquisition before
  initialization and initializes its provider at most once.
- UDP `send_to` rejects lengths outside io_uring's 32-bit limit before pointer
  access, allocation, or submission and returns the caller's buffer.
  Metadata-free `recv_msg` and `recv_from` no longer reject complete payloads
  because discarded ancillary data was truncated.
- Fixed-size socket-option getters return `InvalidData` for unexpected kernel
  output lengths.
- TCP and SCTP accept cancellation leaves pending connections in the listener
  backlog and preserves listener ownership through shutdown.
- SCTP metadata receives keep partial-delivery events enabled and consume
  identifiable partial-delivery abort notifications that the caller did not
  request, including notifications split across receives or buffers, resuming
  at the next intact record even after a dropped receive. Changing the mask
  cannot disable this recovery; unrelated notification fragments do not end
  the discarded data record. If shutdown abandons the
  ring owning a dropped metadata receive, later metadata receives fail
  permanently with `NotConnected`; use a new association.
- SCTP notification decoding enforces each record's declared bounds and
  reports its kind, declared length, and required length when a known record
  is too short.
- TLS writes after logical or physical write shutdown return `BrokenPipe`
  before accepting plaintext and return the caller's buffer. A prior transport
  write error takes precedence.
- TLS protocol and input-buffer failures stop further transport reads;
  authenticated plaintext drains before later reads return `InvalidData`. A
  direct input-buffer failure returns its original error once, then
  `InvalidData` without a detailed payload.
- `transport_write_buffer_size` limits each chunk of TLS ciphertext passed to
  TCP, preserving order without growing FlowIO's ciphertext buffer.
- TLS buffer setup rejects zero or unrepresentable sizes with `InvalidInput`
  and allocation failure with `OutOfMemory`. Recreating an unavailable
  ciphertext buffer also returns allocation errors instead of panicking.
- `tls_server_end_point` rejects malformed certificate DER, including missing,
  reordered, extra, or trailing outer fields, noncanonical lengths, and an
  empty signature or nonzero unused-bit count. It checks structure, not the
  signature's cryptographic validity. SHA-256/384/512-with-RSA identifiers
  accept absent parameters alongside the explicit NULL form.
- DNS parsing uses only the received datagram bytes, so stale buffer contents
  cannot complete a truncated response. Before applying a response code, it
  validates every declared record and matches any echoed question's name,
  type, and class; mismatched negative responses fail over.
- DNS rejects non-QUERY opcodes, invalid UTF-8 labels, and literal dots within
  wire labels. Valid non-ASCII names use ASCII-only case folding.
- DNS query names may have one trailing root dot and must fit 253 presentation
  bytes, 255 encoded bytes, and 63 bytes per label; invalid input is rejected
  before query allocation or I/O. CNAME data must consume its declared length
  exactly and cannot name the root; A/AAAA data must have the required size
  in every section and class.
- DNS resolves only Answer-section IN records; Authority and Additional
  records cannot supply an address or CNAME. For an active name without a
  matching-family address, multiple distinct CNAME targets return
  `InvalidData`.
- DNS follows one CNAME chain with loop detection, separate 16-hop
  per-response and total limits, at most one follow-up query round, and a
  separate compression pointer depth limit. Exhausting the total hop budget
  stops nameserver retries for that family but permits a usable sibling-family
  result.
- DNS combines A and AAAA results without losing a usable address or CNAME to
  the other family's empty, recoverable-error, or NXDOMAIN result. Addresses
  win; equal-ranked CNAME or recoverable-error results prefer A.
- DNS timer-runtime failures preserve their `io::Error` and stop the affected
  family's retries, with an A-side terminal error preventing the AAAA query. A
  completed A address wins over a later AAAA terminal error; ordinary I/O
  errors and per-attempt expiry allow failover.
- System DNS configuration retains the first eight unique valid nameservers;
  duplicates do not consume the limit and later unique entries are omitted.
- Hosts-file and system nameserver parsing ignore invalid UTF-8 inside
  comments and skip only lines with invalid entry text. Hosts files use `#`
  comments; system nameserver files also accept `;` comments.
- Hosts-file aliases match case-insensitively with or without one trailing
  root dot.
- Async transport operations and `Sleep` reject inactive executor contexts
  or incompatible task wakers with `NotConnected`; submitted I/O returns the
  error and buffer only after completion, while `Sleep` reclaims its armed
  timer. Completion wakes waiters on their own executor, and temporary
  same-executor cleanup polls keep already-submitted I/O pending.
- Timeout wrappers reject inactive or foreign executors with
  `TimeoutError::Runtime(NotConnected)` before polling the wrapped future.
- Timer scheduling measures each relative sleep from its own arm time, keeps
  time monotonic after idle periods, and preserves deadlines across
  timer-wheel wraps and cascades. Very large idle waits fit the kernel
  timespec range instead of overflowing.
- `Executor::run` preserves unfinished tasks after `WouldBlock` so a later run
  can resume them; shutdown cancels each remaining task once. Debug builds
  detect standard task wakers used outside their owner thread.
- Runtime cleanup releases completed I/O resources even when internal
  completion accounting reports an error.
- Executor shutdown finishes task destruction before tearing down timers and
  I/O or joining the socket-close worker, even when destructors drop another
  executor or panic. Nested task destruction uses an iterative queue to
  prevent stack exhaustion on long task-ownership chains.
- TLS reads keep unfed ciphertext when records spanning socket reads reach
  rustls's plaintext or handshake-output limit, avoiding a spurious "received
  plaintext buffer full" failure and lost ciphertext. Bytes after an
  authenticated `close_notify` in the same socket read are discarded, with
  authenticated plaintext drained before EOF.

## [0.2.0-alpha.1]

A pre-1.0 release that tightens the public API and clarifies several SCTP
controls.

### Added

- `UnixStream` non-blocking one-shot methods `try_read`, `try_read_append`,
  `try_write`, and `try_writev_projected`, matching `TcpStream`.
- `TcpStream::try_clone_for_split` for obtaining split read/write handles.
- `SctpStream` now implements `Drop` for explicit association cleanup, and
  `SctpRecvInfo` exposes an `end_of_record` message-boundary flag.
- `writev` and `writev_all` on `TcpStream` and `UnixStream` now accept any
  write buffer chain (`IoBuffVec` or `IoBuffReadOnlyVec`).

### Changed

- **Breaking:** SCTP `set_primary_addr` is now `set_primary_dest_addr`, and
  `set_peer_primary_addr` is now `request_peer_use_local_addr`. Behavior and
  socket options are unchanged; the names distinguish choosing a peer
  destination from asking the peer to send to one of this endpoint's local
  addresses.
- **Breaking:** `writev_read_only` and `writev_all_read_only` are folded into the
  now-generic `writev` and `writev_all`. Existing `writev(IoBuffVec)` calls are
  unchanged; read-only callers pass the chain directly to `writev`.
- **Breaking:** `try_push` on `IoBuffVec`, `IoBuffVecMut`, and
  `IoBuffReadOnlyVec` is removed; use `push`, which already returns the segment
  on overflow.

### Removed

- **Breaking:** `SctpStream::remote_addr` — use `peer_addr`.
- **Breaking:** `RuntimeStats`, `Executor::last_stats`, and the public `Executor`
  process-quota and CPU-affinity fields are no longer part of the default API;
  configure the executor through `ExecutorConfig`. Runtime statistics were a
  hand-rolled testing aid rather than a supported observability interface; a
  supported observability API will be introduced separately.
- **Breaking:** `TimerRuntime` and the `Sleep::new_duration` / `new_deadline`
  constructors are no longer public. Use the `sleep`, `sleep_until`, and
  `timeout` functions.

### Fixed

- TLS reads now drain bulk multi-record ciphertext correctly.
- Oversized DNS UDP responses are rejected or failed over before parsing.
- Linux CPU-affinity configuration range-checks the CPU set capacity.
- SCTP dropped partial notifications enter the discard state consistently.
