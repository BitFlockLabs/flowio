# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Alpha prereleases carry no compatibility guarantee.

## [Unreleased]

### Changed

- **Breaking:** `JoinHandle<T>` now resolves to `Result<T, JoinError>`.
  Executor shutdown reports unfinished work as `JoinError::Cancelled` instead
  of returning an uninitialized task result.
- **Breaking:** `TcpStream::from_raw_fd`, `UnixStream::from_raw_fd`, and
  `SctpStream::from_raw_fd` now require an unsafe sole-ownership proof. Prefer
  the new safe `from_owned_fd` constructors, which consume `OwnedFd` and make a
  second safe close owner unrepresentable.
- **Breaking (`test-support` only):** custom `MemoryProvider` implementations
  must now use an audited `unsafe impl` and uphold the documented alignment,
  provenance, size, uniqueness, lifetime, and exact-free contract. Raw `Slab`
  fields are now opaque and can be constructed only by allocator internals.
- `IoBuffReadWrite` now provides `initialized_writable_slice` for userspace
  producers that require `&mut [u8]`. Its default initializes exactly the
  requested writable prefix; `Vec`, `Box<[u8]>`, and `IoBuffMut` specialize the
  operation to preserve bytes already known to be initialized.
- `IoBuffReadWrite` now provides `write_base_len` for relative contiguous-read
  publication. Its default remains zero for overwrite-style flat/custom
  buffers; `IoBuffMut` reports its current payload length so reads append to
  existing payload data.
- The unsafe `IoBuffReadOnly` and `IoBuffReadWrite` contracts now explicitly
  permit a null pointer only for an empty window. Every positive-length range
  must be non-null, suitably aligned, contained in one stable allocation, and
  satisfy the documented initialization and access requirements.
- **Breaking:** `IoBuffMut::payload_unwritten_mut` now exposes spare payload
  capacity as `MaybeUninit<u8>`. Callers initialize the intended prefix and
  publish it with unsafe `payload_set_len_initialized`; safe
  `payload_set_len` cannot expose bytes beyond the tracked initialized
  frontier.
- **Breaking:** `timeout` and `timeout_at` now return `TimeoutError`, whose
  `Elapsed` and `Runtime(io::Error)` variants distinguish deadline expiry from
  timer allocation or runtime failure. The former unit `Elapsed` error type is
  removed. TCP/SCTP connect-timeout helpers preserve runtime errors and map
  only actual expiry to `TimedOut`.
- TLS transport-read scratch is now capped at one 18,437-byte wire record after
  validating the original option. Values below or equal to the cap are
  unchanged; exceptional read-scratch recovery uses the same cap, and write
  scratch remains independently configured. A 64 KiB read option therefore
  requests 47,099 fewer bytes per connection, while ordinary I/O continues to
  reuse the setup allocation.
- **Breaking:** `SctpResetStreams` can no longer be built with a downstream
  struct literal because it carries private all-stream intent. Construct a
  listed-stream request with `incoming`, `outgoing`, or `bidirectional`, or use
  the new `all_incoming`, `all_outgoing`, and `all_bidirectional` constructors
  for the Linux zero-count sentinel, then customize the public fields without
  changing whether the stream list is empty. Generic empty requests and any
  other intent/list mismatch now return `InvalidInput` before a socket-option
  syscall.
- Runtime dependencies have been refreshed while retaining the existing
  feature set and minimum supported Rust version.
- Compatibility documentation now consistently states the binding Linux 5.11
  runtime floor imposed by `IORING_ENTER_EXT_ARG`. The older
  `IORING_OP_CLOSE` and 14-byte `SCTP_EVENTS` requirements do not lower that
  floor or require a legacy SCTP subscription fallback.
- Rich SCTP message operations now construct kernel-visible iovecs,
  address/control storage, and self-referential message headers directly in
  their retained completion payloads. Vectored receive and send futures no
  longer stage iovec arrays, and scalar/vectored send moves the caller-owned
  buffer only after every non-owning field is initialized; public API, wire
  behavior, and completion-time ownership are unchanged.
- Rich SCTP receive now decodes each caller-visible notification once and
  reuses that result for discard recovery and returned metadata. FlowIO-only
  forced notifications remain internal and parse-free; framing, visibility,
  public API, and wire behavior are unchanged.
- DNS response parsing now validates Authority and Additional record owners
  and CNAME targets without materializing discarded strings. Answer-section
  name handling, structural validation, response-code precedence, and
  resolution behavior are unchanged.

### Fixed

- DNS resolution now stops retrying other nameservers when a successfully
  parsed CNAME chain exceeds the remaining total-hop budget. This local policy
  failure still allows the sibling address family to supply a usable address.
- DNS query normalization now removes at most one optional trailing root dot.
  Names with repeated trailing dots return `InvalidInput` before query
  allocation or DNS network I/O, while undotted and single-root-dot names retain
  their existing wire form.
- SCTP receive APIs now reject zero-length caller receive windows with
  `InvalidInput` before kernel submission. This keeps a successful zero-byte
  lean receive unambiguous as clean peer EOF and returns the rental buffer
  unchanged on validation failure. Callers that used a zero-length lean
  receive as a no-op now receive this error instead.
- DNS response parsing now rejects non-QUERY opcodes before question,
  response-code, or resource-record handling. Candidate filtering remains
  allocation-free, while valid QUERY response behavior is unchanged.
- DNS response parsing now rejects a literal `.` inside any direct or
  compressed wire label before question comparison, response-code handling,
  CNAME selection, or follow-up, so distinct label boundaries cannot collapse
  to the same dotted presentation.
- Timeout wrappers now validate their active and origin executor before polling
  the wrapped future. Inactive or foreign polls return
  `TimeoutError::Runtime(NotConnected)` without triggering wrapped-future side
  effects, while valid immediately ready futures remain inner-first and arm no
  timer.
- TCP and SCTP accept cancellation now retires only the readiness wait and
  leaves queued peers in the listener backlog. Listener ownership remains
  valid through cancellation and shutdown, while terminal sockets with
  positive or unclassifiable linger attempt bounded off-thread close admission.
  Full or disconnected admission disables linger best-effort before direct
  close; a failed waiver retains the original positive-linger blocking risk.
- TLS ciphertext draining now treats `transport_write_buffer_size` as a hard
  per-chunk FlowIO bound. One fixed-capacity scalar/vectored adapter preserves
  ciphertext order, never grows the wrapper scratch during ordinary writes,
  and fully submits each owned chunk before collecting the next.
  `rustls_buffer_limit: None` remains an independent rustls-internal buffering
  choice.
- FlowIO-configured SCTP sockets now keep the partial-delivery event subscribed
  whenever receive metadata is enabled. Abort events identifiable as forced
  only for this invariant are consumed internally to resynchronize metadata
  receive before the next intact record; explicitly requested partial-delivery
  notifications remain observable. Notification-mask changes cannot remove
  the mandatory event, while `SctpSocketConfig::data()` plus plain `recv`
  remains lean.
- `tls_server_end_point` now derives a channel-binding digest only after the
  certificate DER is consumed exactly as the ordered `TBSCertificate`,
  `signatureAlgorithm`, and `signatureValue` outer fields. Malformed, missing,
  reordered, extra, or trailing certificate structure returns `None`; the
  existing supported digest mapping is unchanged.
- TLS client scratch sizes are validated before allocation. Zero or
  unrepresentable capacities return `InvalidInput`, initial reservation
  failure returns `OutOfMemory`, and the exceptional missing-scratch recovery
  path is also fallible instead of panicking.
- Malformed matching-ID DNS datagrams are now rejected by the shared
  candidate/full-parser name walker without constructing and then discarding
  an `io::Error`. Full parsing preserves the same error kinds and messages,
  while candidate rejection performs no heap allocation.
- DNS resolution now accepts CNAME and address data only from the Answer
  section. Authority and Additional records remain structurally parsed but
  cannot seed or extend resolution; an Answer CNAME without an Answer address
  uses the existing bounded follow-up query.
- DNS responses with an echoed question now match its name, type, and class
  before applying NXDOMAIN or another nonzero response code. Mismatched
  negative responses fail over instead of terminating the logical lookup;
  questionless failover-class responses and questionless-NXDOMAIN draining
  retain their existing behavior.
- DNS responses now validate every declared record before applying NXDOMAIN or
  another response code. All sections and classes require sound record framing,
  exact A/AAAA data lengths, and fully consumed CNAME names; only Answer+IN
  records remain eligible to supply resolution data.
- DNS response names now require valid UTF-8 in every literal label, including
  compressed suffixes. The shared allocation-free candidate/validation walker
  rejects invalid labels before echoed-question comparison, response-code
  handling, or CNAME follow-up; valid non-ASCII text remains supported with
  ASCII-only case folding.
- DNS lookup now rejects normalized query names over 253 presentation bytes,
  encoded names over 255 bytes, and labels over 63 bytes with `InvalidInput`
  before allocating or sending a query packet. CNAME RDATA must consume its
  declared length exactly, so both overruns and trailing bytes are treated as
  malformed responses while valid compressed and uncompressed names remain
  accepted.
- DNS A and AAAA outcomes are now combined without letting one family's empty,
  recoverable-error, or contradictory NXDOMAIN result discard the other
  family's usable address or CNAME. Ranked outcome selection is deterministic:
  addresses win, same-rank CNAME and recoverable-error conflicts remain
  A-first, and terminal timer/runtime failures retain their documented
  no-address precedence.
- DNS CNAME traversal now follows one linear Answer-only chain with explicit
  loop detection, independent 16-hop per-response and total limits, at most one
  follow-up query round, and a separate compression-pointer depth limit.
- TCP and Unix immediate, partial-async, and all-async projected writes now,
  after any async runtime-context validation succeeds, validate a
  declared-empty projection once instead of bypassing its stale nonempty
  output or implementation error; valid empty writes still allocate and
  submit nothing.
- `ProviderOwnedPool` no longer exposes broad mutable access to either half of
  its self-referential provider/pool relation. Narrow inlined operations retain
  the existing allocation behavior while keeping pool teardown before provider
  teardown.
- Dev-only intrusive-list cursors now advance past a pending node before
  unlinking it, so removing the next forward or backward node cannot leave the
  cursor pointing at cleared links.
- `SlabAllocator` now rejects slab acquisition before initialization and
  initializes its backing provider at most once, preventing safe code from
  formatting a slab before the provider accepts the required alignment.
- Pooled retained iovec scratch now keeps its heap-stable sidecar pool alive
  until block return. Parent-pool movement, detached projected-write payloads,
  and parent-first teardown can no longer leave scratch Drop with a dangling
  owner pointer.
- TLS partial writes accept a custom read-only buffer whose empty window uses
  a null pointer, returning zero without consulting that pointer or forming a
  raw slice from it.
- TLS partial and exact reads initialize their caller-owned plaintext
  destination before forming a mutable byte slice. Each nonempty destination
  that reaches plaintext polling is initialized once, while kernel read paths
  continue writing directly into raw spare capacity without a mandatory
  zeroing pass.
- Contiguous stream, UDP, SCTP, and TLS reads preserve existing logical buffer
  contents on zero progress and publish positive progress from the captured
  destination base. Exact-read EOF/errors and datagram metadata/truncation
  errors publish any bytes actually received before returning the error.
- `Executor::run` now preserves unfinished tasks when it returns `WouldBlock`;
  a later run resumes the same scheduler state, while executor shutdown
  cancels each remaining task exactly once.
- Debug builds assert that standard task wakers are cloned, woken, and dropped
  only on their owner thread; release builds retain the zero-cost
  single-threaded waker path.
- Fresh and reused `IoBuffMut` capacity cannot become readable through safe
  length growth before the caller initializes it.

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
  socket options are unchanged; the names distinguish choosing which peer
  address we send to from requesting that the peer use one of our local
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
