# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Alpha prereleases carry no compatibility guarantee.

## [Unreleased]

### Changed

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

### Fixed

- DNS A and AAAA outcomes are now combined without letting one family's empty,
  recoverable-error, or contradictory NXDOMAIN result discard the other
  family's usable address or CNAME. Ranked outcome selection is deterministic:
  addresses win, same-rank CNAME and recoverable-error conflicts remain
  A-first, and terminal timer/runtime failures retain their documented
  no-address precedence.
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
