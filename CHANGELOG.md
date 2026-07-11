# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Alpha prereleases carry no compatibility guarantee.

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
