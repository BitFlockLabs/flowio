# flowio

flowio is a single-threaded Rust async runtime and transport library built on Linux `io_uring`.

It provides an executor, timers, buffer types, and concrete Unix, TCP, UDP,
SCTP, TLS-client, and DNS helper APIs. I/O uses owned buffers: pass a buffer
into an operation, get it back with the result.

This is an alpha release (`0.2.0-alpha.1`). The API is unstable and may change
between alpha releases; public API changes are recorded in the changelog. It is
not recommended for production yet.

## Install

Requires Linux kernel 5.11 or newer and Rust 1.88 or newer. The runtime is
built on `io_uring` and requires `IORING_ENTER_EXT_ARG`; executor construction
fails with `Unsupported` when the running kernel does not report that feature.
The crate does not build or run on non-Linux targets.

```toml
[dependencies]
flowio = "0.2.0-alpha.1"
```

From the repository:

```toml
[dependencies]
flowio = { git = "https://github.com/BitFlockLabs/flowio" }
```

## Documentation

Full API documentation for released versions is published at
[docs.rs/flowio](https://docs.rs/flowio).

## API surface

The supported user-facing surface is the documented `runtime` and `net`
modules. The feature-gated `test-support` and `fuzzing` exports exist only for
the crate's own tests and fuzzing; they are hidden from the generated
documentation and are not a stable downstream contract. This is a pre-1.0
crate, and deliberate compatibility changes are recorded in `CHANGELOG.md`.

- `runtime` provides the executor, reactor configuration, timers, and buffers.
- `net` provides Unix, TCP, UDP, one-to-one SCTP, client-side TLS, and DNS.

## Usage

This example uses a reusable buffer pool. It deliberately uses `write_all` and
`read_exact` because the example's five-byte frame requires complete-buffer
semantics; protocols that already track partial progress should use `write`
and `read` as described below.

```rust
use flowio::net::unix::UnixStream;
use flowio::runtime::buffer::pool::{IoBuffPool, IoBuffPoolConfig};
use flowio::runtime::executor::Executor;
use std::io;

fn main() -> io::Result<()> {
    let mut pool = IoBuffPool::new(IoBuffPoolConfig {
        headroom: 0,
        payload: 64,
        tailroom: 0,
        objs_per_slab: 16,
    })
    .map_err(io::Error::other)?;
    pool.init();

    let mut executor = Executor::new()?;

    executor.run(async move {
        let (mut left, mut right) = UnixStream::pair().unwrap();

        let mut send = pool.alloc().unwrap();
        send.payload_append(b"hello").unwrap();

        let (write_res, _send) = left.write_all(send).await;
        write_res.unwrap();

        let recv = pool.alloc().unwrap();
        let (read_res, recv) = right.read_exact(recv, 5).await;
        read_res.unwrap();

        assert_eq!(recv.bytes(), b"hello");
    })?;

    Ok(())
}
```

The same ownership pattern is used across the transport APIs:

```rust
# use flowio::net::unix::UnixStream;
# use flowio::runtime::buffer::IoBuffMut;
# async fn example(mut stream: UnixStream, buffer: IoBuffMut, len: usize) {
let (result, buffer) = stream.read_exact(buffer, len).await;
# let _ = (result, buffer);
# }
```

Direct writes into spare `IoBuffMut` payload capacity use
`payload_unwritten_mut()`, which returns `MaybeUninit<u8>` slots. Initialize
the intended prefix and then publish it with the unsafe
`payload_set_len_initialized()` operation. Safe `payload_set_len()` can only
move the visible length within bytes the buffer already knows are initialized;
it never makes fresh pooled or heap capacity readable. Borrowing the spare
slice conservatively discards knowledge of any initialized-but-hidden bytes,
because a `MaybeUninit` slot may be safely de-initialized.

Userspace producers that require `&mut [u8]` use the unsafe
`IoBuffReadWrite::initialized_writable_slice()` hook after validating the
requested prefix against `writable_len()`. The provided implementation
initializes exactly that prefix. `IoBuffMut` reuses its initialized frontier,
`Vec<u8>` initializes only bytes beyond its logical length, and `Box<[u8]>`
reuses its fully initialized storage. TLS plaintext reads use this hook once
for each nonempty destination that reaches plaintext polling. Direct kernel
reads deliberately continue using raw writable capacity, so they do not pay a
mandatory userspace initialization pass.

Unsafe custom buffer implementations may return a null pointer only when the
corresponding readable or writable window is empty. Every positive-length
range must be non-null, suitably aligned, contained in one stable allocation,
and satisfy the trait's initialization and access contract. TLS partial writes
handle an empty readable window without consulting its pointer or constructing
a raw slice from it.

Contiguous read and receive operations report byte counts relative to the
operation while publishing them relative to the destination's captured write
base. `IoBuffMut` defines that base as its current payload length, so positive
progress appends without overwriting an existing payload. Flat buffers and
custom implementations keep the provided base of zero unless they explicitly
opt into append-style publication. A zero-byte completion or an error before
any progress does not publish a new length, so the buffer's existing logical
contents remain unchanged. Exact reads publish any completed prefix before
returning EOF or another terminal error.

## Fast-path guidance

flowio's fast path is the steady-state per-task, per-message, or per-I/O path
after setup capacity has been acquired. “Preferred” below means the API avoids
specific work visible in its implementation; it is not a claim that every
workload is faster without measurement.

| Concern | Preferred on the fast path | Avoid on the fast path | Why / alternative |
|---|---|---|---|
| Runtime lifecycle | One long-lived `Executor::run` boundary per runtime thread | Constructing an `Executor` or entering `run` per request | Construction initializes `io_uring`, stable owner storage, task queues, and timers. A stalled run preserves its tasks for a later `run`, but steady-state work should still spawn inside one active run. |
| Task admission | `Executor::spawn`; use `try_spawn` when failure must return the future | `spawn` when the future owns a response or cleanup obligation | Tasks use fixed-size slots acquired in slabs. `try_spawn` preserves the unpolled future on allocation pressure; `spawn` maps the error and drops it. |
| Fixed-shape buffers | Pre-acquired `IoBuffPool` slots | `IoBuffMut::new` per message | Pool reuse avoids per-buffer allocator traffic while warmed capacity is available. Use `new` for setup or genuinely variable shapes. |
| Frozen buffers | `freeze`, `clone`, `slice`, or `try_mut` when ownership permits | `make_mut` on a shared buffer | Shared `make_mut` allocates and copies. Keep exclusive `IoBuffMut` ownership or use `try_mut` when copying is not acceptable. |
| TCP/Unix stream I/O | `read` / `write` when the protocol tracks partial progress | `_exact` / `_all` for data that does not require complete-buffer semantics | Plain-stream partial APIs make one transport submission. Complete APIs may process a completion and resubmit. |
| TLS plaintext I/O | `TlsClientStream::read` / `write` when the protocol tracks partial plaintext | TLS `_exact` / `_all` when complete plaintext is not required | Partial TLS APIs avoid looping for the caller's complete plaintext buffer, but TLS record processing may still require multiple raw TCP operations. |
| Payload shape | Contiguous APIs for one byte range; vectored/projected APIs for existing segmentation | Building a chain for one contiguous range or coalescing segmented data just to call `write` | Match the API to existing ownership; vectored paths construct bounded iovec metadata, while coalescing copies bytes. |
| Immediate deadline edge | `try_read`, `try_write`, and `try_writev_projected` only after a deadline has already reached zero | Polling `try_*` as the normal async path | `try_*` makes one direct nonblocking syscall and returns `WouldBlock`; normal async methods register reactor work and wake the task. |
| UDP peer selection | Connected `send` / `recv` for a stable peer; `recv_msg` when truncation must be detected | `send_to` / `recv_from` for a fixed peer | Connected calls avoid per-datagram address handling. Use address-bearing methods when the peer actually varies. |
| SCTP data | `SctpSocketConfig::data()` plus `send` / `recv` when data sizing is guaranteed | Rich notification/metadata APIs when metadata is unused | The lean APIs avoid ancillary metadata and event processing, but `recv` does not expose EOR/truncation. Use `send_msg` / `recv_msg` when stream, PPID, flags, EOR, truncation, or notifications matter. |
| Timers | One `timeout_at` or `timeout` around a protocol phase when a deadline is required | A separate timeout around every tiny I/O step | Each armed deadline consumes timer-wheel state and expiry/cancellation work. Preserve finer timers when protocol semantics require them. |
| DNS/TLS setup | Reuse `DnsResolver`, resolved addresses, connectors, and established TLS streams | Resolving names or handshaking in a per-message loop | Resolver setup/query construction and TLS handshakes may allocate and perform setup I/O. |

`JoinHandle<T>` resolves to `Result<T, JoinError>`. Normal completion returns
`Ok(T)`; dropping the owning executor cancels unfinished tasks and makes their
handles return `Err(JoinError::Cancelled)`. If `Executor::run` returns
`WouldBlock`, live tasks remain owned by that executor and a later `run` resumes
them alongside its new root future.

Only one `Executor::run` may be active on a thread. Poll runtime I/O and timer
futures only through the executor that submitted or armed them. Polling without
an active FlowIO run or through another executor returns `NotConnected`.
Unsubmitted rental I/O returns its buffer immediately; submitted I/O retains
the buffer until the original completion and then returns it with that error.

Standard task `Waker` values must be cloned, woken, and dropped on the thread
that owns their executor. Debug builds assert this contract; release builds
keep the direct, allocation-free owner-thread wake path. FlowIO intentionally
has no cross-thread task-waker relay. Use an application-owned channel or
reactor-layer signaling for cross-thread work, then create or wake FlowIO tasks
on the owner thread.

`timeout` and `timeout_at` return `TimeoutError::Elapsed` only when the
deadline wins. Timer allocation or runtime failures are returned as
`TimeoutError::Runtime(error)` with the original `io::ErrorKind`. TCP and SCTP
`connect_timeout` helpers translate only true expiry to `TimedOut` and preserve
runtime failures such as `OutOfMemory`.

### Warming a buffer pool

`IoBuffPool` acquires slabs lazily. If allocator activity must be absent from a
known steady-state working set, acquire that many slots during setup and return
them before entering the measured or latency-critical path:

```rust
use flowio::runtime::buffer::pool::{IoBuffPool, IoBuffPoolConfig};

let mut pool = IoBuffPool::new(IoBuffPoolConfig {
    headroom: 0,
    payload: 1024,
    tailroom: 0,
    objs_per_slab: 64,
})
.unwrap();
pool.init();

let mut warm = Vec::with_capacity(64);
for _ in 0..64 {
    warm.push(pool.alloc().unwrap());
}
drop(warm); // all 64 slots are now reusable

let mut buffer = pool.alloc().unwrap();
buffer.payload_append(b"frame").unwrap();
```

Task, timer, retained-payload, and buffer pools also acquire backing slabs on
demand. Warm representative runtime work before measuring allocator behavior.
Reactor completion-state count is hard-capped by `ring_entries`, and retained
vectored scratch rejects more than 1024 active iovecs. Task and timer slab
counts are currently allocator-limited rather than user-configurable hard
limits.

Complete-buffer APIs remain the intended choice when a protocol requires an
exact frame. They are “avoid” choices only when the caller already has partial
progress logic and would otherwise duplicate the same retry work.

For TCP read/write split ownership, call `TcpStream::try_clone_for_split`
during connection setup only. It duplicates the connected descriptor; both
handles share one kernel TCP stream and socket options.

Use vectored APIs only when the payload is already segmented. For one
contiguous payload, the contiguous APIs avoid iovec construction and
projected-write scratch storage.

For repeated outbound TCP or SCTP connection attempts, use `TcpConnector` or
`SctpConnector` and reuse the connector-owned attempt slots. Connection
establishment is setup/connection-path work, not the per-message fast path;
every attempt still creates and configures a fresh socket. The one-shot connect
helpers are appropriate for isolated attempts.

For fixed-peer UDP, call `connect` once and use `send` and `recv`. Use
`send_to` and `recv_from` when the peer changes per datagram. Use `recv_msg`
when connected UDP must detect datagram truncation.

For data-only SCTP associations, configure the socket with
`SctpSocketConfig::data()` and use `SctpStream::send` and `SctpStream::recv`.
Use `send_msg`, `recv_msg`, and their vectored variants when per-message SCTP
metadata or notifications matter.

On sockets configured by FlowIO, enabling `recv_rcvinfo` also keeps the SCTP
partial-delivery event subscribed even when the requested notification mask
sets `partial_delivery` to false. Linux uses that event when it abandons a
partial delivery, so metadata receive can retire its internal discard state
before delivering the next intact record. An abort event identifiable as
subscribed only for this invariant is consumed internally; callers that
explicitly request partial-delivery notifications continue to receive them. A
later `set_notification_mask` call cannot remove the event while metadata
receive requires it. `SctpSocketConfig::data()` leaves both metadata and
notifications disabled, so plain `recv` retains the lean path.

When the requested notification mask is empty, PDAPI is the sole kernel event,
so even its short-buffer fragments remain internal through EOR. If other
notification types are requested, a caller buffer that truncates an event
before it can be parsed completely retains the normal `InvalidData` behavior.

`SctpStream::from_owned_fd` and `SctpStream::from_raw_fd` adopt ownership but
do not inspect or configure the socket. Before using metadata receive on an
externally configured descriptor, the caller must provide nonblocking mode,
`SCTP_RECVRCVINFO`, and the `SCTP_PARTIAL_DELIVERY_EVENT` subscription needed
for discard resynchronization. Calling FlowIO's `set_notification_mask` later
queries the descriptor's current receive-info setting and preserves that
dependency, but adoption itself remains syscall-free.

SCTP stream reset is a setup/control-plane operation. The generic
`SctpResetStreams::incoming`, `outgoing`, and `bidirectional` constructors are
for one or more listed stream IDs; an empty list is rejected with
`InvalidInput` before a socket-option syscall. Use the explicit
`all_incoming`, `all_outgoing`, or `all_bidirectional` constructor when the
association-wide Linux zero-count sentinel is intended:

```rust
use flowio::net::sctp::SctpResetStreams;

let mut request = SctpResetStreams::all_outgoing();
request.assoc_id = 7;
assert!(request.streams.is_empty());
```

`SctpResetStreams` now carries private intent state, so downstream struct
literals no longer compile. Start with the matching constructor and then set
the still-public `assoc_id` or `flags` field when custom values are needed. A
listed request's `streams` may be replaced only with another nonempty list; an
`all_*` request must keep its list empty. An intent/list mismatch is
`InvalidInput`. This control API does not add work to established per-message
SCTP send or receive paths.

Keep DNS resolution in setup or control paths when the protocol permits.
Resolve names once, retain the resulting addresses, and put timer deadlines
around protocol phases rather than every individual message to reduce timer
bookkeeping.

## Configuration

`Executor::new()` uses the default ring size and scheduling quota. Use
`Executor::new_with_config()` when those values matter:

```rust
use flowio::runtime::executor::{Executor, ExecutorConfig};
use flowio::runtime::reactor::ReactorConfig;
use std::io;

fn main() -> io::Result<()> {
    let _executor = Executor::new_with_config(ExecutorConfig {
        reactor: ReactorConfig { ring_entries: 512 },
        process_quota: 64,
        cpu_affinity: None,
    })?;

    Ok(())
}
```

`TlsClientOptions::transport_read_buffer_size` is validated as a nonzero
setup option, then capped at 18,437 bytes for the effective per-connection read
scratch: one maximum TLS wire record. Smaller values retain their requested
size, the write scratch remains independently configured, and ordinary TLS I/O
reuses both setup allocations. Raising the read option above the cap therefore
does not increase the raw read size or reserve additional wrapper read scratch.

SCTP has separate socket and association configuration types. Basic
one-to-one SCTP works through `SctpListener`, `SctpConnector`, and
`SctpStream`; advanced SCTP options depend on kernel support.

## Limitations

- Linux only. The runtime is built on `io_uring`.
- Single-threaded task execution. A runtime instance and its tasks, buffers, and
  wakers are owner-thread state and are not cross-thread APIs; a runtime is used
  from the one thread that runs it.
- Alpha API; compatibility may change between alpha releases.
- Task and timer pools acquire fixed-size slabs on demand and currently have no
  user-configurable total slab cap. Sleep allocation failure is an
  `io::Error`; timeout wrappers return it through `TimeoutError::Runtime`.
  Callers that require a strict process-memory ceiling must enforce one
  externally.
- Dropping an in-flight read can discard bytes from a racing completion. Treat
  read cancellation or timeout as a protocol boundary unless your protocol has
  its own recovery path.
- TLS is client-side only.
- SCTP behavior depends on the host kernel and loaded SCTP support. Some socket
  options may fail even when basic messaging works.
- There is no built-in metrics or tracing exporter.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
