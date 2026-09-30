# flowio

flowio is a single-threaded Rust async runtime and transport library built on
Linux `io_uring` for x86-64.

It provides an executor, timers, buffer types, and concrete Unix, TCP, UDP,
SCTP, TLS-client, and DNS helper APIs. I/O uses owned buffers: pass a buffer
into an operation, get it back with the result.

This is an alpha release (`0.2.0-alpha.1`). The API may change between alpha
releases; public API changes are recorded in [CHANGELOG.md](https://github.com/BitFlockLabs/flowio/blob/main/CHANGELOG.md).
It is not recommended for production yet.

## Install

Requires x86-64 Linux 5.11 or newer and Rust 1.88 or newer. Executor construction
returns `Unsupported` if the kernel lacks `IORING_ENTER_EXT_ARG`, which timed
waits require. Other CPU architectures are not supported or validated, and the
crate does not build or run on non-Linux targets.

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

Full API documentation is published at [docs.rs/flowio](https://docs.rs/flowio).

## API surface

The supported user-facing surface is the documented
[`runtime`](https://docs.rs/flowio/latest/flowio/runtime/index.html) and
[`net`](https://docs.rs/flowio/latest/flowio/net/index.html) modules. `runtime`
provides the executor, reactor configuration, timers, and buffers; `net`
provides Unix, TCP, UDP, one-to-one SCTP, client-side TLS, and DNS.

The `test-support` and `fuzzing` features expose helpers for the crate's tests
and fuzzing. They are hidden from generated documentation and are not a stable
downstream contract.

## Usage

This example reuses a buffer pool and transfers one five-byte frame.
`write_all` and `read_exact` complete the frame; use `write` and `read` when
the protocol tracks partial progress itself.

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

## Runtime and I/O guidance

The fast path is steady-state task, message, and I/O processing after setup.
Keep one executor active on each runtime thread and reuse connections, resolved
addresses, and fixed-shape buffers. These choices avoid specific allocation,
copying, and setup work; measure latency and throughput for the actual workload.

I/O returns the caller's buffer with its result. `IoBuffMut` receives append to
its payload; frozen buffers can share bytes without copying. Pools acquire
slabs lazily, so acquire and return the expected working set during setup and
exercise representative task, timer, and I/O work before measuring allocations.
The [buffer pool example](https://docs.rs/flowio/latest/flowio/runtime/buffer/pool/struct.IoBuffPool.html)
shows how to warm a pool. Buffer initialization and custom-buffer safety rules
are documented in the [buffer module](https://docs.rs/flowio/latest/flowio/runtime/buffer/index.html).

| Concern | Prefer | Avoid | Reason |
|---|---|---|---|
| Runtime | One long-lived `Executor::run` per owner thread | Executor construction or `run` per request | Runtime setup initializes the ring, task queues, and timers. |
| Tasks | `try_spawn` when failure must return the future | `spawn` when dropping rejected work would lose a cleanup obligation | `try_spawn` returns the unpolled future; `spawn` drops it on failure. |
| Buffers | Warmed `IoBuffPool` slots for repeated layouts | `IoBuffMut::new` per fixed-shape message | Pool reuse avoids allocator work while capacity is available. |
| Shared bytes | `freeze`, `clone`, `slice`, or exclusive `try_mut` | `make_mut` on shared data | Shared mutation allocates and copies. |
| TCP/Unix I/O | Partial `read` / `write` when the protocol tracks progress | `_exact` / `_all` without a complete-frame requirement | Complete operations can resubmit after a partial completion. |
| TLS I/O | Partial plaintext APIs when the protocol tracks progress | `_exact` / `_all` without a complete-frame requirement | Even partial plaintext I/O can need multiple TCP operations for TLS records. |
| Payload shape | Contiguous APIs for one range; vectored/projected APIs for segmented data | Splitting a contiguous range or copying segments together | Vectored APIs build bounded iovec metadata; coalescing copies bytes. |
| Expired deadline | One `try_read`, `try_write`, or `try_writev_projected` attempt | Polling `try_*` as the async readiness loop | Immediate methods attempt nonblocking I/O without registering a waiter. |
| UDP | Connected `send` / `recv` for a fixed peer | Address-bearing calls when the peer is stable | Connected calls avoid per-datagram address handling; use `recv_msg` to detect truncation. |
| SCTP | `SctpSocketConfig::data()` with `send` / `recv` when receive sizes are guaranteed | Lean `recv` when metadata, truncation detection, or record recovery is needed | Message APIs report record boundaries and manage partial-record recovery. |
| Deadlines | A timeout around the protocol phase | A separate timer per small I/O step unless required | Each armed timer consumes state and expiry/cancellation work. |
| Connection setup | Reused connectors, resolver, addresses, and TLS configuration | DNS lookups and connection setup in the message loop | Connectors reuse connect slots, but each attempt creates a socket. |

Use complete-buffer APIs when framing requires them. Choose heap-backed buffers
when layouts vary; use vectored APIs when the data is already segmented.

Keep runtime state, transport handles, FlowIO buffers, polled futures, and task
wakers on their owner thread. One `Executor::run` may be active on a thread;
a queued operation stays with its originating executor. Read cancellation can
discard bytes from a racing completion, so treat it as a protocol boundary
unless the protocol provides recovery.

Submitted I/O uses at most `ring_entries` operation slots; nonempty vectored
operations accept at most 1,024 active segments. Task and timer pools grow in slabs without
a configurable total cap. Handle pressure errors rather than retrying in a tight
loop; a strict process-memory ceiling requires an external limit.

Detailed contracts belong to these modules:

- [Runtime](https://docs.rs/flowio/latest/flowio/runtime/index.html): owner-thread
  rules, allocation, cancellation, socket closing, and shutdown.
- [Transports](https://docs.rs/flowio/latest/flowio/net/index.html): buffer return,
  runtime-context errors, accept errors, and bounded caller retry.
- [TCP](https://docs.rs/flowio/latest/flowio/net/tcp/index.html) and
  [Unix](https://docs.rs/flowio/latest/flowio/net/unix/index.html): partial,
  complete, vectored, and immediate I/O; TCP split ownership.
- [UDP](https://docs.rs/flowio/latest/flowio/net/udp/index.html): peer selection,
  truncation, and live socket-address queries.
- [SCTP](https://docs.rs/flowio/latest/flowio/net/sctp/index.html): data and
  signaling configuration, socket adoption, notifications, and receive recovery.
- [TLS](https://docs.rs/flowio/latest/flowio/net/tls/index.html): handshake,
  buffering, cancellation, and channel-binding validation.
- [DNS](https://docs.rs/flowio/latest/flowio/net/resolver/index.html): lookup
  ordering, parsing and result limits, and per-attempt and aggregate deadlines.

## Configuration

`Executor::new()` uses the default ring size and scheduling quota. Use
`Executor::new_with_config()` to set them:

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

Configure TLS buffering with
[`TlsClientOptions`](https://docs.rs/flowio/latest/flowio/net/tls/struct.TlsClientOptions.html).
SCTP separates socket and association configuration; see
[`SctpSocketConfig`](https://docs.rs/flowio/latest/flowio/net/sctp/struct.SctpSocketConfig.html)
and the [SCTP module](https://docs.rs/flowio/latest/flowio/net/sctp/index.html).

## Limitations

- Runtime execution is single-threaded; its state and task wakers are not
  cross-thread APIs.
- Task and timer storage has no configurable total slab cap. Sleep allocation
  failures return `io::Error`; timeout wrappers use `TimeoutError::Runtime`.
- Socket setup allocates, and positive socket linger can delay shutdown.
- TLS is client-side only, and rustls owns additional protocol buffers.
- SCTP needs kernel support; advanced socket options may fail even when basic
  one-to-one messaging works.
- There is no built-in metrics or tracing exporter.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](https://github.com/BitFlockLabs/flowio/blob/main/LICENSE-APACHE))
- MIT license ([LICENSE-MIT](https://github.com/BitFlockLabs/flowio/blob/main/LICENSE-MIT))

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
