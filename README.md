# flowio

A single-threaded async runtime built on Linux `io_uring`.

> **Status:** Early development (0.x). The API is unstable and may change
> without notice. Not recommended for production use.

flowio provides a zero-allocation fast-path runtime with transport
implementations for Unix, TCP, UDP, and one-to-one SCTP sockets. All I/O uses
a rental buffer pattern — callers pass buffers by value and receive them back
alongside the result on completion.

## Requirements

- Linux (kernel 5.11+ recommended for full `io_uring` support)
- Rust 2024 edition

## Quick start

```rust
use flowio::net::unix::UnixStream;
use flowio::runtime::buffer::IoBuffMut;
use flowio::runtime::executor::Executor;

let mut executor = Executor::new()?;
executor.run(async {
    let (mut left, mut right) = UnixStream::pair().unwrap();

    let mut buf = IoBuffMut::new(0, 64, 0).unwrap();
    buf.payload_append(b"hello").unwrap();

    let (res, _) = left.write_all(buf).await;
    res.unwrap();

    let recv = IoBuffMut::new(0, 64, 0).unwrap();
    let (res, recv) = right.read_exact(recv, 5).await;
    res.unwrap();
    assert_eq!(recv.bytes(), b"hello");
})?;
```

## Modules

### `runtime`

| Type | Description |
|------|-------------|
| `Executor` | Main async runtime entry point |
| `ExecutorConfig` | Runtime configuration (CPU affinity, ring size) |
| `JoinHandle<T>` | Handle for spawned tasks; detaches on drop |
| `ReactorConfig` | `io_uring` ring configuration |
| `sleep` / `sleep_until` | Timer-wheel backed sleep futures |
| `timeout` / `timeout_at` | Deadline wrappers for any future |

### `runtime::buffer`

| Type | Description |
|------|-------------|
| `IoBuffMut` | Mutable exclusive buffer with headroom / payload / tailroom regions |
| `IoBuff` | Frozen, reference-counted, cheaply clonable |
| `IoBuffView` | Read-only byte subview of a frozen buffer |
| `IoBuffVecMut<N>` | Mutable vectored buffer chain for scatter I/O |
| `IoBuffVec<N>` | Frozen vectored chain for gather I/O |
| `IoBuffPool` | Slab-backed zero-alloc pool allocator |

### `net`

| Type | Description |
|------|-------------|
| `TcpStream` | Connected TCP stream |
| `TcpListener` | TCP server accepting connections |
| `TcpConnector` | TCP client connection builder |
| `UdpSocket` | UDP datagram socket |
| `UnixStream` | Unix domain stream socket |
| `SctpStream` | One-to-one SCTP message stream |
| `SctpListener` | SCTP server accepting associations |
| `SctpConnector` | SCTP client connector |

SCTP is a first-class transport. The SCTP surface includes socket/association
configuration (`SctpSocketConfig`, `SctpAssocConfig`), notification parsing
(`SctpNotification`, `SctpNotificationKind`), and both rich
(`send_msg`/`recv_msg`) and fast (`send`/`recv`) data paths.

## Buffer rental pattern

flowio uses ownership-passing I/O — buffers are moved into operations and
returned on completion:

```rust
// Buffer is moved into read_exact and returned alongside the result.
let (result, buf) = stream.read_exact(buf, len).await;
```

This eliminates borrow-checker friction with `io_uring` completion-based I/O
and enables the runtime to maintain a zero-allocation fast path.

## Design constraints

- **Zero-allocation fast path** — no hidden heap allocation in runtime or
  error plumbing.
- **No panics on caller errors** — all caller-controlled error paths return
  `Result`, never abort the process.
- **Single-threaded** — buffers use non-atomic reference counts; the runtime
  is not `Send`.

## License

Licensed under either of

- [Apache License, Version 2.0](LICENSE-APACHE)
- [MIT License](LICENSE-MIT)

at your option.
