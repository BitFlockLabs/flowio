# flowio

flowio is a single-threaded Rust async runtime and transport library built on Linux `io_uring`.

It provides an executor, timers, buffer types, and concrete Unix, TCP, UDP,
SCTP, TLS-client, and DNS helper APIs. I/O uses owned buffers: pass a buffer
into an operation, get it back with the result.

This is an alpha release (`0.1.1-alpha.25`). The API is unstable and may change
without notice. It is not recommended for production yet.

## Install

Requires Linux and Rust 1.88 or newer. The runtime is built on `io_uring`; the
crate does not build or run on non-Linux targets.

```toml
[dependencies]
flowio = "0.1.1-alpha.25"
```

From the repository:

```toml
[dependencies]
flowio = { git = "https://github.com/BitFlockLabs/flowio" }
```

## Usage

This example uses `write_all` and `read_exact` to keep the framing obvious.
For hot loops, see the fast-path guidance below.

```rust
use flowio::net::unix::UnixStream;
use flowio::runtime::buffer::IoBuffMut;
use flowio::runtime::executor::Executor;
use std::io;

fn main() -> io::Result<()> {
    let mut executor = Executor::new()?;

    executor.run(async {
        let (mut left, mut right) = UnixStream::pair().unwrap();

        let mut send = IoBuffMut::new(0, 64, 0).unwrap();
        send.payload_append(b"hello").unwrap();

        let (write_res, _send) = left.write_all(send).await;
        write_res.unwrap();

        let recv = IoBuffMut::new(0, 64, 0).unwrap();
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

## Fast-path guidance

Use one long-lived `Executor` per thread. Building an executor is setup work.

For fixed-size steady-state buffers, use `IoBuffPool` and recycle
`IoBuffMut` values instead of allocating a fresh buffer for each operation:

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

let mut buffer = pool.alloc().unwrap();
buffer.payload_append(b"frame").unwrap();
```

On stream transports and TLS, `read` and `write` are the lowest-overhead APIs
when the caller can handle short reads and writes. Use `read_exact` and
`write_all` only when the protocol really wants complete-buffer semantics.

Use vectored APIs only when the payload is already segmented. For one
contiguous payload, use the contiguous APIs.

For repeated outbound TCP or SCTP connection attempts, use `TcpConnector` or
`SctpConnector` and reuse the connector. The one-shot connect helpers are
convenience APIs.

For fixed-peer UDP, call `connect` once and use `send` and `recv`. Use
`send_to` and `recv_from` when the peer changes per datagram. Use `recv_msg`
when connected UDP must detect datagram truncation.

For data-only SCTP associations, configure the socket with
`SctpSocketConfig::data()` and use `SctpStream::send` and `SctpStream::recv`.
Use `send_msg`, `recv_msg`, and their vectored variants when per-message SCTP
metadata or notifications matter.

DNS resolution and fine-grained timer wrapping belong in setup or control
paths. Resolve names once, keep the resulting addresses, and put deadlines
around protocol phases rather than every individual message when possible.

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

SCTP has separate socket and association configuration types. Basic
one-to-one SCTP works through `SctpListener`, `SctpConnector`, and
`SctpStream`; advanced SCTP options depend on kernel support.

## Limitations

- Linux only. The runtime is built on `io_uring`.
- Single-threaded runtime. FlowIO buffers are not a cross-thread ownership API.
- Alpha API; compatibility may change without notice.
- Dropping an in-flight read can discard bytes from a racing completion. Treat
  read cancellation or timeout as a protocol boundary unless your protocol has
  its own recovery path.
- TLS is client-side only.
- SCTP behavior depends on the host kernel and loaded SCTP support. Some socket
  options may fail even when basic messaging works.
- There is no built-in metrics or tracing exporter.

## License

Licensed under either Apache-2.0 or MIT, at your option.
