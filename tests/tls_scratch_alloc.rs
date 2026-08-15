#[path = "common/counting_allocator.rs"]
mod counting_allocator;

use counting_allocator::{
    CountingAllocator, ThreadLocalAllocationSnapshot, assert_allocation_failure_consumed,
    fail_next_allocation_of_size,
};
use flowio::net::tcp::TcpStream;
use flowio::net::tls::{TlsClientOptions, TlsClientStream};
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::os::fd::OwnedFd;
use std::sync::Arc;

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

struct ConstructorFixture {
    stream: TcpStream,
    _peer: std::net::TcpStream,
    config: Arc<ClientConfig>,
    server_name: ServerName<'static>,
}

fn constructor_fixture() -> ConstructorFixture {
    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("TLS scratch listener bind failed");
    let client = std::net::TcpStream::connect(
        listener
            .local_addr()
            .expect("TLS scratch listener address failed"),
    )
    .expect("TLS scratch client connect failed");
    let (peer, _) = listener.accept().expect("TLS scratch peer accept failed");
    let owned: OwnedFd = client.into();

    ConstructorFixture {
        stream: TcpStream::from_owned_fd(owned),
        _peer: peer,
        config: Arc::new(
            ClientConfig::builder()
                .with_root_certificates(RootCertStore::empty())
                .with_no_client_auth(),
        ),
        server_name: ServerName::try_from("localhost").expect("TLS scratch server name invalid"),
    }
}

fn construct(
    fixture: ConstructorFixture,
    options: TlsClientOptions,
) -> io::Result<TlsClientStream> {
    TlsClientStream::new(fixture.stream, fixture.config, fixture.server_name, options)
}

fn expect_error_kind(result: io::Result<TlsClientStream>, expected: io::ErrorKind) -> io::Error {
    match result {
        Ok(_) => panic!("TLS scratch construction unexpectedly succeeded"),
        Err(err) => {
            assert_eq!(err.kind(), expected, "unexpected TLS scratch error: {err}");
            err
        }
    }
}

fn impossible_tls_scratch_geometry_returns_invalid_input_without_panicking() {
    expect_error_kind(
        construct(
            constructor_fixture(),
            TlsClientOptions {
                rustls_buffer_limit: None,
                transport_read_buffer_size: 0,
                transport_write_buffer_size: 1,
            },
        ),
        io::ErrorKind::InvalidInput,
    );
    expect_error_kind(
        construct(
            constructor_fixture(),
            TlsClientOptions {
                rustls_buffer_limit: None,
                transport_read_buffer_size: 1,
                transport_write_buffer_size: 0,
            },
        ),
        io::ErrorKind::InvalidInput,
    );

    let read_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        construct(
            constructor_fixture(),
            TlsClientOptions {
                rustls_buffer_limit: None,
                transport_read_buffer_size: usize::MAX,
                transport_write_buffer_size: 1,
            },
        )
    }));
    let read_error = expect_error_kind(
        read_result.expect("read scratch geometry panicked"),
        io::ErrorKind::InvalidInput,
    );
    assert!(
        read_error
            .to_string()
            .contains("transport_read_buffer_size")
    );

    let write_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        construct(
            constructor_fixture(),
            TlsClientOptions {
                rustls_buffer_limit: None,
                transport_read_buffer_size: 1,
                transport_write_buffer_size: usize::MAX,
            },
        )
    }));
    let write_error = expect_error_kind(
        write_result.expect("write scratch geometry panicked"),
        io::ErrorKind::InvalidInput,
    );
    assert!(
        write_error
            .to_string()
            .contains("transport_write_buffer_size")
    );
}

fn tls_scratch_reservation_failures_return_out_of_memory_and_reclaim_partial_work() {
    const READ_SIZE: usize = 4_093;
    const WRITE_SIZE: usize = 8_191;
    let options = TlsClientOptions {
        rustls_buffer_limit: None,
        transport_read_buffer_size: READ_SIZE,
        transport_write_buffer_size: WRITE_SIZE,
    };

    let first_fixture = constructor_fixture();
    let first_config = Arc::clone(&first_fixture.config);
    fail_next_allocation_of_size(READ_SIZE);
    let before = ThreadLocalAllocationSnapshot::current();
    expect_error_kind(
        construct(first_fixture, options),
        io::ErrorKind::OutOfMemory,
    );
    assert_allocation_failure_consumed();
    // The snapshot starts after fixture setup; failed construction consumes
    // the stream and records its pre-existing RuntimeFdCore's final deallocation.
    ThreadLocalAllocationSnapshot::current().assert_delta_since(before, 0, 1);
    drop(first_config);

    let second_fixture = constructor_fixture();
    let second_config = Arc::clone(&second_fixture.config);
    fail_next_allocation_of_size(WRITE_SIZE);
    let before = ThreadLocalAllocationSnapshot::current();
    expect_error_kind(
        construct(second_fixture, options),
        io::ErrorKind::OutOfMemory,
    );
    assert_allocation_failure_consumed();
    ThreadLocalAllocationSnapshot::current().assert_delta_since(before, 1, 2);
    drop(second_config);
}

fn oversized_read_scratch_reserves_one_wire_record_and_keeps_write_independent() {
    const REQUESTED_READ_SIZE: usize = 64 * 1024;
    const EFFECTIVE_READ_SIZE: usize = 18_437;
    const WRITE_SIZE: usize = 8_189;
    let options = TlsClientOptions {
        rustls_buffer_limit: None,
        transport_read_buffer_size: REQUESTED_READ_SIZE,
        transport_write_buffer_size: WRITE_SIZE,
    };

    let read_fixture = constructor_fixture();
    let read_config = Arc::clone(&read_fixture.config);
    fail_next_allocation_of_size(EFFECTIVE_READ_SIZE);
    let before = ThreadLocalAllocationSnapshot::current();
    expect_error_kind(construct(read_fixture, options), io::ErrorKind::OutOfMemory);
    assert_allocation_failure_consumed();
    // The snapshot starts after fixture setup; failed construction consumes
    // the stream and records its pre-existing RuntimeFdCore's final deallocation.
    ThreadLocalAllocationSnapshot::current().assert_delta_since(before, 0, 1);
    drop(read_config);

    let write_fixture = constructor_fixture();
    let write_config = Arc::clone(&write_fixture.config);
    fail_next_allocation_of_size(WRITE_SIZE);
    let before = ThreadLocalAllocationSnapshot::current();
    expect_error_kind(
        construct(write_fixture, options),
        io::ErrorKind::OutOfMemory,
    );
    assert_allocation_failure_consumed();
    ThreadLocalAllocationSnapshot::current().assert_delta_since(before, 1, 2);
    drop(write_config);
}

fn one_byte_tls_scratch_and_zero_rustls_limit_remain_valid() {
    let stream = construct(
        constructor_fixture(),
        TlsClientOptions {
            rustls_buffer_limit: Some(0),
            transport_read_buffer_size: 1,
            transport_write_buffer_size: 1,
        },
    )
    .expect("valid minimal TLS scratch sizes should construct");
    drop(stream);
}

#[test]
fn tls_scratch_constructor_is_fallible_and_accepts_valid_minimums() {
    impossible_tls_scratch_geometry_returns_invalid_input_without_panicking();
    tls_scratch_reservation_failures_return_out_of_memory_and_reclaim_partial_work();
    oversized_read_scratch_reserves_one_wire_record_and_keeps_write_independent();
    one_byte_tls_scratch_and_zero_rustls_limit_remain_valid();
}
