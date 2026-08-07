#[path = "common/counting_allocator.rs"]
mod counting_allocator;

use std::cell::{Cell, RefCell};
use std::io::{self, Read};
use std::mem::size_of;
use std::net::{Ipv4Addr, SocketAddr};
use std::rc::Rc;
use std::time::Duration;

use counting_allocator::{
    CountingAllocator, assert_allocation_failure_consumed, fail_next_allocation_of_size,
    finish_counting_allocations_of_size, start_counting_allocations_of_size,
};
use flowio::net::tcp::TcpStream;
use flowio::net::{WritevPieces, WritevProjection};

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

const PIECES: usize = 17;
const BYTE: u8 = 0x5a;

fn connected_tcp_pair() -> (TcpStream, std::net::TcpStream) {
    let listener = std::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("loopback listener should bind");
    let address = listener
        .local_addr()
        .expect("listener address should exist");
    let peer = std::net::TcpStream::connect(address).expect("loopback peer should connect");
    peer.set_read_timeout(Some(Duration::from_secs(5)))
        .expect("peer read timeout should configure");
    let (stream, _) = listener
        .accept()
        .expect("loopback connection should accept");
    stream
        .set_nonblocking(true)
        .expect("FlowIO endpoint should be nonblocking");
    stream
        .set_nodelay(true)
        .expect("FlowIO endpoint should disable Nagle");
    (TcpStream::from_owned_fd(stream.into()), peer)
}

struct FixedProjection {
    bytes: [u8; PIECES],
}

impl FixedProjection {
    fn new() -> Self {
        Self {
            bytes: [BYTE; PIECES],
        }
    }
}

impl WritevProjection for FixedProjection {
    fn writev_count_and_len(&self) -> (usize, usize) {
        (PIECES, PIECES)
    }

    fn project_writev<'a>(&'a self, pieces: &mut WritevPieces<'a>) -> io::Result<()> {
        for byte in &self.bytes {
            pieces.push(std::slice::from_ref(byte))?;
        }
        Ok(())
    }
}

struct DropTrackedProjection {
    bytes: [u8; PIECES],
    projection_calls: Rc<Cell<usize>>,
    drops: Rc<Cell<usize>>,
}

impl Drop for DropTrackedProjection {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

impl WritevProjection for DropTrackedProjection {
    fn writev_count_and_len(&self) -> (usize, usize) {
        (PIECES, PIECES)
    }

    fn project_writev<'a>(&'a self, pieces: &mut WritevPieces<'a>) -> io::Result<()> {
        self.projection_calls.set(self.projection_calls.get() + 1);
        for byte in &self.bytes {
            pieces.push(std::slice::from_ref(byte))?;
        }
        Ok(())
    }
}

struct ReentrantProjection {
    bytes: [u8; PIECES],
    inner_stream: RefCell<TcpStream>,
    inner_result: Cell<Option<io::ErrorKind>>,
    returned_inner: RefCell<Option<DropTrackedProjection>>,
    inner_projection_calls: Rc<Cell<usize>>,
    inner_drops: Rc<Cell<usize>>,
    outer_projection_calls: Cell<usize>,
    outer_drops: Rc<Cell<usize>>,
}

impl ReentrantProjection {
    fn take_returned_inner(&self) -> DropTrackedProjection {
        self.returned_inner
            .borrow_mut()
            .take()
            .expect("nested projected source should be returned")
    }
}

impl Drop for ReentrantProjection {
    fn drop(&mut self) {
        self.outer_drops.set(self.outer_drops.get() + 1);
    }
}

impl WritevProjection for ReentrantProjection {
    fn writev_count_and_len(&self) -> (usize, usize) {
        (PIECES, PIECES)
    }

    fn project_writev<'a>(&'a self, pieces: &mut WritevPieces<'a>) -> io::Result<()> {
        self.outer_projection_calls
            .set(self.outer_projection_calls.get() + 1);
        assert_eq!(
            self.outer_projection_calls.get(),
            1,
            "outer projection should run once"
        );

        let inner = DropTrackedProjection {
            bytes: [0xa5; PIECES],
            projection_calls: Rc::clone(&self.inner_projection_calls),
            drops: Rc::clone(&self.inner_drops),
        };
        let (result, returned) = self.inner_stream.borrow_mut().try_writev_projected(inner);
        let error = result.expect_err("forced nested scratch allocation should fail");
        self.inner_result.set(Some(error.kind()));
        let previous = self.returned_inner.borrow_mut().replace(returned);
        assert!(previous.is_none(), "nested source should be returned once");

        for byte in &self.bytes {
            pieces.push(std::slice::from_ref(byte))?;
        }
        Ok(())
    }
}

#[test]
fn projected_write_reentry_allocation_failure_returns_source_before_projection() {
    let scratch_bytes = PIECES
        .checked_mul(size_of::<libc::iovec>())
        .expect("17-iovec scratch size should fit");
    let (mut inner_stream, mut inner_peer) = connected_tcp_pair();

    start_counting_allocations_of_size(scratch_bytes);
    let (warm_result, _warm_source) = inner_stream.try_writev_projected(FixedProjection::new());
    assert_eq!(
        warm_result.expect("normal 17-piece warmup should write"),
        PIECES
    );
    assert_eq!(finish_counting_allocations_of_size(), 1);
    let mut warm_bytes = [0u8; PIECES];
    inner_peer
        .read_exact(&mut warm_bytes)
        .expect("peer should receive the warmup payload");
    assert_eq!(warm_bytes, [BYTE; PIECES]);

    let (mut outer_stream, mut outer_peer) = connected_tcp_pair();
    let inner_projection_calls = Rc::new(Cell::new(0));
    let inner_drops = Rc::new(Cell::new(0));
    let outer_drops = Rc::new(Cell::new(0));
    let outer_source = ReentrantProjection {
        bytes: [0x3c; PIECES],
        inner_stream: RefCell::new(inner_stream),
        inner_result: Cell::new(None),
        returned_inner: RefCell::new(None),
        inner_projection_calls: Rc::clone(&inner_projection_calls),
        inner_drops: Rc::clone(&inner_drops),
        outer_projection_calls: Cell::new(0),
        outer_drops: Rc::clone(&outer_drops),
    };

    fail_next_allocation_of_size(scratch_bytes);
    let (outer_result, returned_outer) = outer_stream.try_writev_projected(outer_source);
    assert_allocation_failure_consumed();

    assert_eq!(
        outer_result.expect("outer projected write should continue"),
        PIECES
    );
    assert_eq!(returned_outer.outer_projection_calls.get(), 1);
    assert_eq!(
        returned_outer.inner_result.get(),
        Some(io::ErrorKind::WouldBlock)
    );
    assert_eq!(inner_projection_calls.get(), 0);
    assert_eq!(inner_drops.get(), 0);
    assert_eq!(outer_drops.get(), 0);

    let returned_inner = returned_outer.take_returned_inner();
    assert_eq!(inner_drops.get(), 0);
    drop(returned_inner);
    assert_eq!(inner_drops.get(), 1);

    let mut outer_bytes = [0u8; PIECES];
    outer_peer
        .read_exact(&mut outer_bytes)
        .expect("peer should receive the outer payload");
    assert_eq!(outer_bytes, [0x3c; PIECES]);

    inner_peer
        .set_nonblocking(true)
        .expect("inner peer should become nonblocking");
    let mut unexpected = [0u8; 1];
    let error = inner_peer
        .read(&mut unexpected)
        .expect_err("failed nested write should send no payload");
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);

    drop(returned_outer);
    assert_eq!(outer_drops.get(), 1);
    assert_eq!(inner_drops.get(), 1);
}
