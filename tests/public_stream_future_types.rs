//! Downstream-style compile coverage for the nameable TCP/Unix operation futures.

// The regression intentionally spells the complete public method signatures.
#![allow(clippy::type_complexity)]

use flowio::net::tcp::TcpStream;
use flowio::net::unix::UnixStream;
use flowio::net::{
    ReadExactAppendFuture, ReadExactFuture, ReadFuture, ReadvExactFuture, ReadvFuture,
    WriteAllFuture, WriteFuture, WritevAllFuture, WritevAllProjectedFuture, WritevFuture,
    WritevPieces, WritevProjectedFuture, WritevProjection,
};
use flowio::runtime::buffer::IoBuffMut;
use flowio::runtime::buffer::iobuffvec::{IoBuffVec, IoBuffVecMut};
use std::any::TypeId;
use std::io;

const SEGMENTS: usize = 4;

struct EmptyProjection;

impl WritevProjection for EmptyProjection {
    fn writev_count_and_len(&self) -> (usize, usize) {
        (0, 0)
    }

    fn project_writev<'a>(&'a self, _pieces: &mut WritevPieces<'a>) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn tcp_and_unix_stream_operation_futures_are_publicly_nameable() {
    assert_ne!(
        TypeId::of::<ReadExactAppendFuture<'static, UnixStream>>(),
        TypeId::of::<ReadExactFuture<'static, IoBuffMut, UnixStream>>(),
        "append and generic exact-read futures must remain distinct public types"
    );

    let _: for<'a> fn(&'a mut TcpStream, Vec<u8>, usize) -> ReadFuture<'a, Vec<u8>, TcpStream> =
        TcpStream::read::<Vec<u8>>;
    let _: for<'a> fn(&'a mut TcpStream, Vec<u8>) -> WriteFuture<'a, Vec<u8>, TcpStream> =
        TcpStream::write::<Vec<u8>>;
    let _: for<'a> fn(&'a mut TcpStream, Vec<u8>) -> WriteAllFuture<'a, Vec<u8>, TcpStream> =
        TcpStream::write_all::<Vec<u8>>;
    let _: for<'a> fn(
        &'a mut TcpStream,
        Vec<u8>,
        usize,
    ) -> ReadExactFuture<'a, Vec<u8>, TcpStream> = TcpStream::read_exact::<Vec<u8>>;
    let _: for<'a> fn(&'a mut TcpStream, IoBuffMut, usize) -> ReadExactAppendFuture<'a, TcpStream> =
        TcpStream::read_exact_append;
    let _: for<'a> fn(
        &'a mut TcpStream,
        IoBuffVecMut<SEGMENTS>,
    ) -> ReadvFuture<'a, SEGMENTS, TcpStream> = TcpStream::readv::<SEGMENTS>;
    let _: for<'a> fn(
        &'a mut TcpStream,
        IoBuffVec<SEGMENTS>,
    ) -> WritevFuture<'a, IoBuffVec<SEGMENTS>, SEGMENTS, TcpStream> =
        TcpStream::writev::<IoBuffVec<SEGMENTS>, SEGMENTS>;
    let _: for<'a> fn(
        &'a mut TcpStream,
        IoBuffVec<SEGMENTS>,
    ) -> WritevAllFuture<'a, IoBuffVec<SEGMENTS>, SEGMENTS, TcpStream> =
        TcpStream::writev_all::<IoBuffVec<SEGMENTS>, SEGMENTS>;
    let _: for<'a> fn(
        &'a mut TcpStream,
        EmptyProjection,
    ) -> WritevProjectedFuture<'a, EmptyProjection, TcpStream> =
        TcpStream::writev_projected::<EmptyProjection>;
    let _: for<'a> fn(
        &'a mut TcpStream,
        EmptyProjection,
    ) -> WritevAllProjectedFuture<'a, EmptyProjection, TcpStream> =
        TcpStream::writev_all_projected::<EmptyProjection>;
    let _: for<'a> fn(
        &'a mut TcpStream,
        IoBuffVecMut<SEGMENTS>,
        usize,
    ) -> ReadvExactFuture<'a, SEGMENTS, TcpStream> = TcpStream::readv_exact::<SEGMENTS>;

    let _: for<'a> fn(&'a mut UnixStream, Vec<u8>, usize) -> ReadFuture<'a, Vec<u8>, UnixStream> =
        UnixStream::read::<Vec<u8>>;
    let _: for<'a> fn(&'a mut UnixStream, Vec<u8>) -> WriteFuture<'a, Vec<u8>, UnixStream> =
        UnixStream::write::<Vec<u8>>;
    let _: for<'a> fn(&'a mut UnixStream, Vec<u8>) -> WriteAllFuture<'a, Vec<u8>, UnixStream> =
        UnixStream::write_all::<Vec<u8>>;
    let _: for<'a> fn(
        &'a mut UnixStream,
        Vec<u8>,
        usize,
    ) -> ReadExactFuture<'a, Vec<u8>, UnixStream> = UnixStream::read_exact::<Vec<u8>>;
    let _: for<'a> fn(
        &'a mut UnixStream,
        IoBuffMut,
        usize,
    ) -> ReadExactAppendFuture<'a, UnixStream> = UnixStream::read_exact_append;
    let _: for<'a> fn(
        &'a mut UnixStream,
        IoBuffVecMut<SEGMENTS>,
    ) -> ReadvFuture<'a, SEGMENTS, UnixStream> = UnixStream::readv::<SEGMENTS>;
    let _: for<'a> fn(
        &'a mut UnixStream,
        IoBuffVec<SEGMENTS>,
    ) -> WritevFuture<'a, IoBuffVec<SEGMENTS>, SEGMENTS, UnixStream> =
        UnixStream::writev::<IoBuffVec<SEGMENTS>, SEGMENTS>;
    let _: for<'a> fn(
        &'a mut UnixStream,
        IoBuffVec<SEGMENTS>,
    ) -> WritevAllFuture<'a, IoBuffVec<SEGMENTS>, SEGMENTS, UnixStream> =
        UnixStream::writev_all::<IoBuffVec<SEGMENTS>, SEGMENTS>;
    let _: for<'a> fn(
        &'a mut UnixStream,
        EmptyProjection,
    ) -> WritevProjectedFuture<'a, EmptyProjection, UnixStream> =
        UnixStream::writev_projected::<EmptyProjection>;
    let _: for<'a> fn(
        &'a mut UnixStream,
        EmptyProjection,
    ) -> WritevAllProjectedFuture<'a, EmptyProjection, UnixStream> =
        UnixStream::writev_all_projected::<EmptyProjection>;
    let _: for<'a> fn(
        &'a mut UnixStream,
        IoBuffVecMut<SEGMENTS>,
        usize,
    ) -> ReadvExactFuture<'a, SEGMENTS, UnixStream> = UnixStream::readv_exact::<SEGMENTS>;
}
