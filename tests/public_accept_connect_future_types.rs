//! Downstream-style compile coverage for public TCP and SCTP setup futures.

use flowio::net::sctp::{
    AcceptFuture as SctpAcceptFuture, ConnectFuture as SctpConnectFuture,
    ConnectTimeoutFuture as SctpConnectTimeoutFuture, SctpConnector, SctpListener,
};
use flowio::net::tcp::{
    AcceptFuture as TcpAcceptFuture, ConnectFuture as TcpConnectFuture,
    ConnectTimeoutFuture as TcpConnectTimeoutFuture, OwnedConnectFuture, OwnedConnectTimeoutFuture,
    TcpConnector, TcpListener, TcpStream,
};
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

#[test]
fn tcp_accept_and_connect_method_futures_are_publicly_nameable() {
    let _: fn(&TcpListener) -> bool = TcpListener::is_terminal;
    let _: for<'a> fn(&'a mut TcpListener) -> TcpAcceptFuture<'a> = TcpListener::accept;
    let _: for<'a> fn(&'a mut TcpConnector, SocketAddr) -> io::Result<TcpConnectFuture<'a>> =
        TcpConnector::connect;
    let _: for<'a> fn(
        &'a mut TcpConnector,
        SocketAddr,
        Duration,
    ) -> io::Result<TcpConnectTimeoutFuture<'a>> = TcpConnector::connect_timeout;
    let _: fn(SocketAddr) -> io::Result<OwnedConnectFuture> = TcpStream::connect;
    let _: fn(SocketAddr, Duration) -> io::Result<OwnedConnectTimeoutFuture> =
        TcpStream::connect_timeout;
}

#[test]
fn sctp_accept_and_connect_method_futures_are_publicly_nameable() {
    let _: fn(&SctpListener) -> bool = SctpListener::is_terminal;
    let _: for<'a> fn(&'a mut SctpListener) -> SctpAcceptFuture<'a> = SctpListener::accept;
    let _: for<'a> fn(&'a mut SctpConnector, SocketAddr) -> io::Result<SctpConnectFuture<'a>> =
        SctpConnector::connect;
    let _: for<'a> fn(
        &'a mut SctpConnector,
        SocketAddr,
        Duration,
    ) -> io::Result<SctpConnectTimeoutFuture<'a>> = SctpConnector::connect_timeout;
}
