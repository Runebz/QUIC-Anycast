use mio::{Poll, Events, Token, Interest};
use mio::net::UdpSocket;

const MAX_DATAGRAM_SIZE = 1350;

fn main() {
    let mut poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(1024);

    let mut socket = UdpSocket::bind("127.0.0.1:4433".parse().unwrap()).unwrap();
    poll.registry().register(&mut socket, Token(0), Interest::READABLE).unwrap();

    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION);
}

