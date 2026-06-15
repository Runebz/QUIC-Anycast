// Copyright (C) 2019, Cloudflare, Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
// IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
// THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
// PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#[macro_use]
extern crate log;

use quiche::{PreferredAddress, h3::NameValue};

use ring::rand::*;

const MAX_DATAGRAM_SIZE: usize = 1350;

fn main() {
    env_logger::init();
    let mut buf = [0; 65535];
    let mut out = [0; MAX_DATAGRAM_SIZE];

    let mut args = std::env::args();

    let cmd = &args.next().unwrap();

    if args.len() != 1 {
        println!("Usage: {cmd} URL");
        println!("\nSee tools/apps/ for more complete implementations.");
        return;
    }

    let mut url = url::Url::parse(&args.next().unwrap()).unwrap();

    // Setup the event loop.
    let mut poll = mio::Poll::new().unwrap();
    let mut events = mio::Events::with_capacity(1024);

    // Resolve server address.
    let mut peer_addr = url.socket_addrs(|| None).unwrap()[0];

    // Bind to INADDR_ANY or IN6ADDR_ANY depending on the IP family of the
    // server address. This is needed on macOS and BSD variants that don't
    // support binding to IN6ADDR_ANY for both v4 and v6.
    let bind_addr = match peer_addr {
        std::net::SocketAddr::V4(_) => "0.0.0.0:4434",
        std::net::SocketAddr::V6(_) => "[::]:4434",
    };

    // Create the UDP socket backing the QUIC connection, and register it with
    // the event loop.
    let mut socket =
        mio::net::UdpSocket::bind(bind_addr.parse().unwrap()).unwrap();
    poll.registry()
        .register(&mut socket, mio::Token(0), mio::Interest::READABLE)
        .unwrap();

    // Create the configuration for the QUIC connection.
    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();

    // *CAUTION*: this should not be set to `false` in production!!!
    config.verify_peer(false);

    config
        .set_application_protos(quiche::h3::APPLICATION_PROTOCOL)
        .unwrap();

    config.set_max_idle_timeout(5000);
    config.set_max_recv_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_max_send_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_initial_max_data(10_000_000);
    config.set_initial_max_stream_data_bidi_local(1_000_000);
    config.set_initial_max_stream_data_bidi_remote(1_000_000);
    config.set_initial_max_stream_data_uni(1_000_000);
    config.set_initial_max_streams_bidi(100);
    config.set_initial_max_streams_uni(100);
    config.set_disable_active_migration(false);
    config.set_active_connection_id_limit(100);

    // Generate a random source connection ID for the connection.
    let mut scid = [0; quiche::MAX_CONN_ID_LEN];
    SystemRandom::new().fill(&mut scid[..]).unwrap();

    let mut scid = quiche::ConnectionId::from_ref(&scid);

    // new_scid only gets created if the client receives a preferred_address
    let mut new_scid = [0; quiche::MAX_CONN_ID_LEN];
    // Get local address.
    let local_addr = socket.local_addr().unwrap();

    let mut received_preferred_address: Option<PreferredAddress> = None;

    let h3_config = quiche::h3::Config::new().unwrap();

    let mut req_start: Option<std::time::Instant> = None;

    // Prepare request.
    let mut path = String::from(url.path());

    if let Some(query) = url.query() {
        path.push('?');
        path.push_str(query);
    }
    
    'request: loop {
        let mut req_sent = false;
        let mut response_done = false;
        let mut http3_conn = None;
        // Create a QUIC connection and initiate handshake.
        let mut conn =
            quiche::connect(url.domain(), &scid, local_addr, peer_addr, &mut config)
                .unwrap();
        
        info!(
            "connecting to {:} from {:} with scid {}",
            peer_addr,
            socket.local_addr().unwrap(),
            hex_dump(&scid)
        );

        let (write, send_info) = conn.send(&mut out).expect("initial send failed");

        while let Err(e) = socket.send_to(&out[..write], send_info.to) {
            if e.kind() == std::io::ErrorKind::WouldBlock {
                debug!("send() would block");
                continue;
            }

            panic!("send() failed: {e:?}");
        }

        debug!("written {write}");

        let req = vec![
            quiche::h3::Header::new(b":method", b"GET"),
            quiche::h3::Header::new(b":scheme", url.scheme().as_bytes()),
            quiche::h3::Header::new(
                b":authority",
                url.host_str().unwrap().as_bytes(),
            ),
            quiche::h3::Header::new(b":path", path.as_bytes()),
            quiche::h3::Header::new(b"user-agent", b"quiche"),
        ];
        if req_start.is_none() {
            req_start = Some(std::time::Instant::now());
        }

        'poll: loop {
            poll.poll(&mut events, conn.timeout()).unwrap();

            
            // Read incoming UDP packets from the socket and feed them to quiche,
            // until there are no more packets to read.
            'read: loop {
                // If the event loop reported no events, it means that the timeout
                // has expired, so handle it without attempting to read packets. We
                // will then proceed with the send loop.
                if events.is_empty() {
                    debug!("timed out");

                    conn.on_timeout();

                    break 'read;
                }

                let (len, from) = match socket.recv_from(&mut buf) {
                    Ok(v) => v,

                    Err(e) => {
                        // There are no more UDP packets to read, so end the read
                        // loop.
                        if e.kind() == std::io::ErrorKind::WouldBlock {
                            debug!("recv() would block");
                            break 'read;
                        }

                        panic!("recv() failed: {e:?}");
                    },
                };

                debug!("got {len} bytes");

                let recv_info = quiche::RecvInfo {
                    to: local_addr,
                    from,
                };

                // Process potentially coalesced packets.
                let read = match conn.recv(&mut buf[..len], recv_info) {
                    Ok(v) => v,

                    Err(e) => {
                        error!("recv failed: {e:?}");
                        continue 'read;
                    },
                };

                debug!("processed {read} bytes");
            }

            debug!("done reading");
            if conn.is_established()
                && received_preferred_address.is_none() 
                && let Some(tp) = conn.peer_transport_params() 
                && let Some(pa) = &tp.preferred_address 
            {
                received_preferred_address = Some(pa.clone());
                if pa.ipv6_address == "::".parse::<std::net::Ipv6Addr>().unwrap() && pa.ipv6_port == 0 {
                    url = url::Url::parse(format!("https://{}:{}{}", pa.ipv4_address, pa.ipv4_port, path).as_str()).unwrap();
                } else {
                    url = url::Url::parse(format!("https://[{}]:{}{}", pa.ipv6_address, pa.ipv6_port, path).as_str()).unwrap();
                }
                info!("new url: {}", url.as_str());

                conn.close(true, 0x100, b"kthxbye").unwrap();
                info!("closed connection due to receiving pref_addr");

                // Flush the CONNECTION_CLOSE frame so the server actually stops sending
                loop {
                    match conn.send(&mut out) {
                        Ok((write, send_info)) => {
                            let _ = socket.send_to(&out[..write], send_info.to);
                        }
                        Err(quiche::Error::Done) => break,
                        Err(e) => {
                            error!("send failed during close flush: {:?}", e);
                            break;
                        }
                    }
                }

                peer_addr = url.socket_addrs(|| None).unwrap()[0];

                SystemRandom::new().fill(&mut new_scid[..]).unwrap();

                scid = quiche::ConnectionId::from_ref(&new_scid);
                break 'poll;
            }

            if conn.is_closed() {
                info!("connection closed, {:?}", conn.stats());
                break;
            }

            
            // Create a new HTTP/3 connection once the QUIC connection is established.
            if conn.is_established() && http3_conn.is_none() {
                http3_conn = Some(
                    quiche::h3::Connection::with_transport(&mut conn, &h3_config)
                    .expect("Unable to create HTTP/3 connection, check the server's uni stream limit and window size"),
                );
            }

            // Send HTTP requests once the QUIC connection is established, and until
            // all requests have been sent.
            if let Some(h3_conn) = &mut http3_conn 
                && !req_sent {
                    info!("sending HTTP request {req:?}");

                    h3_conn.send_request(&mut conn, &req, true).unwrap();

                    req_sent = true;
            }

            if let Some(http3_conn) = &mut http3_conn {
                // Process HTTP/3 events.
                loop {
                    match http3_conn.poll(&mut conn) {
                        Ok((stream_id, quiche::h3::Event::Headers { list, .. })) => {
                            info!(
                                "got response headers {:?} on stream id {}",
                                hdrs_to_strings(&list),
                                stream_id
                            );
                        },

                        Ok((stream_id, quiche::h3::Event::Data)) => {
                            while let Ok(read) =
                                http3_conn.recv_body(&mut conn, stream_id, &mut buf)
                            {
                                debug!(
                                    "got {read} bytes of response data on stream {stream_id}"
                                );

                                print!("{}", unsafe {
                                    std::str::from_utf8_unchecked(&buf[..read])
                                });
                            }
                        },

                        Ok((_stream_id, quiche::h3::Event::Finished)) => {
                            info!(
                                "response received in {:?}, closing...",
                                req_start.unwrap().elapsed()
                            );
                            response_done = true;
                            //conn.close(true, 0x100, b"kthxbye").unwrap();
                        },

                        Ok((_stream_id, quiche::h3::Event::Reset(e))) => {
                            error!("request was reset by peer with {e}, closing...");

                            conn.close(true, 0x100, b"kthxbye").unwrap();
                        },

                        Ok((_, quiche::h3::Event::PriorityUpdate)) => unreachable!(),

                        Ok((goaway_id, quiche::h3::Event::GoAway)) => {
                            info!("GOAWAY id={goaway_id}");
                        },

                        Err(quiche::h3::Error::Done) => {
                            break;
                        },

                        Err(e) => {
                            error!("HTTP/3 processing failed: {e:?}");

                            break;
                        },
                    }
                }
            }

            if response_done {
                conn.close(true, 0x100, b"kthxbye").unwrap();
                info!("connection closed, {:?}", conn.stats());
                break 'request
            }

            // Generate outgoing QUIC packets and send them on the UDP socket, until
            // quiche reports that there are no more packets to be sent.
            loop {
                let (write, send_info) = match conn.send(&mut out) {
                    Ok(v) => {
                        debug!("sending result: {v:?}");
                        v
                    },

                    Err(quiche::Error::Done) => {
                        debug!("done writing");
                        break;
                    },

                    Err(e) => {
                        error!("send failed: {e:?}");

                        conn.close(false, 0x1, b"fail").ok();
                        break;
                    },
                };

                if let Err(e) = socket.send_to(&out[..write], send_info.to) {
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        debug!("send() would block");
                        break;
                    }

                    panic!("send() failed: {e:?}");
                }

                debug!("written {write} to {:?}", send_info.to);
            }

            if conn.is_closed() {
                info!("connection closed, {:?}", conn.stats());
                break 'request;
            }
        }
    }
}

fn hex_dump(buf: &[u8]) -> String {
    let vec: Vec<String> = buf.iter().map(|b| format!("{b:02x}")).collect();

    vec.join("")
}

pub fn hdrs_to_strings(hdrs: &[quiche::h3::Header]) -> Vec<(String, String)> {
    hdrs.iter()
        .map(|h| {
            let name = String::from_utf8_lossy(h.name()).to_string();
            let value = String::from_utf8_lossy(h.value()).to_string();

            (name, value)
        })
        .collect()
}
