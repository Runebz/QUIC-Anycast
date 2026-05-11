use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use mio::{Poll, Events, Token, Interest};
use mio::net::UdpSocket;
use quiche::Connection;
use quiche::h3::{Event, NameValue};
use ring::rand::{SystemRandom, SecureRandom};

const MAX_DATAGRAM_SIZE: usize = 1350;

struct PartialResponse {
    headers: Option<Vec<quiche::h3::Header>>,

    body: Vec<u8>,

    written: usize,
}

struct Client {
    conn: Connection,

    http3_conn: Option<quiche::h3::Connection>,

    partial_responses: HashMap<u64, PartialResponse>,
}

fn main() {
    let mut buf = [0; 65535];
    let mut out = [0; MAX_DATAGRAM_SIZE];

    let mut poll = Poll::new().unwrap();
    let mut clients: HashMap<Vec<u8>, Client> = HashMap::new();
    let mut events = Events::with_capacity(1024);

    let mut socket = UdpSocket::bind("127.0.0.1:4433".parse().unwrap()).unwrap();
    poll.registry().register(&mut socket, Token(0), Interest::READABLE).unwrap();

    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();
    let mut h3_config = quiche::h3::Config::new().unwrap();

    config.load_cert_chain_from_pem_file("certs/cert.crt").unwrap();
    config.load_priv_key_from_pem_file("certs/cert.key").unwrap();
    config.set_application_protos(quiche::h3::APPLICATION_PROTOCOL).unwrap();

    config.set_max_idle_timeout(5000);
    config.set_max_recv_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_max_send_udp_payload_size(MAX_DATAGRAM_SIZE);

    config.set_initial_max_data(10_000_000);
    config.set_initial_max_stream_data_bidi_local(1_000_000);
    config.set_initial_max_stream_data_bidi_remote(1_000_000);
    config.set_initial_max_streams_bidi(100);

    config.set_initial_max_streams_uni(100);
    config.set_initial_max_stream_data_uni(1_000_000);

    let rng = SystemRandom::new();
    let conn_id_seed = ring::hmac::Key::generate(ring::hmac::HMAC_SHA256, &rng).unwrap();

    let local_addr = socket.local_addr().unwrap();

    loop {
        poll.poll(&mut events, Some(Duration::from_millis(100))).expect("poll failed");

        for _event in events.iter() {
            while let Ok(v) = socket.recv_from(&mut buf) {
               let (len, from) = v;
               let pkt_buf = &mut buf[..len];

               let hdr = match quiche::Header::from_slice(pkt_buf, quiche::MAX_CONN_ID_LEN) {
                   Ok(v) => v,
                   Err(e) => {
                       eprintln!("failed to parse incoming header: {:?}", e);
                       continue;
                   }
               };

               println!("received packet {hdr:?}");
                                

               let conn_id = ring::hmac::sign(&conn_id_seed, &hdr.dcid);
               let conn_id = &conn_id.as_ref()[..quiche::MAX_CONN_ID_LEN];

               let client = clients.entry(conn_id.to_vec()).or_insert_with(|| {
                    let mut scid = [0; quiche::MAX_CONN_ID_LEN];
                    //scid.copy_from_slice(&conn_id);
                    
                    rng.fill(&mut scid).unwrap();

                    let scid = quiche::ConnectionId::from_ref(&scid);

                    let conn = quiche::accept(&scid, None, local_addr, from, &mut config).unwrap();

                    println!("adding new client...");

                    Client { conn, http3_conn: None, partial_responses: HashMap::new() }
               });

               let recv_info = quiche::RecvInfo {
                   from,
                   to: socket.local_addr().unwrap(),
               };

               println!("receive info: {:?}", recv_info);

               match client.conn.recv(pkt_buf, recv_info) {
                   Ok(_) => {},
                   Err(e) => {
                       eprintln!("recv failed: {:?}", e);
                       continue;
                   }
               }

               if (client.conn.is_in_early_data() || client.conn.is_established()) && client.http3_conn.is_none() {
                   println!("{} QUIC handshake is complete, trying HTTP/3", client.conn.trace_id());

                   let h3_conn = match quiche::h3::Connection::with_transport(&mut client.conn, &h3_config) {
                       Ok(v) => v,
                       Err(e) => {
                           eprintln!("failed to make a HTTP/3 connection: {e}");
                           continue;
                       }

                   };

                   client.http3_conn = Some(h3_conn);

               }

               if client.http3_conn.is_some() {
                   for stream_id in client.conn.writable() {
                       handle_writable(client, stream_id);
                   }
               }

               if let Some(http3_conn) = client.http3_conn.as_mut() {
                   // processes HTTP/3 events
                   loop {
                       match http3_conn.poll(&mut client.conn) {
                           Ok((stream_id, quiche::h3::Event::Headers { list, .. })) => {
                               handle_request(&mut client.conn, http3_conn, stream_id, &list, &mut client.partial_responses, "files",);
                           },

                           Ok((stream_id, quiche::h3::Event::Data)) => {
                               println!("{} got data on stream id {}", client.conn.trace_id(), stream_id);
                           },

                           Ok((_stream_id, quiche::h3::Event::Finished)) => (),

                           Ok((_stream_id, quiche::h3::Event::Reset { .. })) => (),

                           Ok((_prioritized_element_id, quiche::h3::Event::PriorityUpdate)) => (),

                           Ok((_goaway_id, quiche::h3::Event::GoAway)) => (),

                           Err(quiche::h3::Error::Done) => {
                               break;
                           },

                           Err(e) => {
                               eprintln!("{} HTTP/3 error {:?}", client.conn.trace_id(), e);
                               break;
                           }
                       }

                   }
               } 

               for client in clients.values_mut() {
                   loop {
                       let (write, send_info) = match client.conn.send(&mut out) {
                           Ok(v) => v,

                           Err(quiche::Error::Done) => {
                               println!("{} done writing", client.conn.trace_id());
                               break;
                           },

                           Err(e) => {
                               eprintln!("{} failed writing: {:?}", client.conn.trace_id(), e);

                               client.conn.close(false, 0x1, b"fail").ok();
                               break;
                           }
                       };

                       if let Err(e) = socket.send_to(&out[..write], send_info.to) {
                           if e.kind() == std::io::ErrorKind::WouldBlock {
                               println!("send() would block");
                               break;
                           }

                           panic!("send() failed: {e:?}");
                       }
                   }
               }

               clients.retain(|_, ref mut c| {
                   !c.conn.is_closed()
               });
            }
        }
    }
}

fn handle_writable(client: &mut Client, stream_id: u64) {
    let conn = &mut client.conn;
    let http3_conn = &mut client.http3_conn.as_mut().unwrap();

    if !client.partial_responses.contains_key(&stream_id) {
        return;
    }

    let resp = client.partial_responses.get_mut(&stream_id).unwrap();

    if let Some(ref headers) = resp.headers {
        match http3_conn.send_response(conn, stream_id, headers, false) {
            Ok(_) => (),
            Err(quiche::h3::Error::StreamBlocked) => {
                return;
            },
            Err(e) => {
                eprintln!("{} stream send failed {:?}", conn.trace_id(), e);
                return;
            },

        }
    }

    resp.headers = None;

    let body = &resp.body[resp.written..];

    let written = match http3_conn.send_body(conn, stream_id, body, false) {
        Ok(v) => v,
        Err(quiche::h3::Error::Done) => 0,
        Err(e) => {
            client.partial_responses.remove(&stream_id);
            eprintln!("{} stream send failed {:?}", conn.trace_id(), e);
            return;
        },
    };

    resp.written += written;

    if resp.written == resp.body.len() {
        client.partial_responses.remove(&stream_id);
    }
}

fn handle_request(conn: &mut Connection, http3_conn: &mut quiche::h3::Connection, 
    stream_id: u64, headers: &[quiche::h3::Header], partial_responses: &mut HashMap<u64, PartialResponse>, root: &str) {
    println!("{} got request on stream id {}", conn.trace_id(), stream_id);

    conn.stream_shutdown(stream_id, quiche::Shutdown::Read, 0).unwrap();

    let (headers, body) = build_response(root, headers);

    match http3_conn.send_response(conn, stream_id, &headers, false) {
        Ok(v) => v,

        Err(quiche::h3::Error::StreamBlocked) => {
            let response = PartialResponse {
                headers: Some(headers),
                body,
                written: 0,
            };

            partial_responses.insert(stream_id, response);
            return;
        },

        Err(e) => {
            eprintln!("{} stream send failed {:?}", conn.trace_id(), e);
            return;
        },
    }

    let written = match http3_conn.send_body(conn, stream_id, &body, true) {
        Ok(v) => v,

        Err(quiche::h3::Error::Done) => 0,

        Err(e) => {
            eprintln!("{} stream send failed {:?}", conn.trace_id(), e);
            return;
        },
    };

    if written < body.len() {
        let response = PartialResponse {
            headers: None,
            body, 
            written,
        };

        partial_responses.insert(stream_id, response);
    }
}

fn build_response(root: &str, request: &[quiche::h3::Header],) -> (Vec<quiche::h3::Header>, Vec<u8>) {
    let mut file_path = std::path::PathBuf::from(root);
    let mut path = std::path::Path::new("");
    let mut method = None;

    for hdr in request {
        match hdr.name() {
            b":path" =>
                path = std::path::Path::new(
                    std::str::from_utf8(hdr.value()).unwrap(),
                ),
            b"method" => method = Some(hdr.value()),

            _ => (),
        }
    }

    println!("received path {:#?}", path);

    let (status, body) = match method {
        Some(b"GET") => {
            for c in path.components() {
                if let std::path::Component::Normal(v) = c {
                    file_path.push(v);
                } 
            }

            match std::fs::read(file_path.as_path()) {
                Ok(data) => (200, data),

                Err(_) => (404, b"Not Found!".to_vec()),
            }
        },

        _ => (405, Vec::new()),
    };

    let headers = vec![
        quiche::h3::Header::new(b":status", status.to_string().as_bytes()),
        quiche::h3::Header::new(b"server", b"quiche"),
        quiche::h3::Header::new(b"content-length", body.len().to_string().as_bytes())
    ];

    (headers, body)
}
