//! A zero-dependency socket.io CLIENT.
//!
//! Connects to any socket.io v5 / engine.io 4 server (this crate's `Server`
//! or the reference Node implementation) and speaks the same wire protocol.

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::time::Duration;

use crate::crypto;
use crate::json::Json;
use crate::protocol::{self, EngineType, PacketType};
use crate::websocket;

pub type ClientHandler = Arc<dyn Fn(&Client, &[Json]) + Send + Sync>;

/// A connected socket.io client.
#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    writer: Mutex<TcpStream>,
    handlers: RwLock<HashMap<String, Vec<ClientHandler>>>,
    on_connect: Mutex<Option<Arc<dyn Fn(&Client) + Send + Sync>>>,
    on_disconnect: Mutex<Option<Arc<dyn Fn(&Client) + Send + Sync>>>,
    nsp: String,
    sid: RwLock<String>,
    ack_waiters: Mutex<HashMap<u64, mpsc::Sender<Vec<Json>>>>,
    next_ack_id: AtomicU64,
    closed: AtomicBool,
    bin_pending: Mutex<Option<protocol::Packet>>,
}

impl Client {
    /// Establish a connection to a socket.io server.
    ///
    /// `addr` is a socket address such as `"127.0.0.1:3000"`. A namespace can
    /// be supplied with the second parameter.
    pub fn connect(addr: &str) -> io::Result<Client> {
        Client::connect_ns(addr, "/")
    }

    /// Establish a connection to a namespace on a socket.io server.
    pub fn connect_ns(addr: &str, nsp: &str) -> io::Result<Client> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        let nsp = if nsp.starts_with('/') { nsp.to_string() } else { format!("/{nsp}") };

        // HTTP WebSocket handshake.
        {
            let mut w = &stream;
            let key = crypto::base64_encode(&random_16());
            let host = addr.to_string();
            let req = format!(
                "GET /socket.io/?EIO=4&transport=websocket HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
            );
            w.write_all(req.as_bytes())?;
        }
        // Read and validate the 101 response.
        let head = read_until_blank(stream.try_clone()?)?;
        if !head.starts_with("HTTP/1.1 101") {
            return Err(io::Error::other(format!("handshake failed: {}", first_line(&head))));
        }

        let inner = Arc::new(ClientInner {
            writer: Mutex::new(stream.try_clone()?),
            handlers: RwLock::new(HashMap::new()),
            on_connect: Mutex::new(None),
            on_disconnect: Mutex::new(None),
            nsp: nsp.clone(),
            sid: RwLock::new(String::new()),
            ack_waiters: Mutex::new(HashMap::new()),
            next_ack_id: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            bin_pending: Mutex::new(None),
        });
        let client = Client { inner: inner.clone() };
        let inner2 = inner.clone();
        let reader = stream.try_clone()?;
        std::thread::spawn(move || read_loop(inner2, reader, nsp));
        Ok(client)
    }

    /// Register an event listener.
    pub fn on(&self, event: &str, handler: impl Fn(&Client, &[Json]) + Send + Sync + 'static) {
        self.inner
            .handlers
            .write()
            .unwrap()
            .entry(event.to_string())
            .or_default()
            .push(Arc::new(handler));
    }

    /// Callback fired once the server confirms the connection.
    pub fn on_connect(&self, f: impl Fn(&Client) + Send + Sync + 'static) {
        *self.inner.on_connect.lock().unwrap() = Some(Arc::new(f));
    }

    /// Callback fired when the connection is lost.
    pub fn on_disconnect(&self, f: impl Fn(&Client) + Send + Sync + 'static) {
        *self.inner.on_disconnect.lock().unwrap() = Some(Arc::new(f));
    }

    pub fn namespace(&self) -> &str {
        &self.inner.nsp
    }

    /// The server-assigned engine id (empty until the connect confirmation).
    pub fn sid(&self) -> String {
        self.inner.sid.read().unwrap().clone()
    }

    pub fn connected(&self) -> bool {
        !self.inner.sid.read().unwrap().is_empty() && !self.inner.closed.load(Ordering::Relaxed)
    }

    /// Emit an event to the server.
    pub fn emit(&self, event: &str, data: Json) -> io::Result<()> {
        self.send_packet(&protocol::Packet {
            type_: PacketType::Event,
            nsp: self.inner.nsp.clone(),
            id: None,
            data: Json::Array(vec![Json::Str(event.to_string()), data]),
            attachments: 0,
            binary: Vec::new(),
        })
    }

    /// Emit an event and wait for the server's acknowledgement.
    pub fn emit_with_ack(&self, event: &str, data: Json, timeout: Duration) -> Result<Vec<Json>, String> {
        let id = self.inner.next_ack_id.fetch_add(1, Ordering::Relaxed) + 1;
        let (tx, rx) = mpsc::channel();
        self.inner.ack_waiters.lock().unwrap().insert(id, tx);
        let p = protocol::Packet {
            type_: PacketType::Event,
            nsp: self.inner.nsp.clone(),
            id: Some(id),
            data: Json::Array(vec![Json::Str(event.to_string()), data]),
            attachments: 0,
            binary: Vec::new(),
        };
        self.send_packet(&p).map_err(|e| e.to_string())?;
        rx.recv_timeout(timeout).map_err(|_| "ack timeout".to_string())
    }

    fn send_packet(&self, p: &protocol::Packet) -> io::Result<()> {
        let text = protocol::encode_message(p);
        let mut writer = self.inner.writer.lock().unwrap();
        websocket::write_frame(&mut *writer, websocket::OP_TEXT, text.as_bytes(), true)?;
        writer.flush()
    }

    /// Close the connection.
    pub fn disconnect(&self) {
        let mut writer = self.inner.writer.lock().unwrap();
        let _ = websocket::write_frame(&mut *writer, websocket::OP_CLOSE, &[], true);
        let _ = writer.flush();
        self.inner.closed.store(true, Ordering::Relaxed);
    }
}

fn read_loop(inner: Arc<ClientInner>, mut stream: TcpStream, nsp: String) {
    // Respond to a server-initiated engine ping with a pong.
    loop {
        let frame = match websocket::read_frame(&mut stream) {
            Ok(f) => f,
            Err(_) => break,
        };
        match frame.opcode {
            websocket::OP_PING => {
                let _ = {
                    let mut w = inner.writer.lock().unwrap();
                    websocket::write_frame(&mut *w, websocket::OP_PONG, &frame.payload, true)
                };
            }
            websocket::OP_CLOSE => break,
            websocket::OP_PONG => {}
            websocket::OP_CONT | websocket::OP_TEXT | websocket::OP_BINARY => {
                let mut payload = frame.payload;
                if !frame.fin {
                    // Assemble continuation frames.
                    loop {
                        let c = match websocket::read_frame(&mut stream) {
                            Ok(c) => c,
                            Err(_) => {
                                close(&inner);
                                return;
                            }
                        };
                        match c.opcode {
                            websocket::OP_CONT => {
                                payload.extend_from_slice(&c.payload);
                                if c.fin {
                                    break;
                                }
                            }
                            websocket::OP_PING => {
                                let _ = {
                                    let mut w = inner.writer.lock().unwrap();
                                    websocket::write_frame(&mut *w, websocket::OP_PONG, &c.payload, true)
                                };
                            }
                            websocket::OP_CLOSE => {
                                close(&inner);
                                return;
                            }
                            _ => {}
                        }
                    }
                }

                if frame.opcode == websocket::OP_BINARY {
                    // A binary attachment for a pending binary packet.
                    if let Some(p) = inner.bin_pending.lock().unwrap().take() {
                        let mut bins = vec![payload];
                        let mut expected = p.attachments;
                        while bins.len() < expected {
                            match websocket::read_frame(&mut stream) {
                                Ok(c) if c.opcode == websocket::OP_BINARY => {
                                    if !c.fin {
                                        let mut pl = c.payload;
                                        loop {
                                            let c2 = match websocket::read_frame(&mut stream) {
                                                Ok(c2) => c2,
                                                Err(_) => return close(&inner),
                                            };
                                            if c2.opcode == websocket::OP_CONT {
                                                pl.extend_from_slice(&c2.payload);
                                                if c2.fin {
                                                    break;
                                                }
                                            } else if c2.opcode == websocket::OP_PONG {
                                                continue;
                                            } else {
                                                return close(&inner);
                                            }
                                        }
                                        bins.push(pl);
                                    } else {
                                        bins.push(c.payload);
                                    }
                                }
                                _ => return close(&inner),
                            }
                        }
                        expected = p.attachments; // placeholder, bins already filled
                        let _ = expected;
                        let mut p = p;
                        p.binary = bins;
                        p.data = protocol::reconstruct(p.data, &p.binary);
                        dispatch(&inner, &p);
                    }
                    continue;
                }

                let text = String::from_utf8_lossy(&payload);
                handle_engine(&inner, &text, &nsp);
            }
            _ => {}
        }
    }
    close(&inner);
}

fn handle_engine(inner: &Arc<ClientInner>, text: &str, nsp: &str) {
    let (engine, payload) = match protocol::decode_engine(text) {
        Ok(v) => v,
        Err(_) => return,
    };
    match engine {
        EngineType::Open => {
            // Server announced its session; open can carry a JSON payload.
            if let Some(pl) = payload {
                if let Ok(json) = Json::parse(&pl) {
                    if let Json::Object(m) = &json {
                        if let Some(Json::Str(sid)) = m.get("sid") {
                            *inner.sid.write().unwrap() = sid.clone();
                        }
                    }
                }
            }
            // Ask to join the requested namespace.
            let connect = if nsp == "/" {
                "40".to_string()
            } else {
                format!("40{},", nsp)
            };
            let _ = send_text(inner, &connect);
        }
        EngineType::Ping => {
            let _ = send_text(inner, "3");
        }
        EngineType::Pong => {}
        EngineType::Close => close(inner),
        EngineType::Message => {
            if let Some(sio) = payload {
                handle_socketio(inner, &sio);
            }
        }
        EngineType::Upgrade | EngineType::Noop => {}
    }
}

fn handle_socketio(inner: &Arc<ClientInner>, text: &str) {
    let Ok(p) = protocol::decode_socketio(text) else { return };

    match p.type_ {
        PacketType::BinaryEvent | PacketType::BinaryAck => {
            if p.attachments > 0 {
                // Wait for the binary attachments to arrive as WS binary frames.
                *inner.bin_pending.lock().unwrap() = Some(p);
                return;
            }
            dispatch(inner, &p);
        }
        _ => {
            // A plain CONNECT confirmation is the trigger for on_connect.
            if p.type_ == PacketType::Connect {
                if let Json::Object(m) = &p.data {
                    if let Some(Json::Str(sid)) = m.get("sid") {
                        *inner.sid.write().unwrap() = sid.clone();
                    }
                }
                if let Some(f) = inner.on_connect.lock().unwrap().take() {
                    f(&Client { inner: inner.clone() });
                }
            }
            dispatch(inner, &p);
        }
    }
}

fn dispatch(inner: &Arc<ClientInner>, p: &protocol::Packet) {
    let client = Client { inner: inner.clone() };
    match p.type_ {
        PacketType::Event | PacketType::BinaryEvent => {
            // The protocol requires an ack whenever the sender requested one.
            if let Some(id) = p.id {
                let ack = protocol::Packet {
                    type_: PacketType::Ack,
                    nsp: p.nsp.clone(),
                    id: Some(id),
                    data: Json::Array(Vec::new()),
                    attachments: 0,
                    binary: Vec::new(),
                };
                let _ = client.send_packet(&ack);
            }
            if let Json::Array(arr) = &p.data {
                if arr.is_empty() {
                    return;
                }
                let name = match &arr[0] {
                    Json::Str(s) => s.clone(),
                    _ => return,
                };
                let args: Vec<Json> = arr[1..].to_vec();
                let handlers = inner
                    .handlers
                    .read()
                    .unwrap()
                    .get(&name)
                    .cloned()
                    .unwrap_or_default();
                for h in handlers {
                    h(&client, &args);
                }
            }
        }
        PacketType::Ack | PacketType::BinaryAck => {
            if let Some(id) = p.id {
                if let Some(tx) = inner.ack_waiters.lock().unwrap().remove(&id) {
                    let args = match &p.data {
                        Json::Array(a) => a.clone(),
                        _ => Vec::new(),
                    };
                    let _ = tx.send(args);
                }
            }
        }
        _ => {}
    }
}

fn send_text(inner: &Arc<ClientInner>, s: &str) -> io::Result<()> {
    let mut writer = inner.writer.lock().unwrap();
    websocket::write_frame(&mut *writer, websocket::OP_TEXT, s.as_bytes(), true)?;
    writer.flush()
}

fn close(inner: &Arc<ClientInner>) {
    if inner.closed.swap(true, Ordering::Relaxed) {
        return;
    }
    if let Some(f) = inner.on_disconnect.lock().unwrap().take() {
        f(&Client { inner: inner.clone() });
    }
}

fn random_16() -> Vec<u8> {
    static C: AtomicU64 = AtomicU64::new(1);
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let a = C.fetch_add(1, Ordering::Relaxed).wrapping_mul(0x9e3779b97f4a7c15);
    let b = t.wrapping_mul(0xbf58476d1ce4e5b9) ^ a;
    let mut out = Vec::with_capacity(16);
    for i in 0..4u8 {
        let w = b.wrapping_add((i as u64) << 57).rotate_left(7) ^ a;
        out.extend_from_slice(&w.to_le_bytes());
    }
    out
}

fn read_until_blank(mut stream: TcpStream) -> io::Result<String> {
    let mut buf = Vec::new();
    let mut one = [0u8; 1];
    loop {
        if stream.read(&mut one)? == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "no response"));
        }
        buf.push(one[0]);
        if buf.len() >= 4 && buf[buf.len() - 4..] == *b"\r\n\r\n" {
            break;
        }
        if buf.len() > 32 * 1024 {
            return Err(io::Error::other("response head too large"));
        }
    }
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

fn first_line(head: &str) -> String {
    head.lines().next().unwrap_or("").to_string()
}