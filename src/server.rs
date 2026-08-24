//! The socket.io-compatible server engine.
//!
//! A thread-per-connection, zero-dependency implementation that speaks
//! engine.io + socket.io over RFC 6455 WebSocket, and is interoperable with
//! the reference JS client and server. Includes namespaces, rooms,
//! broadcasting, packet acknowledgements and binary events.

use std::collections::{HashMap, HashSet};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::http;
use crate::json::Json;
use crate::protocol::{self, EngineType, Packet, PacketType};
use crate::websocket;

/// A user event handler. `args` is the event's non-name arguments.
pub type EventHandler = Arc<dyn Fn(&Socket, &[Json]) + Send + Sync>;

const DEFAULT_NS: &str = "/";

/// Namespace-scoped shared state.
struct NamespaceState {
    connect_handlers: Mutex<Vec<EventHandler>>,
    disconnect_handlers: Mutex<Vec<EventHandler>>,
    events: RwLock<HashMap<String, Vec<EventHandler>>>,
    sid_events: RwLock<HashMap<String, HashMap<String, Vec<EventHandler>>>>,
    sids: RwLock<HashSet<String>>,
    rooms: RwLock<HashMap<String, HashSet<String>>>,
    sid_rooms: RwLock<HashMap<String, HashSet<String>>>,
    auth: RwLock<HashMap<String, Json>>,
    ack_waiters: Mutex<HashMap<u64, mpsc::Sender<Vec<Json>>>>,
    next_ack_id: AtomicU64,
}

impl NamespaceState {
    fn new() -> Self {
        NamespaceState {
            connect_handlers: Mutex::new(Vec::new()),
            disconnect_handlers: Mutex::new(Vec::new()),
            events: RwLock::new(HashMap::new()),
            sid_events: RwLock::new(HashMap::new()),
            sids: RwLock::new(HashSet::new()),
            rooms: RwLock::new(HashMap::new()),
            sid_rooms: RwLock::new(HashMap::new()),
            auth: RwLock::new(HashMap::new()),
            ack_waiters: Mutex::new(HashMap::new()),
            next_ack_id: AtomicU64::new(0),
        }
    }
}

/// The physical WebSocket connection.
struct WireConn {
    writer: Mutex<TcpStream>,
}

impl WireConn {
    fn send_packet(&self, p: &Packet) -> io::Result<()> {
        let mut packet = p.clone();
        if packet.type_ == PacketType::Event
            || packet.type_ == PacketType::Ack
        {
            if contains_bytes(&packet.data) {
                protocol::encode_binary(&mut packet);
            }
        }
        let text = protocol::encode_message(&packet);
        let mut writer = self.writer.lock().unwrap();
        websocket::write_frame(&mut *writer, websocket::OP_TEXT, text.as_bytes(), false)?;
        for b in &packet.binary {
            websocket::write_frame(&mut *writer, websocket::OP_BINARY, b, false)?;
        }
        writer.flush()
    }

    fn send_engine(&self, bytes: &str) -> io::Result<()> {
        let mut writer = self.writer.lock().unwrap();
        websocket::write_frame(&mut *writer, websocket::OP_TEXT, bytes.as_bytes(), false)?;
        writer.flush()
    }
}

fn contains_bytes(v: &Json) -> bool {
    match v {
        Json::Bytes(_) => true,
        Json::Array(items) => items.iter().any(contains_bytes),
        Json::Object(map) => map.values().any(contains_bytes),
        _ => false,
    }
}

/// The shared server state.
struct Ctx {
    namespaces: RwLock<HashMap<String, Arc<NamespaceState>>>,
    conns: RwLock<HashMap<String, Arc<WireConn>>>,
    conn_ns: Mutex<HashMap<String, HashSet<String>>>,
    running: AtomicBool,
}

fn get_ns(ctx: &Arc<Ctx>, name: &str) -> Arc<NamespaceState> {
    let name = normalize_nsp(name);
    {
        let read = ctx.namespaces.read().unwrap();
        if let Some(ns) = read.get(&name) {
            return ns.clone();
        }
    }
    let ns = Arc::new(NamespaceState::new());
    let mut write = ctx.namespaces.write().unwrap();
    write.entry(name).or_insert_with(|| ns.clone());
    ns.clone()
}

fn normalize_nsp(name: &str) -> String {
    if name.is_empty() || name == DEFAULT_NS {
        DEFAULT_NS.to_string()
    } else if !name.starts_with('/') {
        format!("/{}", name)
    } else {
        name.to_string()
    }
}

/// A socket.io SERVER object (`io`).
#[derive(Clone)]
pub struct SocketIo {
    ctx: Arc<Ctx>,
}

impl SocketIo {
    pub fn new() -> Self {
        SocketIo {
            ctx: Arc::new(Ctx {
                namespaces: RwLock::new(HashMap::new()),
                conns: RwLock::new(HashMap::new()),
                conn_ns: Mutex::new(HashMap::new()),
                running: AtomicBool::new(true),
            }),
        }
    }

    /// Bind and start accepting connections on `addr`.
    pub fn bind(&self, addr: impl std::net::ToSocketAddrs) -> io::Result<Server> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(false)?;
        let listener = Arc::new(listener);
        let ctx = self.ctx.clone();
        let listener2 = listener.clone();
        let handle = Arc::new(std::thread::spawn(move || {
            for stream in listener2.incoming() {
                if !ctx.running.load(Ordering::Relaxed) {
                    break;
                }
                let Ok(stream) = stream else { continue };
                let _ = stream.set_nodelay(true);
                let sid = generate_sid();
                let c = ctx.clone();
                // One owned thread per physical connection so the accept loop
                // keeps accepting new clients instead of being blocked by the
                // first inline connection handler.
                std::thread::spawn(move || spawn_conn(c, stream, sid));
            }
        }));
        Ok(Server { ctx: self.ctx.clone(), listener, handle })
    }

    /// Register a handler on the default namespace. `connection` and
    /// `disconnect` are lifecycle events; any other name is a normal listener.
    pub fn on(
        &self,
        event: &str,
        handler: impl Fn(&Socket, &[Json]) + Send + Sync + 'static,
    ) {
        self.of(DEFAULT_NS).on(event, handler);
    }

    /// Attach to a namespace.
    pub fn of(&self, name: &str) -> Namespace {
        Namespace { ctx: self.ctx.clone(), name: normalize_nsp(name) }
    }

    /// Emit to every socket on the default namespace.
    pub fn emit(&self, event: &str, data: Json) {
        self.of(DEFAULT_NS).emit(event, data);
    }

    /// Broadcast to everyone in a room (default namespace).
    pub fn to(&self, room: &str) -> Broadcast {
        self.of(DEFAULT_NS).to(room)
    }

    /// List connected sockets on the default namespace.
    pub fn sockets(&self) -> Vec<Socket> {
        self.of(DEFAULT_NS).sockets()
    }

    /// Number of live physical connections.
    pub fn connection_count(&self) -> usize {
        self.ctx.conns.read().unwrap().len()
    }
}

impl Default for SocketIo {
    fn default() -> Self {
        Self::new()
    }
}

/// A bound, running socket.io server.
pub struct Server {
    ctx: Arc<Ctx>,
    listener: Arc<TcpListener>,
    #[allow(dead_code)]
    handle: Arc<JoinHandle<()>>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.ctx.running.store(false, Ordering::Relaxed);
    }
}

impl Server {
    pub fn addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub fn io(&self) -> SocketIo {
        SocketIo { ctx: self.ctx.clone() }
    }

    pub fn on(
        &self,
        event: &str,
        handler: impl Fn(&Socket, &[Json]) + Send + Sync + 'static,
    ) {
        self.io().on(event, handler);
    }
    pub fn of(&self, name: &str) -> Namespace {
        self.io().of(name)
    }
    pub fn emit(&self, event: &str, data: Json) {
        self.io().emit(event, data);
    }
    pub fn to(&self, room: &str) -> Broadcast {
        self.io().to(room)
    }
    pub fn sockets(&self) -> Vec<Socket> {
        self.io().sockets()
    }

    /// Stop accepting new connections.
    pub fn close(&self) -> io::Result<()> {
        self.ctx.running.store(false, Ordering::Relaxed);
        Ok(())
    }
}

/// Handle for a namespace (`io.of("/chat")`).
#[derive(Clone)]
pub struct Namespace {
    ctx: Arc<Ctx>,
    name: String,
}

impl Namespace {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn on(
        &self,
        event: &str,
        handler: impl Fn(&Socket, &[Json]) + Send + Sync + 'static,
    ) {
        let handler: EventHandler = Arc::new(handler);
        let ns = get_ns(&self.ctx, &self.name);
        match event {
            "connection" => ns.connect_handlers.lock().unwrap().push(handler),
            "disconnect" => ns.disconnect_handlers.lock().unwrap().push(handler),
            _ => {
                let mut events = ns.events.write().unwrap();
                events.entry(event.to_string()).or_default().push(handler);
            }
        }
    }

    /// Register lifecycle handlers specifically (identical to `on` for
    /// `"connection"` / `"disconnect"` which are handled specially anyway).
    pub fn on_connect(&self, handler: impl Fn(&Socket, &[Json]) + Send + Sync + 'static) {
        get_ns(&self.ctx, &self.name)
            .connect_handlers
            .lock()
            .unwrap()
            .push(Arc::new(handler));
    }
    pub fn on_disconnect(&self, handler: impl Fn(&Socket, &[Json]) + Send + Sync + 'static) {
        get_ns(&self.ctx, &self.name)
            .disconnect_handlers
            .lock()
            .unwrap()
            .push(Arc::new(handler));
    }

    pub fn emit(&self, event: &str, data: Json) {
        let ns = get_ns(&self.ctx, &self.name);
        broadcast_emit(&self.ctx, &ns, &self.name, &[], &HashSet::new(), event, data);
    }

    pub fn to(&self, room: &str) -> Broadcast {
        Broadcast {
            ctx: self.ctx.clone(),
            nsp: self.name.clone(),
            rooms: vec![room.to_string()],
            exclude: HashSet::new(),
        }
    }

    pub fn sockets(&self) -> Vec<Socket> {
        let sids: Vec<String> = {
            let ns = get_ns(&self.ctx, &self.name);
            let guard = ns.sids.read().unwrap();
            guard.iter().cloned().collect()
        };
        sids.into_iter()
            .map(|sid| Socket { ctx: self.ctx.clone(), nsp: self.name.clone(), sid })
            .collect()
    }
}

/// A broadcast operator from `socket.to(room)` / `socket.broadcast()` etc.
#[derive(Clone)]
pub struct Broadcast {
    ctx: Arc<Ctx>,
    nsp: String,
    rooms: Vec<String>,
    exclude: HashSet<String>,
}

impl Broadcast {
    pub fn emit(&self, event: &str, data: Json) {
        let ns = get_ns(&self.ctx, &self.nsp);
        let sids: Vec<String> = if self.rooms.is_empty() {
            ns.sids.read().unwrap().iter().cloned().collect()
        } else {
            let rooms = ns.rooms.read().unwrap();
            let mut set = HashSet::new();
            for r in &self.rooms {
                if let Some(s) = rooms.get(r) {
                    set.extend(s.iter().cloned());
                }
            }
            set.into_iter().collect()
        };
        let packet = event_packet(&self.nsp, event, data);
        for sid in sids {
            if self.exclude.contains(&sid) {
                continue;
            }
            send_packet(&self.ctx, &sid, &packet);
        }
    }
}

/// A handle to a single connected socket.
#[derive(Clone)]
pub struct Socket {
    ctx: Arc<Ctx>,
    nsp: String,
    sid: String,
}

impl Socket {
    pub fn id(&self) -> &str {
        &self.sid
    }

    pub fn namespace(&self) -> &str {
        &self.nsp
    }

    /// The auth/handshake payload supplied on connect (if any).
    pub fn data(&self) -> Json {
        get_ns(&self.ctx, &self.nsp)
            .auth
            .read()
            .unwrap()
            .get(&self.sid)
            .cloned()
            .unwrap_or(Json::Null)
    }

    /// Emit an event to this socket.
    pub fn emit(&self, event: &str, data: Json) {
        let p = event_packet(&self.nsp, event, data);
        send_packet(&self.ctx, &self.sid, &p);
    }

    /// Emit and block up to `timeout` for the remote acknowledgement.
    pub fn emit_with_ack(&self, event: &str, data: Json, timeout: Duration) -> Result<Vec<Json>, String> {
        let ns = get_ns(&self.ctx, &self.nsp);
        let id = ns.next_ack_id.fetch_add(1, Ordering::Relaxed) + 1;
        let (tx, rx) = mpsc::channel();
        ns.ack_waiters.lock().unwrap().insert(id, tx);
        let p = Packet {
            type_: PacketType::Event,
            nsp: self.nsp.clone(),
            id: Some(id),
            data: Json::Array(vec![Json::Str(event.to_string()), data]),
            attachments: 0,
            binary: Vec::new(),
        };
        send_packet(&self.ctx, &self.sid, &p);
        rx.recv_timeout(timeout).map_err(|_| "ack timeout".to_string())
    }

    /// Add a per-socket listener.
    pub fn on(
        &self,
        event: &str,
        handler: impl Fn(&Socket, &[Json]) + Send + Sync + 'static,
    ) {
        let handler: EventHandler = Arc::new(handler);
        let ns = get_ns(&self.ctx, &self.nsp);
        ns.sid_events
            .write()
            .unwrap()
            .entry(self.sid.clone())
            .or_default()
            .entry(event.to_string())
            .or_default()
            .push(handler);
    }

    pub fn join(&self, room: &str) {
        let ns = get_ns(&self.ctx, &self.nsp);
        ns.rooms
            .write()
            .unwrap()
            .entry(room.to_string())
            .or_default()
            .insert(self.sid.clone());
        ns.sid_rooms
            .write()
            .unwrap()
            .entry(self.sid.clone())
            .or_default()
            .insert(room.to_string());
    }

    pub fn leave(&self, room: &str) {
        let ns = get_ns(&self.ctx, &self.nsp);
        let mut rooms = ns.rooms.write().unwrap();
        if let Some(r) = rooms.get_mut(room) {
            r.remove(&self.sid);
            if r.is_empty() {
                rooms.remove(room);
            }
        }
        drop(rooms);
        let mut sid_rooms = ns.sid_rooms.write().unwrap();
        if let Some(rs) = sid_rooms.get_mut(&self.sid) {
            rs.remove(room);
            if rs.is_empty() {
                sid_rooms.remove(&self.sid);
            }
        }
    }

    pub fn rooms(&self) -> Vec<String> {
        get_ns(&self.ctx, &self.nsp)
            .sid_rooms
            .read()
            .unwrap()
            .get(&self.sid)
            .map(|r| r.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Broadcast to every socket in this namespace except this one.
    pub fn broadcast(&self) -> Broadcast {
        let mut exclude = HashSet::new();
        exclude.insert(self.sid.clone());
        Broadcast { ctx: self.ctx.clone(), nsp: self.nsp.clone(), rooms: Vec::new(), exclude }
    }

    /// Broadcast to a room, excluding this socket.
    pub fn to(&self, room: &str) -> Broadcast {
        let mut exclude = HashSet::new();
        exclude.insert(self.sid.clone());
        Broadcast { ctx: self.ctx.clone(), nsp: self.nsp.clone(), rooms: vec![room.to_string()], exclude }
    }

    /// Disconnect this socket.
    pub fn disconnect(&self) {
        ns_disconnect(&self.ctx, &self.nsp, &self.sid);
    }
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

fn event_packet(nsp: &str, event: &str, data: Json) -> Packet {
    Packet {
        type_: PacketType::Event,
        nsp: nsp.to_string(),
        id: None,
        data: Json::Array(vec![Json::Str(event.to_string()), data]),
        attachments: 0,
        binary: Vec::new(),
    }
}

fn send_packet(ctx: &Arc<Ctx>, sid: &str, p: &Packet) {
    if let Some(conn) = ctx.conns.read().unwrap().get(sid) {
        let _ = conn.send_packet(p);
    }
}

fn broadcast_emit(
    ctx: &Arc<Ctx>,
    ns: &NamespaceState,
    nsp: &str,
    rooms: &[String],
    exclude: &HashSet<String>,
    event: &str,
    data: Json,
) {
    let sids: Vec<String> = if rooms.is_empty() {
        ns.sids.read().unwrap().iter().cloned().collect()
    } else {
        let room_map = ns.rooms.read().unwrap();
        let mut set = HashSet::new();
        for r in rooms {
            if let Some(s) = room_map.get(r) {
                set.extend(s.iter().cloned());
            }
        }
        set.into_iter().collect()
    };
    let packet = event_packet(nsp, event, data);
    for sid in sids {
        if exclude.contains(&sid) {
            continue;
        }
        send_packet(ctx, &sid, &packet);
    }
}

fn ns_connect(ctx: &Arc<Ctx>, nsp: &str, sid: &str, auth: Json) {
    let ns = get_ns(ctx, nsp);
    {
        ns.sids.write().unwrap().insert(sid.to_string());
        ns.auth.write().unwrap().insert(sid.to_string(), auth);
    }
    ctx.conn_ns
        .lock()
        .unwrap()
        .entry(sid.to_string())
        .or_default()
        .insert(normalize_nsp(nsp));

    let confirm = Packet {
        type_: PacketType::Connect,
        nsp: nsp.to_string(),
        id: None,
        data: Json::Object(
            [("sid".to_string(), Json::Str(sid.to_string()))].into_iter().collect(),
        ),
        attachments: 0,
        binary: Vec::new(),
    };
    send_packet(ctx, sid, &confirm);

    let sock = Socket { ctx: ctx.clone(), nsp: nsp.to_string(), sid: sid.to_string() };
    let handlers = ns.connect_handlers.lock().unwrap().clone();
    drop(ns);
    for h in handlers {
        h(&sock, &[]);
    }
}

fn ns_disconnect(ctx: &Arc<Ctx>, nsp: &str, sid: &str) {
    let ns = get_ns(ctx, nsp);
    {
        ns.sids.write().unwrap().remove(sid);
        ns.auth.write().unwrap().remove(sid);
        ns.sid_events.write().unwrap().remove(sid);
    }
    // Remove from all rooms.
    {
        let sid_rooms = ns.sid_rooms.write().unwrap().get(sid).cloned();
        if let Some(rooms) = sid_rooms {
            let mut room_map = ns.rooms.write().unwrap();
            for r in rooms {
                if let Some(s) = room_map.get_mut(&r) {
                    s.remove(sid);
                    if s.is_empty() {
                        room_map.remove(&r);
                    }
                }
            }
        }
        ns.sid_rooms.write().unwrap().remove(sid);
    }
    ctx.conn_ns
        .lock()
        .unwrap()
        .get_mut(sid)
        .map(|set| set.remove(&normalize_nsp(nsp)));

    let sock = Socket { ctx: ctx.clone(), nsp: nsp.to_string(), sid: sid.to_string() };
    let handlers = ns.disconnect_handlers.lock().unwrap().clone();
    drop(ns);
    for h in handlers {
        h(&sock, &[]);
    }
}

fn cleanup_all(ctx: &Arc<Ctx>, sid: &str) {
    let sns: Vec<String> = ctx
        .conn_ns
        .lock()
        .unwrap()
        .get(sid)
        .map(|s| s.iter().cloned().collect())
        .unwrap_or_default();
    for nsp in sns {
        ns_disconnect(ctx, &nsp, sid);
    }
    ctx.conns.write().unwrap().remove(sid);
    ctx.conn_ns.lock().unwrap().remove(sid);
}

fn dispatch_packet(ctx: &Arc<Ctx>, sid: &str, p: &Packet) {
    match p.type_ {
        PacketType::Connect => {
            let auth = if matches!(p.data, Json::Object(_)) { p.data.clone() } else { Json::Null };
            ns_connect(ctx, &p.nsp, sid, auth);
        }
        PacketType::Event | PacketType::BinaryEvent => {
            // The protocol requires an ack whenever the sender requested one.
            if let Some(id) = p.id {
                let ack = Packet {
                    type_: PacketType::Ack,
                    nsp: p.nsp.clone(),
                    id: Some(id),
                    data: Json::Array(Vec::new()),
                    attachments: 0,
                    binary: Vec::new(),
                };
                send_packet(ctx, sid, &ack);
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
                fire_event(ctx, &p.nsp, sid, &name, &args);
            }
        }
        PacketType::Ack | PacketType::BinaryAck => {
            if let Some(id) = p.id {
                let tx = {
                    let ns = get_ns(ctx, &p.nsp);
                    let mut guard = ns.ack_waiters.lock().unwrap();
                    guard.remove(&id)
                };
                if let Some(tx) = tx {
                    let args = match &p.data {
                        Json::Array(a) => a.clone(),
                        _ => Vec::new(),
                    };
                    let _ = tx.send(args);
                }
            }
        }
        PacketType::Disconnect => {
            ns_disconnect(ctx, &p.nsp, sid);
        }
        PacketType::ConnectError => {}
    }
}

fn fire_event(ctx: &Arc<Ctx>, nsp: &str, sid: &str, name: &str, args: &[Json]) {
    let ns = get_ns(ctx, nsp);
    let mut handlers: Vec<EventHandler> = ns
        .events
        .read()
        .unwrap()
        .get(name)
        .cloned()
        .unwrap_or_default();
    if let Some(per_sid) = ns.sid_events.read().unwrap().get(sid) {
        if let Some(h) = per_sid.get(name) {
            handlers.extend(h.iter().cloned());
        }
    }
    let sock = Socket { ctx: ctx.clone(), nsp: nsp.to_string(), sid: sid.to_string() };
    drop(ns);
    for h in handlers {
        h(&sock, args);
    }
}

// ---------------------------------------------------------------------------
// Connection I/O
// ---------------------------------------------------------------------------

struct ConnReader {
    stream: TcpStream,
}

impl Read for ConnReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stream.read(buf)
    }
}

/// Read one logical message, answering pings/close on the connection writer.
fn read_message(reader: &mut ConnReader, conn: &WireConn) -> io::Result<websocket::Message> {
    fn pong(conn: &WireConn, payload: &[u8]) {
        if let Ok(mut w) = conn.writer.lock() {
            let _ = websocket::write_frame(&mut *w, websocket::OP_PONG, payload, false);
            let _ = w.flush();
        }
    }
    fn close_echo(conn: &WireConn, payload: &[u8]) {
        if let Ok(mut w) = conn.writer.lock() {
            let _ = websocket::write_frame(&mut *w, websocket::OP_CLOSE, payload, false);
            let _ = w.flush();
        }
    }

    loop {
        let frame = websocket::read_frame(reader)?;
        match frame.opcode {
            websocket::OP_PING => pong(conn, &frame.payload),
            websocket::OP_PONG => {}
            websocket::OP_CLOSE => {
                close_echo(conn, &frame.payload);
                return Ok(websocket::Message::Close {
                    code: 1000,
                    reason: String::new(),
                });
            }
            websocket::OP_TEXT | websocket::OP_BINARY => {
                let mut payload = frame.payload;
                if !frame.fin {
                    loop {
                        let c = websocket::read_frame(reader)?;
                        match c.opcode {
                            websocket::OP_CONT => {
                                payload.extend_from_slice(&c.payload);
                                if c.fin {
                                    break;
                                }
                            }
                            websocket::OP_PING => pong(conn, &c.payload),
                            websocket::OP_PONG => {}
                            websocket::OP_CLOSE => {
                                close_echo(conn, &c.payload);
                                return Ok(websocket::Message::Close {
                                    code: 1000,
                                    reason: String::new(),
                                });
                            }
                            _ => return Err(io::Error::other("bad continuation")),
                        }
                    }
                }
                return Ok(if frame.opcode == websocket::OP_BINARY {
                    websocket::Message::Binary(payload)
                } else {
                    websocket::Message::Text(payload)
                });
            }
            _ => return Err(io::Error::other("invalid opcode")),
        }
    }
}

fn spawn_conn(ctx: Arc<Ctx>, mut stream: TcpStream, sid: String) {
    let req = match http::Request::read(&mut stream) {
        Ok(r) => r,
        Err(_) => return,
    };
    let key = match req.header("sec-websocket-key") {
        Some(k) => k.to_string(),
        None => return,
    };
    let accept = websocket::accept_key(&key);
    let head = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {}\r\n\r\n",
        accept
    );
    if stream.write_all(head.as_bytes()).is_err() {
        return;
    }

    let Ok(writer) = stream.try_clone() else { return };
    let conn = Arc::new(WireConn { writer: Mutex::new(writer) });
    ctx.conns.write().unwrap().insert(sid.clone(), conn.clone());

    if conn.send_engine(&protocol::engine_open(&sid)).is_err() {
        cleanup_all(&ctx, &sid);
        return;
    }

    let mut reader = ConnReader {
        stream: stream.try_clone().unwrap_or_else(|_| stream.try_clone().unwrap()),
    };

    loop {
        let msg = match read_message(&mut reader, &conn) {
            Ok(m) => m,
            Err(_) => break,
        };
        match msg {
            websocket::Message::Close { .. } => break,
            websocket::Message::Text(text) => {
                let s = String::from_utf8_lossy(&text);
                if !handle_engine(&ctx, &mut reader, &conn, &sid, &s) {
                    break;
                }
            }
            // A stray binary frame with no pending attachment is ignored.
            websocket::Message::Binary(_) => {}
            _ => {}
        }
    }
    cleanup_all(&ctx, &sid);
}

/// Process one engine.io text packet. Returns false when the connection ended.
fn handle_engine(
    ctx: &Arc<Ctx>,
    reader: &mut ConnReader,
    conn: &WireConn,
    sid: &str,
    text: &str,
) -> bool {
    let (engine, payload) = match protocol::decode_engine(text) {
        Ok(v) => v,
        Err(_) => return true,
    };
    match engine {
        EngineType::Ping => {
            let _ = conn.send_engine("3");
        }
        EngineType::Pong => {}
        EngineType::Close => return false,
        EngineType::Upgrade | EngineType::Noop => {}
        EngineType::Open => {}
        EngineType::Message => {
            if let Some(sio) = payload {
                handle_socketio(ctx, reader, conn, sid, &sio);
            }
        }
    }
    true
}

fn handle_socketio(
    ctx: &Arc<Ctx>,
    reader: &mut ConnReader,
    conn: &WireConn,
    sid: &str,
    text: &str,
) {
    let Ok(mut p) = protocol::decode_socketio(text) else { return };

    // Collect binary attachments for binary packets.
    if matches!(p.type_, PacketType::BinaryEvent | PacketType::BinaryAck)
        && p.attachments > 0
    {
        let mut bins = Vec::with_capacity(p.attachments);
        let mut broke = false;
        for _ in 0..p.attachments {
            match read_message(reader, conn) {
                Ok(websocket::Message::Binary(b)) => bins.push(b),
                _ => {
                    broke = true;
                    break;
                }
            }
        }
        if broke {
            return;
        }
        p.binary = bins;
        p.data = protocol::reconstruct(p.data, &p.binary);
    }

    dispatch_packet(ctx, sid, &p);
}

fn generate_sid() -> String {
    static C: AtomicU64 = AtomicU64::new(0);
    let n = now_nanos() ^ C.fetch_add(1, Ordering::Relaxed).wrapping_mul(0x9e3779b97f4a7c15);
    format!("{n:016x}")
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}