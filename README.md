# WeSocket

A socket.io-compatible realtime engine in **pure Rust** — lighter, faster and
dramatically leaner than the reference stack.

**Powered by Vexify.**

## Why WeSocket?

- **Zero dependencies** — `Cargo.toml` has an empty `[dependencies]` section.
  The WebSocket frame layer (RFC 6455), HTTP/1.1 handshake, SHA-1 / Base64
  primitives, JSON model and the engine.io / socket.io protocol are all
  implemented from scratch on top of `std`.
- **Faster & leaner** — a thread-per-connection model with `Arc`/`Mutex`/`RwLock`
  shared state, no async runtime and no event-loop machinery, so memory and CPU
  overhead stay minimal.
- **Wire-compatible** — speaks the same engine.io v4 + socket.io v5 protocol as
  the reference implementations, so it interoperates with the official JS
  client and server.
- **Feature-complete** — events, namespaces, rooms, broadcasting,
  acknowledgements (both directions) and binary payloads.

## Features

| Feature | API |
| --- | --- |
| Events | `socket.on("chat", ...)` / `socket.emit("chat", data)` |
| Namespaces | `io.of("/chat").on("connection", ...)` |
| Rooms | `socket.join("lobby")` / `socket.to("room").emit(...)` |
| Broadcast | `socket.broadcast().emit(...)` |
| Acknowledgement | `socket.emit_with_ack("ping", data, timeout)` |
| Binary | `Json::Bytes(..)` payloads |

## Quick start

Add WeSocket to your `Cargo.toml`:

```toml
[dependencies]
wesocket = { path = "path/to/wesocket" }
```

### Server

```rust
use wesocket::{Json, SocketIo};

let io = SocketIo::new();
io.on("connection", |socket, _| {
    let id = socket.id().to_owned();
    println!("connected: {id}");
    socket.join("lobby");

    socket.on("echo", |sock, args| {
        let data = args.first().cloned().unwrap_or_else(|| Json::Str("?".into()));
        sock.emit("echo reply", data);
        sock.to("lobby").emit("chat", Json::Str("someone echoed".into()));
    });
});

// A separate namespace.
io.of("/chat").on("connection", |socket, _| {
    socket.on("msg", |sock, args| {
        let text = args.first().cloned().unwrap_or(Json::Null);
        sock.broadcast().emit("msg", text);
    });
});

let server = io.bind("0.0.0.0:3000")?;
println!("listening on {}", server.addr()?);
std::thread::park();
```

### Client

```rust
use wesocket::{Client, Json};
use std::time::Duration;

let client = Client::connect("127.0.0.1:3000")?;
client.on("echo reply", |_c, args| {
    println!("echo reply: {:?}", args);
});
client.emit("echo", Json::Str("hello".into()))?;

// Synchronous acknowledgement with timeout.
let reply = client.emit_with_ack("ping", Json::Str("hi".into()), Duration::from_secs(2))?;
```

## Examples & tests

```sh
# Run the echo/chat server, then talk to it
cargo run --example echo_server
cargo run --example echo_client

# Run the test suite (unit + end-to-end + doc tests)
cargo test
```

## Layout

| Source | Responsibility |
| --- | --- |
| [`crypto.rs`](src/crypto.rs) | SHA-1 + Base64 (WebSocket handshake) |
| [`http.rs`](src/http.rs) | HTTP/1.1 request parsing |
| [`websocket.rs`](src/websocket.rs) | RFC 6455 frame encode/decode |
| [`protocol.rs`](src/protocol.rs) | engine.io + socket.io packet coding |
| [`server.rs`](src/server.rs) | namespaces, rooms, broadcast, acks |
| [`client.rs`](src/client.rs) | zero-dependency socket.io client |

## License

Apache-2.0 — see [LICENSE](LICENSE).