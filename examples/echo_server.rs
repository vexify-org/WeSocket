//! WeSocket echo/chat server example — Powered by Vexify.
//!
//! Usage: `cargo run --example echo_server [port]`

use std::time::Duration;

use wesocket::{Json, Socket, SocketIo};

fn main() {
    let port: u16 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(3000);

    let io = SocketIo::new();

    // Default namespace: a tiny chat + echo demo.
    io.on("connection", |socket, _| {
        let id = socket.id().to_owned();
        println!("[server] connected id={id}");
        socket.join("lobby");

        let s2 = socket.clone();
        socket.on("echo", move |sock, args| {
            let data = args.first().cloned().unwrap_or_else(|| Json::Str("?".into()));
            println!("[server] echo from {}: {}", sock.id(), data.to_json());
            sock.emit("echo reply", data);
            sock.to("lobby").emit("chat", Json::Str("someone echoed".into()));
        });
        let _ = s2;

        // Server-initiated ack: ask the client to confirm, then show the result.
        let sid = socket.id().to_owned();
        let owned = Socket::clone(socket);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            let _ = owned.emit_with_ack("server ping", Json::Str("from server".into()), Duration::from_secs(2));
            println!("[server] sent server ping to {sid}");
        });
    });

    // A named namespace.
    io.of("/chat").on("connection", |socket, _| {
        println!("[server] connected to /chat: {}", socket.id());
        socket.on("msg", move |sock, args| {
            let text = args.first().cloned().unwrap_or(Json::Null);
            sock.broadcast().emit("msg", text);
        });
    });

    let server = io.bind(("0.0.0.0", port)).expect("bind failed");
    println!("[server] WeSocket listening on {}", server.addr().unwrap());

    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}