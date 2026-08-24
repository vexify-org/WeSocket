//! WeSocket echo/chat client example — Powered by Vexify.
//!
//! Usage: `cargo run --example echo_client [addr] [namespace]`
//!
//! Connects to the `echo_server` example, echoes a message, waits for the
//! server's ack, and answers the server-initiated ping.

use std::thread;
use std::time::Duration;

use wesocket::{Client, Json};

fn main() {
    let addr = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:3000".into());
    let nsp = std::env::args().nth(2).unwrap_or_else(|| "/".into());

    // Connect (NAMESPACE_AGNOSTIC: connect() joins the default namespace).
    let client = Client::connect(&addr).expect("connect failed");
    println!("[client] connecting to {addr} on namespace {nsp}");

    // Reply to the server's namespace join.
    client.on_connect(move |_c| {
        println!("[client] connected (sid={})", "?");
    });

    // Server echoes back whatever we sent.
    client.on("echo reply", |_c, args| {
        println!("[client] echo reply: {:?}", args);
    });

    // Server-initiated ack: it sent `server ping` with an ack id.
    client.on("server ping", |_c, args| {
        println!("[client] got server ping: {:?}", args);
    });

    // Demo the ack round-trip from the client side.
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(300));
        println!("[client] emitting 'echo' + binary...");
        let bytes: Vec<u8> = b"hello vexify from client".to_vec();
        let _ = client.emit("echo", Json::Bytes(bytes));

        // A client-initiated ack with the same payload.
        match client.emit_with_ack("echo", Json::Str("ack me".into()), Duration::from_secs(2)) {
            Ok(_) => println!("[client] ack received"),
            Err(e) => println!("[client] ack failed: {e}"),
        }
    });

    // Keep the process alive.
    loop {
        thread::sleep(Duration::from_secs(3600));
    }
}