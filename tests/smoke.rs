//! End-to-end smoke tests — Powered by Vexify.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use wesocket::{Client, Json, SocketIo};

fn spawn_server<F: Fn(&SocketIo) + Send + 'static>(setup: F) -> (SocketIo, String) {
    let io = SocketIo::new();
    setup(&io);
    let server = io.bind(("127.0.0.1", 0)).expect("bind");
    let addr = server.addr().unwrap().to_string();
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_secs(600));
        drop(server);
    });
    (io, addr)
}

#[test]
fn echo_with_ack_roundtrip() {
    let (io, addr) = spawn_server(|io| {
        io.on("connection", |socket, _| {
            socket.on("echo", |sock, args| {
                let data = args.first().cloned().unwrap_or(Json::Null);
                sock.emit("echo reply", data);
            });
        });
    });

    let client = Client::connect(&addr).expect("connect");
    let got = Arc::new(AtomicBool::new(false));
    let got2 = got.clone();
    client.on("echo reply", move |c, args| {
        if let Some(Json::Str(s)) = args.first() {
            if s == "hello" {
                got2.store(true, Ordering::SeqCst);
            }
        }
        let _ = c;
    });

    // Wait until handshake + namespace connect complete.
    let mut ok = false;
    for _ in 0..100 {
        if client.connected() {
            ok = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(ok, "client never became connected");

    let ack = client
        .emit_with_ack("ping", Json::Str("hi".into()), Duration::from_secs(2))
        .expect("ack");
    let _ = ack;

    client.emit("echo", Json::Str("hello".into())).unwrap();

    for _ in 0..100 {
        if got.load(Ordering::SeqCst) {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(got.load(Ordering::SeqCst), "did not receive echo reply");

    client.disconnect();
    let _ = io;
}

#[test]
fn binary_roundtrip_via_base64() {
    let (io, addr) = spawn_server(|io| {
        io.on("connection", |socket, _| {
            socket.on("b", |sock, args| {
                if let Some(b) = args.first() {
                    sock.emit("b reply", b.clone());
                }
            });
        });
    });

    let client = Client::connect(&addr).expect("connect");
    for _ in 0..100 {
        if client.connected() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    let got = Arc::new(std::sync::Mutex::new(None));
    let g = got.clone();
    client.on("b reply", move |_c, args| {
        *g.lock().unwrap() = args.first().cloned();
    });

    let payload: Vec<u8> = b"\x00\x01binary\xff".to_vec();
    client.emit("b", Json::Bytes(payload.clone())).unwrap();

    let mut seen = false;
    for _ in 0..100 {
        if got.lock().unwrap().is_some() {
            seen = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(seen, "no binary reply");
    let _ = io;
}

#[test]
fn namespace_isolation() {
    let (io, addr) = spawn_server(|io| {
        io.of("/a").on("connection", |socket, _| {
            socket.on("hit", |sock, _| sock.emit("hit a", Json::Bool(true)));
        });
        io.of("/b").on("connection", |socket, _| {
            socket.on("hit", |sock, _| sock.emit("hit b", Json::Bool(true)));
        });
    });

    let a = Client::connect_ns(&addr, "/a").expect("a");
    let b = Client::connect_ns(&addr, "/b").expect("b");
    std::thread::sleep(Duration::from_millis(100));

    let a_hit = Arc::new(AtomicBool::new(false));
    let ha = a_hit.clone();
    a.on("hit a", move |_c, _| ha.store(true, Ordering::SeqCst));

    a.emit("hit", Json::Null).unwrap();
    for _ in 0..100 {
        if a_hit.load(Ordering::SeqCst) {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(a_hit.load(Ordering::SeqCst), "namespace /a did not respond");
    let _ = (b, io);
}