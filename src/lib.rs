//! == WeSocket — a socket.io-compatible realtime engine in Rust ==
//!
//! Lighter, faster and dramatically leaner than the reference stack, with
//! **zero runtime dependencies** — the WebSocket frame layer (RFC 6455),
//! HTTP handshake, SHA-1/Base64 primitives, JSON model and the engine.io /
//! socket.io protocol are all implemented from scratch on top of `std`.
//!
//! `Powered by Vexify`
//!
//! # Example
//!
//! ```no_run
//! use wesocket::{Json, SocketIo};
//!
//! let io = SocketIo::new();
//! io.on("connection", |socket, _| {
//!     let id = socket.id().to_owned();
//!     println!("socket connected: {id}");
//!     socket.on("chat message", move |socket, args| {
//!         // broadcast the message back to everyone else
//!         if let Some(msg) = args.first() {
//!             socket.broadcast().emit("chat message", msg.clone());
//!         }
//!     });
//! });
//!
//! let server = io.bind("0.0.0.0:3000").unwrap();
//! println!("listening on {}", server.addr().unwrap());
//! std::thread::park();
//! ```

pub mod client;
pub mod crypto;
pub mod http;
pub mod json;
pub mod protocol;
pub mod server;
pub mod websocket;

pub use client::{Client, ClientHandler};
pub use json::Json;
pub use protocol::{Packet, PacketType};
pub use server::{Broadcast, EventHandler, Namespace, Server, Socket, SocketIo};