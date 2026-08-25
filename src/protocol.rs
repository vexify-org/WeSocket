//! Engine.IO + Socket.IO packet coding (protocol v5 / EIO 4).
//!
//! Mirrors the behaviour of the official `socket.io-parser` and `engine.io`
//! so that this crate is wire-compatible with the reference implementations.

use crate::json::Json;

/// Engine.IO packet types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EngineType {
    Open = 0,
    Close = 1,
    Ping = 2,
    Pong = 3,
    Message = 4,
    Upgrade = 5,
    Noop = 6,
}

/// Socket.IO packet types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PacketType {
    Connect = 0,
    Disconnect = 1,
    Event = 2,
    Ack = 3,
    ConnectError = 4,
    BinaryEvent = 5,
    BinaryAck = 6,
}

/// A decoded Socket.IO packet.
#[derive(Debug, Clone)]
pub struct Packet {
    pub type_: PacketType,
    pub nsp: String,
    pub id: Option<u64>,
    pub data: Json,
    /// Number of binary attachments expected (binary packets only).
    pub attachments: usize,
    /// Reconstructed binary payloads (binary packets only).
    pub binary: Vec<Vec<u8>>,
}

/// Build the Engine.IO `open` packet sent right after the HTTP handshake.
pub fn engine_open(sid: &str) -> String {
    format!(
        "0{{\"sid\":\"{}\",\"upgrades\":[\"websocket\"],\"pingInterval\":25000,\"pingTimeout\":20000,\"maxPayload\":1000000}}",
        sid
    )
}

/// Encode a Socket.IO packet and wrap it in an Engine.IO `message` frame.
pub fn encode_message(p: &Packet) -> String {
    let mut s = String::new();
    s.push_str(&(p.type_ as u8).to_string());

    if matches!(p.type_, PacketType::BinaryEvent | PacketType::BinaryAck) {
        s.push_str(&p.attachments.to_string());
        s.push('-');
    }

    if p.nsp != "/" {
        s.push_str(&p.nsp);
        s.push(',');
    }

    if let Some(id) = p.id {
        s.push_str(&id.to_string());
    }

    if !p.data.is_null() {
        s.push_str(&p.data.to_json());
    }

    format!("4{}", s)
}

/// Encode an Engine.IO control packet (ping, pong, close, noop, open...).
pub fn engine_control(t: EngineType, payload: &str) -> String {
    format!("{}{}", t as u8, payload)
}

/// Decode a raw engine packet from a websocket text frame.
/// Returns the engine type and, for `message`, the socket.io text.
pub fn decode_engine(text: &str) -> Result<(EngineType, Option<String>), String> {
    if text.is_empty() {
        return Err("empty engine packet".into());
    }
    let t = text.as_bytes()[0];
    let t = match t {
        b'0' => EngineType::Open,
        b'1' => EngineType::Close,
        b'2' => EngineType::Ping,
        b'3' => EngineType::Pong,
        b'4' => EngineType::Message,
        b'5' => EngineType::Upgrade,
        b'6' => EngineType::Noop,
        _ => return Err(format!("unknown engine type '{}'", t as char)),
    };
    let payload = text[1..].to_string();
    Ok((
        t,
        if matches!(t, EngineType::Message) { Some(payload) } else { None },
    ))
}

/// Decode a Socket.IO packet from the text that follows the engine `4`.
pub fn decode_socketio(text: &str) -> Result<Packet, String> {
    let b = text.as_bytes();
    if b.is_empty() {
        return Err("empty socket.io packet".into());
    }
    let first = b[0];
    let type_ = match first {
        b'0' => PacketType::Connect,
        b'1' => PacketType::Disconnect,
        b'2' => PacketType::Event,
        b'3' => PacketType::Ack,
        b'4' => PacketType::ConnectError,
        b'5' => PacketType::BinaryEvent,
        b'6' => PacketType::BinaryAck,
        _ => return Err(format!("unknown packet type '{}'", first as char)),
    };

    let mut p = Packet {
        type_,
        nsp: "/".to_string(),
        id: None,
        data: Json::Null,
        attachments: 0,
        binary: Vec::new(),
    };

    let mut pos = 1;

    // Attachments count for binary packets.
    if matches!(type_, PacketType::BinaryEvent | PacketType::BinaryAck) {
        let start = pos;
        while pos < b.len() && b[pos] != b'-' {
            pos += 1;
        }
        if pos >= b.len() {
            return Err("illegal attachments".into());
        }
        let n: usize = text[start..pos].parse().map_err(|_| "illegal attachments")?;
        p.attachments = n;
        pos += 1; // skip '-'
    }

    // Namespace.
    if pos < b.len() && b[pos] == b'/' {
        let start = pos;
        while pos < b.len() {
            if b[pos] == b',' {
                break;
            }
            pos += 1;
        }
        p.nsp = text[start..pos].to_string();
        if pos < b.len() && b[pos] == b',' {
            pos += 1;
        }
    }

    // Ack id: a run of digits.
    if pos < b.len() && b[pos].is_ascii_digit() {
        let start = pos;
        while pos < b.len() && b[pos].is_ascii_digit() {
            pos += 1;
        }
        p.id = text[start..pos].parse().ok();
    }

    // Payload.
    if pos < b.len() {
        let json = Json::parse(&text[pos..])?;
        p.data = json;
    }

    validate_payload(&p)?;
    Ok(p)
}

fn validate_payload(p: &Packet) -> Result<(), String> {
    use PacketType::*;
    let ok = match p.type_ {
        Connect => matches!(p.data, Json::Null | Json::Object(_)),
        Disconnect => p.data.is_null(),
        ConnectError => {
            matches!(p.data, Json::Str(_) | Json::Object(_) | Json::Null)
        }
        Event | BinaryEvent => match &p.data {
            Json::Array(a) => !a.is_empty(),
            _ => false,
        },
        Ack | BinaryAck => matches!(p.data, Json::Array(_) | Json::Null),
    };
    if ok {
        Ok(())
    } else {
        Err("invalid payload for packet type".into())
    }
}

/// Replace raw `Json::Bytes` values with socket.io binary placeholders and
/// collect the attachment list. Promotes an event/ack into a binary packet.
pub fn encode_binary(p: &mut Packet) {
    let mut binaries = Vec::new();
    p.data = to_placeholders(p.data.clone(), &mut binaries);
    p.attachments = binaries.len();
    p.binary = binaries;
    if !p.binary.is_empty()
        && matches!(p.type_, PacketType::Event | PacketType::Ack)
    {
        p.type_ = match p.type_ {
            PacketType::Event => PacketType::BinaryEvent,
            _ => PacketType::BinaryAck,
        };
    }
}

fn to_placeholders(v: Json, bins: &mut Vec<Vec<u8>>) -> Json {
    match v {
        Json::Bytes(b) => {
            let num = bins.len();
            bins.push(b);
            Json::Object(
                [
                    ("_placeholder".to_string(), Json::Bool(true)),
                    ("num".to_string(), Json::Number(num as f64)),
                ]
                .into_iter()
                .collect(),
            )
        }
        Json::Array(items) => {
            Json::Array(items.into_iter().map(|i| to_placeholders(i, bins)).collect())
        }
        Json::Object(map) => Json::Object(
            map.into_iter()
                .map(|(k, val)| (k, to_placeholders(val, bins)))
                .collect(),
        ),
        other => other,
    }
}

/// Fill binary placeholders with reconstructed attachment bytes.
pub fn reconstruct(data: Json, binaries: &[Vec<u8>]) -> Json {
    fn fill_one(v: Json, bins: &[Vec<u8>]) -> Json {
        match v {
            Json::Object(map) => {
                let is_placeholder =
                    map.get("_placeholder") == Some(&Json::Bool(true)) && map.contains_key("num");
                if is_placeholder {
                    if let Some(Json::Number(n)) = map.get("num") {
                        let idx = *n as usize;
                        if let Some(b) = bins.get(idx) {
                            return Json::Bytes(b.clone());
                        }
                    }
                    return Json::Null;
                }
                Json::Object(
                    map.into_iter()
                        .map(|(k, val)| (k, fill_one(val, bins)))
                        .collect(),
                )
            }
            Json::Array(items) => {
                Json::Array(items.into_iter().map(|i| fill_one(i, bins)).collect())
            }
            other => other,
        }
    }
    fill_one(data, binaries)
}