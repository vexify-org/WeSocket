//! RFC 6455 WebSocket frame encoding/decoding and the handshake digest.
//!
//! Implements just enough of the protocol to be interoperable with browser
//! and socket.io clients: text/binary messages, fragmentation, ping/pong and
//! close. Client→server frames are masked; server→client frames are not.

use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::crypto::{base64_encode, sha1};

/// The RFC 6455 magic GUID appended to the handshake key.
pub const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// Compute the `Sec-WebSocket-Accept` value for a handshake key.
pub fn accept_key(key: &str) -> String {
    let mut input = String::with_capacity(key.len() + WS_GUID.len());
    input.push_str(key);
    input.push_str(WS_GUID);
    base64_encode(&sha1(input.as_bytes()))
}

/// A decoded WebSocket frame header.
pub struct Frame {
    pub fin: bool,
    pub opcode: u8,
    pub payload: Vec<u8>,
}

/// High-level result of reading one logical message (fragment assembly done).
pub enum Message {
    Text(Vec<u8>),
    Binary(Vec<u8>),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close { code: u16, reason: String },
}

// Opcodes
pub const OP_CONT: u8 = 0x0;
pub const OP_TEXT: u8 = 0x1;
pub const OP_BINARY: u8 = 0x2;
pub const OP_CLOSE: u8 = 0x8;
pub const OP_PING: u8 = 0x9;
pub const OP_PONG: u8 = 0xA;

/// Write one frame to `w`. Set `mask = true` when acting as a client.
pub fn write_frame(w: &mut impl Write, opcode: u8, payload: &[u8], mask: bool) -> io::Result<()> {
    let _fin = true;
    let mut head = Vec::with_capacity(10);
    head.push((0x80 | (opcode & 0x0f)) as u8);

    let len = payload.len();
    let mask_bit: u8 = if mask { 0x80 } else { 0x00 };
    match len {
        0..=125 => head.push(mask_bit | len as u8),
        126..=65535 => {
            head.push(mask_bit | 126);
            head.extend_from_slice(&(len as u16).to_be_bytes());
        }
        _ => {
            head.push(mask_bit | 127);
            head.extend_from_slice(&(len as u64).to_be_bytes());
        }
    }

    if mask {
        let key = random_mask();
        head.extend_from_slice(&key);
        let masked: Vec<u8> = payload
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ key[i % 4])
            .collect();
        w.write_all(&head)?;
        w.write_all(&masked)?;
    } else {
        w.write_all(&head)?;
        w.write_all(payload)?;
    }
    w.flush()
}

fn random_mask() -> [u8; 4] {
    // Cheap-but-good-enough deterministic PRNG seeded from time + counter.
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = counter_mix(COUNTER.fetch_add(1, Ordering::Relaxed) ^ now_nanos());
    let bytes = n.to_le_bytes();
    [bytes[0], bytes[1], bytes[2], bytes[3]]
}

fn counter_mix(mut n: u64) -> u64 {
    n ^= n >> 33;
    n = n.wrapping_mul(0xff51afd7ed558ccd);
    n
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Read a single frame from `r`, applying masking if the frame is masked.
pub fn read_frame(r: &mut impl Read) -> io::Result<Frame> {
    let mut hdr = [0u8; 2];
    r.read_exact(&mut hdr)?;
    let fin = hdr[0] & 0x80 != 0;
    let opcode = hdr[0] & 0x0f;
    let masked = hdr[1] & 0x80 != 0;
    let mut len = (hdr[1] & 0x7f) as u64;

    if len == 126 {
        let mut b = [0u8; 2];
        r.read_exact(&mut b)?;
        len = u16::from_be_bytes(b) as u64;
    } else if len == 127 {
        let mut b = [0u8; 8];
        r.read_exact(&mut b)?;
        len = u64::from_be_bytes(b);
    }

    let limit = 64 * 1024 * 1024;
    if len > limit {
        return Err(io::Error::other("frame too large"));
    }

    let mut key = [0u8; 4];
    if masked {
        r.read_exact(&mut key)?;
    }

    let mut payload = vec![0u8; len as usize];
    r.read_exact(&mut payload)?;

    if masked {
        for (i, b) in payload.iter_mut().enumerate() {
            *b ^= key[i % 4];
        }
    }

    Ok(Frame { fin, opcode, payload })
}

/// Read one logical message, assembling continuation frames and responding
/// to control frames immediately so the caller only sees payload messages.
///
/// `ping_respond` causes ping frames to be answered with a pong inline.
pub fn read_message(r: &mut impl Read, w: &mut impl Write, ping_respond: bool) -> io::Result<Message> {
    loop {
        let frame = read_frame(r)?;
        match frame.opcode {
            OP_CLOSE => {
                let (code, reason) = decode_close(&frame.payload);
                let _ = write_frame(w, OP_CLOSE, &frame.payload, false);
                return Ok(Message::Close { code, reason });
            }
            OP_PING => {
                if ping_respond {
                    let _ = write_frame(w, OP_PONG, &frame.payload, false);
                }
                continue;
            }
            OP_PONG => continue,
            OP_TEXT | OP_BINARY => {
                let mut payload = frame.payload;
                // Assemble any continuation frames.
                if !frame.fin {
                    loop {
                        let c = read_frame(r)?;
                        match c.opcode {
                            OP_CONT => {
                                payload.extend_from_slice(&c.payload);
                                if c.fin {
                                    break;
                                }
                            }
                            OP_PING => {
                                if ping_respond {
                                    let _ = write_frame(w, OP_PONG, &c.payload, false);
                                }
                            }
                            OP_PONG => {}
                            _ => {
                                return Err(io::Error::other("interleaved control frame"));
                            }
                        }
                    }
                }
                return Ok(if frame.opcode == OP_BINARY {
                    Message::Binary(payload)
                } else {
                    Message::Text(payload)
                });
            }
            _ => return Err(io::Error::other("unsupported opcode")),
        }
    }
}

fn decode_close(payload: &[u8]) -> (u16, String) {
    if payload.len() >= 2 {
        let code = u16::from_be_bytes([payload[0], payload[1]]);
        let reason = String::from_utf8_lossy(&payload[2..]).into_owned();
        (code, reason)
    } else {
        (1005, String::new())
    }
}