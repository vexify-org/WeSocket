//! Minimal HTTP/1.1 helpers (request parsing + response writing).
//!
//! The server only needs HTTP long enough to perform the WebSocket
//! handshake, so this stays deliberately small.

use std::collections::HashMap;
use std::io::{self, Read, Write};

/// A parsed HTTP request head.
#[derive(Debug, Default)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub query: HashMap<String, String>,
    pub headers: HashMap<String, String>,
}

impl Request {
    /// Read a full request head (up to the blank line) from a stream.
    pub fn read(r: &mut impl Read) -> io::Result<Request> {
        let head = read_head(r)?;
        Self::parse(&head)
    }

    pub fn parse(head: &str) -> io::Result<Request> {
        let mut lines = head.split("\r\n");
        let start = lines.next().ok_or_else(|| io::Error::other("empty head"))?;
        let mut parts = start.split_whitespace();
        let method = parts.next().unwrap_or("").to_string();
        let raw_path = parts.next().unwrap_or("").to_string();
        let (path, query) = split_query(&raw_path);
        let mut headers = HashMap::new();
        for line in lines {
            if let Some((k, v)) = line.split_once(':') {
                headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
            }
        }
        Ok(Request { method, path, query, headers })
    }

    pub fn header(&self, key: &str) -> Option<&str> {
        self.headers.get(key).map(|s| s.as_str())
    }
}

fn split_query(raw_path: &str) -> (String, HashMap<String, String>) {
    match raw_path.split_once('?') {
        Some((p, q)) => {
            let mut map = HashMap::new();
            for pair in q.split('&') {
                if let Some((k, v)) = pair.split_once('=') {
                    map.insert(percent_decode(k), percent_decode(v));
                } else if !pair.is_empty() {
                    map.insert(percent_decode(pair), String::new());
                }
            }
            (p.to_string(), map)
        }
        None => (raw_path.to_string(), HashMap::new()),
    }
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let (Some(h), Some(l)) = (hex(b[i + 1]), hex(b[i + 2])) {
                out.push(h * 16 + l);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Read bytes until the `\r\n\r\n` terminator. Returns the raw head.
pub fn read_head(r: &mut impl Read) -> io::Result<String> {
    let mut buf = Vec::with_capacity(1024);
    let mut one = [0u8; 1];
    let mut count = 0usize;
    loop {
        if r.read(&mut one)? == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "connection closed during handshake"));
        }
        buf.push(one[0]);
        count += 1;
        if buf.len() >= 4 && buf[buf.len() - 4..] == *b"\r\n\r\n" {
            break;
        }
        if count > 64 * 1024 {
            return Err(io::Error::other("request head too large"));
        }
    }
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

/// Write an HTTP status line + headers + body.
pub fn write_response(w: &mut impl Write, code: u16, reason: &str, headers: &[(&str, &str)]) -> io::Result<()> {
    let mut s = format!("HTTP/1.1 {} {}\r\n", code, reason);
    for (k, v) in headers {
        s.push_str(&format!("{}: {}\r\n", k, v));
    }
    s.push_str("Content-Length: 0\r\n");
    s.push_str("Connection: close\r\n");
    s.push_str("\r\n");
    w.write_all(s.as_bytes())
}