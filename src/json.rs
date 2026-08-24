//! A tiny, dependency-free JSON value model.
//!
//! socket.io exchanges event payloads as JSON, so we need a value type that
//! round-trips arbitrary data without pulling in a JSON crate. This module
//! implements a practical subset: `null / bool / number / string / array /
//! object`, plus a recursive-descent parser and serializer.

use std::collections::BTreeMap;

/// A dynamically-typed JSON value.
#[derive(Debug, Clone, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    Number(f64),
    Str(String),
    Array(Vec<Json>),
    Object(BTreeMap<String, Json>),
    /// Raw binary payload (used for socket.io binary events/acks).
    Bytes(Vec<u8>),
}

impl Json {
    /// Parse a JSON document from a UTF-8 string slice.
    pub fn parse(s: &str) -> Result<Json, String> {
        let mut p = Parser::new(s);
        let v = p.value()?;
        p.ws();
        if !p.rest().is_empty() {
            return Err(format!("trailing characters at byte {}", p.pos));
        }
        Ok(v)
    }

    /// Serialize to a compact JSON string.
    pub fn to_json(&self) -> String {
        let mut out = String::new();
        self.write(&mut out);
        out
    }

    fn write(&self, out: &mut String) {
        match self {
            Json::Null => out.push_str("null"),
            Json::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            Json::Number(n) => write_number(*n, out),
            Json::Str(s) => write_string(s, out),
            Json::Array(arr) => {
                out.push('[');
                for (i, item) in arr.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    item.write(out);
                }
                out.push(']');
            }
            Json::Object(map) => {
                out.push('{');
                for (i, (k, v)) in map.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write_string(k, out);
                    out.push(':');
                    v.write(out);
                }
                out.push('}');
            }
            // No canonical JSON form for raw bytes; fall back to base64.
            Json::Bytes(b) => write_string(&crate::crypto::base64_encode(b), out),
        }
    }

    /// Return the raw bytes of a `Bytes` value, or `None` otherwise.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Json::Bytes(b) => Some(b),
            _ => None,
        }
    }

    /// True if this is the JSON `null`.
    pub fn is_null(&self) -> bool {
        matches!(self, Json::Null)
    }

    /// True if this is a JSON number / integer value.
    pub fn is_number(&self) -> bool {
        matches!(self, Json::Number(_))
    }

    /// Borrow this value as a string, if it is a string.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Json::Str(s) => Some(s),
            _ => None,
        }
    }
}

/// Builders for ergonomic construction from Rust data.
impl From<bool> for Json {
    fn from(v: bool) -> Self {
        Json::Bool(v)
    }
}
impl From<&str> for Json {
    fn from(v: &str) -> Self {
        Json::Str(v.to_string())
    }
}
impl From<String> for Json {
    fn from(v: String) -> Self {
        Json::Str(v)
    }
}
impl From<f64> for Json {
    fn from(v: f64) -> Self {
        Json::Number(v)
    }
}
impl From<i64> for Json {
    fn from(v: i64) -> Self {
        Json::Number(v as f64)
    }
}
impl From<Vec<Json>> for Json {
    fn from(v: Vec<Json>) -> Self {
        Json::Array(v)
    }
}

impl Default for Json {
    fn default() -> Self {
        Json::Null
    }
}

fn write_number(n: f64, out: &mut String) {
    if n == n.trunc() && n.is_finite() && n.abs() < 1e15 {
        out.push_str(&format!("{}", n as i64));
    } else {
        out.push_str(&format!("{}", n));
    }
}

fn write_string(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(s: &'a str) -> Self {
        Parser { bytes: s.as_bytes(), pos: 0 }
    }
    fn rest(&self) -> &'a [u8] {
        &self.bytes[self.pos..]
    }
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }
    fn next(&mut self) -> Option<u8> {
        let c = self.peek();
        if c.is_some() {
            self.pos += 1;
        }
        c
    }
    fn ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }
    fn expect(&mut self, b: u8) -> Result<(), String> {
        if self.next() == Some(b) {
            Ok(())
        } else {
            Err(format!("expected '{}' at byte {}", b as char, self.pos))
        }
    }

    fn value(&mut self) -> Result<Json, String> {
        self.ws();
        match self.peek() {
            Some(b'n') => self.literal("null", Json::Null),
            Some(b't') => self.literal("true", Json::Bool(true)),
            Some(b'f') => self.literal("false", Json::Bool(false)),
            Some(b'"') => self.string().map(Json::Str),
            Some(b'[') => self.array(),
            Some(b'{') => self.object(),
            Some(c) if c == b'-' || c.is_ascii_digit() => self.number(),
            _ => Err(format!("unexpected character at byte {}", self.pos)),
        }
    }

    fn literal(&mut self, lit: &str, v: Json) -> Result<Json, String> {
        if self.rest().starts_with(lit.as_bytes()) {
            self.pos += lit.len();
            Ok(v)
        } else {
            Err(format!("invalid literal at byte {}", self.pos))
        }
    }

    fn string(&mut self) -> Result<String, String> {
        self.expect(b'"')?;
        let mut out = String::new();
        loop {
            match self.next() {
                None => return Err("unterminated string".into()),
                Some(b'"') => return Ok(out),
                Some(b'\\') => match self.next() {
                    Some(b'"') => out.push('"'),
                    Some(b'\\') => out.push('\\'),
                    Some(b'/') => out.push('/'),
                    Some(b'n') => out.push('\n'),
                    Some(b'r') => out.push('\r'),
                    Some(b't') => out.push('\t'),
                    Some(b'b') => out.push('\u{0008}'),
                    Some(b'f') => out.push('\u{000C}'),
                    Some(b'u') => {
                        let hi = self.hex4().ok_or("bad \\u escape")?;
                        if (0xD800..=0xDBFF).contains(&hi) {
                            let lo = if self.next() == Some(b'\\') && self.next() == Some(b'u') {
                                self.hex4().ok_or("bad surrogate pair")?
                            } else {
                                return Err("unpaired surrogate".into());
                            };
                            let cp = 0x10000 + ((hi as u32 - 0xD800) << 10) + (lo as u32 - 0xDC00);
                            out.push(char::from_u32(cp).ok_or("invalid codepoint")?);
                        } else {
                            out.push(char::from_u32(hi as u32).ok_or("invalid codepoint")?);
                        }
                    }
                    _ => return Err("invalid escape".into()),
                },
                Some(c) if c < 0x20 => return Err("control char in string".into()),
                Some(c) => {
                    // UTF-8 continuation: read the code point from bytes.
                    let len = utf8_len(c);
                    let slice = self.bytes[self.pos - 1..]
                        .get(..len)
                        .ok_or("truncated utf8")?;
                    let s = std::str::from_utf8(slice).map_err(|_| "invalid utf8".to_string())?;
                    out.push_str(s);
                    self.pos += len - 1;
                }
            }
        }
    }

    fn hex4(&mut self) -> Option<u32> {
        if self.pos + 4 > self.bytes.len() {
            return None;
        }
        let mut v = 0u32;
        for i in 0..4 {
            let c = self.bytes[self.pos + i];
            let d = match c {
                b'0'..=b'9' => (c - b'0') as u32,
                b'a'..=b'f' => (c - b'a' + 10) as u32,
                b'A'..=b'F' => (c - b'A' + 10) as u32,
                _ => return None,
            };
            v = v * 16 + d;
        }
        self.pos += 4;
        Some(v)
    }

    fn array(&mut self) -> Result<Json, String> {
        self.expect(b'[')?;
        let mut arr = Vec::new();
        self.ws();
        if self.peek() == Some(b']') {
            self.pos += 1;
            return Ok(Json::Array(arr));
        }
        loop {
            arr.push(self.value()?);
            self.ws();
            match self.next() {
                Some(b',') => continue,
                Some(b']') => return Ok(Json::Array(arr)),
                _ => return Err("expected ',' or ']'".into()),
            }
        }
    }

    fn object(&mut self) -> Result<Json, String> {
        self.expect(b'{')?;
        let mut map = BTreeMap::new();
        self.ws();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(Json::Object(map));
        }
        loop {
            self.ws();
            let key = self.string()?;
            self.ws();
            self.expect(b':')?;
            let val = self.value()?;
            map.insert(key, val);
            self.ws();
            match self.next() {
                Some(b',') => continue,
                Some(b'}') => return Ok(Json::Object(map)),
                _ => return Err("expected ',' or '}'".into()),
            }
        }
    }

    fn number(&mut self) -> Result<Json, String> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        while matches!(self.peek(), Some(c) if c.is_ascii_digit() || c == b'.' || c == b'e' || c == b'E' || c == b'+') {
            self.pos += 1;
        }
        let text = std::str::from_utf8(&self.bytes[start..self.pos]).unwrap();
        text.parse::<f64>()
            .map(Json::Number)
            .map_err(|_| format!("invalid number '{}'", text))
    }
}

fn utf8_len(b: u8) -> usize {
    if b < 0x80 {
        1
    } else if b < 0xe0 {
        2
    } else if b < 0xf0 {
        3
    } else {
        4
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let doc = r#"{"a":1,"b":[true,false,null],"c":"hi\n\"x\"","n":-3.5}"#;
        let v = Json::parse(doc).unwrap();
        assert_eq!(v.to_json(), "{\"a\":1,\"b\":[true,false,null],\"c\":\"hi\\n\\\"x\\\"\",\"n\":-3.5}");
        let v2 = Json::parse(&v.to_json()).unwrap();
        assert_eq!(v, v2);
    }

    #[test]
    fn unicode_escape() {
        let v = Json::parse(r#""\u4f60\u597d""#).unwrap();
        assert_eq!(v, Json::Str("你好".into()));
    }
}