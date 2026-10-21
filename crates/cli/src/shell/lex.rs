//! The shell's tokenizer: one command per line, words split on whitespace, with byte
//! literals for arbitrary keys and values.
//!
//! - A bare word is its UTF-8 bytes. `#` starts a comment at the beginning of a word.
//! - `"…"` takes escapes: `\n`, `\t`, `\r`, `\0`, `\\`, `\"` and `\xHH` (any byte).
//! - `'…'` is literal: no escapes.
//! - A word `0x…` (hex digits) is those bytes; `b64:…` is base64.
//! - Quoted parts and bare parts join into one word when nothing separates them.

/// One word of a command line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Word {
    /// Its bytes, after escapes and literals.
    pub bytes: Vec<u8>,
    /// Whether any part was quoted: a quoted `--x` or `0x…` is plain text, not a flag or a
    /// literal.
    pub quoted: bool,
}

impl Word {
    /// The word as text, if it is valid UTF-8.
    pub(crate) fn text(&self) -> Option<&str> {
        std::str::from_utf8(&self.bytes).ok()
    }

    /// Whether this is a flag (`--name`, unquoted).
    pub(crate) fn is_flag(&self) -> bool {
        !self.quoted && self.bytes.starts_with(b"--")
    }
}

/// Splits `line` into words. An unterminated quote or a bad escape or literal is an error.
pub(crate) fn split(line: &str) -> Result<Vec<Word>, String> {
    let mut words = Vec::new();
    let mut chars = line.chars().peekable();
    loop {
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        match chars.peek() {
            None | Some('#') => break,
            _ => {}
        }
        let mut bytes = Vec::new();
        let mut quoted = false;
        let mut bare = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_whitespace() {
                break;
            }
            chars.next();
            match c {
                '"' => {
                    quoted = true;
                    bytes.extend(std::mem::take(&mut bare).into_bytes());
                    loop {
                        match chars.next() {
                            None => return Err("unterminated \"".into()),
                            Some('"') => break,
                            Some('\\') => bytes.extend(escape(&mut chars)?),
                            Some(c) => {
                                let mut buf = [0u8; 4];
                                bytes.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                            }
                        }
                    }
                }
                '\'' => {
                    quoted = true;
                    bytes.extend(std::mem::take(&mut bare).into_bytes());
                    loop {
                        match chars.next() {
                            None => return Err("unterminated '".into()),
                            Some('\'') => break,
                            Some(c) => {
                                let mut buf = [0u8; 4];
                                bytes.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                            }
                        }
                    }
                }
                c => bare.push(c),
            }
        }
        // A whole unquoted word may be a hex or base64 literal; inside a joined word the bare
        // parts are plain text.
        if quoted {
            bytes.extend(bare.into_bytes());
        } else {
            bytes = literal(&bare)?;
        }
        words.push(Word { bytes, quoted });
    }
    Ok(words)
}

/// A bare word's bytes: `0x…` hex, `b64:…` base64, else its UTF-8.
fn literal(word: &str) -> Result<Vec<u8>, String> {
    if let Some(hex) = word.strip_prefix("0x")
        && !hex.is_empty()
    {
        if hex.len() % 2 != 0 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(format!("bad hex literal {word}"));
        }
        return Ok((0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex digits"))
            .collect());
    }
    if let Some(b64) = word.strip_prefix("b64:") {
        return crate::cmd::schema::bytes_from_json(&serde_json::json!({ "base64": b64 }))
            .map_err(|_| format!("bad base64 literal {word}"));
    }
    Ok(word.as_bytes().to_vec())
}

fn escape(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> Result<Vec<u8>, String> {
    Ok(match chars.next() {
        Some('n') => vec![b'\n'],
        Some('t') => vec![b'\t'],
        Some('r') => vec![b'\r'],
        Some('0') => vec![0],
        Some('\\') => vec![b'\\'],
        Some('"') => vec![b'"'],
        Some('x') => {
            let hex: String = chars.by_ref().take(2).collect();
            if hex.len() != 2 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(format!("bad escape \\x{hex}"));
            }
            vec![u8::from_str_radix(&hex, 16).expect("hex digits")]
        }
        Some(c) => return Err(format!("unknown escape \\{c}")),
        None => return Err("a \\ ends the line".into()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(line: &str) -> Vec<Vec<u8>> {
        split(line).unwrap().into_iter().map(|w| w.bytes).collect()
    }

    #[test]
    fn words_quotes_escapes_and_literals() {
        assert_eq!(bytes("get t r1  f:q"), [&b"get"[..], b"t", b"r1", b"f:q"]);
        assert_eq!(
            bytes(r#"put "a b" "x\ny\x00""#),
            [&b"put"[..], b"a b", b"x\ny\x00"]
        );
        assert_eq!(bytes(r#"'it\n' "\"q\"""#), [&br"it\n"[..], b"\"q\""]);
        assert_eq!(
            bytes("0xff00 b64:/wA= 0x"),
            [&[0xff, 0x00][..], &[0xff, 0x00], b"0x"]
        );
        assert_eq!(bytes(r#"pre"fix" "0xff""#), [&b"prefix"[..], b"0xff"]);
        assert_eq!(bytes("scan t # a comment"), [&b"scan"[..], b"t"]);
        assert!(bytes("   ").is_empty());
        let w = split(r#"--limit "--x""#).unwrap();
        assert!(w[0].is_flag() && !w[1].is_flag());
    }

    #[test]
    fn errors() {
        assert!(split("\"open").is_err());
        assert!(split("'open").is_err());
        assert!(split(r#""\q""#).is_err());
        assert!(split(r#""\x1""#).is_err());
        assert!(split("0xabc").is_err());
        assert!(split("b64:!!").is_err());
    }
}
