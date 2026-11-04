//! Bearer tokens with scopes (D313 §6; owner, Q2 and Q6): `read`, `write` and `admin`, each
//! optionally limited to memory namespaces. Tokens come from a file or the environment,
//! never a flag, and are compared by their SHA-256 digests in constant time, so a file may
//! hold digests instead of tokens.

use sha2::{Digest, Sha256};

/// What a caller may do. Each scope includes the ones below it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Scope {
    Read,
    Write,
    Admin,
}

/// Who is calling: the token that matched (its line, `token`), its scope, and its namespace
/// limit. Sessions are bound to `token` (D313 §3).
#[derive(Debug, Clone)]
pub(crate) struct Caller {
    pub token: u32,
    pub scope: Scope,
    /// `None`: every namespace. Used by the memory calls (ticket 8).
    #[allow(dead_code)]
    pub namespaces: Option<Vec<String>>,
}

impl Caller {
    /// The caller when no tokens are configured (a loopback server): everything.
    pub(crate) fn anonymous() -> Self {
        Self {
            token: u32::MAX,
            scope: Scope::Admin,
            namespaces: None,
        }
    }
}

struct Entry {
    digest: [u8; 32],
    scope: Scope,
    namespaces: Option<Vec<String>>,
}

/// The configured tokens. Empty: no auth (allowed only on loopback, by the bind rule).
pub(crate) struct Tokens(Vec<Entry>);

impl std::fmt::Debug for Tokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never the digests.
        write!(f, "Tokens({} configured)", self.0.len())
    }
}

impl Tokens {
    /// Parses `text`: one token per line, `read|write|admin [ns=a,b] <token>` or with
    /// `sha256:<hex>` in place of the token. Blank lines and `#` comments are skipped. An
    /// error names the line, never its token.
    pub(crate) fn parse(text: &str, source: &str) -> Result<Self, String> {
        let mut entries = Vec::new();
        for (i, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let bad = |why: &str| format!("{source}, line {}: {why}", i + 1);
            let mut words = line.split_whitespace();
            let scope = match words.next() {
                Some("read") => Scope::Read,
                Some("write") => Scope::Write,
                Some("admin") => Scope::Admin,
                _ => return Err(bad("a line starts with read, write or admin")),
            };
            let mut namespaces = None;
            let mut secret = None;
            for w in words {
                if let Some(list) = w.strip_prefix("ns=") {
                    let ns: Vec<String> = list
                        .split(',')
                        .filter(|n| !n.is_empty())
                        .map(str::to_owned)
                        .collect();
                    if ns.is_empty() {
                        return Err(bad("ns= names at least one namespace"));
                    }
                    namespaces = Some(ns);
                } else if secret.is_none() {
                    secret = Some(w);
                } else {
                    return Err(bad("one token per line"));
                }
            }
            let Some(secret) = secret else {
                return Err(bad("the line has no token"));
            };
            let digest = match secret.strip_prefix("sha256:") {
                Some(hex) => parse_digest(hex).ok_or_else(|| bad("sha256: takes 64 hex digits"))?,
                None => Sha256::digest(secret.as_bytes()).into(),
            };
            entries.push(Entry {
                digest,
                scope,
                namespaces,
            });
        }
        Ok(Self(entries))
    }

    /// Whether any token is configured.
    pub(crate) fn any(&self) -> bool {
        !self.0.is_empty()
    }

    /// The caller a bearer token identifies, or `None` for no match. Every entry is
    /// compared, each in constant time, so the time taken doesn't depend on which matched.
    pub(crate) fn caller(&self, token: &str) -> Option<Caller> {
        let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let mut found = None;
        for (i, e) in self.0.iter().enumerate() {
            if constant_time_eq(&e.digest, &digest) && found.is_none() {
                found = Some(Caller {
                    token: u32::try_from(i).unwrap_or(u32::MAX - 1),
                    scope: e.scope,
                    namespaces: e.namespaces.clone(),
                });
            }
        }
        found
    }
}

fn parse_digest(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(hex.get(2 * i..2 * i + 2)?, 16).ok()?;
    }
    Some(out)
}

fn constant_time_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// The token in an `Authorization: Bearer <token>` header value.
pub(crate) fn bearer(header: Option<&str>) -> Option<&str> {
    header?.strip_prefix("Bearer ").map(str::trim)
}

#[cfg(test)]
mod tests {
    use super::{Scope, Sha256, Tokens};
    use sha2::Digest;

    #[test]
    fn tokens_parse_with_scopes_namespaces_and_digests() {
        let digest: String = Sha256::digest(b"hashed")
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let t = Tokens::parse(
            &format!(
                "# workers\nwrite w0rker\n\nread ns=team-a,team-b r3ader\nadmin sha256:{digest}\n"
            ),
            "tokens",
        )
        .unwrap();
        assert_eq!(t.caller("w0rker").unwrap().scope, Scope::Write);
        let r = t.caller("r3ader").unwrap();
        assert_eq!(
            (r.scope, r.namespaces.unwrap()),
            (Scope::Read, vec!["team-a".to_owned(), "team-b".to_owned()])
        );
        assert_eq!(t.caller("hashed").unwrap().scope, Scope::Admin);
        assert!(t.caller("guess").is_none());
        assert!(t.caller("").is_none());
    }

    #[test]
    fn a_bad_line_names_the_line_not_the_token() {
        let e = Tokens::parse("write ok\nowner s3cret\n", "tokens").unwrap_err();
        assert!(e.contains("line 2") && !e.contains("s3cret"), "{e}");
        assert!(
            Tokens::parse("read\n", "t")
                .unwrap_err()
                .contains("no token")
        );
        assert!(
            Tokens::parse("read a b\n", "t")
                .unwrap_err()
                .contains("one token")
        );
        assert!(
            Tokens::parse("read sha256:abc\n", "t")
                .unwrap_err()
                .contains("64 hex")
        );
    }

    #[test]
    fn scopes_include_the_ones_below() {
        assert!(Scope::Admin >= Scope::Write && Scope::Write >= Scope::Read);
    }
}
