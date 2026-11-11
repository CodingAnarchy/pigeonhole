//! Bearer tokens with scopes (D313 §6; owner, Q2 and Q6): `read`, `write` and `admin`, each
//! optionally limited to memory namespaces, and `cluster` for node-to-node calls (D317, ICR
//! 0117). Tokens come from a file or the environment,
//! never a flag, and are compared by their SHA-256 digests in constant time, so a file may
//! hold digests instead of tokens.

use sha2::{Digest, Sha256};

/// What a caller may do. `read`, `write` and `admin` each include the ones before them;
/// `cluster` (a node of a D317 cluster calling another) stands apart: it includes none of
/// them, and none of them includes it, so an admin token can't push a map and a node's
/// token can't read data. Not ordered: [`Scope::allows`] is the only comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Scope {
    Read,
    Write,
    Admin,
    Cluster,
}

impl Scope {
    /// Whether a caller with this scope may make a call that needs `need`.
    pub(crate) fn allows(self, need: Scope) -> bool {
        let rank = |s: Scope| match s {
            Scope::Read => Some(0),
            Scope::Write => Some(1),
            Scope::Admin => Some(2),
            Scope::Cluster => None,
        };
        match (rank(self), rank(need)) {
            (Some(have), Some(need)) => have >= need,
            (None, None) => true,
            _ => false,
        }
    }
}

/// Who a session belongs to (D317 ticket 3): its token's SHA-256 digest, the same on every
/// node that lists the token and stable when a token file is reordered or reloaded;
/// [`ANONYMOUS`] without tokens. Never logged or put in an error (cli692's S1).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct Principal([u8; 32]);

impl Principal {
    /// The principal of the token whose SHA-256 digest is `digest`.
    pub(crate) const fn from_digest(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    /// Its digest, which a forwarded call carries to the partition's node (D317 ticket 3).
    pub(crate) fn digest(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Redacted (cli692's R1 on ticket 3): a `Caller` or session printed with `{:?}` shows
/// whether it's anonymous, never the digest.
impl std::fmt::Debug for Principal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if *self == ANONYMOUS {
            "Principal(anonymous)"
        } else {
            "Principal(..)"
        })
    }
}

/// The principal of a server without tokens.
pub(crate) const ANONYMOUS: Principal = Principal([0; 32]);

/// Who is calling: the token that matched (its `principal`), its scope, and its namespace
/// limit. Sessions are bound to `principal` (D313 §3, D317 ticket 3).
#[derive(Debug, Clone)]
pub(crate) struct Caller {
    /// Whose sessions it may use, and whose per-token cap it counts against.
    pub principal: Principal,
    pub scope: Scope,
    /// `None`: every namespace. Used by the memory calls (ticket 8).
    #[allow(dead_code)]
    pub namespaces: Option<Vec<String>>,
    /// The reply goes back as HTTP/JSON, larger than its protobuf: a read reserves twice its
    /// cap for it (#1567, cli692's D2). Set by the HTTP front-end.
    pub json: bool,
}

impl Caller {
    /// The caller when no tokens are configured (a loopback server): everything.
    pub(crate) fn anonymous() -> Self {
        Self {
            principal: ANONYMOUS,
            scope: Scope::Admin,
            namespaces: None,
            json: false,
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
    /// Parses `text`: one token per line, `read|write|admin|cluster [ns=a,b] <token>` or with
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
                Some("cluster") => Scope::Cluster,
                _ => return Err(bad("a line starts with read, write, admin or cluster")),
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

    /// The caller a forwarded call's token digest identifies here (D317 ticket 3): this
    /// node's own entry for it, so its scope and namespaces are this node's, or `None` when
    /// it isn't listed. Compared as [`caller`](Self::caller) compares.
    pub(crate) fn caller_by_digest(&self, digest: &[u8; 32]) -> Option<Caller> {
        let mut found = None;
        for e in &self.0 {
            if constant_time_eq(&e.digest, digest) && found.is_none() {
                found = Some(Caller {
                    principal: Principal::from_digest(e.digest),
                    scope: e.scope,
                    namespaces: e.namespaces.clone(),
                    json: false,
                });
            }
        }
        found
    }

    /// The caller a bearer token identifies, or `None` for no match. Every entry is
    /// compared, each in constant time, so the time taken doesn't depend on which matched.
    pub(crate) fn caller(&self, token: &str) -> Option<Caller> {
        let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let mut found = None;
        for e in &self.0 {
            if constant_time_eq(&e.digest, &digest) && found.is_none() {
                found = Some(Caller {
                    principal: Principal::from_digest(e.digest),
                    scope: e.scope,
                    namespaces: e.namespaces.clone(),
                    json: false,
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
    use super::{Principal, Scope, Sha256, Tokens};
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

    /// The same token is the same principal, its SHA-256, whatever the file's line order and
    /// whether a line holds the token or its digest: nodes agree on it (cli692's R2 on
    /// D317 ticket 3).
    #[test]
    fn a_tokens_principal_is_its_digest_whatever_the_file() {
        let digest: [u8; 32] = Sha256::digest(b"s3cret").into();
        let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
        let files = [
            "write s3cret\nread other\n".to_owned(),
            "read other\nwrite s3cret\n".to_owned(),
            format!("read other\nwrite sha256:{hex}\n"),
        ];
        for f in &files {
            let t = Tokens::parse(f, "tokens").unwrap();
            assert!(
                t.caller("s3cret").unwrap().principal == Principal::from_digest(digest),
                "{f}"
            );
            assert!(t.caller("other").unwrap().principal != Principal::from_digest(digest));
        }
    }

    /// A caller printed with `{:?}` shows no digest (cli692's R1 on D317 ticket 3).
    #[test]
    fn a_callers_debug_has_no_digest() {
        let digest: [u8; 32] = Sha256::digest(b"s3cret").into();
        let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
        let c = Tokens::parse("write s3cret\n", "t")
            .unwrap()
            .caller("s3cret")
            .unwrap();
        let shown = format!("{c:?}");
        assert!(shown.contains("Principal(..)"), "{shown}");
        assert!(
            !shown.contains(&hex) && !shown.contains(&format!("{digest:?}")),
            "{shown}"
        );
        assert!(format!("{:?}", super::Caller::anonymous()).contains("Principal(anonymous)"));
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
    fn scopes_include_the_ones_below_and_cluster_stands_apart() {
        use Scope::{Admin, Cluster, Read, Write};
        assert!(Admin.allows(Write) && Admin.allows(Read) && Write.allows(Read));
        assert!(!Read.allows(Write) && !Write.allows(Admin));
        for s in [Read, Write, Admin] {
            assert!(!s.allows(Cluster) && !Cluster.allows(s), "{s:?}");
        }
        assert!(Cluster.allows(Cluster));
        let t = Tokens::parse("cluster n0de\n", "t").unwrap();
        assert_eq!(t.caller("n0de").unwrap().scope, Cluster);
    }
}
