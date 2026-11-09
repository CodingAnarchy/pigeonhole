//! Server-side embedding for the memory tools (D311 T7, `--embed-url`): an OpenAI-compatible
//! `/embeddings` endpoint, called synchronously.
//!
//! The request is `POST {url}` with `{"model": NAME, "input": TEXT}`, and the vector is the
//! response's `data[0].embedding`. The API key comes from the environment variable
//! `--embed-key-env` names and goes in an `Authorization: Bearer` header; it never appears in
//! a message this module returns. Built only with the `embed` feature (on by default), which
//! brings `ureq` with rustls on the ring provider, trusting the operating system's root
//! certificates (`rustls-native-certs`).

use crate::exit::Failure;

/// The longest `phdb mcp` waits for one embedding: one agent waits on its own call.
/// `phdb serve` passes a shorter one (`--embed-timeout-ms`, 10 s by default).
pub(crate) const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// The largest response body read.
#[cfg(feature = "embed")]
const MAX_RESPONSE_BYTES: u64 = 16 << 20;

/// An embedding endpoint.
pub(crate) struct Endpoint {
    #[cfg(feature = "embed")]
    agent: ureq::Agent,
    #[cfg(feature = "embed")]
    url: String,
    model: String,
    #[cfg(feature = "embed")]
    key: Option<String>,
}

impl Endpoint {
    /// The endpoint at `url` for `model`, with the key from the environment variable
    /// `key_env` if given (a variable that's unset or empty is a usage failure, at startup).
    /// One embedding waits at most `timeout`.
    pub(crate) fn new(
        url: &str,
        model: &str,
        key_env: Option<&str>,
        timeout: std::time::Duration,
    ) -> Result<Self, Failure> {
        if !(url.starts_with("https://") || url.starts_with("http://")) {
            return Err(Failure::usage(&format!(
                "--embed-url {url}: an http:// or https:// URL"
            )));
        }
        if key_env.is_some() && url.starts_with("http://") && !loopback(url) {
            return Err(Failure::usage(&format!(
                "--embed-url {url}: an API key goes only over https:// (or plain http:// to \
                 localhost), never in cleartext across the network"
            )));
        }
        #[cfg(feature = "embed")]
        {
            let key = match key_env {
                Some(var) => match std::env::var(var) {
                    Ok(k) if !k.is_empty() => Some(k),
                    _ => {
                        return Err(Failure::usage(&format!(
                            "--embed-key-env {var}: the variable is not set"
                        )));
                    }
                },
                None => None,
            };
            use ureq::tls::{Certificate, RootCerts, TlsConfig, TlsProvider};
            // The process's rustls provider, which ureq takes when none is configured. An
            // error means one is installed already, which serves as well.
            let _ = rustls::crypto::ring::default_provider().install_default();
            let mut tls = TlsConfig::builder().provider(TlsProvider::Rustls);
            if url.starts_with("https://") {
                let found = rustls_native_certs::load_native_certs();
                if found.certs.is_empty() {
                    let why = found.errors.first().map(ToString::to_string);
                    return Err(Failure::unsupported(format!(
                        "--embed-url {url}: no root certificates found in the system's trust \
                         store{}",
                        why.map(|w| format!(" ({w})")).unwrap_or_default()
                    )));
                }
                let roots: Vec<Certificate<'static>> = found
                    .certs
                    .iter()
                    .map(|c| Certificate::from_der(c.as_ref()).to_owned())
                    .collect();
                tls = tls.root_certs(RootCerts::from(roots));
            }
            let tls = tls.build();
            let agent = ureq::Agent::config_builder()
                .timeout_global(Some(timeout))
                // A connection that isn't up within this is an endpoint that can't be
                // reached, not a slow one: shorter than the call's own bound, so it ends as
                // a connect timeout (`EmbedFault::Unreachable`). Windows retries a SYN to a
                // closed port for about 2 s rather than failing at once (#1617).
                .timeout_resolve(Some(connect_timeout(timeout)))
                .timeout_connect(Some(connect_timeout(timeout)))
                // No redirects: the bearer key goes only to the URL given.
                .max_redirects(0)
                .http_status_as_error(false)
                .tls_config(tls)
                .build()
                .into();
            Ok(Self {
                agent,
                url: url.to_owned(),
                model: model.to_owned(),
                key,
            })
        }
        #[cfg(not(feature = "embed"))]
        {
            let _ = (model, key_env, timeout);
            Err(Failure::unsupported(
                "--embed-url: this phdb was built without the `embed` feature".into(),
            ))
        }
    }

    /// The model name the endpoint is asked for.
    pub(crate) fn model(&self) -> &str {
        &self.model
    }

    /// `text`'s embedding. A failure's message names the endpoint and what went wrong, never
    /// the key.
    pub(crate) fn embed(&self, text: &str) -> Result<Vec<f32>, Failure> {
        self.embed_classified(text).map_err(|(_, f)| f)
    }

    /// [`embed`](Self::embed), with what kind of failure it was, for a server to answer by
    /// (ai1249's R2 on #1617).
    pub(crate) fn embed_classified(&self, text: &str) -> Result<Vec<f32>, (EmbedFault, Failure)> {
        #[cfg(feature = "embed")]
        {
            let fail = |kind: EmbedFault, m: String| {
                (
                    kind,
                    Failure::input(format!("embedding endpoint {}: {m}", self.url)),
                )
            };
            // A transport error: the call's own timeout, or the endpoint unreachable.
            let transport = |e: ureq::Error| match e {
                // No connection within its bound: unreachable, like a refused one.
                ureq::Error::Timeout(ureq::Timeout::Connect | ureq::Timeout::Resolve) => {
                    fail(EmbedFault::Unreachable, e.to_string())
                }
                ureq::Error::Timeout(_) => fail(EmbedFault::Timeout, e.to_string()),
                e => fail(EmbedFault::Unreachable, e.to_string()),
            };
            let body = serde_json::json!({"model": self.model, "input": text}).to_string();
            let mut req = self
                .agent
                .post(&self.url)
                .header("Content-Type", "application/json");
            if let Some(k) = &self.key {
                req = req.header("Authorization", &format!("Bearer {k}"));
            }
            let mut resp = req.send(body).map_err(transport)?;
            let status = resp.status().as_u16();
            let text = resp
                .body_mut()
                .with_config()
                .limit(MAX_RESPONSE_BYTES)
                .read_to_string()
                .map_err(|e| match e {
                    ureq::Error::BodyExceedsLimit(_) => {
                        fail(EmbedFault::BadReply, format!("reading the response: {e}"))
                    }
                    e => transport(e),
                })?;
            if status != 200 {
                let mut brief: String = text.chars().take(300).collect();
                if brief.len() < text.len() {
                    brief.push('…');
                }
                return Err(fail(
                    EmbedFault::of_status(status),
                    format!("HTTP {status}: {brief}"),
                ));
            }
            parse(&text).map_err(|m| fail(EmbedFault::BadReply, m))
        }
        #[cfg(not(feature = "embed"))]
        {
            let _ = text;
            unreachable!("an Endpoint is only made with the embed feature")
        }
    }
}

/// How long an embedding call waits for its connection (name resolution, then connect and
/// TLS, each): half the call's bound, at most 3 s.
#[cfg_attr(not(feature = "embed"), allow(dead_code))]
fn connect_timeout(call: std::time::Duration) -> std::time::Duration {
    (call / 2).min(std::time::Duration::from_secs(3))
}

/// What kind of failure an embedding call met (ai1249's R2 on #1617). `phdb serve` answers
/// each with its own code; `phdb mcp` reports them all alike.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(feature = "embed"), allow(dead_code))]
pub(crate) enum EmbedFault {
    /// The call took longer than the endpoint's timeout.
    Timeout,
    /// The endpoint couldn't be reached (refused, or no connection within half the call's
    /// bound, at most 3 s), or answered that it's failing or busy (5xx, 408, 429): worth
    /// retrying.
    Unreachable,
    /// The endpoint refused the input (400, 413, 422): the caller's text, say too long for
    /// the model.
    Rejected,
    /// Any other answer (a bad key, a wrong path), or a reply that isn't an embedding: the
    /// server's configuration.
    BadReply,
}

impl EmbedFault {
    /// The kind of a non-200 answer.
    #[cfg_attr(not(feature = "embed"), allow(dead_code))]
    fn of_status(status: u16) -> Self {
        match status {
            400 | 413 | 422 => Self::Rejected,
            408 | 429 | 500..=599 => Self::Unreachable,
            _ => Self::BadReply,
        }
    }
}

/// Whether an `http://` URL's host is this machine: `localhost`, `127.x.x.x` or `[::1]`
/// (simp724's E1 on #1389).
fn loopback(url: &str) -> bool {
    let rest = url.trim_start_matches("http://");
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host = authority.rsplit('@').next().unwrap_or("");
    let host = if let Some(v6) = host.strip_prefix('[') {
        v6.split(']').next().unwrap_or("")
    } else {
        host.split(':').next().unwrap_or("")
    };
    host.eq_ignore_ascii_case("localhost")
        || host == "::1"
        || host
            .parse::<std::net::Ipv4Addr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// `data[0].embedding` of an OpenAI `/embeddings` response.
#[cfg_attr(not(feature = "embed"), allow(dead_code))]
fn parse(body: &str) -> Result<Vec<f32>, String> {
    let v: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("the response isn't JSON: {e}"))?;
    let Some(items) = v["data"][0]["embedding"].as_array() else {
        return Err("the response has no data[0].embedding".into());
    };
    if items.is_empty() {
        return Err("the response's embedding is empty".into());
    }
    items
        .iter()
        .map(|x| {
            x.as_f64()
                .map(|f| f as f32)
                .filter(|f| f.is_finite())
                .ok_or_else(|| "the response's embedding holds a non-number".to_owned())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An `https://` endpoint loads the system's roots (no request is made), and a URL of
    /// another scheme or an unset key variable is refused.
    #[cfg(feature = "embed")]
    #[test]
    fn endpoints_load_the_system_roots_and_refuse_bad_flags() {
        let e = Endpoint::new("https://example.invalid/v1/embeddings", "m", None, TIMEOUT).unwrap();
        assert_eq!(e.model(), "m");
        assert!(Endpoint::new("ftp://example.invalid/", "m", None, TIMEOUT).is_err());
        let unset = Endpoint::new(
            "http://127.0.0.1:1/v1/embeddings",
            "m",
            Some("PHDB_EMBED_TEST_KEY_NEVER_SET"),
            TIMEOUT,
        );
        assert!(unset.is_err());
    }

    /// A key never goes in cleartext across the network: plain `http://` with a key only to
    /// this machine (simp724's E1 on #1389).
    #[test]
    fn a_key_needs_https_or_loopback() {
        for (url, ok) in [
            ("http://localhost:11434/v1/embeddings", true),
            ("http://LOCALHOST/v1/embeddings", true),
            ("http://127.0.0.1:8080/v1/embeddings", true),
            ("http://127.1.2.3/v1/embeddings", true),
            ("http://[::1]:8080/v1/embeddings", true),
            ("http://user@localhost/v1", true),
            ("http://example.com/v1/embeddings", false),
            ("http://10.0.0.5:8080/v1/embeddings", false),
            ("http://localhost.example.com/v1", false),
            ("http://127.0.0.1@example.com/v1", false),
        ] {
            assert_eq!(loopback(url), ok, "{url}");
        }
        // The variable is set (PATH), so only the URL decides.
        let refused = Endpoint::new(
            "http://example.com/v1/embeddings",
            "m",
            Some("PATH"),
            TIMEOUT,
        );
        assert!(refused.is_err());
        assert!(
            Endpoint::new("http://example.com/v1/embeddings", "m", None, TIMEOUT).is_ok()
                == cfg!(feature = "embed")
        );
    }

    #[test]
    fn parse_takes_the_first_embedding_and_refuses_the_rest() {
        let ok = r#"{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.5,-1,2e-3]}],"model":"m"}"#;
        assert_eq!(parse(ok).unwrap(), vec![0.5, -1.0, 0.002]);
        for bad in [
            "nope",
            r#"{"data":[]}"#,
            r#"{"data":[{"embedding":[]}]}"#,
            r#"{"data":[{"embedding":[1,"x"]}]}"#,
            r#"{"error":{"message":"bad key"}}"#,
        ] {
            assert!(parse(bad).is_err(), "{bad}");
        }
    }
}
