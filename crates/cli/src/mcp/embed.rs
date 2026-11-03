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

/// The longest the server waits for one embedding.
#[cfg(feature = "embed")]
const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// The largest response body read.
#[cfg(feature = "embed")]
const MAX_RESPONSE_BYTES: u64 = 16 << 20;

/// An embedding endpoint.
pub(super) struct Endpoint {
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
    pub(super) fn new(url: &str, model: &str, key_env: Option<&str>) -> Result<Self, Failure> {
        if !(url.starts_with("https://") || url.starts_with("http://")) {
            return Err(Failure::usage(&format!(
                "--embed-url {url}: an http:// or https:// URL"
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
                .timeout_global(Some(TIMEOUT))
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
            let _ = (model, key_env);
            Err(Failure::unsupported(
                "--embed-url: this phdb was built without the `embed` feature".into(),
            ))
        }
    }

    /// The model name the endpoint is asked for.
    pub(super) fn model(&self) -> &str {
        &self.model
    }

    /// `text`'s embedding. A failure's message names the endpoint and what went wrong, never
    /// the key.
    pub(super) fn embed(&self, text: &str) -> Result<Vec<f32>, Failure> {
        #[cfg(feature = "embed")]
        {
            let fail = |m: String| Failure::input(format!("embedding endpoint {}: {m}", self.url));
            let body = serde_json::json!({"model": self.model, "input": text}).to_string();
            let mut req = self
                .agent
                .post(&self.url)
                .header("Content-Type", "application/json");
            if let Some(k) = &self.key {
                req = req.header("Authorization", &format!("Bearer {k}"));
            }
            let mut resp = req.send(body).map_err(|e| fail(e.to_string()))?;
            let status = resp.status().as_u16();
            let text = resp
                .body_mut()
                .with_config()
                .limit(MAX_RESPONSE_BYTES)
                .read_to_string()
                .map_err(|e| fail(format!("reading the response: {e}")))?;
            if status != 200 {
                let mut brief: String = text.chars().take(300).collect();
                if brief.len() < text.len() {
                    brief.push('…');
                }
                return Err(fail(format!("HTTP {status}: {brief}")));
            }
            parse(&text).map_err(fail)
        }
        #[cfg(not(feature = "embed"))]
        {
            let _ = text;
            unreachable!("an Endpoint is only made with the embed feature")
        }
    }
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
        let e = Endpoint::new("https://example.invalid/v1/embeddings", "m", None).unwrap();
        assert_eq!(e.model(), "m");
        assert!(Endpoint::new("ftp://example.invalid/", "m", None).is_err());
        let unset = Endpoint::new(
            "http://127.0.0.1:1/v1/embeddings",
            "m",
            Some("PHDB_EMBED_TEST_KEY_NEVER_SET"),
        );
        assert!(unset.is_err());
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
