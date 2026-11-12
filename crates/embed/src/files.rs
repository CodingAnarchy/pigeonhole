//! Finding a model's files: a given directory, or the cache (filled by a download), each
//! file checked against its pinned SHA-256.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::model::{File, Spec};

/// `$PIGEONHOLE_EMBED_CACHE`: the cache's root, in place of the OS's.
pub(crate) const CACHE_ENV: &str = "PIGEONHOLE_EMBED_CACHE";
/// `PIGEONHOLE_EMBED_OFFLINE=1`: never download.
pub(crate) const OFFLINE_ENV: &str = "PIGEONHOLE_EMBED_OFFLINE";

/// Where a model's files come from.
pub(crate) enum Source<'a> {
    /// A directory holding them: never written to.
    Dir(&'a Path),
    /// The cache directory for the model, filled by a download unless `offline`.
    Cache { dir: PathBuf, offline: bool },
}

/// The cache's root: `explicit`, else `$PIGEONHOLE_EMBED_CACHE`, else the OS's cache
/// directory joined with `pigeonhole/models`. `None` if no home or cache directory is known.
pub(crate) fn cache_root(
    explicit: Option<&Path>,
    env: &dyn Fn(&str) -> Option<String>,
) -> Option<PathBuf> {
    if let Some(p) = explicit {
        return Some(p.to_path_buf());
    }
    let set = |k: &str| env(k).filter(|v| !v.is_empty()).map(PathBuf::from);
    if let Some(p) = set(CACHE_ENV) {
        return Some(p);
    }
    let os = if cfg!(windows) {
        set("LOCALAPPDATA")
    } else if cfg!(target_os = "macos") {
        set("HOME").map(|h| h.join("Library").join("Caches"))
    } else {
        set("XDG_CACHE_HOME").or_else(|| set("HOME").map(|h| h.join(".cache")))
    }?;
    Some(os.join("pigeonhole").join("models"))
}

/// Whether `PIGEONHOLE_EMBED_OFFLINE` asks for no downloads (`1`, `true` or `yes`).
pub(crate) fn offline_env(env: &dyn Fn(&str) -> Option<String>) -> bool {
    env(OFFLINE_ENV)
        .is_some_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
}

/// `file`'s bytes, checked against its pinned SHA-256: read from the source, downloaded into
/// the cache first if it's missing there and downloads are allowed.
pub(crate) fn load(spec: &Spec, file: &File, source: &Source<'_>) -> Result<Vec<u8>> {
    let (dir, may_download) = match source {
        Source::Dir(d) => (
            d.to_path_buf(),
            Err("LoadOptions::dir names the directory to load from"),
        ),
        Source::Cache { dir, offline: true } => (dir.clone(), Err("downloads are off (offline)")),
        Source::Cache {
            dir,
            offline: false,
        } => (dir.clone(), Ok(())),
    };
    let path = dir.join(file.name);
    match std::fs::read(&path) {
        Ok(bytes) => {
            check(&path, file, &bytes)?;
            Ok(bytes)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            if let Err(why) = may_download {
                return Err(Error::Missing {
                    path,
                    url: spec.url(file),
                    why,
                });
            }
            fetch(spec, file, &path)
        }
        Err(source) => Err(Error::Io { path, source }),
    }
}

fn hex(digest: &[u8]) -> String {
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// `Checksum` unless `bytes` hash to `file`'s pinned SHA-256.
pub(crate) fn check(path: &Path, file: &File, bytes: &[u8]) -> Result<()> {
    let actual = hex(&Sha256::digest(bytes));
    if actual == file.sha256 {
        Ok(())
    } else {
        Err(Error::Checksum {
            path: path.to_path_buf(),
            expected: file.sha256.to_owned(),
            actual,
        })
    }
}

#[cfg(feature = "download")]
/// Writes `bytes` to `path` atomically: a temporary file in the same directory, checked,
/// then renamed into place. A checksum mismatch deletes the temporary file.
pub(crate) fn store(path: &Path, file: &File, bytes: &[u8]) -> Result<()> {
    fn io(p: &Path) -> impl FnOnce(std::io::Error) -> Error + '_ {
        move |source| Error::Io {
            path: p.to_path_buf(),
            source,
        }
    }
    let dir = path.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(dir).map_err(io(dir))?;
    let tmp = dir.join(format!(".{}.{}.part", file.name, std::process::id()));
    check(path, file, bytes)?;
    std::fs::write(&tmp, bytes).map_err(io(&tmp))?;
    // Another process may have put the same verified file there first: the rename replaces
    // it with identical bytes.
    std::fs::rename(&tmp, path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        io(path)(e)
    })
}

#[cfg(feature = "download")]
fn fetch(spec: &Spec, file: &File, path: &Path) -> Result<Vec<u8>> {
    let url = spec.url(file);
    let bytes = download::get(&url)?;
    store(path, file, &bytes).map_err(|e| match e {
        // Say it was the download that didn't match, and keep nothing.
        Error::Checksum {
            expected, actual, ..
        } => Error::Download {
            url: url.clone(),
            message: format!("SHA-256 {actual}, expected {expected}: nothing was kept"),
        },
        e => e,
    })?;
    Ok(bytes)
}

#[cfg(not(feature = "download"))]
fn fetch(spec: &Spec, file: &File, path: &Path) -> Result<Vec<u8>> {
    Err(Error::Missing {
        path: path.to_path_buf(),
        url: spec.url(file),
        why: "this build has no `download` feature",
    })
}

#[cfg(feature = "download")]
mod download {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::error::{Error, Result};
    use std::io::Read;

    /// Redirects followed at most: huggingface.co's `resolve` URL to its CDN, and a relative
    /// one inside the hub.
    const MAX_REDIRECTS: usize = 5;
    /// The largest file accepted (the weights are 133 MB).
    const MAX_BYTES: u64 = 1 << 30;
    /// How long a download may go without receiving anything before it fails.
    const STALL: Duration = Duration::from_secs(60);

    /// Whether `host` is Hugging Face's: `huggingface.co`, `hf.co`, or a subdomain of either.
    pub(super) fn trusted(host: &str) -> bool {
        let host = host.to_ascii_lowercase();
        ["huggingface.co", "hf.co"]
            .iter()
            .any(|d| host == *d || host.ends_with(&format!(".{d}")))
    }

    fn agent(url: &str) -> Result<ureq::Agent> {
        use ureq::tls::{Certificate, RootCerts, TlsConfig, TlsProvider};
        let fail = |message: String| Error::Download {
            url: url.to_owned(),
            message,
        };
        let found = rustls_native_certs::load_native_certs();
        if found.certs.is_empty() {
            let why = found
                .errors
                .first()
                .map(|e| format!(" ({e})"))
                .unwrap_or_default();
            return Err(fail(format!(
                "no root certificates in the system's trust store{why}"
            )));
        }
        let roots: Vec<Certificate<'static>> = found
            .certs
            .iter()
            .map(|c| Certificate::from_der(c.as_ref()).to_owned())
            .collect();
        // The ring provider for this agent only: a library doesn't install a process default.
        let tls = TlsConfig::builder()
            .provider(TlsProvider::Rustls)
            .unversioned_rustls_crypto_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .root_certs(RootCerts::from(roots))
            .build();
        Ok(ureq::Agent::config_builder()
            .timeout_connect(Some(Duration::from_secs(30)))
            // A stall, not a total (the audit's S-B3): ureq's body timeout is for the whole
            // body, and the weights are 133 MB, so a link under about 18 Mbit/s could never
            // finish. Each read may wait `STALL`; `MAX_BYTES` bounds the size.
            .timeout_per_read(Some(STALL))
            // Redirects are followed here, each checked against `trusted`.
            .max_redirects(0)
            .http_status_as_error(false)
            .tls_config(tls)
            .build()
            .into())
    }

    /// The body at `url`, following redirects only to Hugging Face's hosts.
    pub(super) fn get(url: &str) -> Result<Vec<u8>> {
        let agent = agent(url)?;
        let fail = |message: String| Error::Download {
            url: url.to_owned(),
            message,
        };
        let mut at = url.to_owned();
        for _ in 0..=MAX_REDIRECTS {
            let uri: ureq::http::Uri = at.parse().map_err(|e| fail(format!("{at}: {e}")))?;
            if uri.scheme_str() != Some("https") || !uri.host().is_some_and(trusted) {
                return Err(fail(format!(
                    "refusing a redirect off huggingface.co, to {at}"
                )));
            }
            let resp = agent.get(&at).call().map_err(|e| fail(e.to_string()))?;
            let status = resp.status();
            if status.is_redirection() {
                let next = resp
                    .headers()
                    .get("location")
                    .and_then(|l| l.to_str().ok())
                    .ok_or_else(|| fail(format!("a {status} with no Location")))?;
                at = if next.starts_with('/') {
                    format!(
                        "{}://{}{next}",
                        uri.scheme_str().unwrap_or("https"),
                        uri.host().unwrap_or_default()
                    )
                } else {
                    next.to_owned()
                };
                continue;
            }
            if !status.is_success() {
                return Err(fail(format!("HTTP {status}")));
            }
            let mut body = Vec::new();
            resp.into_body()
                .into_reader()
                .take(MAX_BYTES + 1)
                .read_to_end(&mut body)
                .map_err(|e| fail(e.to_string()))?;
            if body.len() as u64 > MAX_BYTES {
                return Err(fail(format!("larger than {MAX_BYTES} bytes")));
            }
            return Ok(body);
        }
        Err(fail(format!("more than {MAX_REDIRECTS} redirects")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Model;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let pairs: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k| pairs.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone())
    }

    #[test]
    fn the_cache_root_is_explicit_then_the_variable_then_the_os_cache() {
        let e = env(&[
            (CACHE_ENV, "/c"),
            ("HOME", "/h"),
            ("XDG_CACHE_HOME", "/x"),
            ("LOCALAPPDATA", "/l"),
        ]);
        assert_eq!(
            cache_root(Some(Path::new("/e")), &e),
            Some(PathBuf::from("/e"))
        );
        assert_eq!(cache_root(None, &e), Some(PathBuf::from("/c")));
        let e = env(&[
            (CACHE_ENV, ""),
            ("HOME", "/h"),
            ("XDG_CACHE_HOME", "/x"),
            ("LOCALAPPDATA", "/l"),
        ]);
        let want = if cfg!(windows) {
            PathBuf::from("/l")
        } else if cfg!(target_os = "macos") {
            PathBuf::from("/h/Library/Caches")
        } else {
            PathBuf::from("/x")
        };
        assert_eq!(
            cache_root(None, &e),
            Some(want.join("pigeonhole").join("models"))
        );
        if !cfg!(windows) && !cfg!(target_os = "macos") {
            let e = env(&[("HOME", "/h")]);
            assert_eq!(
                cache_root(None, &e),
                Some(PathBuf::from("/h/.cache/pigeonhole/models"))
            );
        }
        assert_eq!(cache_root(None, &env(&[])), None);
    }

    #[test]
    fn offline_takes_1_true_or_yes() {
        for v in ["1", "true", "YES", " yes "] {
            assert!(offline_env(&env(&[(OFFLINE_ENV, v)])), "{v}");
        }
        for v in ["", "0", "no", "false"] {
            assert!(!offline_env(&env(&[(OFFLINE_ENV, v)])), "{v}");
        }
        assert!(!offline_env(&env(&[])));
    }

    fn scratch(name: &str) -> PathBuf {
        let d =
            std::env::temp_dir().join(format!("pigeonhole-embed-{name}-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&d);
        d
    }

    #[test]
    fn a_missing_file_offline_or_in_a_dir_says_where_to_fetch_it() {
        let spec = Model::BgeSmallEnV15.spec();
        let dir = scratch("missing");
        let f = &spec.files[crate::model::CONFIG];
        for source in [
            Source::Dir(&dir),
            Source::Cache {
                dir: dir.clone(),
                offline: true,
            },
        ] {
            let e = load(spec, f, &source);
            assert_eq!(
                e.as_ref().map_err(Error::code).err(),
                Some(pigeonhole::ErrorCode::Io)
            );
            match e {
                Err(Error::Missing { path, url, .. }) => {
                    assert_eq!(path, dir.join("config.json"));
                    assert_eq!(
                        url,
                        "https://huggingface.co/BAAI/bge-small-en-v1.5/resolve/\
                         5c38ec7c405ec4b44b94cc5a9bb96e735b38267a/config.json"
                    );
                }
                other => panic!("{other:?}"),
            }
        }
    }

    #[test]
    fn a_file_that_fails_its_checksum_is_refused_and_left_alone() {
        let spec = Model::BgeSmallEnV15.spec();
        let dir = scratch("checksum");
        let path = dir.join("config.json");
        std::fs::write(&path, b"{}").unwrap();
        let e = load(spec, &spec.files[crate::model::CONFIG], &Source::Dir(&dir));
        let code = e.as_ref().map_err(Error::code).err();
        assert_eq!(code, Some(pigeonhole::ErrorCode::Corruption));
        match e {
            Err(Error::Checksum {
                expected, actual, ..
            }) => {
                assert_eq!(expected, spec.files[crate::model::CONFIG].sha256);
                assert_eq!(
                    actual,
                    "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
                );
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(std::fs::read(&path).unwrap(), b"{}");
    }

    #[cfg(feature = "download")]
    #[test]
    fn store_writes_only_verified_bytes_and_leaves_no_temporary_file() {
        let file = File {
            name: "x.bin",
            sha256: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
        };
        let dir = scratch("store").join("sub");
        let path = dir.join("x.bin");
        assert!(matches!(
            store(&path, &file, b"nope"),
            Err(Error::Checksum { .. })
        ));
        assert!(!path.exists());
        store(&path, &file, b"hello").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"hello");
        let left: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(left, ["x.bin"]);
    }

    #[cfg(feature = "download")]
    #[test]
    fn only_hugging_face_hosts_are_trusted() {
        for h in [
            "huggingface.co",
            "cdn-lfs.huggingface.co",
            "us.aws.cdn.hf.co",
            "HF.CO",
        ] {
            assert!(download::trusted(h), "{h}");
        }
        for h in [
            "evilhuggingface.co",
            "huggingface.co.evil.com",
            "hf.com",
            "example.org",
        ] {
            assert!(!download::trusted(h), "{h}");
        }
    }
}
