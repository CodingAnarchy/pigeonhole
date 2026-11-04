//! The worker's side: a small synchronous HTTP client for the example's API.

use std::io::{BufRead, BufReader, Read};
use std::time::Duration;

use serde::Deserialize;
use serde_json::{Value, json};

/// A failed call.
#[derive(Debug)]
pub enum ClientError {
    /// The server answered with an error: its HTTP status and the body's stable `code`.
    Api {
        /// The HTTP status.
        status: u16,
        /// The body's `code`, for example `Claimed`, `Lost` or `Busy`.
        code: String,
        /// The body's message.
        message: String,
    },
    /// No answer: the connection failed, or the reply was lost. For a write, **the outcome
    /// is unknown**: it may have committed.
    Transport(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Api {
                status,
                code,
                message,
            } => write!(f, "{status} {code}: {message}"),
            Self::Transport(e) => write!(f, "transport: {e}"),
        }
    }
}

impl std::error::Error for ClientError {}

impl ClientError {
    /// Whether the server said, with its stable code, that this call wrote nothing and can
    /// be sent again as it is.
    pub fn retryable(&self) -> bool {
        matches!(self, Self::Api { code, .. } if code == "Busy" || code == "Conflict")
    }

    /// Whether a write's outcome is unknown: no answer, or the database's own
    /// `OutcomeUnknown`. Retry only an idempotent request (the transfer's `op_id`).
    pub fn outcome_unknown(&self) -> bool {
        match self {
            Self::Transport(_) => true,
            Self::Api { code, .. } => code == "OutcomeUnknown",
        }
    }
}

/// A claim's fencing token: pass it to [`Client::renew`] and [`Client::complete`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claim {
    /// The token the lease holds while this claim lasts.
    pub fence: String,
}

/// A job in a page of [`Client::jobs`].
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct JobSummary {
    /// The job's id.
    pub id: String,
    /// `queued` or `done`.
    pub status: Option<String>,
    /// The current claim's fencing token, if a claim is live.
    pub holder: Option<String>,
    /// The result, once done.
    pub result: Option<String>,
}

/// A page of jobs. A page cut by its budget can end inside a job's row: the next page then
/// starts with the same id and the rest of its columns.
#[derive(Debug, Clone, Deserialize)]
pub struct Page {
    /// The jobs, in key order.
    pub jobs: Vec<JobSummary>,
    /// Where the next page starts, or `None` after the last page.
    pub next: Option<String>,
}

/// One server-sent event from [`Client::events`].
#[derive(Debug, Clone)]
pub struct StreamedEvent {
    /// The event's id: pass it back to [`Client::events`] to resume after it.
    pub id: Option<String>,
    /// `commit`, or `error` for the stream's last event.
    pub event: String,
    /// The event's JSON data.
    pub data: Value,
}

/// A client for one server.
#[derive(Debug, Clone)]
pub struct Client {
    base: String,
    token: Option<String>,
    agent: ureq::Agent,
}

impl Client {
    /// A client for the server at `base` (for example `http://127.0.0.1:7070`), sending
    /// `token` as a bearer token if given.
    pub fn new(base: impl Into<String>, token: Option<String>) -> Self {
        let agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .timeout_global(Some(Duration::from_secs(30)))
            .build()
            .into();
        Self {
            base: base.into(),
            token,
            agent,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    fn post(&self, path: &str, body: &Value) -> Result<Value, ClientError> {
        let mut req = self.agent.post(self.url(path));
        if let Some(t) = &self.token {
            req = req.header("Authorization", format!("Bearer {t}"));
        }
        Self::answer(req.send_json(body))
    }

    fn get(&self, path: &str) -> Result<Value, ClientError> {
        let mut req = self.agent.get(self.url(path));
        if let Some(t) = &self.token {
            req = req.header("Authorization", format!("Bearer {t}"));
        }
        Self::answer(req.call())
    }

    /// A response as JSON (`null` for an empty body), or the server's error.
    fn answer(
        resp: Result<ureq::http::Response<ureq::Body>, ureq::Error>,
    ) -> Result<Value, ClientError> {
        let mut resp = resp.map_err(|e| ClientError::Transport(e.to_string()))?;
        let status = resp.status().as_u16();
        let mut text = String::new();
        resp.body_mut()
            .as_reader()
            .read_to_string(&mut text)
            .map_err(|e| ClientError::Transport(e.to_string()))?;
        let body: Value = if text.is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&text).unwrap_or(Value::Null)
        };
        if (200..300).contains(&status) {
            return Ok(body);
        }
        let field = |k: &str| {
            body.get(k)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        };
        let code = field("code");
        if code.is_empty() {
            // An error without the API's body came from something in between (a proxy, a
            // load balancer): it says nothing about whether a write committed.
            return Err(ClientError::Transport(format!(
                "HTTP {status} without the API's error body"
            )));
        }
        Err(ClientError::Api {
            status,
            code,
            message: field("message"),
        })
    }

    /// Queues a job. `Exists` if the id is taken.
    pub fn create_job(&self, id: &str, spec: &str) -> Result<(), ClientError> {
        self.post("/jobs", &json!({ "id": id, "spec": spec }))
            .map(drop)
    }

    /// One job, as JSON.
    pub fn job(&self, id: &str) -> Result<Value, ClientError> {
        self.get(&format!("/jobs/{id}"))
    }

    /// A page of jobs of at most `bytes` (D308's measure), after `after` (a previous page's
    /// `next`).
    pub fn jobs(&self, after: Option<&str>, bytes: Option<u64>) -> Result<Page, ClientError> {
        let mut path = String::from("/jobs?");
        if let Some(a) = after {
            path.push_str(&format!("after={a}&"));
        }
        if let Some(b) = bytes {
            path.push_str(&format!("bytes={b}"));
        }
        let v = self.get(&path)?;
        serde_json::from_value(v).map_err(|e| ClientError::Transport(e.to_string()))
    }

    /// Tries to claim a job: its fencing token, or `None` when another worker holds it or
    /// it's done.
    ///
    /// The claim carries a fresh random claim id, and a claim whose outcome is unknown (no
    /// reply) is sent again with the **same** claim id: if the first attempt landed, the server
    /// recognises its own token and hands it back, so a lost reply never leaves the job held
    /// by a claim nobody knows about (simp724's L1 on #1491).
    pub fn claim(&self, id: &str, worker: &str) -> Result<Option<Claim>, ClientError> {
        let claim_id = new_claim_id();
        let mut tried = 0;
        loop {
            tried += 1;
            match self.claim_with_id(id, worker, &claim_id) {
                Err(e) if e.outcome_unknown() && tried < 3 => continue,
                r => return r,
            }
        }
    }

    /// [`claim`](Self::claim) with a claim id the caller chose: the same claim id again is the same
    /// claim, answered with the same token if it holds the job.
    pub fn claim_with_id(
        &self,
        id: &str,
        worker: &str,
        claim_id: &str,
    ) -> Result<Option<Claim>, ClientError> {
        match self.post(
            &format!("/jobs/{id}/claim"),
            &json!({ "worker": worker, "claim_id": claim_id }),
        ) {
            Ok(v) => Ok(v.get("fence").and_then(Value::as_str).map(|f| Claim {
                fence: f.to_owned(),
            })),
            Err(ClientError::Api { code, .. }) if code == "Claimed" || code == "Done" => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Extends a claim. `false` when it was lost (expired, or taken by another worker).
    ///
    /// Renew at least every third of the TTL. Treat a renewal that fails **or gets no
    /// answer** (an `Err`) as lost too, and stop working on the job: a renewal whose outcome
    /// is unknown may not have extended the claim.
    pub fn renew(&self, id: &str, claim: &Claim) -> Result<bool, ClientError> {
        Self::fenced(self.post(
            &format!("/jobs/{id}/renew"),
            &json!({ "fence": claim.fence }),
        ))
    }

    /// Records a job's result and releases it. `false` when the claim was lost: the result
    /// wasn't recorded, and the job belongs to whoever holds it now.
    pub fn complete(&self, id: &str, claim: &Claim, result: &str) -> Result<bool, ClientError> {
        Self::fenced(self.post(
            &format!("/jobs/{id}/complete"),
            &json!({ "fence": claim.fence, "result": result }),
        ))
    }

    fn fenced(r: Result<Value, ClientError>) -> Result<bool, ClientError> {
        match r {
            Ok(_) => Ok(true),
            Err(ClientError::Api { code, .. }) if code == "Lost" => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Runs every queued job it can claim, one at a time, until a full pass over the jobs
    /// finds nothing left to claim. `work` turns a job's id into its result. Returns the
    /// ids this worker completed.
    pub fn work_until_idle(
        &self,
        worker: &str,
        mut work: impl FnMut(&str) -> String,
    ) -> Result<Vec<String>, ClientError> {
        let mut completed = Vec::new();
        loop {
            let mut claimed_any = false;
            let mut after: Option<String> = None;
            loop {
                let page = self.jobs(after.as_deref(), None)?;
                for job in page
                    .jobs
                    .iter()
                    .filter(|j| j.status.as_deref() == Some("queued") && j.holder.is_none())
                {
                    let Some(claim) = self.claim(&job.id, worker)? else {
                        continue;
                    };
                    claimed_any = true;
                    let result = work(&job.id);
                    if self.complete(&job.id, &claim, &result)? {
                        completed.push(job.id.clone());
                    }
                }
                match page.next {
                    Some(n) => after = Some(n),
                    None => break,
                }
            }
            if !claimed_any {
                return Ok(completed);
            }
        }
    }

    /// Opens an account with its first balance.
    pub fn open_account(&self, id: &str, balance: i64) -> Result<(), ClientError> {
        self.post(&format!("/accounts/{id}"), &json!({ "balance": balance }))
            .map(drop)
    }

    /// An account's balance.
    pub fn balance(&self, id: &str) -> Result<i64, ClientError> {
        let v = self.get(&format!("/accounts/{id}"))?;
        v.get("balance")
            .and_then(Value::as_i64)
            .ok_or_else(|| ClientError::Transport("no balance".into()))
    }

    /// Moves `amount` from `from` to `to`, once per `op_id`: `true` if this call applied it,
    /// `false` if an earlier call with the same `op_id` already had.
    ///
    /// Retries are safe because of the op id. A `Busy`, or an unknown outcome (no answer, or
    /// `OutcomeUnknown`), sends the same request again after a pause, up to `attempts`
    /// times. The server's `transact` already retried any `Conflict`.
    pub fn transfer(
        &self,
        op_id: &str,
        from: &str,
        to: &str,
        amount: i64,
        attempts: u32,
    ) -> Result<bool, ClientError> {
        let body = json!({ "op_id": op_id, "from": from, "to": to, "amount": amount });
        let mut pause = Duration::from_millis(10);
        let mut tried = 0;
        loop {
            tried += 1;
            match self.post("/transfer", &body) {
                Ok(v) => return Ok(v.get("applied").and_then(Value::as_bool).unwrap_or(false)),
                Err(e) if tried < attempts && (e.retryable() || e.outcome_unknown()) => {
                    std::thread::sleep(pause);
                    pause = (pause * 2).min(Duration::from_secs(1));
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// The commits to `jobs` as they happen, from after the event `after` (an earlier event's
    /// id) or from now. The iterator blocks for each event; it ends when the stream does.
    pub fn events(&self, after: Option<&str>) -> Result<Events, ClientError> {
        // A stream has no overall deadline.
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .into();
        let mut req = agent.get(self.url("/events"));
        if let Some(t) = &self.token {
            req = req.header("Authorization", format!("Bearer {t}"));
        }
        if let Some(a) = after {
            req = req.header("Last-Event-ID", a);
        }
        let resp = req
            .call()
            .map_err(|e| ClientError::Transport(e.to_string()))?;
        if !resp.status().is_success() {
            return Self::answer(Ok(resp)).map(|_| unreachable!("an error status"));
        }
        Ok(Events {
            lines: Box::new(BufReader::new(resp.into_body().into_reader())),
        })
    }
}

/// The events of [`Client::events`].
pub struct Events {
    lines: Box<dyn BufRead + Send>,
}

impl std::fmt::Debug for Events {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Events")
    }
}

impl Iterator for Events {
    type Item = Result<StreamedEvent, ClientError>;

    /// Reads one event: its `id:`, `event:` and `data:` lines up to the blank line that ends
    /// it. Comment lines (`:`, the server's keepalives) are skipped.
    fn next(&mut self) -> Option<Self::Item> {
        let (mut id, mut event, mut data) = (None, String::from("message"), String::new());
        let mut seen = false;
        loop {
            let mut line = String::new();
            match self.lines.read_line(&mut line) {
                Ok(0) => return None,
                Ok(_) => {}
                Err(e) => return Some(Err(ClientError::Transport(e.to_string()))),
            }
            let line = line.trim_end_matches(['\r', '\n']);
            if line.is_empty() {
                if !seen {
                    continue;
                }
                let data = serde_json::from_str(&data).unwrap_or(Value::Null);
                return Some(Ok(StreamedEvent { id, event, data }));
            }
            if line.starts_with(':') {
                continue;
            }
            let (field, value) = line
                .split_once(':')
                .map_or((line, ""), |(f, v)| (f, v.strip_prefix(' ').unwrap_or(v)));
            seen = true;
            match field {
                "id" => id = Some(value.to_owned()),
                "event" => event = value.to_owned(),
                "data" => {
                    if !data.is_empty() {
                        data.push('\n');
                    }
                    data.push_str(value);
                }
                _ => {}
            }
        }
    }
}

/// A random enough id for a claim: 128 bits from the standard library's randomly keyed
/// hasher, the time and a counter. An application would use a UUID v4 or its own random
/// source; this keeps the example free of another dependency.
fn new_claim_id() -> String {
    use std::hash::{BuildHasher, Hasher};
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let half = |salt: u64| {
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u128(now);
        h.write_u64(n);
        h.write_u64(salt);
        h.finish()
    };
    format!("{:016x}{:016x}", half(1), half(2))
}
