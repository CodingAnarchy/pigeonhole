//! `phdb promote`: turns a replica's file (`phdb serve --replica-of`'s, #1436) into a
//! primary, offline: it then takes writes, at the last of the primary's commits it applied.
//! A running replica server is promoted with `Admin.Promote` instead.

use std::path::Path;

use serde_json::json;

use crate::ExitStatus;
use crate::args::Cli;
use crate::exit::Failure;
use crate::open;
use crate::output::{Column, Report};

pub(crate) fn run(cli: &Cli, file: &Path) -> Result<Report, Failure> {
    let db = open::for_write(cli, file, false)?;
    let promoted = match db.replicated_seqno() {
        Ok(Some(applied)) => db.promote().map(|token| (applied, token)),
        Ok(None) => {
            let _ = db.close();
            return Err(Failure::usage(&format!(
                "{} isn't a replica: there is nothing to promote",
                file.display()
            )));
        }
        Err(e) => Err(e),
    }
    .map_err(|e| Failure::db("promote", &e));
    let closed = db.close().map_err(|e| Failure::db("close", &e));
    let (applied, token) = promoted?;
    closed?;
    let token = token.map(|t| super::schema::base64_encode(t.as_bytes()));
    let summary = vec![
        ("path", json!(file.display().to_string())),
        ("applied_primary_seqno", json!(applied)),
        ("primary_token", json!(token)),
    ];
    let document = summary
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect();
    Ok(Report {
        command: "promote",
        summary,
        columns: Vec::<Column>::new(),
        rows: Vec::new(),
        document,
        status: ExitStatus::SUCCESS,
    })
}
