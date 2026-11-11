//! The contract's encodings (ICR 0112): binary protobuf for gRPC, and proto3's JSON mapping
//! for HTTP/JSON, round-tripping the shapes the ICR documents.

use super::proto::{
    self, BeginRequest, Durability, ErrorDetail, MutateRequest, ReturnedCell, RoutingKey, RowKey,
    RowWrites, Value, Write, returned_cell, routing_key, value, write,
};
use prost::Message;

fn mutation() -> MutateRequest {
    MutateRequest {
        writes: Some(RowWrites {
            row: Some(RowKey {
                table: "jobs".into(),
                row: b"job:\x00\xff".to_vec().into(),
            }),
            writes: vec![
                Write {
                    op: Some(write::Op::Put(proto::Put {
                        family: "state".into(),
                        qualifier: b"status".to_vec().into(),
                        timestamp: None,
                        value: Some(Value {
                            kind: Some(value::Kind::Bytes(b"queued".to_vec().into())),
                        }),
                    })),
                },
                Write {
                    op: Some(write::Op::Incr(proto::Incr {
                        family: "n".into(),
                        qualifier: b"runs".to_vec().into(),
                        timestamp: Some(1_700_000_000_000_000),
                        delta: -3,
                    })),
                },
            ],
        }),
        durability: Durability::Sync as i32,
    }
}

#[test]
fn messages_round_trip_through_protobuf_and_json() {
    let m = mutation();
    let decoded = MutateRequest::decode(m.encode_to_vec().as_slice()).unwrap();
    assert_eq!(decoded, m);
    let json = serde_json::to_value(&m).unwrap();
    let back: MutateRequest = serde_json::from_value(json).unwrap();
    assert_eq!(back, m);
}

#[test]
fn json_follows_the_documented_mapping() {
    let json = serde_json::to_value(mutation()).unwrap();
    let writes = &json["writes"];
    // Bytes are base64 (D313 Q3), including keys that aren't UTF-8.
    assert_eq!(writes["row"]["row"], "am9iOgD/");
    // Field names are the proto names; enums are their names.
    assert_eq!(json["durability"], "DURABILITY_SYNC");
    // 64-bit integers are strings, as proto3's JSON mapping says (JavaScript can't hold
    // every 64-bit value as a number).
    assert_eq!(writes["writes"][1]["incr"]["timestamp"], "1700000000000000");
    assert_eq!(writes["writes"][1]["incr"]["delta"], "-3");
    // A oneof is the set field's name.
    assert!(writes["writes"][0]["put"]["value"]["bytes"].is_string());
}

#[test]
fn json_input_takes_either_field_spelling() {
    // Proto names (snake_case) and the lowerCamelCase JSON names both parse.
    let snake: proto::GetRequest =
        serde_json::from_str(r#"{"table":"t","row":"cg==","snapshot_id":"s1"}"#).unwrap();
    let camel: proto::GetRequest =
        serde_json::from_str(r#"{"table":"t","row":"cg==","snapshotId":"s1"}"#).unwrap();
    assert_eq!(snake, camel);
    assert_eq!(snake.snapshot_id, "s1");
}

#[test]
fn begin_carries_a_routing_key_for_partitions() {
    // D317's C1: the key is in v1 already, so a single-node client sends it from the start.
    let begin = BeginRequest {
        durability: 0,
        route: Some(RoutingKey {
            key: Some(routing_key::Key::Row(RowKey {
                table: "acct".into(),
                row: b"alice".to_vec().into(),
            })),
        }),
    };
    let json = serde_json::to_value(&begin).unwrap();
    assert_eq!(json["route"]["row"]["table"], "acct");
    let by_partition: BeginRequest =
        serde_json::from_str(r#"{"route":{"partition":"p7"}}"#).unwrap();
    assert_eq!(
        by_partition.route.unwrap().key,
        Some(routing_key::Key::Partition("p7".into()))
    );
}

#[test]
fn a_returned_cells_error_is_a_value_not_a_failed_rpc() {
    // D314 / perf2's S1 on #1473: the commit succeeded; one cell's read after it failed.
    let cell = ReturnedCell {
        family: "n".into(),
        qualifier: b"c".to_vec().into(),
        timestamp: 0,
        value: Some(returned_cell::Value::Error(ErrorDetail {
            code: "Io".into(),
            code_num: 1,
            message: "read failed".into(),
            retry_safe: false,
            retry_after_ms: 0,
            retry_from: 0,
        })),
    };
    let json = serde_json::to_value(&cell).unwrap();
    assert_eq!(json["error"]["code"], "Io");
    // A field at its default is left out (proto3's JSON mapping): absent means false here,
    // and a client reads it so.
    assert!(json["error"].get("retry_safe").is_none());
    let back: ReturnedCell = serde_json::from_value(json).unwrap();
    assert_eq!(back, cell);
}

#[test]
fn every_service_is_generated_with_a_server_and_a_client() {
    // Naming each type is the check: the contract's services exist, server and client.
    fn exists<T>() {}
    exists::<proto::data_server::DataServer<()>>();
    exists::<proto::data_client::DataClient<()>>();
    exists::<proto::transactions_server::TransactionsServer<()>>();
    exists::<proto::snapshots_server::SnapshotsServer<()>>();
    exists::<proto::feed_server::FeedServer<()>>();
    exists::<proto::schema_server::SchemaServer<()>>();
    exists::<proto::graph_server::GraphServer<()>>();
    exists::<proto::vectors_server::VectorsServer<()>>();
    exists::<proto::text_server::TextServer<()>>();
    exists::<proto::memory_server::MemoryServer<()>>();
    exists::<proto::admin_server::AdminServer<()>>();
}

#[test]
fn a_conflict_says_to_retry_from_begin() {
    // simp724's E2 on #1496: `retry_safe` alone would let a client re-send `Commit`, which
    // the tombstone answers with the same `Conflict` forever.
    let conflict = ErrorDetail {
        code: "Conflict".into(),
        code_num: 13,
        message: "a read changed".into(),
        retry_safe: true,
        retry_after_ms: 0,
        retry_from: proto::RetryFrom::Begin as i32,
    };
    let json = serde_json::to_value(&conflict).unwrap();
    assert_eq!(json["retry_from"], "RETRY_FROM_BEGIN");
    assert_eq!(json["retry_safe"], true);
}

#[test]
fn a_quiet_stream_still_moves_its_token() {
    // perf287's S2 on #1496: a progress event carries only a token.
    let event = proto::FeedEvent {
        token: b"\x01\x02".to_vec().into(),
        event: Some(proto::feed_event::Event::Progress(proto::Progress {})),
        partition: String::new(),
    };
    let json = serde_json::to_value(&event).unwrap();
    assert_eq!(json["token"], "AQI=");
    assert!(json["progress"].is_object());
    let back: proto::FeedEvent = serde_json::from_value(json).unwrap();
    assert_eq!(back, event);
}

// ---- `--embed-local`'s startup advice (#1345 PR 2) ----

/// The line comes only when the shards are pinned, and names only the variables still unset:
/// the placement's CPU list for `CANDLE_CPU_MASK` and the embedder's thread count for
/// `CANDLE_NUM_THREADS` (cli692's N1 and N2 on #1671). `RAYON_NUM_THREADS` alone places
/// nothing, so it doesn't silence it.
#[test]
fn embed_local_advice_names_the_placements_cpus_until_both_are_set() {
    use std::ffi::OsString;
    let env =
        |set: &'static [&'static str]| move |k: &str| set.contains(&k).then(|| OsString::from("1"));
    let pinned = || Some("2-7,10".to_owned());
    let advice = |set: &'static [&'static str]| super::candle_advice(pinned(), 4, &env(set));

    let line = advice(&[]).expect("pinned, nothing set: advice");
    assert!(line.starts_with("phdb serve: --embed-local:"), "{line}");
    assert!(
        line.ends_with("export CANDLE_CPU_MASK=2-7,10 CANDLE_NUM_THREADS=4"),
        "{line}"
    );
    assert!(advice(&["RAYON_NUM_THREADS"]).is_some());
    // The mask alone leaves the pool a thread per core: the count is still asked for.
    let line = advice(&["CANDLE_CPU_MASK"]).expect("mask only: advice");
    assert!(line.ends_with("export CANDLE_NUM_THREADS=4"), "{line}");
    let line = advice(&["CANDLE_NUM_THREADS"]).expect("count only: advice");
    assert!(line.ends_with("export CANDLE_CPU_MASK=2-7,10"), "{line}");
    assert_eq!(advice(&["CANDLE_CPU_MASK", "CANDLE_NUM_THREADS"]), None);
    // No placement (`--unpin`, or nothing pinned): nothing to say.
    assert_eq!(super::candle_advice(None, 4, &env(&[])), None);
    assert_eq!(super::embed_threads(0), 1);
    assert_eq!(super::embed_threads(3), 3);
    assert_eq!(super::embed_threads(30), 4);
}

/// `phdb serve`'s open with these arguments (`--create`, and `--shards` when given): the
/// shard count it opened with.
fn served_shards(file: &std::path::Path, shards: Option<usize>) -> usize {
    use clap::Parser;
    let mut argv = vec![
        "phdb".to_owned(),
        "serve".to_owned(),
        file.display().to_string(),
        "--create".to_owned(),
    ];
    if let Some(n) = shards {
        argv.extend(["--shards".to_owned(), n.to_string()]);
    }
    let cli = crate::args::Cli::try_parse_from(argv).unwrap();
    let crate::args::Command::Serve(args) = &cli.command else {
        unreachable!("parsed as serve")
    };
    let db = super::open(&cli, args).unwrap();
    let n = db.info().unwrap().shards;
    db.close().unwrap();
    n
}

/// perf287's g70d on #1697: a restarted server without `--shards` keeps the server's default
/// shard count (half the physical cores), rather than taking the library's one per core,
/// since the count isn't stored in the file (ICR 0132).
#[test]
fn a_restart_without_shards_keeps_the_servers_default_count() {
    let dir = std::env::temp_dir().join(format!(
        "phdb-serve-shards-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("s.phdb");
    let default = super::placement::default_shards();
    assert_eq!(served_shards(&file, None), default, "created");
    assert_eq!(served_shards(&file, None), default, "restarted");
    // `--shards` reshards on purpose, and a restart without it goes back to the default.
    assert_eq!(served_shards(&file, Some(default + 1)), default + 1);
    assert_eq!(served_shards(&file, None), default);
    let _ = std::fs::remove_dir_all(&dir);
}
