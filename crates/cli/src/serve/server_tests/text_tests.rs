use super::*;

// ---- Text (D313 ticket 7c, D309, ICR 0105) ----

fn text_hits(r: &Json) -> Vec<String> {
    r["hits"]
        .as_array()
        .map(|h| {
            h.iter()
                .map(|h| h["id"].as_str().unwrap().to_owned())
                .collect()
        })
        .unwrap_or_default()
}

#[test]
fn a_text_index_is_created_written_searched_rebuilt_and_verified() {
    let s = Server::start("", limits());
    let spec = json!({"name": "tx", "source_table": "t", "source_family": "f",
                      "source_qualifier": b64(b"body"), "stop_words": true});
    let (status, e) = s.rpc("Text/CreateIndex", &spec, None);
    assert_eq!(status, 200, "{e}");
    let (_, e) = s.rpc("Text/CreateIndex", &spec, None);
    assert_eq!(e["code"].as_str(), Some("TableExists"), "{e}");
    let mut again = spec.clone();
    again["if_missing"] = json!(true);
    assert_eq!(s.rpc("Text/CreateIndex", &again, None).0, 200);
    for (id, text) in [
        ("doc:1", "We chose Postgres in the March meeting"),
        ("doc:2", "The meeting moved to April"),
        ("note:3", "State of the Art"),
    ] {
        let (status, e) = s.rpc(
            "Text/Put",
            &json!({"index": "tx", "id": b64(id.as_bytes()), "text": text}),
            None,
        );
        assert_eq!(status, 200, "{id}: {e}");
    }
    // The source column holds the text: Put writes through the index.
    let (_, r) = s.post(
        "Get",
        &json!({"table": "t", "row": b64(b"doc:2"), "family": "f",
                                       "qualifier": b64(b"body")}),
        None,
    );
    assert!(r["cell"].is_object(), "{r}");
    // The syntax.
    let (status, r) = s.rpc(
        "Text/Search",
        &json!({"index": "tx", "syntax": "postgres \"march meeting\" meeting", "k": 10}),
        None,
    );
    assert_eq!(status, 200, "{r}");
    assert_eq!(text_hits(&r), [b64(b"doc:1"), b64(b"doc:2")]);
    // Clauses, by field name: `term` as given (so "March" misses a lowercased index), `text`
    // and a phrase's text tokenised (so "March Meeting" matches), with highlights whose
    // clause indexes are the request's.
    let (status, r) = s.rpc(
        "Text/Search",
        &json!({"index": "tx", "k": 10, "highlight": true, "clauses": {"clauses": [
            {"term": "March"},
            {"phrase": {"text": "March Meeting"}},
            {"fuzzy": {"term": "postgress", "fuzziness": "FUZZINESS_AUTO"}},
            {"text": "April"},
        ]}}),
        None,
    );
    assert_eq!(status, 200, "{r}");
    assert_eq!(text_hits(&r), [b64(b"doc:1"), b64(b"doc:2")]);
    let top = &r["hits"][0];
    assert_eq!(
        top["text"].as_str(),
        Some("We chose Postgres in the March meeting")
    );
    let clauses: Vec<u64> = top["matches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["clause"].as_u64().unwrap_or(0))
        .collect();
    assert!(
        clauses.contains(&1) && clauses.contains(&2) && !clauses.contains(&0),
        "{top}"
    );
    let phrase = top["matches"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["clause"].as_u64() == Some(1))
        .unwrap();
    let (start, end) = (
        phrase["start"].as_str().unwrap().parse::<usize>().unwrap(),
        phrase["end"].as_str().unwrap().parse::<usize>().unwrap(),
    );
    assert_eq!(
        &"We chose Postgres in the March meeting"[start..end],
        "March meeting"
    );
    // A phrase of only stop words, or of nothing, matches nothing.
    for text in ["the", ""] {
        let (status, r) = s.rpc(
            "Text/Search",
            &json!({"index": "tx", "k": 10,
                    "clauses": {"clauses": [{"phrase": {"text": text}}]}}),
            None,
        );
        assert_eq!(status, 200, "{text:?}: {r}");
        assert!(text_hits(&r).is_empty(), "{text:?}: {r}");
    }
    // Only ids under a prefix.
    let (_, r) = s.rpc(
        "Text/Search",
        &json!({"index": "tx", "syntax": "state meeting", "k": 10, "id_prefix": b64(b"note:")}),
        None,
    );
    assert_eq!(text_hits(&r), [b64(b"note:3")]);
    // Refused: a repeated clause (it would shift the clauses' indexes), k out of range, a
    // bad query, a slop past 65,535, no query, an index that doesn't exist.
    for (req, code) in [
        (
            json!({"index": "tx", "k": 1, "clauses": {"clauses": [{"term": "a"}, {"text": "b"}, {"term": "a"}]}}),
            "InvalidArgument",
        ),
        (
            json!({"index": "tx", "k": 0, "syntax": "a"}),
            "InvalidArgument",
        ),
        (
            json!({"index": "tx", "k": 10_001, "syntax": "a"}),
            "InvalidArgument",
        ),
        (
            json!({"index": "tx", "k": 1, "syntax": "\"unclosed"}),
            "InvalidArgument",
        ),
        (
            json!({"index": "tx", "k": 1, "clauses": {"clauses": [{"phrase": {"text": "a b", "slop": 70_000}}]}}),
            "InvalidArgument",
        ),
        (json!({"index": "tx", "k": 1}), "InvalidArgument"),
        (
            json!({"index": "nope", "k": 1, "syntax": "a"}),
            "TableNotFound",
        ),
    ] {
        let (status, e) = s.rpc("Text/Search", &req, None);
        assert_eq!(e["code"].as_str(), Some(code), "{req}: {status} {e}");
    }
    // A delete, then the index's own check and a rebuild that changes nothing.
    assert_eq!(
        s.rpc(
            "Text/Delete",
            &json!({"index": "tx", "id": b64(b"doc:2")}),
            None
        )
        .0,
        200
    );
    let (_, r) = s.rpc(
        "Text/Search",
        &json!({"index": "tx", "syntax": "meeting", "k": 10}),
        None,
    );
    assert_eq!(text_hits(&r), [b64(b"doc:1")]);
    let (status, v) = s.rpc("Text/Verify", &json!({"index": "tx"}), None);
    assert_eq!(status, 200, "{v}");
    assert_eq!(v["documents"], json!("2"), "{v}");
    assert!(v.get("forward_mismatch").is_none_or(|m| m == "0"), "{v}");
    assert!(v.get("source_mismatch").is_none_or(|m| m == "0"), "{v}");
    let (status, r) = s.rpc("Text/Rebuild", &json!({"index": "tx"}), None);
    assert_eq!(status, 200, "{r}");
    assert_eq!(r["documents"], json!("2"), "{r}");
    assert!(r.get("changed").is_none_or(|c| c == "0"), "{r}");
    // A catalog change on a replica drops the text handles with the rest (#1436); the next
    // call opens the index again by name.
    let service = &s.shared.router.service;
    assert!(service.texts.cached("tx").is_some());
    service.tables_changed();
    assert!(service.texts.cached("tx").is_none());
    let (_, r) = s.rpc(
        "Text/Search",
        &json!({"index": "tx", "syntax": "meeting", "k": 10}),
        None,
    );
    assert_eq!(text_hits(&r), [b64(b"doc:1")]);
    // Shutdown's close waits for a rebuild, and refuses one after it (ai1249's S1).
    let texts = &s.shared.router.service.texts;
    texts.close();
    let (_, e) = s.rpc("Text/Rebuild", &json!({"index": "tx"}), None);
    assert_eq!(e["code"].as_str(), Some("Closed"), "{e}");
    // Over gRPC.
    let hits = s.block_on(async {
        let channel = tonic::transport::Channel::from_shared(s.url.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        proto::text_client::TextClient::new(channel)
            .search(proto::TextSearchRequest {
                index: "tx".into(),
                k: 5,
                query: Some(proto::text_search_request::Query::Syntax("postgres".into())),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner()
            .hits
    });
    assert_eq!(hits.len(), 1);
    assert_eq!(&hits[0].id[..], b"doc:1");
}
