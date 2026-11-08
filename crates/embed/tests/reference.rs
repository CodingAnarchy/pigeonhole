//! The local model against the reference (D318 §7): `fixture.py --reference` embeds
//! `REFERENCE_TEXTS` with sentence-transformers (PyTorch) at the pinned revision, and this
//! checks `LocalEmbedder` gives the same vectors, as passages (in one batch, so padding is
//! exercised) and as queries.
//!
//! It needs the model's files: set `PIGEONHOLE_EMBED_MODEL_DIR` to a directory holding them
//! (CI restores them from its cache). Unset, the test says so and passes.

use pigeonhole_embed::{LocalEmbedder, Model};

/// The least cosine between a vector and its reference.
const MIN_COSINE: f64 = 0.9999;
/// The largest difference in any one component (f32 summation order differs between candle
/// and PyTorch).
const MAX_COMPONENT: f32 = 1e-4;

fn reference() -> serde_json::Value {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/reference.json");
    serde_json::from_slice(&std::fs::read(path).expect("tests/reference.json")).unwrap()
}

fn floats(v: &serde_json::Value) -> Vec<f32> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_f64().unwrap() as f32)
        .collect()
}

fn compare(what: &str, got: &[f32], want: &[f32]) {
    assert_eq!(got.len(), want.len(), "{what}: dimension");
    let dot: f64 = got
        .iter()
        .zip(want)
        .map(|(a, b)| f64::from(*a) * f64::from(*b))
        .sum();
    let norm = |v: &[f32]| v.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>().sqrt();
    let cos = dot / (norm(got) * norm(want));
    let worst = got
        .iter()
        .zip(want)
        .map(|(a, b)| (a - b).abs())
        .fold(0.0f32, f32::max);
    assert!(
        cos >= MIN_COSINE && worst <= MAX_COMPONENT,
        "{what}: cosine {cos}, largest component difference {worst}"
    );
    assert!((norm(got) - 1.0).abs() < 1e-5, "{what}: not unit length");
}

#[test]
fn the_local_model_matches_the_reference_vectors() {
    let Some(dir) = std::env::var_os("PIGEONHOLE_EMBED_MODEL_DIR") else {
        eprintln!("PIGEONHOLE_EMBED_MODEL_DIR is unset: skipping the reference comparison");
        return;
    };
    let r = reference();
    let model = Model::BgeSmallEnV15;
    assert_eq!(r["revision"], model.revision());
    let e = LocalEmbedder::open_dir(model, dir).unwrap();
    let texts = r["texts"].as_array().unwrap();
    let all: Vec<&str> = texts.iter().map(|t| t["text"].as_str().unwrap()).collect();
    let passages = e.embed_passages(&all).unwrap();
    assert_eq!(passages.len(), all.len());
    for (t, got) in texts.iter().zip(&passages) {
        let text = t["text"].as_str().unwrap();
        let short: String = text.chars().take(40).collect();
        compare(&format!("passage {short:?}"), got, &floats(&t["passage"]));
        let q = e.embed_query(text).unwrap();
        compare(&format!("query {short:?}"), &q, &floats(&t["query"]));
    }
    // Embedding alone gives what the batch gave.
    for (text, batched) in all.iter().zip(&passages) {
        compare(
            "alone vs batched",
            &e.embed_passages(&[text]).unwrap()[0],
            batched,
        );
    }
    let m = e.embedding_model();
    assert_eq!((m.id(), m.dim()), ("BAAI/bge-small-en-v1.5@5c38ec7c", 384));
}
