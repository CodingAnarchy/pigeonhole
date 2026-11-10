//! The download path against huggingface.co. It needs the network, so it's `#[ignore]`d:
//! CI's reference job runs it with `--ignored` to fill its model cache on a miss
//! (`PIGEONHOLE_EMBED_CACHE`), then points the reference test at the filled directory.

#![cfg(feature = "download")]

use pigeonhole_embed::{LoadOptions, LocalEmbedder, Model};

#[test]
#[ignore = "downloads the model (133 MB) from huggingface.co"]
fn a_load_fills_the_cache_and_a_second_load_is_offline() {
    let root = std::env::var_os("PIGEONHOLE_EMBED_CACHE")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!("pigeonhole-embed-dl-{}", std::process::id()))
        });
    let model = Model::BgeSmallEnV15;
    let e = LocalEmbedder::open(model, &LoadOptions::new().cache_dir(&root)).unwrap();
    let dir = root.join(format!("bge-small-en-v1.5@{}", model.revision()));
    for f in ["model.safetensors", "tokenizer.json", "config.json"] {
        assert!(dir.join(f).is_file(), "{f}");
    }
    // Nothing but the three files: no temporary file is left behind.
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 3);
    let v = e.embed_query("hello").unwrap();
    // Offline, from the cache it filled, the same vector.
    let again = LocalEmbedder::open(model, &LoadOptions::new().cache_dir(&root).offline()).unwrap();
    assert_eq!(again.embed_query("hello").unwrap(), v);
    println!("cached in {}", dir.display());
}
