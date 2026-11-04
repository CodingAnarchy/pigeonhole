//! Regenerates `crates/cli/src/serve/proto/` from `proto/pigeonhole/v1/*.proto` (D313,
//! ICR 0112): the prost messages, the tonic services and the pbjson serde impls (proto3's
//! JSON mapping, which `/v1` HTTP speaks). Run from anywhere in the repository; it finds
//! the root from its own manifest's path.

use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize()?;
    let proto_dir = root.join("proto");
    let out = root.join("crates/cli/src/serve/proto");
    std::fs::create_dir_all(&out)?;

    let mut files: Vec<PathBuf> = std::fs::read_dir(proto_dir.join("pigeonhole/v1"))?
        .map(|e| e.map(|e| e.path()))
        .collect::<Result<_, _>>()?;
    files.retain(|p| p.extension().is_some_and(|x| x == "proto"));
    files.sort();

    let fds = protox::compile(&files, [&proto_dir])?;
    let descriptor_bytes = prost::Message::encode_to_vec(&fds);

    // Messages and services. `bytes` fields are `bytes::Bytes`, so a request's large values
    // are slices of its buffer, not copies (perf287 on D313).
    let mut config = prost_build::Config::new();
    config.bytes(["."]);
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        // The transport (connect, serve) is chosen in `serve` itself, not generated here.
        .build_transport(false)
        .out_dir(&out)
        .compile_fds_with_config(fds, config)?;

    // proto3's canonical JSON mapping, for HTTP/JSON.
    pbjson_build::Builder::new()
        .register_descriptors(&descriptor_bytes)?
        .out_dir(&out)
        .preserve_proto_field_names()
        .build(&[".pigeonhole.v1"])?;

    println!("regenerated {}", out.display());
    Ok(())
}
