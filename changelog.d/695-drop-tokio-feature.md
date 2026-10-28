### Removed
- **Breaking (before the 1.0 API freeze):** the `pigeonhole` crate's `tokio` feature. It gated no code: it only turned on `async`, which is on by default. A `Cargo.toml` that names it (`features = ["tokio"]`) fails to resolve; drop it, or name `async` instead. The async API runs on Tokio, smol, async-std or any executor, as before.
