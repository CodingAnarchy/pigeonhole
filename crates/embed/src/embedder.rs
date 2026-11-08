//! [`LocalEmbedder`]: a model loaded once, embedding on a thread pool of its own.

use std::path::{Path, PathBuf};

use candle_core::{DType, Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::bert::{BertModel, Config};
use pigeonhole_memory::EmbeddingModel;
use tokenizers::{PaddingParams, PaddingStrategy, Tokenizer, TruncationParams};

use crate::error::{Error, Result, model as candle};
use crate::files::{self, Source};
use crate::model::{CONFIG, Model, Spec, TOKENIZER, WEIGHTS};

/// Texts embedded in one forward pass at most.
const BATCH: usize = 32;

/// How [`LocalEmbedder::open`] finds the model's files and runs it.
#[derive(Debug, Clone, Default)]
pub struct LoadOptions {
    dir: Option<PathBuf>,
    cache: Option<PathBuf>,
    offline: bool,
    threads: Option<usize>,
}

impl LoadOptions {
    /// The defaults: the cache, downloading what's missing, on `min(available parallelism,
    /// 4)` threads.
    pub fn new() -> Self {
        Self::default()
    }

    /// Loads from `dir`, which holds the model's three files, and never downloads or writes:
    /// for an air-gapped install. The files are still checked against their pinned SHA-256s.
    pub fn dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.dir = Some(dir.into());
        self
    }

    /// The cache's root, in place of `$PIGEONHOLE_EMBED_CACHE` and the OS's cache directory.
    /// The model's files go in `<root>/<model>@<revision>/`.
    pub fn cache_dir(mut self, root: impl Into<PathBuf>) -> Self {
        self.cache = Some(root.into());
        self
    }

    /// Never touches the network: a file missing from the cache is
    /// [`Error::Missing`](crate::Error::Missing). `PIGEONHOLE_EMBED_OFFLINE=1` does the same.
    pub fn offline(mut self) -> Self {
        self.offline = true;
        self
    }

    /// The embedder's threads (at least 1). Unset: `min(available parallelism, 4)`.
    pub fn threads(mut self, n: usize) -> Self {
        self.threads = Some(n.max(1));
        self
    }
}

/// A model loaded in this process, embedding text on the CPU.
///
/// It's `Send` and `Sync`: share one across threads, which take turns on its thread pool.
pub struct LocalEmbedder {
    model: Model,
    spec: &'static Spec,
    bert: BertModel,
    tokenizer: Tokenizer,
    pool: rayon::ThreadPool,
}

impl std::fmt::Debug for LocalEmbedder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalEmbedder")
            .field("model", &self.model)
            .field("threads", &self.pool.current_num_threads())
            .finish_non_exhaustive()
    }
}

impl LocalEmbedder {
    /// Loads `model`: its files from [`LoadOptions::dir`] or the cache (downloaded once if
    /// missing and allowed), each checked against its pinned SHA-256, then the weights.
    ///
    /// Errors: [`Missing`](Error::Missing) for a file that isn't there and may not be
    /// downloaded, [`Checksum`](Error::Checksum) for one that isn't the pinned file,
    /// [`Download`](Error::Download), [`Io`](Error::Io), and [`Model`](Error::Model) or
    /// [`Tokenizer`](Error::Tokenizer) if the files don't load.
    pub fn open(model: Model, options: &LoadOptions) -> Result<Self> {
        let env = |k: &str| std::env::var(k).ok();
        let spec = model.spec();
        let cache;
        let source = match &options.dir {
            Some(d) => Source::Dir(d),
            None => {
                let root =
                    files::cache_root(options.cache.as_deref(), &env).ok_or_else(|| Error::Io {
                        path: PathBuf::from("$HOME"),
                        source: std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "no cache directory: set PIGEONHOLE_EMBED_CACHE, or use \
                             LoadOptions::cache_dir or LoadOptions::dir",
                        ),
                    })?;
                cache = root.join(spec.dir);
                Source::Cache {
                    dir: cache,
                    offline: options.offline || files::offline_env(&env),
                }
            }
        };
        let config = files::load(spec, &spec.files[CONFIG], &source)?;
        let tokenizer = files::load(spec, &spec.files[TOKENIZER], &source)?;
        let weights = files::load(spec, &spec.files[WEIGHTS], &source)?;

        let config: Config = serde_json::from_slice(&config)
            .map_err(|e| Error::Model(format!("config.json: {e}")))?;
        let mut tokenizer =
            Tokenizer::from_bytes(&tokenizer).map_err(|e| Error::Tokenizer(e.to_string()))?;
        tokenizer
            .with_truncation(Some(TruncationParams {
                max_length: spec.max_tokens,
                ..TruncationParams::default()
            }))
            .map_err(|e| Error::Tokenizer(e.to_string()))?;
        tokenizer.with_padding(Some(PaddingParams {
            strategy: PaddingStrategy::BatchLongest,
            ..PaddingParams::default()
        }));
        let vb = VarBuilder::from_buffered_safetensors(weights, DType::F32, &Device::Cpu)
            .map_err(candle)?;
        let bert = BertModel::load(vb, &config).map_err(candle)?;

        let threads = options
            .threads
            .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get().min(4)));
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|i| format!("pigeonhole-embed-{i}"))
            .build()
            .map_err(|e| Error::Threads(e.to_string()))?;
        Ok(Self {
            model,
            spec,
            bert,
            tokenizer,
            pool,
        })
    }

    /// [`open`](Self::open) from `dir` only: [`LoadOptions::new().dir(dir)`](LoadOptions::dir).
    pub fn open_dir(model: Model, dir: impl AsRef<Path>) -> Result<Self> {
        Self::open(model, &LoadOptions::new().dir(dir.as_ref()))
    }

    /// The model.
    pub fn model(&self) -> Model {
        self.model
    }

    /// The record a store pins at create: the model's [`id`](Model::id) (with its revision),
    /// its dimension, and `F32`.
    pub fn embedding_model(&self) -> EmbeddingModel {
        EmbeddingModel::new(self.spec.id, self.spec.dim, pigeonhole::Element::F32)
            .expect("an f32 model is a valid EmbeddingModel")
    }

    /// The embeddings of texts to remember: one unit-length vector each, in order. Text
    /// longer than the model's limit (512 tokens) is truncated.
    pub fn embed_passages(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        let mut out = Vec::with_capacity(texts.len());
        for chunk in texts.chunks(BATCH) {
            out.extend(self.embed(chunk.iter().map(|t| (*t).to_owned()).collect())?);
        }
        Ok(out)
    }

    /// The embedding of a text to recall by: the model's query instruction, then `query`.
    pub fn embed_query(&self, query: &str) -> Result<Vec<f32>> {
        let mut v = self.embed(vec![format!("{}{query}", self.spec.query_prefix)])?;
        Ok(v.remove(0))
    }

    /// One batch: tokenised (padded to its longest), run, CLS-pooled and L2-normalised.
    fn embed(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        self.pool.install(|| {
            let enc = self
                .tokenizer
                .encode_batch(texts, true)
                .map_err(|e| Error::Tokenizer(e.to_string()))?;
            let rows = enc.len();
            let len = enc.first().map_or(0, |e| e.get_ids().len());
            let mut ids = Vec::with_capacity(rows * len);
            let mut mask = Vec::with_capacity(rows * len);
            for e in &enc {
                ids.extend_from_slice(e.get_ids());
                mask.extend_from_slice(e.get_attention_mask());
            }
            self.forward(ids, mask, (rows, len)).map_err(candle)
        })
    }

    /// The model on one padded batch: CLS-pooled and L2-normalised rows.
    fn forward(
        &self,
        ids: Vec<u32>,
        mask: Vec<u32>,
        shape: (usize, usize),
    ) -> candle_core::Result<Vec<Vec<f32>>> {
        let ids = Tensor::from_vec(ids, shape, &Device::Cpu)?;
        let mask = Tensor::from_vec(mask, shape, &Device::Cpu)?;
        let types = ids.zeros_like()?;
        let hidden = self.bert.forward(&ids, &types, Some(&mask))?;
        // CLS pooling: each row's first token.
        let cls = hidden.narrow(1, 0, 1)?.squeeze(1)?;
        let norm = cls.sqr()?.sum_keepdim(1)?.sqrt()?;
        cls.broadcast_div(&norm)?.to_vec2::<f32>()
    }
}
