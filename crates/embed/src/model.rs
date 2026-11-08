//! The models this crate runs, each pinned to one revision and its files' SHA-256s.

/// A model this crate can run.
///
/// Each is pinned to one upstream revision, and its [`id`](Model::id) names that revision: a
/// store created with one refuses vectors from any other model, including the same model run
/// elsewhere (another runtime, or a quantised copy), whose vectors can differ.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Model {
    /// [`BAAI/bge-small-en-v1.5`](https://huggingface.co/BAAI/bge-small-en-v1.5) (MIT): a
    /// 12-layer BERT, 384 dimensions, up to 512 tokens (longer text is truncated). CLS
    /// pooling, L2-normalised. Queries take bge's retrieval instruction; passages none. It's
    /// the model `pigeonhole-memory`'s published recall numbers were measured with (D311 T5).
    BgeSmallEnV15,
}

/// One model file: its name and pinned SHA-256.
pub(crate) struct File {
    pub(crate) name: &'static str,
    pub(crate) sha256: &'static str,
}

/// A model's pinned facts.
pub(crate) struct Spec {
    /// The Hugging Face repository.
    pub(crate) repo: &'static str,
    /// The pinned revision (a commit id).
    pub(crate) revision: &'static str,
    /// The [`EmbeddingModel`](pigeonhole_memory::EmbeddingModel) id.
    pub(crate) id: &'static str,
    /// The cache directory's name.
    pub(crate) dir: &'static str,
    pub(crate) dim: u32,
    /// The longest input, in tokens.
    pub(crate) max_tokens: usize,
    /// Prepended to a query, not to a passage.
    pub(crate) query_prefix: &'static str,
    pub(crate) files: [File; 3],
}

pub(crate) const WEIGHTS: usize = 0;
pub(crate) const TOKENIZER: usize = 1;
pub(crate) const CONFIG: usize = 2;

const BGE_SMALL_EN_V15: Spec = Spec {
    repo: "BAAI/bge-small-en-v1.5",
    revision: "5c38ec7c405ec4b44b94cc5a9bb96e735b38267a",
    id: "BAAI/bge-small-en-v1.5@5c38ec7c",
    dir: "bge-small-en-v1.5@5c38ec7c405ec4b44b94cc5a9bb96e735b38267a",
    dim: 384,
    max_tokens: 512,
    query_prefix: "Represent this sentence for searching relevant passages: ",
    files: [
        File {
            name: "model.safetensors",
            sha256: "3c9f31665447c8911517620762200d2245a2518d6e7208acc78cd9db317e21ad",
        },
        File {
            name: "tokenizer.json",
            sha256: "d241a60d5e8f04cc1b2b3e9ef7a4921b27bf526d9f6050ab90f9267a1f9e5c66",
        },
        File {
            name: "config.json",
            sha256: "094f8e891b932f2000c92cfc663bac4c62069f5d8af5b5278c4306aef3084750",
        },
    ],
};

impl Model {
    pub(crate) fn spec(self) -> &'static Spec {
        match self {
            Model::BgeSmallEnV15 => &BGE_SMALL_EN_V15,
        }
    }

    /// The id a store pins: the repository and the revision's first 8 hex digits, as
    /// `BAAI/bge-small-en-v1.5@5c38ec7c`.
    pub fn id(self) -> &'static str {
        self.spec().id
    }

    /// The embeddings' dimension.
    pub fn dim(self) -> u32 {
        self.spec().dim
    }

    /// The pinned upstream revision (a full commit id).
    pub fn revision(self) -> &'static str {
        self.spec().revision
    }
}

impl Spec {
    /// The pinned revision's download URL for `file`.
    pub(crate) fn url(&self, file: &File) -> String {
        format!(
            "https://huggingface.co/{}/resolve/{}/{}",
            self.repo, self.revision, file.name
        )
    }
}
