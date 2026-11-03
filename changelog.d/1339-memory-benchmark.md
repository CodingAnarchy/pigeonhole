### Added
- `pigeonhole-bench`: `memrecall`, the agent-memory recall benchmark on LongMemEval and LoCoMo (D311 T5, #1339). It measures recall per signal and fused, latency by namespace size, and size per 100k memories, using a fixture script that pins its datasets and embedding model. The results are in the agent memory guide.

### Changed
- `pigeonhole-memory`: recall's default recency weight is 0.25, down from 1 (D311 call 4, amended on T5's results, #1339). At 1, recency outweighed what a query asked for: on LongMemEval-S fused NDCG@10 was 0.789, below keyword alone at 0.864. At 0.25, fused recall beats every single signal on both benchmarks. To keep the old ranking, set `recency` to 1 on `Weights::default()` (in Python, `Weights(recency=1)`).
