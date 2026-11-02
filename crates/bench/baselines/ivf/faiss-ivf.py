#!/usr/bin/env python3
"""D306 PR 3: Faiss IVF-Flat on SIFT1M, like for like with `ivfsift`: one thread,
one query at a time, the same nprobes, recall@10 against the dataset's ground truth, and
per-query latency p50/p99 (us). nlist = 1000 lists (as ivfsift's default target length),
trained on the first 50,000 vectors (as bulk_load samples). Prints JSON lines.

    python3 crates/bench/baselines/ivf/faiss-ivf.py SIFT_DIR
"""
import json, sys, time
import numpy as np
import faiss

def fvecs(p):
    a = np.fromfile(p, dtype="int32")
    d = a[0]
    return a.reshape(-1, d + 1)[:, 1:].copy().view("float32")

def ivecs(p):
    a = np.fromfile(p, dtype="int32")
    d = a[0]
    return a.reshape(-1, d + 1)[:, 1:].copy()

def main():
    sift = sys.argv[1]
    faiss.omp_set_num_threads(1)
    base = fvecs(f"{sift}/sift_base.fvecs")
    queries = fvecs(f"{sift}/sift_query.fvecs")
    truth = ivecs(f"{sift}/sift_groundtruth.ivecs")[:, :10]
    d = base.shape[1]
    quant = faiss.IndexFlatL2(d)
    index = faiss.IndexIVFFlat(quant, d, 1000)
    t = time.time()
    index.train(base[:50000])
    index.add(base)
    print(json.dumps({"row": "faiss_build", "secs": round(time.time() - t, 1),
                      "vectors": int(index.ntotal), "lists": 1000}), flush=True)
    for nprobe in [4, 8, 16, 32, 64, 128]:
        index.nprobe = nprobe
        for rep in range(2):  # the first pass untimed, as ivfsift's warm rows
            lat, found = [], 0
            for i in range(len(queries)):
                q = queries[i:i + 1]
                s = time.perf_counter_ns()
                _, ids = index.search(q, 10)
                lat.append(time.perf_counter_ns() - s)
                found += len(set(ids[0].tolist()) & set(truth[i].tolist()))
        lat.sort()
        print(json.dumps({"row": "faiss_warm", "nprobe": nprobe,
                          "recall10": round(found / (len(queries) * 10), 4),
                          "p50_us": round(lat[len(lat) // 2] / 1e3, 1),
                          "p99_us": round(lat[int((len(lat) - 1) * 0.99)] / 1e3, 1)}), flush=True)

main()
