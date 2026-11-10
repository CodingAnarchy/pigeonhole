"""Concurrent memory recalls under asyncio (#1560 PR 4, ai1249's B1): `CONCURRENCY` tasks
recalling in a loop, each with `await store.recall_async(...)` or with
`await asyncio.to_thread(store.recall, ...)`, while one more task commits a small write every
millisecond and times it. It prints one JSON line per mode: recalls per second, the recall's
p50/p99 latency, and the commits' p50/p99 latency meanwhile, which shows whether the recalls
hold up the event loop or the asyncio bridge's thread that commits are completed on.

    python crates/python/bench/recall_concurrency.py                # 5,000 memories, 5 s
    python crates/python/bench/recall_concurrency.py --memories 50000 --seconds 20

Recalls are warm after a first pass, so this measures the CPU side, which is B1's question:
an async recall's polls run on the one bridge thread, and `to_thread` runs blocking recalls
on the default executor's threads.
"""

import argparse
import asyncio
import json
import random
import statistics
import tempfile
import time
from pathlib import Path

import pigeonhole as ph
from pigeonhole import memory as mem

WORDS = (
    "deploy postgres cache memory agent index budget window release schema meeting "
    "tuesday alice bob vector search note plan review latency"
).split()
DIM = 32


def build(dirname: str, memories: int, seed: int):
    db = ph.open(Path(dirname) / "recall.phdb", create=True, shards=2,
                 durability=ph.Durability.NONE)
    store = mem.Store.create(db, "bench", embedding=mem.EmbeddingModel("m", DIM, "f32"))
    rng = random.Random(seed)
    for i in range(memories):
        text = " ".join(rng.choice(WORDS) for _ in range(rng.randint(4, 12)))
        emb = [rng.uniform(-1, 1) for _ in range(DIM)]
        store.remember(f"ns{i % 8}", f"{text} #{i}", embedding=emb,
                       session=f"s{i % 3}", importance=rng.random())
    db.flush()
    table = db.table("commits").family("f").create_if_missing()
    return db, store, table


def query(rng: random.Random) -> dict:
    return dict(
        text=" ".join(rng.choice(WORDS) for _ in range(3)),
        embedding=[rng.uniform(-1, 1) for _ in range(DIM)],
        session=rng.choice([None, "s1"]),
        k=10,
    )


def pct(xs: list[float], p: float) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * len(xs)))] if xs else float("nan")


async def run_mode(mode: str, store, table, ns: list[str], seconds: float, concurrency: int,
                   seed: int) -> dict:
    stop = time.perf_counter() + seconds
    recall_lat: list[float] = []
    commit_lat: list[float] = []

    async def recaller(i: int):
        rng = random.Random(seed * 1000 + i)
        while time.perf_counter() < stop:
            q = query(rng)
            started = time.perf_counter()
            if mode == "recall_async":
                await store.recall_async(ns, **q)
            else:
                await asyncio.to_thread(store.recall, ns, **q)
            recall_lat.append(time.perf_counter() - started)

    async def committer():
        n = 0
        while time.perf_counter() < stop:
            started = time.perf_counter()
            await table.mutate(f"{mode}-{n:08}").put("f", b"q", b"v").commit_async()
            commit_lat.append(time.perf_counter() - started)
            n += 1
            await asyncio.sleep(0.001)

    started = time.perf_counter()
    await asyncio.gather(committer(), *[recaller(i) for i in range(concurrency)])
    elapsed = time.perf_counter() - started
    us = 1e6
    return dict(
        mode=mode,
        concurrency=concurrency,
        recalls_per_s=round(len(recall_lat) / elapsed, 1),
        recall_p50_us=round(statistics.median(recall_lat) * us),
        recall_p99_us=round(pct(recall_lat, 0.99) * us),
        commits=len(commit_lat),
        commit_p50_us=round(statistics.median(commit_lat) * us),
        commit_p99_us=round(pct(commit_lat, 0.99) * us),
    )


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--memories", type=int, default=5_000)
    ap.add_argument("--seconds", type=float, default=5.0)
    ap.add_argument("--concurrency", type=int, default=16)
    ap.add_argument("--seed", type=int, default=1560)
    args = ap.parse_args()
    with tempfile.TemporaryDirectory() as d:
        db, store, table = build(d, args.memories, args.seed)
        ns = [f"ns{i}" for i in range(8)]
        # Warm: one pass of each query shape.
        rng = random.Random(args.seed)
        for _ in range(50):
            store.recall(ns, **query(rng))
        # Each mode twice, alternating, so drift shows.
        for mode in ("recall_async", "to_thread", "to_thread", "recall_async"):
            r = asyncio.run(run_mode(mode, store, table, ns, args.seconds, args.concurrency,
                                     args.seed))
            print(json.dumps(r), flush=True)
        store.close()
        db.close()


if __name__ == "__main__":
    main()
