#!/usr/bin/env python3
"""The D311 T5 (#1339) recall fixture: LongMemEval and LoCoMo as memories and questions,
embedded once with one fixed open model, for `examples/memrecall.rs`.

    python3 fixture.py OUT_DIR [--datasets lme_s,lme_m,locomo] [--lme-m-questions 50]

Needs `sentence-transformers` (see requirements.txt next to this file). Downloads into
OUT_DIR/raw (checked against the SHA-256 below once pinned), then writes for each dataset D:

- `D.mem.jsonl`: one memory per line, in write order: `ns`, `session`, `date` (µs since
  the epoch, from the session's date), `unit` (the dataset's turn id) and `text`;
- `D.mem.f32`: the memories' embeddings, little-endian `f32`, `dim` each, in that order;
- `D.q.jsonl`: one question per line: `ns`, `question`, `date`, `type`, and `evidence`
  (session ids for LongMemEval, turn ids for LoCoMo);
- `D.q.f32`: the questions' embeddings (with the model's query instruction);
- `manifest.json`: the model, its revision, the dimension, the files' hashes and counts.

The memory unit is what an agent would remember: for LongMemEval, a user turn and the
assistant reply after it; for LoCoMo, one dialog turn. A namespace is one LongMemEval
question's haystack, or one LoCoMo conversation. Abstention questions (LongMemEval `_abs`,
LoCoMo category 5) have no evidence and are left out.
"""

import argparse
import datetime as dt
import hashlib
import json
import os
import struct
import sys
import urllib.request

MODEL = "BAAI/bge-small-en-v1.5"
# bge's instruction for retrieval queries; passages take none.
QUERY_PREFIX = "Represent this sentence for searching relevant passages: "

# name -> (url, sha256 or None until pinned)
SOURCES = {
    "lme_s": (
        "https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned/resolve/main/longmemeval_s_cleaned.json",
        "d6f21ea9d60a0d56f34a05b609c79c88a451d2ae03597821ea3d5a9678c3a442",
    ),
    "lme_m": (
        "https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned/resolve/main/longmemeval_m_cleaned.json",
        None,
    ),
    "locomo": (
        "https://raw.githubusercontent.com/snap-research/locomo/main/data/locomo10.json",
        "79fa87e90f04081343b8c8debecb80a9a6842b76a7aa537dc9fdf651ea698ff4",
    ),
}


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch(raw, name):
    url, want = SOURCES[name]
    path = os.path.join(raw, os.path.basename(url))
    if not os.path.exists(path):
        print(f"downloading {url}", file=sys.stderr)
        tmp = path + ".part"
        urllib.request.urlretrieve(url, tmp)
        os.replace(tmp, path)
    got = sha256(path)
    if want is not None and got != want:
        sys.exit(f"{path}: sha256 {got}, expected {want}")
    return path, got


def micros(d):
    return int(d.replace(tzinfo=dt.timezone.utc).timestamp() * 1_000_000)


def lme_date(s):
    # "2023/05/20 (Sat) 02:21"
    return micros(dt.datetime.strptime(s.split(" (")[0] + " " + s.split(") ")[1], "%Y/%m/%d %H:%M"))


def locomo_date(s):
    # "1:56 pm on 8 May, 2023"
    return micros(dt.datetime.strptime(s, "%I:%M %p on %d %B, %Y"))


def longmemeval(path, limit):
    items = json.load(open(path))
    mems, qs = [], []
    kept = 0
    for it in items:
        if it["question_id"].endswith("_abs"):
            continue
        if limit is not None and kept == limit:
            break
        kept += 1
        ns = "q:" + it["question_id"]
        sessions = sorted(
            zip(it["haystack_dates"], it["haystack_session_ids"], it["haystack_sessions"]),
            key=lambda s: lme_date(s[0]),
        )
        for date, sid, turns in sessions:
            t = lme_date(date)
            i = 0
            while i < len(turns):
                if turns[i]["role"] != "user":
                    i += 1
                    continue
                text = "user: " + turns[i]["content"].strip()
                if i + 1 < len(turns) and turns[i + 1]["role"] == "assistant":
                    text += "\nassistant: " + turns[i + 1]["content"].strip()
                    j = i + 2
                else:
                    j = i + 1
                mems.append({"ns": ns, "session": sid, "date": t, "unit": f"{sid}:{i}", "text": text})
                i = j
        qs.append(
            {
                "ns": ns,
                "question": it["question"],
                "date": lme_date(it["question_date"]),
                "type": it["question_type"],
                "evidence": it["answer_session_ids"],
            }
        )
    return mems, qs


def locomo(path):
    convs = json.load(open(path))
    mems, qs = [], []
    for c in convs:
        ns = "c:" + c["sample_id"]
        conv = c["conversation"]
        n = 1
        last = 0
        while f"session_{n}" in conv:
            t = locomo_date(conv[f"session_{n}_date_time"])
            for k, turn in enumerate(conv[f"session_{n}"]):
                # Turns of a session share its date: a µs apart keeps their order.
                mems.append(
                    {
                        "ns": ns,
                        "session": f"{c['sample_id']}:s{n}",
                        "date": t + k,
                        "unit": turn["dia_id"],
                        "text": f"{turn['speaker']}: {turn['text'].strip()}",
                    }
                )
            last = t + len(conv[f"session_{n}"])
            n += 1
        units = {m["unit"] for m in mems if m["ns"] == ns}
        for qa in c["qa"]:
            # A few evidence ids don't name a turn (malformed in the dataset): kept only if
            # one does.
            evidence = [e for e in qa.get("evidence") or [] if e in units]
            if qa.get("category") == 5 or not evidence:
                continue
            qs.append(
                {
                    "ns": ns,
                    "question": qa["question"],
                    "date": last + 1,
                    "type": str(qa.get("category")),
                    "evidence": evidence,
                }
            )
    return mems, qs


def embed(model, texts, prefix=""):
    """Embeds unique texts once (LongMemEval's haystacks share sessions)."""
    uniq = sorted(set(texts))
    print(f"embedding {len(uniq)} unique of {len(texts)}", file=sys.stderr)
    vecs = model.encode(
        [prefix + t for t in uniq],
        batch_size=64,
        normalize_embeddings=True,
        show_progress_bar=True,
        convert_to_numpy=True,
    )
    at = {t: i for i, t in enumerate(uniq)}
    return [vecs[at[t]] for t in texts]


def write_f32(path, vecs):
    with open(path, "wb") as f:
        for v in vecs:
            f.write(struct.pack(f"<{len(v)}f", *v.tolist()))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--datasets", default="lme_s,lme_m,locomo")
    ap.add_argument("--lme-m-questions", type=int, default=50)
    a = ap.parse_args()
    raw = os.path.join(a.out, "raw")
    os.makedirs(raw, exist_ok=True)

    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer(MODEL, device="cpu")
    manifest = {
        "model": MODEL,
        "revision": getattr(model[0].auto_model.config, "_commit_hash", None),
        "dim": model.get_sentence_embedding_dimension(),
        "datasets": {},
    }
    # Resumable: a dataset whose four files and summary are written is kept as it is.
    done_path = os.path.join(a.out, "datasets.jsonl")
    done = {}
    if os.path.exists(done_path):
        for line in open(done_path):
            d = json.loads(line)
            done.update(d)
    for name in a.datasets.split(","):
        files = [os.path.join(a.out, f"{name}.{x}") for x in ("mem.jsonl", "mem.f32", "q.jsonl", "q.f32")]
        if all(os.path.exists(f) for f in files):
            if name not in done:
                # Written by a run that kept no summary: rebuilt from the files.
                mems = [json.loads(l) for l in open(files[0])]
                qs = sum(1 for _ in open(files[2]))
                dim = manifest["dim"]
                assert os.path.getsize(files[1]) == 4 * dim * len(mems), files[1]
                assert os.path.getsize(files[3]) == 4 * dim * qs, files[3]
                done[name] = {
                    "source": SOURCES[name][0],
                    "sha256": fetch(raw, name)[1],
                    "memories": len(mems),
                    "questions": qs,
                    "namespaces": len({m["ns"] for m in mems}),
                }
            manifest["datasets"][name] = done[name]
            print(f"{name}: already written", file=sys.stderr)
            continue
        path, digest = fetch(raw, name)
        if name == "locomo":
            mems, qs = locomo(path)
        else:
            mems, qs = longmemeval(path, a.lme_m_questions if name == "lme_m" else None)
        with open(os.path.join(a.out, f"{name}.mem.jsonl"), "w") as f:
            for m in mems:
                f.write(json.dumps(m) + "\n")
        with open(os.path.join(a.out, f"{name}.q.jsonl"), "w") as f:
            for q in qs:
                f.write(json.dumps(q) + "\n")
        write_f32(os.path.join(a.out, f"{name}.mem.f32"), embed(model, [m["text"] for m in mems]))
        write_f32(
            os.path.join(a.out, f"{name}.q.f32"),
            embed(model, [q["question"] for q in qs], QUERY_PREFIX),
        )
        manifest["datasets"][name] = {
            "source": SOURCES[name][0],
            "sha256": digest,
            "memories": len(mems),
            "questions": len(qs),
            "namespaces": len({m["ns"] for m in mems}),
        }
        print(json.dumps({name: manifest["datasets"][name]}), file=sys.stderr)
        with open(done_path, "a") as f:
            f.write(json.dumps({name: manifest["datasets"][name]}) + "\n")
    with open(os.path.join(a.out, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)


if __name__ == "__main__":
    main()
