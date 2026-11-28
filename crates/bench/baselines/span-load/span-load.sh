#!/usr/bin/env bash
# perf287: #2221's cluster load with transactions across partitions (D329) and change-feed
# subscribers, on a 3-node cluster on one host (plan approved 2026-11-27,
# .coord/inbox/261127-perf287-2221-plan.md). Run as root on the AX102 in tmux after
# `source ~/.cargo/env`, or on the VM with SCALE=small (below).
#
#   perf287-2221-span.sh <SHA_A> [<SHA_B>]
#
# One SHA: REPS repeats of each shape on it. Two: per shape and repeat, a b b a.
# The cluster: nodes n1 n2 n3 on 127.0.0.1:7471-7473, each owning one partition (p1 p2 p3)
# and holding a replica of the one before it (p1's replica on n2, p2's on n3, p3's on n1),
# replica_acks 0, or with D335=1 1 for t256's and l's k1 rows. The table `span` (family f) is
# split at "b" and "c": rows a* in p1, b* in p2, c* in p3. Every node has the change feed on.
# A fresh cluster per run: files made with `phdb cluster init-partition`, the table made in
# each partition's file with `phdb shell`, the same first map in each node's directory.
# Shapes (serve_span_load's presets, crates/cli/examples/serve_span_load.rs):
#   t256  256 callers, closed loop, single-partition Mutates (throughput; D335's base)
#   l     64 callers paced at RATE_L, single-partition Mutates (latency; D335's p99 base)
#   m     64 callers paced at RATE_M, 10% spanning x2, 1% conflicts, 16 subscribers
#   s     64 callers paced at RATE_S, every op spanning x3, 1% conflicts, 16 subscribers
#   h     m, with n2 stopped (SIGSTOP) SECS/3 into the measured phase for 5 s
# RATE_L, unless set: half the first t256 (k=0) run's ops/s, then fixed for every later run.
# RATE_S, unless set: RATE_L / 6 (each s operation is three partitions' writes and a 2PC).
# RATE_M (m and h), unless set: RATE_L. 2026-11-27's box run delivered 43.6k ops/s for m at
# RATE_L 72,816, so set it under m's capacity for m's latency row.
# D335=1 (one SHA only): t256 and l run k0 k1 k1 k0 per repeat, k being every partition's
# replica_acks in the map (D335, #2238): its bar is k=1 p99 <= k=0 p99 + 1 ms (l) and k=1
# ops/s >= 0.9 x k=0 (t256). The other shapes run at k=0 (spanning transactions on a k >= 1
# partition are refused until D335 PR 4). A k=1 run's FUNCTIONAL check: the owners'
# replica-ack wait histogram counted writes.
# CPUs (SCALE=full): n1 0-3,16-19; n2 4-7,20-23; n3 8-11,24-27; client 12-15,28-31. Each node
# serves with --shards 8 (its partition and its replica copy share them), --change-feed
# 1 GiB, a backup dir (replicas bootstrap from it), and the session flags below only with
# OLD_SESSION_CAPS=1.
# SCALE=small: no pinning, --shards 2, --change-feed 64 MiB, 20 s measured, 5 s warm-up.
# FUNCTIONAL per run: each node's cmdline names the arm's phdb; every node up until the client
# ends (h: n2 resumes); 3 partitions serving and 3 replicas following before the load; the
# client's RESULT line, and no FUNCTIONAL FAILED from it; l, m, s: ops/s at least 90% of the
# shape's rate (h: 85%, n2 is stopped 5 of the 90 s); m, s, h: ext_prepares > 0, no share in
# doubt at the end, no subscriber stream dropped; every replica following in the last metrics
# sample; every run: metrics read from 3/3 nodes before and after the phase (nodes_read), and
# no caller lost at the join (callers_lost). Diagnostics per run: each node's stderr, and each node's /metrics
# lines naming flush, compaction, stall, commit, external, in_doubt, feed or replica, every
# 1 s, timestamped (<run>.mx).
# Output: OUT (default $S/out-2221-span): <rep>-<arm>-<shape>-k<k>.*, summary.txt (RESULT
# lines), and the archive $OUT.tar.gz.
set -u
A=${1:?usage: perf287-2221-span.sh <SHA_A> [<SHA_B>]}
B=${2:-}
SCALE=${SCALE:-full}
S=${S:-/root/scratch/perf287}
REPO=${REPO:-https://github.com/CodingAnarchy/pigeonhole}
REPS=${REPS:-3}
SHAPES=${SHAPES:-t256 l m s h}
OUT=${OUT:-$S/out-2221-span}
# The data directory: a literal path, removed only by name below.
W=/data/scratch/perf287/s2221
[ "$SCALE" = small ] && W=/target/span2221dry/data
mkdir -p $OUT "$(dirname $W)"
[ -f ~/.cargo/env ] && source ~/.cargo/env
unset PIGEONHOLE_TRACE
ulimit -n 1048576 2>/dev/null || ulimit -n 524288
if [ "$SCALE" = full ]; then
  CPU=(0-3,16-19 4-7,20-23 8-11,24-27); CLIENT=12-15,28-31; SHARDS=8; FEED=1073741824; SECS=90; WARM=15
else
  CPU=("" "" ""); CLIENT=""; SHARDS=2; FEED=67108864; SECS=20; WARM=5
fi
for sha in $A $B; do
  [ ${#sha} -eq 40 ] || { echo "FAILED: $sha isn't a full 40-char SHA"; exit 1; }
done
echo "=== 2221 span start $(date -u +%H:%M:%SZ) a ${A:0:12} b ${B:0:12} scale $SCALE reps $REPS shapes $SHAPES session caps $([ -n "${OLD_SESSION_CAPS:-}" ] && echo "raised (OLD_SESSION_CAPS)" || echo default)"

# Each arm: its own clone and target, built once.
build() { local arm=$1 sha=$2 sd=$S/src-2221-$1 td=$S/target-2221-$1
  [ -d $sd ] || git clone -q $REPO $sd || { echo "FAILED: clone for $arm"; exit 1; }
  git -C $sd fetch -q $REPO $sha || { echo "FETCH FAILED: $arm"; exit 1; }
  git -C $sd -c advice.detachedHead=false checkout -q -f --detach FETCH_HEAD || exit 1
  [ "$(git -C $sd rev-parse HEAD)" = "$sha" ] || { echo "FUNCTIONAL FAILED: $arm's checkout isn't at $sha"; exit 1; }
  echo "-- build $arm ${sha:0:12} $(date -u +%H:%M:%SZ)"
  CARGO_TARGET_DIR=$td cargo build -q --release --manifest-path $sd/Cargo.toml -p pigeonhole-cli \
    --features serve --bin phdb --example serve_span_load || { echo "FAILED: build $arm"; exit 1; }
  [ -x $td/release/phdb ] && [ -x $td/release/examples/serve_span_load ] ||
    { echo "FUNCTIONAL FAILED: $arm's binaries missing"; exit 1; }
  echo "    functional: $arm built at $sha"
}
build a $A
[ -n "$B" ] && build b $B
# One client for both arms (a's), so only the servers differ.
LOAD=$S/target-2221-a/release/examples/serve_span_load
[ -n "$B" ] && echo "    client source diff a..b: $(git -C $S/src-2221-a diff --stat $A $B -- crates/cli/examples/serve_span_load.rs | tail -1)"

# Sessions: the servers' defaults. Since #2257 (#2253), a committed transaction's record holds
# no session slot. A SHA before #2257 needs OLD_SESSION_CAPS=1: there a committed
# transaction's tombstone counts against the per-token cap (64) for an idle lease (30 s), and
# with no tokens every caller is one principal, so at thousands of transactions a second
# Begin is refused (Overloaded) without the raised caps.
SESSIONS=()
[ -n "${OLD_SESSION_CAPS:-}" ] && SESSIONS=(--max-sessions 65536 --max-sessions-per-token 65536 --txn-idle-ms 2000)
PORTS=(7471 7472 7473)
url() { echo "http://127.0.0.1:${PORTS[$1]}"; }
write_map() { python3 - "$1" "$2" <<'PY'
import json, sys
k = int(sys.argv[2])
m = {"cluster_id": "c1", "map_version": 1,
     "nodes": [{"id": f"n{i}", "address": f"127.0.0.1:{7470 + i}"} for i in (1, 2, 3)],
     "partitions": [{"id": f"p{i}", "node": f"n{i}", "epoch": 1, "replicas": [f"n{i % 3 + 1}"],
                     **({"replica_acks": k} if k else {})}
                    for i in (1, 2, 3)],
     "tables": [{"name": "span", "splits": [{"text": "b"}, {"text": "c"}],
                 "partitions": ["p1", "p2", "p3"]}]}
open(sys.argv[1], "w").write(json.dumps(m, indent=2) + "\n")
PY
}

# Polls n1's status until 3 partitions serve and 3 replica copies follow (120 s at most).
ready() { local o=$1 t=0 js
  while ((t < 120)); do
    js=$($PHDB cluster status --node $(url 0) --format json 2>/dev/null)
    if python3 -c '
import json, sys
s = json.loads(sys.argv[1])
parts = [p for n in s.get("nodes", []) for p in n.get("partitions", [])]
own = [p for p in parts if p.get("role", "owner").lower().endswith("owner") and "serving" in str(p.get("state", "")).lower()]
rep = [p for p in parts if p.get("role", "").lower().endswith("replica") and "following" in str(p.get("replica_state", "")).lower()]
sys.exit(0 if len(own) >= 3 and len(rep) >= 3 else 1)' "$js" 2>/dev/null; then
      echo "    functional $o: 3 partitions serving, 3 replicas following after ${t}s"; return 0
    fi
    sleep 2; t=$((t + 2))
  done
  echo "  FUNCTIONAL FAILED $o: not ready in 120 s"; echo "$js" > $OUT/$o.status-notready.json; return 1
}

RATE_L=${RATE_L:-}
run() { local rep=$1 arm=$2 shape=$3 k=$4 o=$1-$2-$3-k$4 sha=$A
  [ $arm = b ] && sha=$B
  PHDB=$S/target-2221-$arm/release/phdb
  [ -e /data/scratch/perf287/s2221 ] && rm -r /data/scratch/perf287/s2221
  [ -e /target/span2221dry/data ] && rm -r /target/span2221dry/data
  for i in 1 2 3; do
    mkdir -p $W/n$i
    $PHDB cluster init-partition $W/n$i/p$i.phdb --cluster-id c1 --partition p$i > /dev/null ||
      { echo "  FUNCTIONAL FAILED $o: init-partition p$i"; return; }
    $PHDB shell $W/n$i/p$i.phdb -c "create span f" > /dev/null ||
      { echo "  FUNCTIONAL FAILED $o: create span in p$i"; return; }
    write_map $W/n$i/cluster-map.json $k
    # A replica copies its partition from the owner as a backup (D328), so owners need one.
    mkdir -p $W/backups-n$i
  done
  [ "$SCALE" = full ] && fstrim /data > /dev/null 2>&1
  local t=0; while ((t < 180)) && awk '{exit !($1 > 1.0)}' /proc/loadavg; do sleep 10; t=$((t + 10)); done
  echo "run $o $(date -u +%H:%M:%SZ) load $(cut -d' ' -f1 /proc/loadavg)"
  local pids=()
  for i in 0 1 2; do
    local pre=(); [ -n "${CPU[$i]}" ] && pre=(taskset -c ${CPU[$i]})
    "${pre[@]}" $PHDB serve --cluster $W/n$((i + 1)) --node-id n$((i + 1)) --listen 127.0.0.1:${PORTS[$i]} \
      --shards $SHARDS --change-feed $FEED --backup-dir $W/backups-n$((i + 1)) "${SESSIONS[@]}" > $OUT/$o.n$((i + 1)).err 2>&1 &
    pids+=($!)
  done
  if ready $o; then
    # Checked once the nodes serve: right after the fork, taskset may not have exec'd phdb yet (empty cmdline).
    for i in 0 1 2; do
      tr '\0' ' ' < /proc/${pids[$i]}/cmdline > $OUT/$o.n$((i + 1)).cmdline 2>/dev/null
      grep -q "$PHDB serve" $OUT/$o.n$((i + 1)).cmdline && echo "    functional $o: n$((i + 1)) is $arm's phdb" ||
        echo "  FUNCTIONAL FAILED $o: n$((i + 1))'s cmdline isn't $arm's phdb: $(head -c 200 $OUT/$o.n$((i + 1)).cmdline)"
    done
    ( while :; do
        ts=$(date +%s.%N)
        for i in 0 1 2; do
          curl -s -m 1 $(url $i)/metrics 2>/dev/null |
            grep -E 'flush|compact|stall|commit|external|in_doubt|feed|replica' | grep -v '^#' | sed "s/^/$ts n$((i + 1)) /"
        done
        sleep 1
      done > $OUT/$o.mx ) &
    local sampler=$!
    local cargs=(--server $(url 0) --server $(url 1) --server $(url 2) --server-pid ${pids[0]} --server-pid ${pids[1]}
      --server-pid ${pids[2]} --seconds $SECS --warmup-secs $WARM --label $o)
    [ -n "$CLIENT" ] && cargs+=(--client-cpus $CLIENT)
    case $shape in
      t256) cargs+=(--shape t256) ;;
      l) cargs+=(--shape l --rate $RATE_L) ;;
      m) cargs+=(--shape m --rate ${RATE_M:-$RATE_L}) ;;
      s) cargs+=(--shape s --rate ${RATE_S:-$((RATE_L / 6))}) ;;
      h) cargs+=(--shape m --rate ${RATE_M:-$RATE_L} --stall-pid ${pids[1]} --stall-at $((SECS / 3)) --stall-for 5) ;;
    esac
    $LOAD "${cargs[@]}" > $OUT/$o.txt 2> $OUT/$o.client.err || echo "  RUN ERROR $o: the client exited non-zero (see $o.client.err)"
    kill $sampler 2>/dev/null; wait $sampler 2>/dev/null
    for i in 0 1 2; do
      kill -0 ${pids[$i]} 2>/dev/null || echo "  FUNCTIONAL FAILED $o: n$((i + 1)) exited before the client ended"
    done
    local res; res=$(grep '^RESULT ' $OUT/$o.txt)
    if [ -z "$res" ]; then echo "  FUNCTIONAL FAILED $o: no RESULT line"; else
      echo "$res" | tee -a $OUT/summary.txt
      grep -q "FUNCTIONAL FAILED" $OUT/$o.txt && echo "  FUNCTIONAL FAILED $o: $(grep 'FUNCTIONAL FAILED' $OUT/$o.txt)"
      field() { local v; v=$(sed -n "s/.* $1=\([0-9]*\).*/\1/p" <<< "$res"); echo "${v:-0}"; }
      local prep ops doubt drops rate pct nread lost
      prep=$(field ext_prepares); ops=$(field ops_per_s); doubt=$(field in_doubt_end); drops=$(field dropped)
      nread=$(sed -n 's/.* nodes_read=\([0-9]*\/[0-9]*\).*/\1/p' <<< "$res"); lost=$(sed -n 's/.* callers_lost=\([0-9]*\).*/\1/p' <<< "$res")
      [ "$nread" = 3/3 ] && echo "    functional $o: metrics read from 3/3 nodes" ||
        echo "  FUNCTIONAL FAILED $o: metrics read from ${nread:-?} nodes (need 3/3)"
      [ "$lost" = 0 ] && echo "    functional $o: no caller lost" ||
        echo "  FUNCTIONAL FAILED $o: ${lost:-?} callers lost at the join"
      case $shape in
        l) rate=$RATE_L pct=90 ;;
        m) rate=${RATE_M:-$RATE_L} pct=90 ;;
        s) rate=${RATE_S:-$((RATE_L / 6))} pct=90 ;;
        h) rate=${RATE_M:-$RATE_L} pct=85 ;;
        *) rate= ;;
      esac
      if [ -n "$rate" ]; then
        [ $((ops * 100)) -ge $((rate * pct)) ] && echo "    functional $o: $ops ops/s at rate $rate" ||
          echo "  FUNCTIONAL FAILED $o: $ops ops/s under $pct% of $rate"
      fi
      case $shape in
        m|s|h)
          [ "$prep" -gt 0 ] && echo "    functional $o: $prep external prepares" ||
            echo "  FUNCTIONAL FAILED $o: no external prepare counted"
          [ "$doubt" -eq 0 ] && echo "    functional $o: no share in doubt at the end" ||
            echo "  FUNCTIONAL FAILED $o: $doubt shares in doubt at the end"
          [ "$drops" -eq 0 ] && echo "    functional $o: no subscriber stream dropped" ||
            echo "  FUNCTIONAL FAILED $o: $drops subscriber streams dropped" ;;
        t256) [ -z "$RATE_L" ] && [ $k = 0 ] && RATE_L=$((ops / 2)) && echo "    RATE_L set to $RATE_L (half of $o's $ops ops/s)" ;;
      esac
      # The last sample of each node's replica_following (one replica per node); a node with
      # no sample at all counts as not following.
      local notf; notf=$(awk '$3 ~ /^pigeonhole_replica_following/ {v[$2] = $4} END {n = 0; c = 0; for (k in v) { c++; if (v[k] + 0 != 1) n++ }; print n + 3 - c}' $OUT/$o.mx)
      [ "$notf" -eq 0 ] && echo "    functional $o: every replica following in the last sample" ||
        echo "  FUNCTIONAL FAILED $o: $notf replicas not following in the last sample"
      if [ $k -gt 0 ]; then
        # The counts in the last sample's lines (the last timestamp each node wrote), summed.
        local waits; waits=$(grep 'pigeonhole_cluster_replica_ack_wait_seconds_count' $OUT/$o.mx | awk '{last[$2" "$3] = $NF} END {s = 0; for (k in last) s += last[k]; print s + 0}')
        [ "${waits%.*}" -gt 0 ] && echo "    functional $o: replica-ack waits counted ($waits in the last samples)" ||
          echo "  FUNCTIONAL FAILED $o: k=$k but no replica-ack wait counted"
      fi
      sed -n '/^kind/,/^external/p;/^stall:/p' $OUT/$o.txt | sed "s/^/  $o: /"
    fi
  fi
  for p in "${pids[@]}"; do kill -CONT $p 2>/dev/null; kill $p 2>/dev/null; done
  for p in "${pids[@]}"; do
    local n=0; while kill -0 $p 2>/dev/null && ((n < 30)); do sleep 1; n=$((n + 1)); done
    kill -0 $p 2>/dev/null && kill -9 $p 2>/dev/null
  done
  wait 2>/dev/null
}

: > $OUT/summary.txt
D335=${D335:-}
[ -n "$D335" ] && [ -n "$B" ] && { echo "FAILED: D335=1 takes one SHA"; exit 1; }
ks_of() { if [ -n "$D335" ] && { [ $1 = t256 ] || [ $1 = l ]; }; then echo "0 1 1 0"; else echo 0; fi; }
for shape in $SHAPES; do
  if [ $shape != t256 ] && [ -z "$RATE_L" ]; then echo "FAILED: no RATE_L for $shape (run t256 first or set RATE_L)"; exit 1; fi
  for rep in $(seq 1 $REPS); do
    if [ -n "$B" ]; then for arm in a b b a; do run $rep $arm $shape 0; done
    else for k in $(ks_of $shape); do run $rep a $shape $k; done; fi
  done
done
[ -e /data/scratch/perf287/s2221 ] && rm -r /data/scratch/perf287/s2221
[ -e /target/span2221dry/data ] && rm -r /target/span2221dry/data
per=1; [ -n "$B" ] && per=4
want=0; for shape in $SHAPES; do nk=$(ks_of $shape | wc -w); want=$((want + REPS * per * nk)); done
c=$(grep -c '^RESULT ' $OUT/summary.txt)
[ "$c" = "$want" ] && echo "    functional: $c of $want RESULT lines" || echo "    FUNCTIONAL FAILED: $c of $want RESULT lines"
cp "$0" $OUT/
tar -C "$(dirname $OUT)" -czf $OUT.tar.gz "$(basename $OUT)"
echo "SPAN2221-DONE $(date -u +%H:%M:%SZ): $OUT.tar.gz"
