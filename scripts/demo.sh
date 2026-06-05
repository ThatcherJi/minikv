#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COORD_PORT=7000
DEMO_DIR="$ROOT/demo-data"

PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done
}
trap cleanup EXIT

rm -rf "$DEMO_DIR"
mkdir -p "$DEMO_DIR"

cargo build

cargo run --bin coord -- \
  --listen "127.0.0.1:$COORD_PORT" \
  --replicas 2 \
  --vnodes 64 \
  --dead-after-secs 4 \
  --meta "$DEMO_DIR/coord-meta.json" &
PIDS+=("$!")

sleep 1

for i in 1 2 3; do
  port=$((7000 + i))
  cargo run --bin volume -- \
    --id "v$i" \
    --listen "127.0.0.1:$port" \
    --coord "http://127.0.0.1:$COORD_PORT" \
    --data "$DEMO_DIR/v$i" \
    --heartbeat-secs 1 &
  PIDS+=("$!")
done

sleep 3

echo "== cluster after registration =="
cargo run --bin cli -- --coord "http://127.0.0.1:$COORD_PORT" cluster

echo "== put/get before failure =="
cargo run --bin cli -- --coord "http://127.0.0.1:$COORD_PORT" put k1 v1
cargo run --bin cli -- --coord "http://127.0.0.1:$COORD_PORT" get k1

echo "== ring placement for k1 =="
cargo run --bin cli -- --coord "http://127.0.0.1:$COORD_PORT" ring k1

echo "== volume stats via coordinator =="
cargo run --bin cli -- --coord "http://127.0.0.1:$COORD_PORT" volume-stats

echo "== direct volume keys and compaction =="
cargo run --bin cli -- keys "127.0.0.1:7001"
cargo run --bin cli -- compact "127.0.0.1:7001"

echo "== kill one volume =="
kill "${PIDS[1]}"
sleep 6

echo "== cluster after killing one volume =="
cargo run --bin cli -- --coord "http://127.0.0.1:$COORD_PORT" cluster

echo "== read after killing one volume =="
cargo run --bin cli -- --coord "http://127.0.0.1:$COORD_PORT" get k1
