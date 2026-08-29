#!/usr/bin/env bash
set -euo pipefail
BASE="${BASE:-/mnt/vhdx_ext4/eleicoes22}"
TAG="${1:-t2}"
TURNO_BU="${2:-2}"
ROOT="${3:-$BASE/$TAG}"
OUT="${4:-$BASE/audit_out_$TAG}"
WORKERS="${WORKERS:-8}"
UFS=(AC AL AP AM BA CE ES GO MA MT MS MG PA PB PR PE PI RJ RN RS RO RR SC SP SE TO DF ZZ)
mkdir -p "$OUT"
export PYTHONPATH="$BASE${PYTHONPATH:+:$PYTHONPATH}"
echo "== start $(date) tag=$TAG root=$ROOT out=$OUT"
for uf in "${UFS[@]}"; do
  if [[ -f "$OUT/snapshot_${uf}.done.csv" ]]; then echo "== skip $uf (done)"; continue; fi
  if ! compgen -G "$ROOT/$uf/*.logjez" > /dev/null; then echo "== skip $uf (sem logjez)"; continue; fi
  echo "== $uf $(date)"
  python3 -m logaudit.batch --root "$ROOT" --out "$OUT" --uf "$uf" --workers "$WORKERS"
  mv -f "$OUT/snapshot_${uf}.csv" "$OUT/snapshot_${uf}.done.csv"
done
echo "== cobertura $(date)"
python3 - <<PY
import csv
from pathlib import Path
from collections import defaultdict
out = Path("$OUT")
by = defaultdict(lambda: [0, 0])
for p in out.glob("snapshot_*.csv"):
    uf = p.stem.replace("snapshot_", "").replace(".done", "")
    with p.open(encoding="utf-8", errors="replace") as fh:
        for r in csv.DictReader(fh):
            by[uf][0] += 1
            by[uf][1] += int(r.get("votos_sem_titulo") or 0) >= 1
print(f"{'uf':<4} {'n':>7} {'COM':>7} {'SEM':>7} {'COM%':>7}")
N=C=0
for uf,(n,c) in sorted(by.items()):
    print(f"{uf:<4} {n:7d} {c:7d} {n-c:7d} {100*c/n:6.1f}%")
    N+=n; C+=c
if N: print(f"{'BR':<4} {N:7d} {C:7d} {N-C:7d} {100*C/N:6.1f}%")
PY
echo "== paridade presidente turno=$TURNO_BU $(date)"
python3 -m logaudit.paridade_presidente --audit "$OUT" --turno "$TURNO_BU" || true
echo "== fim $(date)"
