#!/usr/bin/env bash
# Reprocessa 1º turno (datas 01–02/10/2022) em todas as UFs e bate log×BU.
set -euo pipefail

ROOT="${1:-/mnt/vhdx/eleicoes22}"
OUT="${2:-$ROOT/audit_out_t1}"
WORKERS="${WORKERS:-8}"
UFS="${UFS:-AC AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO ZZ}"

export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
export LOGAUDIT_DATAS="${LOGAUDIT_DATAS:-01/10/2022,02/10/2022}"

mkdir -p "$OUT"
echo "== t1 start $(date) root=$ROOT out=$OUT datas=$LOGAUDIT_DATAS workers=$WORKERS"

for uf in $UFS; do
  if [[ -f "$OUT/snapshot_${uf}.done.csv" ]]; then
    echo "== skip $uf (done)"
    continue
  fi
  if [[ ! -d "$ROOT/$uf" ]]; then
    echo "== skip $uf (sem pasta $ROOT/$uf)"
    continue
  fi
  echo "== $uf $(date)"
  python3 -m logaudit.batch --root "$ROOT" --out "$OUT" --uf "$uf" --workers "$WORKERS"
  if [[ -f "$OUT/snapshot_${uf}.csv" ]]; then
    mv -f "$OUT/snapshot_${uf}.csv" "$OUT/snapshot_${uf}.done.csv"
  fi
done

echo "== batimento $(date)"
ZIP=""
for c in \
  "$ROOT/tse_bweb/votacao_secao_2022_BR.zip" \
  "$ROOT/votacao_secao_2022_BR.zip"
do
  if [[ -f "$c" && $(stat -c%s "$c" 2>/dev/null || echo 0) -gt 10000000 ]]; then
    ZIP="$c"
    break
  fi
done
if [[ -z "$ZIP" ]]; then
  echo "AVISO: zip BU nao encontrado — pulando batimento"
  exit 0
fi

python3 -m logaudit.batimento_bu \
  --audit "$OUT" --turno 1 --bu "$ZIP" --out "$OUT/join_bu.csv"

echo "== fim $(date)"
echo "snapshots: $(ls "$OUT"/snapshot_*.done.csv 2>/dev/null | wc -l)"
