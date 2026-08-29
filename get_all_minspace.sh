#!/usr/bin/env bash
# Um UF por vez: wget → unzip só *.logjez → rm zip.
# Pico de disco ≈ maior zip (SP) + logjez daquele UF, não a soma do Brasil.
set -euo pipefail

ROOT="${ROOT:-$(pwd)}"
UA="Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/121.0"
BASE="https://cdn.tse.jus.br/estatistica/sead/eleicoes/eleicoes2022/arqurnatot"
TURNO="${TURNO:-1t}"   # 1t ou 2t

UFS=(AC AL AP AM BA CE ES GO MA MT MS MG PA PB PR PE PI RJ RN RS RO RR SC SP SE TO DF ZZ)

cd "$ROOT"

um_uf() {
  local uf="$1"
  local zip="all_${uf}.zip"
  local url="${BASE}/bu_imgbu_logjez_rdv_vscmr_2022_${TURNO}_${uf}.zip"
  mkdir -p "$uf"

  if compgen -G "$uf/*.logjez" > /dev/null; then
    echo "== $uf já tem logjez, pulando"
    rm -f "$zip"
    return 0
  fi

  echo "== $uf  baixando $url"
  wget -c --user-agent="$UA" -O "$zip" "$url"

  echo "== $uf  extraindo só *.logjez"
  unzip -n -j "$zip" "*.logjez" -d "$uf/" || unzip -n "$zip" "*.logjez" -d "$uf/"

  find "$uf" -maxdepth 1 -type f ! -name '*.logjez' -delete

  rm -f "$zip"
  echo "== $uf  $(find "$uf" -name '*.logjez' | wc -l) logjez  $(du -sh "$uf" | cut -f1)"
}

for uf in "${UFS[@]}"; do
  um_uf "$uf"
done
echo "fim  $(du -sh "${UFS[@]}" 2>/dev/null | tail)"
