#!/usr/bin/env python3
"""LSB de QT_VOTOS de Presidente por secao x falha dupla."""
from __future__ import annotations
import csv, math, zipfile
from collections import defaultdict
from pathlib import Path

AUDIT = Path("/mnt/vhdx_ext4/eleicoes22/audit_out")
BUSCA = [Path("/mnt/d/tse_bweb"), Path("/mnt/d"), Path("/mnt/vhdx_ext4/eleicoes22"), Path(".")]

def _zfill(x, n):
    return str(x or "").strip().zfill(n)

def carregar_secao():
    sec = {}
    for p in AUDIT.glob("snapshot_*.csv"):
        uf = p.stem.replace("snapshot_", "").replace(".done", "")
        with p.open(newline="", encoding="utf-8", errors="replace") as fh:
            for r in csv.DictReader(fh):
                mun = _zfill(r.get("municipio"), 5)
                zon = _zfill(r.get("zona"), 4)
                se = _zfill(r.get("secao"), 4)
                if mun == "00000" or not mun.strip("0"):
                    continue
                try:
                    vc = int(r.get("votos_sem_titulo") or 0)
                    cl = int(r.get("tratamento_auth_anomalo") or 0)
                    comp = int(r.get("computados") or 0)
                except ValueError:
                    continue
                sec[(r.get("uf") or uf).upper(), mun, zon, se] = {
                    "dupla": int(vc >= 1), "largo": cl, "comp": comp, "vc": vc,
                    "modelo": r.get("modelo") or "?",
                }
    return sec

def achar_fonte():
    cands = []
    for root in BUSCA:
        if not root.exists():
            continue
        cands += list(root.rglob("votacao_secao*.zip"))
        cands += list(root.rglob("votacao_secao*.csv"))
        cands += list(root.rglob("bweb_1t*.csv"))
    return sorted(set(cands), key=lambda p: p.stat().st_size, reverse=True)

def parse_bu(path: Path, sec):
    out = []
    if path.suffix.lower() == ".zip":
        z = zipfile.ZipFile(path)
        names = [n for n in z.namelist() if n.lower().endswith(".csv")]
        blob = z.read(names[0]) if names else b""
    else:
        blob = path.read_bytes()
    s = None
    for enc in ("latin-1", "utf-8-sig", "utf-8"):
        try:
            s = blob.decode(enc)
            break
        except Exception:
            pass
    if not s:
        return out
    first = s.splitlines()[0]
    sep = ";" if first.count(";") >= first.count(",") else ","
    reader = csv.DictReader(s.splitlines(), delimiter=sep)
    cols = {k.upper(): k for k in (reader.fieldnames or [])}
    def col(*names):
        for n in names:
            if n.upper() in cols:
                return cols[n.upper()]
        return None
    c_uf, c_mun = col("SG_UF", "UF"), col("CD_MUNICIPIO", "NR_MUNICIPIO")
    c_zon, c_sec = col("NR_ZONA"), col("NR_SECAO")
    c_cargo, c_turno = col("DS_CARGO", "CD_CARGO"), col("NR_TURNO")
    c_nr, c_nm, c_qt = col("NR_VOTAVEL", "NR_CANDIDATO"), col("NM_VOTAVEL", "NM_CANDIDATO"), col("QT_VOTOS")
    print("cols", list(cols)[:16], "sep", sep)
    n_ok = 0
    for r in reader:
        cargo = r.get(c_cargo) or ""
        if "resid" not in cargo.lower() and cargo.strip() not in {"1", "01"}:
            continue
        if c_turno and str(r.get(c_turno) or "").strip() not in {"1", "01", ""}:
            continue
        uf = (r.get(c_uf) or "").upper().strip()
        key = (uf, _zfill(r.get(c_mun), 5), _zfill(r.get(c_zon), 4), _zfill(r.get(c_sec), 4))
        if key not in sec:
            continue
        try:
            qt = int(str(r.get(c_qt) or "0").replace(".", ""))
        except ValueError:
            continue
        out.append((key, str(r.get(c_nr) or "").strip(), (r.get(c_nm) or "").strip(), qt))
        n_ok += 1
    print("linhas presidente join", n_ok)
    return out

def z50(impares, n):
    if n == 0:
        return None, None
    p = impares / n
    return p, (p - 0.5) / math.sqrt(0.25 / n)

def main():
    sec = carregar_secao()
    print("secoes audit", len(sec), "COM dupla", sum(v["dupla"] for v in sec.values()))
    fontes = achar_fonte()
    print("fontes", fontes[:6])
    if not fontes:
        raise SystemExit("nao achei votacao_secao em /mnt/d/tse_bweb")
    pares = parse_bu(fontes[0], sec)
    if not pares:
        raise SystemExit("join vazio")
    by = defaultdict(lambda: [0, 0, 0])
    nomes = {}
    for key, nr, nm, qt in pares:
        nomes[nr] = nm or nr
        lab = f"{nr}|{nomes[nr][:28]}"
        d = sec[key]["dupla"]
        by[(lab, d)][0] += 1
        by[(lab, d)][1] += qt % 2
        by[(lab, d)][2] += qt
    print(f"\n{'candidato':<36} {'cut':<4} {'n':>7} {'%impar':>8} {'z':>7} {'votos':>10}")
    keys = sorted({k[0] for k in by}, key=lambda x: -sum(by[(x, i)][2] for i in (0, 1)))
    for lab in keys:
        for corte, nome in ((1, "COM"), (0, "SEM")):
            n, imp, votos = by[(lab, corte)]
            p, z = z50(imp, n)
            ps = f"{100*p:6.2f}" if p is not None else "   n/a"
            zs = f"{z:7.2f}" if z is not None else "    n/a"
            print(f"{lab:<36} {nome:<4} {n:7d} {ps:>8} {zs} {votos:10d}")

if __name__ == "__main__":
    main()
