"""
Bate computados do log (snapshot) com QT_VOTOS Presidente do votacao_secao.

  PYTHONPATH=. python3 -m logaudit.batimento_bu \\
      --audit audit_out_t2b --turno 2 \\
      --bu tse_bweb/votacao_secao_2022_BR.zip
"""
from __future__ import annotations

import csv
import zipfile
from collections import defaultdict
from pathlib import Path


def zf(x, n):
    return str(x or "").strip().zfill(n)


def achar_zip(paths):
    for p in paths:
        p = Path(p)
        if p.is_file() and p.stat().st_size > 1_000_000:
            return p
        if p.is_dir():
            for q in p.rglob("votacao_secao_2022*.zip"):
                if q.stat().st_size > 1_000_000:
                    return q
    return None


def carrega_audit(audit: Path):
    sec = {}
    for p in sorted(audit.glob("snapshot_*.csv")):
        uf0 = p.stem.replace("snapshot_", "").replace(".done", "")
        with p.open(encoding="utf-8", errors="replace") as fh:
            for r in csv.DictReader(fh):
                uf = (r.get("uf") or uf0).upper()
                k = (uf, zf(r.get("municipio"), 5), zf(r.get("zona"), 4), zf(r.get("secao"), 4))
                if k[1] in {"", "00000"}:
                    continue
                r = dict(r)
                r["_uf"] = uf
                r["_k"] = k
                sec[k] = r
    return sec


def carrega_bu(zp: Path, turno: str, keys):
    turno_ok = {"1", "01"} if str(turno) in {"1", "01"} else {"2", "02"}
    z = zipfile.ZipFile(zp)
    name = next(n for n in z.namelist() if n.lower().endswith(".csv"))
    bu = defaultdict(int)
    for r in csv.DictReader(z.read(name).decode("latin-1").splitlines(), delimiter=";"):
        cargo = (r.get("DS_CARGO") or r.get("NM_CARGO") or "").lower()
        if "resid" not in cargo and str(r.get("CD_CARGO") or "") not in {"1", "01"}:
            continue
        if str(r.get("NR_TURNO") or "").strip() not in turno_ok:
            continue
        k = (
            (r.get("SG_UF") or "").upper(),
            zf(r.get("CD_MUNICIPIO"), 5),
            zf(r.get("NR_ZONA"), 4),
            zf(r.get("NR_SECAO"), 4),
        )
        if keys is not None and k not in keys:
            continue
        bu[k] += int(str(r.get("QT_VOTOS") or "0").replace(".", "") or 0)
    return bu


def status(comp, b):
    if b is None:
        return "sem_bu", ""
    gap = b - comp
    if gap == 0:
        return "ok", 0
    ratio = comp / b if b else 1
    if ratio < 0.5 and gap >= 50:
        return "contingencia", gap
    return "parcial", gap


def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--audit", required=True)
    ap.add_argument("--turno", required=True, choices=["1", "2"])
    ap.add_argument("--bu", default="")
    ap.add_argument("--out", default="")
    args = ap.parse_args(argv)

    audit = Path(args.audit)
    cands = [args.bu] if args.bu else []
    cands += [
        "tse_bweb/votacao_secao_2022_BR.zip",
        "/mnt/vhdx/eleicoes22/tse_bweb/votacao_secao_2022_BR.zip",
        "/mnt/vhdx/eleicoes22/votacao_secao_2022_BR.zip",
        "votacao_secao_2022_BR.zip",
    ]
    zp = achar_zip(cands)
    if not zp:
        raise SystemExit("nao achei votacao_secao_2022_BR.zip")

    sec = carrega_audit(audit)
    print(f"audit {audit}  secoes {len(sec)}  turno {args.turno}")
    print(f"BU {zp}  {zp.stat().st_size}")
    bu = carrega_bu(zp, args.turno, set(sec))

    out = Path(args.out) if args.out else audit / f"join_bu_t{args.turno}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)

    cnt = defaultdict(int)
    ufacc = defaultdict(lambda: [0, 0, 0, 0, 0, 0])
    w = None
    with out.open("w", newline="", encoding="utf-8") as fh:
        for r in sec.values():
            k = r["_k"]
            comp = int(r.get("computados") or 0)
            b = bu.get(k)
            st, diff = status(comp, b)
            cnt[st] += 1
            row = {kk: vv for kk, vv in r.items() if not kk.startswith("_")}
            row["bu_pres"] = "" if b is None else b
            row["diff_log_bu"] = diff
            row["status_bu"] = st
            if w is None:
                w = csv.DictWriter(fh, fieldnames=list(row.keys()))
                w.writeheader()
            w.writerow(row)
            if b is None:
                continue
            a = ufacc[k[0]]
            a[0] += 1
            a[2] += comp
            a[3] += b
            a[4] += abs(comp - b)
            if st == "ok":
                a[1] += 1
            if st == "contingencia":
                a[5] += 1

    n = sum(cnt.values())
    njoin = n - cnt["sem_bu"]
    eq = cnt["ok"]
    print(f"gravou {out}")
    print(
        f"status  ok={cnt['ok']}  contingencia={cnt['contingencia']}  "
        f"parcial={cnt['parcial']}  sem_bu={cnt['sem_bu']}"
    )
    if njoin:
        den = max(njoin - cnt["contingencia"], 1)
        print(
            f"eq {eq}/{njoin} ({100 * eq / njoin:.2f}%)  "
            f"eq sem contig {eq}/{den} ({100 * eq / den:.2f}%)"
        )
    print(f"\n{'uf':<4} {'n':>7} {'%eq':>7} {'log':>11} {'BU':>11} {'log-BU':>10} {'contig':>7}")
    tot = [0, 0, 0, 0, 0, 0]
    for uf, a in sorted(ufacc.items()):
        print(
            f"{uf:<4} {a[0]:7d} {100 * a[1] / a[0]:6.2f}% "
            f"{a[2]:11d} {a[3]:11d} {a[2] - a[3]:10d} {a[5]:7d}"
        )
        for i in range(6):
            tot[i] += a[i]
    if tot[0]:
        print(
            f"{'BR':<4} {tot[0]:7d} {100 * tot[1] / tot[0]:6.2f}% "
            f"{tot[2]:11d} {tot[3]:11d} {tot[2] - tot[3]:10d} {tot[5]:7d}"
        )
    print("Y = computados_log vs soma QT_VOTOS Presidente do turno")


if __name__ == "__main__":
    main()
