"""Nas seções contingencia/parcial: tenta original + substituta.

1) outro .logjez da mesma mun/zona/seção
2) *.jez / *ME.jez embutido no 7z
"""
from __future__ import annotations

import csv
import os
import re
import subprocess
import tempfile
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

RE_COMP = re.compile(r"O voto do eleitor foi computado", re.I)
RE_HAB = re.compile(r"Eleitor foi habilitado", re.I)
RE_DATA = re.compile(r"^(\d{2}/\d{2}/\d{4})")
RE_ARQ = re.compile(r"o\d+-(\d{5})(\d{4})(\d{4})", re.I)


def conta_dat(text, datas):
    hab = comp = 0
    for line in text.splitlines():
        m = RE_DATA.match(line)
        if datas and (not m or m.group(1) not in datas):
            continue
        if RE_COMP.search(line):
            comp += 1
        elif RE_HAB.search(line):
            hab += 1
    return hab, comp


def sete(cmd):
    return subprocess.run(cmd, capture_output=True)


def extrai_todos(logjez, tmp):
    r = sete(["7z", "e", "-y", f"-o{tmp}", str(logjez)])
    if r.returncode != 0:
        return []
    return [p for p in tmp.rglob("*") if p.is_file()]


def processa_arquivo(logjez, datas):
    hab = comp = 0
    inner = []
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        files = extrai_todos(logjez, td)
        dats = [p for p in files if p.name.lower() in {"logd.dat", "log.dat"} or p.suffix.lower() == ".dat"]
        if dats:
            text = dats[0].read_bytes().decode("latin-1", "replace")
            hab, comp = conta_dat(text, datas)
        for p in files:
            if p.suffix.lower() in {".jez", ".logjez"} or "me.jez" in p.name.lower():
                sub = td / ("in_" + p.name)
                sub.mkdir(exist_ok=True)
                sete(["7z", "e", "-y", f"-o{sub}", str(p)])
                idats = [q for q in sub.rglob("*") if q.is_file() and q.suffix.lower() == ".dat"]
                if not idats:
                    continue
                t2 = idats[0].read_bytes().decode("latin-1", "replace")
                h2, c2 = conta_dat(t2, datas)
                inner.append({"nome": p.name, "hab": h2, "comp": c2})
    return hab, comp, inner


def indexa_logs(root):
    idx = defaultdict(list)
    for p in root.rglob("*.logjez"):
        m = RE_ARQ.search(p.name)
        if not m:
            continue
        uf = p.parent.name.upper()
        k = (uf, m.group(1), m.group(2), m.group(3))
        idx[k].append(p)
    return idx


def worker(args):
    paths, datas = args
    tot_h = tot_c = 0
    inners = []
    nomes = []
    for p in paths:
        h, c, inn = processa_arquivo(Path(p), datas)
        tot_h += h
        tot_c += c
        inners.extend(inn)
        nomes.append(Path(p).name)
    return {
        "hab_ext": tot_h,
        "comp_ext": tot_c,
        "n_arq": len(paths),
        "arquivos": "|".join(nomes),
        "n_inner": len(inners),
        "comp_inner": sum(x["comp"] for x in inners),
        "hab_inner": sum(x["hab"] for x in inners),
        "inner_nomes": "|".join(x["nome"] for x in inners),
    }


def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--join", required=True)
    ap.add_argument("--root", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--datas", default="29/10/2022,30/10/2022")
    ap.add_argument("--status", default="contingencia,parcial")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args(argv)

    datas = {d.strip() for d in args.datas.split(",") if d.strip()}
    want = {s.strip() for s in args.status.split(",") if s.strip()}

    rows = []
    with open(args.join, encoding="utf-8", errors="replace") as fh:
        for r in csv.DictReader(fh):
            if (r.get("status_bu") or "") not in want:
                continue
            rows.append(r)
    if args.limit:
        rows = rows[: args.limit]
    print(f"secoes alvo {len(rows)}  status={want}")

    root = Path(args.root)
    print("indexando logjez...")
    idx = indexa_logs(root)
    print(f"chaves {len(idx)}")

    jobs, meta = [], []
    for r in rows:
        uf = (r.get("uf") or "").upper()
        k = (uf, str(r.get("municipio") or "").zfill(5),
             str(r.get("zona") or "").zfill(4),
             str(r.get("secao") or "").zfill(4))
        paths = idx.get(k) or []
        if not paths and r.get("arquivo"):
            paths = list(root.joinpath(uf).glob(r["arquivo"])) if uf else []
        jobs.append(([str(p) for p in paths], datas))
        meta.append((r, k, paths))

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    n_eq = n_melhor = n_sem = 0
    print(f"processando {len(jobs)}...")
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        results = list(ex.map(worker, jobs))
    with out.open("w", newline="", encoding="utf-8") as fh:
        w = None
        for i, ((r, k, paths), res) in enumerate(zip(meta, results), 1):
            raw_bu = str(r.get("bu_pres") or "").strip()
            bu = int(raw_bu) if raw_bu else 0
            soma = res["comp_ext"] + res["comp_inner"]
            if not paths:
                flag = "sem_arquivo"
                n_sem += 1
            elif soma == bu and bu:
                flag = "ok_soma"
                n_eq += 1
            elif abs(soma - bu) < abs(int(r.get("computados") or 0) - bu):
                flag = "melhorou"
                n_melhor += 1
            else:
                flag = "ainda_abre"
            row = {
                "uf": k[0], "municipio": k[1], "zona": k[2], "secao": k[3],
                "status_bu": r.get("status_bu"),
                "computados_snapshot": r.get("computados"),
                "bu_pres": bu,
                "diff_antes": r.get("diff_log_bu"),
                **res,
                "comp_soma": soma,
                "diff_depois": (soma - bu) if bu else "",
                "flag": flag,
            }
            if w is None:
                w = csv.DictWriter(fh, fieldnames=list(row.keys()))
                w.writeheader()
            w.writerow(row)
            if i % 50 == 0 or i == len(meta):
                print(f"  {i}/{len(meta)}", flush=True)
    print(f"gravou {out}")
    print(f"ok_soma={n_eq}  melhorou={n_melhor}  sem_arquivo={n_sem}  n={len(meta)}")


if __name__ == "__main__":
    main()
