"""
Ciclos de habilitação no logjez (1º × 2º).

O firmware em geral NÃO grava o título do ELEITOR
("Título digitado pelo mesário" sem o número — spec TSE 2022).
Grava o título do MESÁRIO nas linhas de bio/autorização.

Uso:
  LOGAUDIT_DATAS=02/10/2022 python3 -m logaudit.titulos \\
      --root /mnt/vhdx/eleicoes22 --out ciclos_t1 --uf AC --turno 1

  LOGAUDIT_DATAS=29/10/2022,30/10/2022 python3 -m logaudit.titulos \\
      --root /mnt/vhdx/eleicoes22/t2 --out ciclos_t2 --uf AC --turno 2

  python3 -m logaudit.titulos cruzar --t1 ciclos_t1 --t2 ciclos_t2
"""
from __future__ import annotations

import csv
import os
import re
import subprocess
import tempfile
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path

RE_LINHA = re.compile(
    r"^(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})\s+"
    r"(INFO|ALERTA|ERRO|EXTERNO)\s+"
    r"(\d+)\s+"
    r"(\S+)\s+"
    r"(.*?)\s+"
    r"([0-9A-Fa-f]{16})\s*$"
)
RE_TITULO = re.compile(r"\b(\d{11,12})\b")
RE_ARQ = re.compile(r"o\d+-(\d{5})(\d{4})(\d{4})\.logjez$", re.I)

DATAS_T1 = {"01/10/2022", "02/10/2022"}
DATAS_T2 = {"29/10/2022", "30/10/2022", "31/10/2022"}


def _datas_ok():
    raw = os.environ.get("LOGAUDIT_DATAS", "")
    if not raw:
        return None
    return {d.strip() for d in raw.split(",") if d.strip()}


def parse_secao_arquivo(path: Path):
    m = RE_ARQ.search(path.name)
    if not m:
        return "", "", ""
    return m.group(1), m.group(2), m.group(3)


def extrai_logd(logjez: Path, tmp: Path):
    r = subprocess.run(
        ["7z", "e", "-y", f"-o{tmp}", str(logjez)],
        capture_output=True,
    )
    if r.returncode != 0:
        return None
    for p in tmp.rglob("*"):
        if p.is_file() and p.name.lower() in {"logd.dat", "log.dat"}:
            return p
    files = [p for p in tmp.rglob("*") if p.is_file() and p.suffix.lower() == ".dat"]
    return files[0] if files else None


@dataclass
class Ciclo:
    uf: str = ""
    municipio: str = ""
    zona: str = ""
    secao: str = ""
    turno: str = ""
    arquivo: str = ""
    seq: int = 0
    ts_titulo: str = ""
    ts_hab: str = ""
    ts_comp: str = ""
    titulo_eleitor: str = ""
    titulo_fonte: str = ""
    titulo_mesario: str = ""
    bio_eleitor: str = ""
    auth_mesario: str = ""
    habilitado: int = 0
    computado: int = 0
    ja_votou: int = 0
    titulo_invalido: int = 0
    voto_C: int = 0


def classifica_msg(msg: str):
    m = msg
    ml = msg.lower()

    if "título digitado" in ml or "titulo digitado" in ml:
        return "titulo_digitado", ""
    if "aguardando digitação do título" in ml or "aguardando digitacao do titulo" in ml:
        return "aguarda_titulo", ""
    if "título inválido" in ml or "titulo invalido" in ml:
        return "titulo_invalido", ""
    if "já votou" in ml or "ja votou" in ml:
        return "ja_votou", ""
    if "não possui biometria" in ml or "nao possui biometria" in ml:
        return "sem_cadastro", ""
    if "dedo reconhecido" in ml:
        return "bio_match", ""
    if "digital capturada não corresponde" in ml or "digital capturada nao corresponde" in ml:
        return "bio_fail", ""
    if "solicita digital" in ml and "mesário" not in ml and "mesario" not in ml:
        return "bio_solicita", ""
    if "capturada a digital" in ml and "mesário" not in ml and "mesario" not in ml:
        return "bio_captura", ""
    if "eleitor foi habilitado" in ml:
        return "habilitado", ""
    if "voto do eleitor foi computado" in ml:
        return "computado", ""
    if "habilitação cancelada" in ml or "habilitacao cancelada" in ml:
        return "cancel", ""

    if "encontrada nos arquivos coletados" in ml:
        tit = RE_TITULO.search(m)
        return "auth_match_reg", tit.group(1) if tit else ""
    if "vai salvar a digital em novo arquivo" in ml:
        return "auth_save_new", ""
    if "não é possível associar" in ml or "nao e possivel associar" in ml:
        return "auth_match_post", ""
    if "solicita digital do mesário" in ml or "solicita digital do mesario" in ml:
        return "auth_solicita", ""
    if "pedido de leitura da biometria do mesário" in ml or "pedido de leitura da biometria do mesario" in ml:
        tit = RE_TITULO.search(m)
        return "mes_pedido", tit.group(1) if tit else ""
    if re.search(r"biometria do mes[aá]rio", ml) and "encontrada" in ml:
        tit = RE_TITULO.search(m)
        return "auth_match_reg", tit.group(1) if tit else ""
    if re.search(r"n[aã]o [eé] do mes[aá]rio", ml):
        tit = RE_TITULO.search(m)
        return "auth_no_match", tit.group(1) if tit else ""
    return "outro", ""


def ciclos_de_texto(text: str, meta: dict):
    datas = _datas_ok()
    cur = None
    out = []
    seq = 0
    fails = 0

    def fecha():
        nonlocal cur, seq, fails
        if cur is None:
            return
        if cur.auth_mesario in {"SAVE_NEW", "MATCH_POST"} and cur.computado:
            cur.voto_C = 1
        if cur.ts_titulo or cur.habilitado or cur.computado or cur.titulo_invalido or cur.ja_votou:
            seq += 1
            cur.seq = seq
            out.append(cur)
        cur = None
        fails = 0

    for raw in text.splitlines():
        m = RE_LINHA.match(raw)
        if not m:
            continue
        ts_s, _niv, _ue, _mod, msg, _mac = m.groups()
        try:
            ts = datetime.strptime(ts_s, "%d/%m/%Y %H:%M:%S")
        except ValueError:
            continue
        if datas and ts.strftime("%d/%m/%Y") not in datas:
            continue
        kind, tit = classifica_msg(msg)

        if kind == "titulo_digitado":
            fecha()
            cur = Ciclo(
                **meta,
                ts_titulo=ts_s,
                titulo_fonte="ausente_no_log",
            )
            extra = RE_TITULO.search(msg)
            if extra:
                cur.titulo_eleitor = extra.group(1)
                cur.titulo_fonte = "msg_titulo"
            continue
        if cur is None:
            if kind in {"habilitado", "computado", "sem_cadastro", "bio_match", "bio_fail"}:
                cur = Ciclo(**meta, ts_titulo=ts_s, titulo_fonte="ciclo_sem_titulo_msg")
            else:
                continue

        if kind == "titulo_invalido":
            cur.titulo_invalido = 1
            fecha()
            continue
        if kind == "ja_votou":
            cur.ja_votou = 1
            fecha()
            continue
        if kind == "sem_cadastro":
            cur.bio_eleitor = "sem_cadastro"
        elif kind == "bio_match":
            cur.bio_eleitor = "match"
        elif kind == "bio_fail":
            fails += 1
            if fails >= 4:
                cur.bio_eleitor = "fail4"
        elif kind == "auth_match_reg":
            cur.auth_mesario = "MATCH_REG"
            if tit:
                cur.titulo_mesario = tit
        elif kind == "auth_save_new":
            cur.auth_mesario = "SAVE_NEW"
        elif kind == "auth_match_post":
            cur.auth_mesario = "MATCH_POST"
        elif kind == "auth_no_match" and tit:
            cur.titulo_mesario = cur.titulo_mesario or tit
        elif kind == "cancel":
            cur.auth_mesario = "CANCEL"
            fecha()
            continue
        elif kind == "habilitado":
            cur.habilitado = 1
            cur.ts_hab = ts_s
        elif kind == "computado":
            cur.computado = 1
            cur.ts_comp = ts_s
            fecha()
            continue
    fecha()
    return out


def processa_arquivo(args):
    path, uf, turno = args
    path = Path(path)
    mun, zon, sec = parse_secao_arquivo(path)
    meta = {
        "uf": uf,
        "municipio": mun,
        "zona": zon,
        "secao": sec,
        "turno": turno,
        "arquivo": path.name,
    }
    with tempfile.TemporaryDirectory() as td:
        logd = extrai_logd(path, Path(td))
        if not logd:
            return []
        text = logd.read_bytes().decode("latin-1", "replace")
    return [asdict(c) for c in ciclos_de_texto(text, meta)]


CAMPOS = list(Ciclo.__dataclass_fields__.keys())


def roda_uf(root: Path, out: Path, uf: str, turno: str, workers: int = 8) -> Path:
    pasta = root / uf
    files = sorted(pasta.glob("*.logjez"))
    out.mkdir(parents=True, exist_ok=True)
    dest = out / f"ciclos_{uf}_t{turno}.csv"
    n = 0
    with dest.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=CAMPOS)
        w.writeheader()
        if not files:
            print(f"{uf}: 0 logjez")
            return dest
        args = [(str(f), uf, turno) for f in files]
        with ProcessPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(processa_arquivo, a) for a in args]
            done = 0
            for fut in as_completed(futs):
                rows = fut.result()
                for r in rows:
                    w.writerow(r)
                    n += 1
                done += 1
                if done % 100 == 0 or done == len(files):
                    print(f"  {uf} {done}/{len(files)} arquivos  ciclos={n}", flush=True)
    print(f"gravou {dest}  ciclos={n}")
    return dest


def cruzar(dir_t1: Path, dir_t2: Path, dest: Path) -> None:
    def load(d: Path, turno: str):
        by_mes = {}
        by_el = {}
        n_c = n = 0
        for p in d.glob("ciclos_*_t*.csv"):
            for r in csv.DictReader(p.open(encoding="utf-8", errors="replace")):
                n += 1
                n_c += int(r.get("voto_C") or 0)
                ksec = (
                    (r.get("uf") or "").upper(),
                    (r.get("municipio") or "").zfill(5),
                    (r.get("zona") or "").zfill(4),
                    (r.get("secao") or "").zfill(4),
                )
                tm = (r.get("titulo_mesario") or "").strip()
                if tm:
                    key = ksec + (tm,)
                    by_mes.setdefault(key, {"C": 0, "n": 0})
                    by_mes[key]["n"] += 1
                    by_mes[key]["C"] += int(r.get("voto_C") or 0)
                te = (r.get("titulo_eleitor") or "").strip()
                if te and r.get("titulo_fonte") == "msg_titulo":
                    key = ksec + (te,)
                    by_el.setdefault(key, {"C": 0, "n": 0})
                    by_el[key]["n"] += 1
                    by_el[key]["C"] += int(r.get("voto_C") or 0)
        return by_mes, by_el, n, n_c

    m1, e1, n1, c1 = load(dir_t1, "1")
    m2, e2, n2, c2 = load(dir_t2, "2")
    print(f"t1 ciclos={n1} voto_C={c1}  mesarios={len(m1)} eleitores_com_titulo={len(e1)}")
    print(f"t2 ciclos={n2} voto_C={c2}  mesarios={len(m2)} eleitores_com_titulo={len(e2)}")

    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(
            fh,
            fieldnames=[
                "tipo", "uf", "municipio", "zona", "secao", "id",
                "n1", "c1", "n2", "c2", "padrao",
            ],
        )
        w.writeheader()

        def pad(c1, c2, only1, only2):
            if only1:
                return "so_t1"
            if only2:
                return "so_t2"
            if c1 and c2:
                return "C_C"
            if c1 and not c2:
                return "C_ok"
            if (not c1) and c2:
                return "ok_C"
            return "ok_ok"

        cont = defaultdict(int)
        for key in set(m1) | set(m2):
            a, b = m1.get(key), m2.get(key)
            row = {
                "tipo": "mesario",
                "uf": key[0], "municipio": key[1], "zona": key[2], "secao": key[3],
                "id": key[4],
                "n1": a["n"] if a else 0,
                "c1": a["C"] if a else 0,
                "n2": b["n"] if b else 0,
                "c2": b["C"] if b else 0,
                "padrao": pad(a["C"] if a else 0, b["C"] if b else 0, b is None, a is None),
            }
            cont[row["padrao"]] += 1
            w.writerow(row)
        print("mesario", dict(cont))
        cont.clear()
        for key in set(e1) | set(e2):
            a, b = e1.get(key), e2.get(key)
            row = {
                "tipo": "eleitor",
                "uf": key[0], "municipio": key[1], "zona": key[2], "secao": key[3],
                "id": key[4],
                "n1": a["n"] if a else 0,
                "c1": a["C"] if a else 0,
                "n2": b["n"] if b else 0,
                "c2": b["C"] if b else 0,
                "padrao": pad(a["C"] if a else 0, b["C"] if b else 0, b is None, a is None),
            }
            cont[row["padrao"]] += 1
            w.writerow(row)
        print("eleitor (só se o log trouxe o número)", dict(cont) or "(vazio — esperado)")
    print("gravou", dest)


def main(argv=None):
    import argparse
    import sys
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd")

    p = sub.add_parser("extrair")
    p.add_argument("--root", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--uf", required=True)
    p.add_argument("--turno", required=True, choices=["1", "2"])
    p.add_argument("--workers", type=int, default=8)

    pc = sub.add_parser("cruzar")
    pc.add_argument("--t1", required=True)
    pc.add_argument("--t2", required=True)
    pc.add_argument("--out", default="cruzamento_titulos.csv")

    args_l = list(argv if argv is not None else sys.argv[1:])
    if args_l and args_l[0] not in {"extrair", "cruzar", "-h", "--help"}:
        args_l = ["extrair"] + args_l
    args = ap.parse_args(args_l)

    if args.cmd == "cruzar":
        cruzar(Path(args.t1), Path(args.t2), Path(args.out))
        return
    roda_uf(Path(args.root), Path(args.out), args.uf.upper(), args.turno, args.workers)


if __name__ == "__main__":
    main()
