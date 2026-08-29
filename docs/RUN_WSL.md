# Rodar no WSL

```bash
cd /mnt/vhdx_ext4/eleicoes22
git pull
chmod +x get_all_minspace.sh
ROOT=$(pwd) TURNO=1t ./get_all_minspace.sh

PYTHONPATH=$(pwd) python3 -m logaudit.batch --root . --out audit_out --uf AC --limit 50 --workers 8
```

Nao rode get_all.sh antigo.
