from __future__ import annotations
from dataclasses import dataclass

@dataclass
class ObservacaoParidade:
    tratamento: int
    computados: int
    computados_impar: int
    hab_manual_sem_titulo: int
    votos_sem_titulo: int
    def row(self) -> dict:
        return {
            "tratamento_auth_anomalo": self.tratamento,
            "computados": self.computados,
            "lsb": self.computados_impar,
            "hab_manual_sem_titulo": self.hab_manual_sem_titulo,
            "votos_sem_titulo": self.votos_sem_titulo,
        }

def de_auditor(aud) -> ObservacaoParidade:
    s = aud.snap
    return ObservacaoParidade(
        tratamento=int((s.mes_auth_save_new + s.mes_auth_match_post) > 0),
        computados=s.votos_computados,
        computados_impar=s.votos_computados % 2,
        hab_manual_sem_titulo=s.hab_manual_sem_titulo,
        votos_sem_titulo=s.votos_sem_titulo_mesario,
    )
