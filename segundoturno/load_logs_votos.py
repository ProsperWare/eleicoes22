from datetime import datetime
import os
import dataset
import asyncio
import random
import time
from collections import Counter
from threading import Thread
from queue import Queue
from py7zr import SevenZipFile
from multiprocessing import Pool
from sqlalchemy.pool import NullPool
import sys

CHUNKSIZE = 2500

class LinhaInesperada(Exception):
    pass

START, WAIT_TITULO, WAIT_HABILITADO, WAIT_COMPUTADO = 1, 2, 3, 4
SEGUNDOTURNO = 'Iniciando aplicação - ELEIÇÃO OFICIAL SEGUNDO TURNO - Oficial - 2º turno'
TITULODIGITADO = 'Título digitado pelo mesário'
ELEITORHABILITADO = 'Eleitor foi habilitado'
VOTOCOMPUTADO = 'O voto do eleitor foi computado'

DIGITAL_MESARIO_CAPTURADA = 'Capturada a digital do mes'

class LogVotoStateMachine():
    def __init__(self, lines) -> None:
        self.lines = lines
        self.voto = None
        self.votos = []

    def _get_timestamp(self, line):
        return datetime.strptime(' '.join(line.split()[:2]), '%d/%m/%Y %H:%M:%S')

    def _breakon(self, line, deixas):
        for deixa in deixas:
            if deixa in line:
                raise LinhaInesperada(line)

    def comeca_voto(self, line):
        self.voto = {}
        self.voto['DT_INICIO'] = self._get_timestamp(line)
        self.state = WAIT_COMPUTADO

    def termina_voto(self, line, MESARIO_ID):
        self.voto['DT_FIM'] = self._get_timestamp(line)
        self.voto['TEMPO_SEGUNDOS'] = (self.voto['DT_FIM'] - self.voto['DT_INICIO']).total_seconds()
        self.voto['MESARIO_ID'] = MESARIO_ID
        self.votos.append(self.voto)
        self.voto = None
        self.state = WAIT_TITULO

    def get_votos(self):
        """
        START ----(segundoturno)---> START1
        WAIT_TITULO ----(titulodigitado)---> WAIT_VOTOGP
        WAIT_COMPUTADO ----(votocomputado)---> WAIT_TITULO
        """
        self.state = START
        for i, line in enumerate(self.lines):
            try:
                if self.state == START:
                    if SEGUNDOTURNO in line:
                        self.state = WAIT_TITULO
                elif self.state == WAIT_TITULO:
                    self._breakon(line, {VOTOCOMPUTADO})
                    if TITULODIGITADO in line:
                        MESARIO_ID = 'normal'
                        self.comeca_voto(line)
                elif self.state == WAIT_COMPUTADO:
                    if DIGITAL_MESARIO_CAPTURADA in line and MESARIO_ID == 'normal':
                        MESARIO_ID = linepre.split('\t')[4]
                    if VOTOCOMPUTADO in line:
                        self.termina_voto(line, MESARIO_ID)
                    if TITULODIGITADO in line:
                        MESARIO_ID = 'normal'
                        self.comeca_voto(line)
                linepre=line
            except LinhaInesperada as e:
                raise LinhaInesperada(f'[linha {i+1}] {line}')
        return self.votos

class AttributeValueCount:
    def __init__(self, keys, iterable, *, missing=None):
        self._missing = missing
        self.length = 0
        self._counts = {}
        self.keys = keys
        self.update(iterable)

    def update(self, iterable):
        categories = set(self._counts)
        categories.update(self.keys)
        for length, element in enumerate(iterable, self.length):
            for category in categories:
                try:
                    counter = self._counts[category]
                    counter[element.get(category, self._missing)] += 1
                except KeyError:
                    self._counts[category] = counter = Counter({self._missing: length})
                    pass
#                    self._counts[category] = counter = Counter({self._missing: length})
        self.length = length + 1

    def add(self, element):
        self.update([element])

    def __getitem__(self, key):
        return self._counts[key]

    def summary(self, key=None):
        if key is None:
            for key in self._counts:
                self.summary(key) 
            return 
        print('-- {} --'.format(key))
        for value, count in self._counts[key].items():
            print('{}: {}'.format(value, count))
        return

def _get_mzs(filename):
    cdmun = int(filename[7:12])
    nrzona = int(filename[12:16])
    nrsecao = int(filename[16:20])
    return cdmun, nrzona, nrsecao

def create_table(db):
    table = db.create_table('votosv2')
    table.create_column('idsecao', db.types.text)
    table.create_column('SG_UF', db.types.text)
    table.create_column('CD_MUNICIPIO', db.types.bigint)
    table.create_column('NR_ZONA', db.types.bigint)
    table.create_column('NR_SECAO', db.types.bigint)
    table.create_column('DT_INICIO', db.types.datetime)
    table.create_column('DT_FIM', db.types.datetime)
    table.create_column('TEMPO_SEGUNDOS', db.types.float)
    table.create_column('MESARIO_ID', db.types.text)
    return table

def producer0():
    print("start producer")
    uf = quf.get()
    d = f'2t_logs/{uf}'
    logs = [l for l in os.listdir(d) if l.endswith('.logjez') or l.endswith('.logsajez')]
    # filename = 'o00407-7691000660029.logjez'
    # i = logs.index(filename)
    # logs = logs[i:]
    k = 0
    for log in logs:
        if qlogs.qsize() > 10000:
            qlogs.join()
        qlogs.put((uf, d , log))

def consumer2():
    print("start consumer1")
    regs=[]
    keys=['SG_UF', 'CD_MUNICIPIO', 'MESARIO_ID']
    db = dataset.connect('sqlite:///eleicao22_2t.db')
    # db = dataset.connect('postgresql://postgres:password@localhost:5432/postgres', engine_kwargs={'poolclass': NullPool})
    while True:
        regs += qregs.get()
        if len(regs) > (CHUNKSIZE - 1) or qlogs.qsize() == 0:
            c = AttributeValueCount(keys, regs, missing='Non existent')
            table = db['votosv2']
            table.insert_many(regs, chunk_size=CHUNKSIZE + 500)
            print(f'Consumer2 {datetime.today().strftime("%Y-%m-%d-%H:%M:%S")} regs-{len(regs)} quf-{quf.qsize()} qlogs-{qlogs.qsize()} qregs-{qregs.qsize()}')
            c.summary()
            regs=[]
            if qlogs.qsize() > 0 or quf.qsize() > 0:
                time.sleep(1)
        qregs.task_done()
        # print(f"regs {len(regs)} inseridos")

def producer1():
    print("start producer1")
    while True:
        regs=[]
        uf, d, log = qlogs.get()
    #      print(log)
        try:
            cdmun, nrzona, nrsecao = _get_mzs(log)
            idsecao = f'{uf}_{cdmun}_{nrzona}_{nrsecao}'
#       print(cdmun, nrzona, nrsecao)
            with SevenZipFile(f'{d}/{log}') as f:
                lines = f.readall()['logd.dat'].readlines()
                lines = [line.decode(encoding='iso-8859-1') for line in lines]
                votos = LogVotoStateMachine(lines).get_votos()
                qregs.put([{
                    'idsecao': f'{uf}_{cdmun}_{nrzona}_{nrsecao}',
                    'SG_UF': uf,
                    'CD_MUNICIPIO': cdmun,
                    'NR_ZONA': nrzona,
                    'NR_SECAO': nrsecao,
                    'DT_INICIO': voto['DT_INICIO'],
                    'DT_FIM': voto['DT_FIM'],
                    'TEMPO_SEGUNDOS': voto['TEMPO_SEGUNDOS'],
                    'MESARIO_ID':voto['MESARIO_ID']
                } for voto in votos])
        except Exception as e:
            raise Exception(f'Erro processando log {idsecao}')
        qlogs.task_done()
print("Criando filas e tabelas")

quf=Queue()
qregs=Queue()
qlogs=Queue()
db = dataset.connect('sqlite:///eleicao22_2t.db')
# db = dataset.connect('postgresql://postgres:password@localhost:5432/postgres', engine_kwargs={'poolclass': NullPool})

try:
    table = create_table(db)
    table.delete()
    print("Sucesso ", table.columns, table.find(id=0,_limit=1))
except:
    print("Com Erro")

print("Processando ")

estadosBrasileiros = {
    'AC': 'Acre',
    'AL': 'Alagoas',
    'AP': 'Amapá',
    'AM': 'Amazonas',
    'BA': 'Bahia',
    'CE': 'Ceará',
    'DF': 'Distrito Federal',
    'ES': 'Espírito Santo',
    'GO': 'Goiás',
    'MA': 'Maranhão',
    'MT': 'Mato Grosso',
    'MS': 'Mato Grosso do Sul',
    'MG': 'Minas Gerais',
    'PA': 'Pará',
    'PB': 'Paraíba',
    'PR': 'Paraná',
    'PE': 'Pernambuco',
    'PI': 'Piauí',
    'RJ': 'Rio de Janeiro',
    'RN': 'Rio Grande do Norte',
    'RS': 'Rio Grande do Sul',
    'RO': 'Rondônia',
    'RR': 'Roraima',
    'SC': 'Santa Catarina',
    'SP': 'São Paulo',
    'SE': 'Sergipe',
    'TO': 'Tocantins',
  }

uf = sys.argv[1]
if uf:
    quf.put(uf)
else:
    for uf in estadosBrasileiros.keys():
        quf.put(uf)
producer0()

for i in range(6):
    tc2=Thread(target=producer1)
    tc2.daemon = True
    tc2.start()
for i in range(3):
    tc1=Thread(target=consumer2)
    tc1.daemon = True
    tc1.start()
    time.sleep(10)

while quf.qsize() != 0:
    producer0()    
    print(f'Start {datetime.today().strftime("%Y-%m-%d-%H:%M:%S")} quf-{quf.qsize()} qlogs-{qlogs.qsize()} qregs-{qregs.qsize()}')

qregs.join()
qlogs.join()
quf.join()

# else:
#     print("Criando Tabela")
#     try:
#         with dataset.connect('sqlite:///eleicao22_2t.db') as db:
#              table = create_table(db)
#         print("Sucesso", table.columns, table.find(id=0,_limit=1))
#     except:
#         print("Com Erro")


# with Pool(12) as p:
#     print(p.map(vai, UFS))

# vai('PR')
