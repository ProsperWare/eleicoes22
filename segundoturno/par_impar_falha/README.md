# par impar falha

### Análise sobre um resultado de processamentos e queries.

Tomando os dados do @tonylampada emprestados de onde este repositório foi , alterei o processamento dos dados de log incluindo um status do comportamento do mesário. como pode ser observado no [log de alterações](https://github.com/ProsperWare/eleicoes22/commit/01819a10cee07309fdccbee18e3d09503c64aa19)

A Partir das tabelas geradas, criei dois públicos sumariados por urna:

1 - [Totalização de votos sem Ocorrências na Identificação do Mesário](_select_v_CD_MUNICIPIO_v_NR_ZONA_v_NR_SECAO_0_quant_COALESCE_sum_202212221514.csv)

2 - [Totalização de votos com Ocorrências na Identificação do Mesário](_select_v_CD_MUNICIPIO_v_NR_ZONA_v_NR_SECAO_sum_v_quant_quant_CO_202212221449.csv)

Os votos postados em cada urna podem ser validados contra o boletim de urna no site da justiça eleitoral.

As linhas de log consideradas foram:

  A - Não encontrou digital coletada em nenhum dos arquivos, vai salvar a digital em novo arquivo.

  B - Digital coletada bate com uma digital gravada após o registro inicial de mesário. Não é possível associar a habilitação a um mesário.

Na fase de identificação do eleitor.

O tipo de ocorrência observado deve ainda ser tipificado, mas, ao meu ver, um voto que deveria ser supervisionado por dois mesários depositado sem supervisão por mesários invalida a urna durante a contagem dos votos no voto em papel.

Com este viés criei este resumo:

3 - [88% das Urnas sofreram fraudes tecnicamente comprováveis](88%%20das%20sofreram%20fraudes%20tecnicamente%20comprováveis.pdf)

E utilizando parâmetros estatisticos foi ainda criado este documento:

4 - [O impacto de defeitos observados na biometria do mesário sobre o resultado do segundo turno das eleições 2022v2](O%20impacto%20de%20defeitos%20observados%20na%20biometria%20do%20mesário%20sobre%20o%20resultado%20do%20segundo%20turno%20das%20eleições%20.pdf)
