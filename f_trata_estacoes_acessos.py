
### TRATAMENTO E UNIAO DOS DADOS DE ESTACOES E ACESSOS MÓVEL POR MUNICIPIO 
## RESULTADO É 1 ARQUIVOS CSV NA PASTA ...\RESULTADO\
# 6_ESTACOES_ACESSOS_MOVEIS_MUNICIPIO_OPERADORA_TECNOLOGIA.csv

# FRAGILIDADES DO CÓDIGO:
## Se a fonte alterar os nomes das colunas, será necessário rever o código

def transforma_estacoes_acessos():

  # FRAGILIDADES DO CÓDIGO:
  ## Se a fonte alterar os nomes a estrutura ou a origem dos dados, será necessário rever o código

  import numpy as np
  import pandas as pd
  import os
  from datetime import datetime
  from pathlib import Path
  import logging

  #Configura o log que vai ser salvo no arquivo log_data no dir \LOG
  #Apenas cria o diretorio do log caso não exista
  diretorio = os.getcwd()
  caminho_log = Path(diretorio + "/LOG/") #Para funcionar tanto no windows como unix/linux
  if not os.path.exists(caminho_log):
      os.makedirs(caminho_log)
  #Cria o log e configura (o nome do arquivo tem a data e hora da criação)
  nome_log = str(caminho_log / str(datetime.today().strftime('%Y-%m-%d_%H-%M-%S') + "_log_6_trata_estacoes_acessos") ) + '.txt'
  logger = logging.getLogger()
  fhandler = logging.FileHandler(filename=nome_log, mode='a')
  formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  fhandler.setFormatter(formatter)
  logger.addHandler(fhandler)
  logger.setLevel(logging.DEBUG)

  logging.debug("-----------  Início do log Tratamento Estacoes e Acessos  -----------")

  #Prepara as variáveis de diretório
  diretorio = os.getcwd()
  caminho_import = Path(diretorio + "/RESULTADO/") #Para funcionar tanto no windows como unix/linux
  caminho_export = Path(diretorio + "/RESULTADO/") #Para funcionar tanto no windows como unix/linux
  arquivo_estacoes = '3_RADAR_ANATEL_SITE_TECNO_FREQ.csv'
  arquivo_acessos = '5_ACESSOS_MOVEIS_ANATEL_MES_RECENTE_POR_MUN_OPER_TECNO.csv'

  #Importação  ESTACOES CSV - Por problema de memoria, adotamos a estratégia de carregar por blocos (chunk)
  mylist = []
  for chunk in  pd.read_csv(caminho_import / arquivo_estacoes, decimal=',', dtype={'CodMunicipio': int},
                            sep=';', encoding = 'latin-1', chunksize=100000, low_memory=False):
    mylist.append(chunk)
  dados_estacoes_full = pd.concat(mylist, axis= 0)
  del mylist

  logging.debug("SUCESSO - Importação Estacoes.csv")

  #Importação  ACESSOS CSV - Por problema de memoria, adotamos a estratégia de carregar por blocos (chunk)
  mylist = []
  for chunk in  pd.read_csv(caminho_import / arquivo_acessos, sep=';', decimal=",",
                            encoding = 'latin-1', chunksize=100000, low_memory=False):
    mylist.append(chunk)
  dados_acessos_full = pd.concat(mylist, axis= 0)
  del mylist

  logging.debug("SUCESSO - Importação Acessos.csv")

  #Trata os dados de estacões agrupando por MUNICIPIO/OPERADORA/TECNOLOGIA
  dados_estacoes_agrupado = dados_estacoes_full.groupby(['DATA_EXTRACAO','SiglaUf', 
                                                            'CodMunicipio','MUNICIPIO','POPULACAO','RANGE_POP','CAPITAL',
                                                            'OPERADORA','TECNO'],
                                                            as_index=False, dropna = False)['BANDA_MHZ'].sum()

  logging.debug("SUCESSO - Agrupamento Estacoes.csv")

  # Realiza o join entre df1 e df2 usando 'key' como chave
  estacoes_acessos_joined = pd.merge(dados_acessos_full, dados_estacoes_agrupado, how='left',
                                    right_on=['CodMunicipio', 'OPERADORA','TECNO'], 
                                    left_on=['COD_IBGE', 'OPERADORA', 'TECNO_GERACAO'], 
                                    suffixes=('_df1', '_df2'))

  # Seleciona apenas as colunas desejadas no resultado
  estacoes_acessos_joined_resultado = estacoes_acessos_joined[['COD_IBGE','UF','MUNICIPIO_df1', 
                                                              'OPERADORA','TECNO_GERACAO','BANDA_MHZ','ACESSOS']]

  # Troca o nome das colunas
  estacoes_acessos_joined_final = estacoes_acessos_joined_resultado.rename(columns={'MUNICIPIO_df1': 'MUNICIPIO'})

  #RESULTADO(6)
  #Exporta a junção das estações e acessos a nível de: MUNICIPIO/OPERADORA/TECNOLOGIA
  estacoes_acessos_joined_final.to_csv(caminho_export / '6_ESTACOES_ACESSOS_MOVEIS_MUNICIPIO_OPERADORA_TECNOLOGIA.csv', 
                                  encoding = 'latin-1', 
                                  sep = ';', 
                                  decimal=',', 
                                  index = False)

  logging.debug("SUCESSO - Export(6)-6_ESTACOES_ACESSOS_MOVEIS_MUNICIPIO_OPERADORA_TECNOLOGIA.csv")

  logging.debug("-----------  Fim do log Tratamento Estacoes Acessos  -----------")
  
  return True