
### TRATAMENTO DADOS ACESSOS MÓVEL DA ANATEL A PARTIR DO ARQUIVO ...\DOWNLOAD_ACESSOS\Acessos_Telefonia_Movel_YYYYMM-YYYYMM.csv
## Mantém apenas os registros do mês mais recente do arquivo
## RESULTADO SÃO 2 ARQUIVOS CSV NA PASTA ...\RESULTADO\
# 4_ACESSOS_MOVEIS_ANATEL_MES_RECENTE.csv > CONTEM A EXTRAÇÃO ORIGINAL FILTRANDO O MES MAIS RECENTE = ALGUNS TRATAMENTOS
# 5_ACESSOS_MOVEIS_ANATEL_MES_RECENTE_POR_MUN_OPER_TECNO.csv > É O ARQUIVO ANTERIOR AGRUPADO POR MUNICIPIO, OPERADORA e TECNOLOGIA ...
#    ...SOMANDO A QUANTIDADE DE ACESSOS

# FRAGILIDADES DO CÓDIGO:
## Se a fonte alterar os nomes das colunas, será necessário rever o código

def transforma_acessos():

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
  nome_log = str(caminho_log / str(datetime.today().strftime('%Y-%m-%d_%H-%M-%S') + "_log_5_trata_acessos") ) + '.txt'
  logger = logging.getLogger()
  fhandler = logging.FileHandler(filename=nome_log, mode='a')
  formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  fhandler.setFormatter(formatter)
  logger.addHandler(fhandler)
  logger.setLevel(logging.DEBUG)

  logging.debug("-----------  Início do log Tratamento Mosaico  -----------")

  #Prepara as variáveis de diretório
  diretorio = os.getcwd()
  caminho_import = Path(diretorio + "/DOWNLOAD_ACESSOS/") #Para funcionar tanto no windows como unix/linux
  caminho_export = Path(diretorio + "/RESULTADO/") #Para funcionar tanto no windows como unix/linux

  # O nome do arquivo de acessos varia, portanto precisamos pegar o nome do arquivo que termina com CSV
  # Assume que só vai existir um arquivo csv no diretório
  arquivos_csv = [arquivo for arquivo in os.listdir(caminho_import) if arquivo.endswith('.csv')]
  print(arquivos_csv[0])

  #Importação do CSV. Por problema de memoria, adotamos a estratégia de carregar por blocos (chunk)
  mylist = []
  #Codigo comentado é a versão para rodar no micro e não no colab
  for chunk in  pd.read_csv(caminho_import / arquivos_csv[0], sep=';', encoding = 'UTF-8', chunksize=100000, low_memory=False):
    mylist.append(chunk)
  dados_acessos_full = pd.concat(mylist, axis= 0)
  del mylist

  logging.debug("SUCESSO - Importação Acessos Brasil.csv")

  #Testa se alguma coluna esperada não está no arquivo original e acrescenta no log
  colunas_esperadas = ['Ano','Mês','Grupo Econômico','Empresa','CNPJ','Porte da Prestadora','UF','Município','Código IBGE Município','Código Nacional','Código Nacional (Chip)','Modalidade de Cobrança','Tecnologia','Tecnologia Geração','Tipo de Pessoa','Tipo de Produto','Acessos']
  for coluna in colunas_esperadas:
    if coluna not in dados_acessos_full.columns:
      logging.debug("ERRO - Coluna " + coluna + " não está presente no arquivo brasil.csv")

  #Força o tipo da variável das colunas para float e int
  campos_int = ['Acessos','Código IBGE Município']
  dados_acessos_full[campos_int] = dados_acessos_full[campos_int].apply(pd.to_numeric, downcast='integer', errors='coerce')

  logging.debug("SUCESSO - Conversão de formatos campos Int e Float")

  #Filtra apenas o mês mais recente
  maior_mes = dados_acessos_full['Mês'].max()
  dados_acessos_filtrado = dados_acessos_full[dados_acessos_full['Mês'] == maior_mes].copy()
  del dados_acessos_full

  logging.debug("SUCESSO - Filtro maior mês")

  # Troca o nome da Algar para bater com o arquivo de estações
  dados_acessos_filtrado.loc[dados_acessos_filtrado['Empresa'] == 'ALGAR (CTBC TELECOM)', 'Empresa'] = 'ALGAR'


  #Troca os nomes das colunas
  dados_acessos_filtrado.rename(columns={'Ano':'ANO','Mês':'MES','Grupo Econômico':'GRUPO_ECONOMICO','Empresa':'OPERADORA','CNPJ':'CNPJ','Porte da Prestadora':'PORTE_OPERADORA','UF':'UF','Município':'MUNICIPIO','Código IBGE Município':'COD_IBGE','Código Nacional':'COD_NACIONAL','Código Nacional (Chip)':'COD_NACIONAL_CHIP','Modalidade de Cobrança':'MOD_COBRANCA','Tecnologia':'TECNO','Tecnologia Geração':'TECNO_GERACAO','Tipo de Pessoa':'TIPO_PESSOA','Tipo de Produto':'TIPO_PRODUTO','Acessos':'ACESSOS'}, inplace=True)

  logging.debug("SUCESSO - Troca nomes campos")

  #RESULTADO(1)
  #Exporta o arquivo original com tratado com apenas os registros do ultimo mês
  dados_acessos_filtrado.to_csv(caminho_export / '4_ACESSOS_MOVEIS_ANATEL_MES_RECENTE.csv', 
                                encoding = 'latin-1', 
                                sep = ';', 
                                decimal=',', 
                                index = False)

  logging.debug("SUCESSO - Export(4)-4_ACESSOS_MOVEIS_ANATEL_MES_RECENTE.csv")

  #Trata o dado para agrupar por Município, Tecnologia, Acessos
  dados_acessos_filtrado_agrupado = dados_acessos_filtrado.groupby(['ANO','MES','COD_IBGE','UF','MUNICIPIO',
                                                                  'OPERADORA','TECNO_GERACAO'],
                                                                  as_index=False, dropna=False)['ACESSOS'].sum()
  dados_acessos_filtrado_agrupado.to_csv(caminho_export / '5_ACESSOS_MOVEIS_ANATEL_MES_RECENTE_POR_MUN_OPER_TECNO.csv', 
                                encoding = 'latin-1', 
                                sep = ';', 
                                decimal=',', 
                                index = False)

  logging.debug("SUCESSO - Export(5)-5_ACESSOS_MOVEIS_ANATEL_MES_RECENTE_POR_MUN_OPER_TECNO")

  logging.debug("-----------  Fim do log Tratamento Acessos  -----------")
  
  return True
