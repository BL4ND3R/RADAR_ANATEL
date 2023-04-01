
### TRATAMENTO DADOS LICENCIAMENTO ESTAÇÕES RÁDIO BASE (ERB) MOSAICO ANATEL A PARTIR DO ARQUIVO ...\DOWNLOAD\brasil.csv
## Mantém apenas registros SMP (Telefonica Móvel) e registros de Radio Frequencia (RF) celular das principais operadoras:
## Algar, Claro, Oi, Sercomtel, Tim, Vivo
## RESULTADO SÃO 3 ARQUIVOS CSV NA PASTA ...\RESULTADO\
# 1_RADAR_ANATEL_CELULA_ORIGINAL.csv > CONTEM A EXTRAÇÃO BRUTA SEM GRANDES TRATAMENTOS
# 2_RADAR_ANATEL_CELULA_SEM_DUPLICIDADE.csv > É O ARQUIVO ANTERIOR SEM DUPLICIDADES
# 3_RADAR_ANATEL_SITE_TECNO_FREQ.csv > ARQUIVO A NÍVEL DE ESTACAO/TECNOLOGIA/FREQUENCIA COM A SOMA DA BANDA

# FRAGILIDADES DO CÓDIGO:
## Se a fonte alterar os nomes das colunas, será necessário rever o código
## Caso apareça alguma frequencia nova e seja neessária a padronização, alterar o trecho do código com: ">>>>>>>>>>>"

def transforma():

  import numpy as np
  import pandas as pd
  import os
  import glob
  from datetime import datetime
  import re
  from pathlib import Path
  import logging

  #Configura o log que vai ser salvo no arquivo log_data no dir \LOG
  #Apenas cria o diretorio do log caso não exista
  diretorio = os.getcwd()
  caminho_log = Path(diretorio + "/LOG/") #Para funcionar tanto no windows como unix/linux
  if not os.path.exists(caminho_log):
      os.makedirs(caminho_log)
  #Cria o log e configura (o nome do arquivo tem a data e hora da criação)
  nome_log = str(caminho_log / str(datetime.today().strftime('%Y-%m-%d_%H-%M-%S') + "_log_3_tratamento") ) + '.txt'
  logger = logging.getLogger()
  fhandler = logging.FileHandler(filename=nome_log, mode='a')
  formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  fhandler.setFormatter(formatter)
  logger.addHandler(fhandler)
  logger.setLevel(logging.DEBUG)

  logging.debug("-----------  Início do log Tratamento Mosaico  -----------")

  #Variável com os nomes das operadoras
  operadoras = ['TELEFONICA BRASIL S.A.','TELEFÔNICA BRASIL S.A.','CLARO S.A.','TIM S/A','TIM S A                                                                                                                 ','TIM S/A ','TIM S A','OI MÓVEL S.A.','OI MÓVEL S.A. - EM RECUPERAÇÃO JUDICIAL ','ALGAR TELECOM S/A','SERCOMTEL S.A. TELECOMUNICAÇÕES']

  #Testa se o diretório existe, senão cria \RESULTADO. Se existir o \RESULTADO, limpa o diretório
  caminho_export = Path(diretorio + "/RESULTADO/") #Para funcionar tanto no windows como unix/linux
  if not os.path.exists(caminho_export):
      os.makedirs(caminho_export)
  else:
      files = glob.glob(str(caminho_export / '*'))
      for f in files:
          os.remove(f)

  logging.debug("SUCESSO - Cria/Limpa o Diretório RESULTADO")

  #Importação do CSV. Por problema de memoria, adotamos a estratégia de carregar por blocos (chunk)
  mylist = []
  caminho_import = Path(diretorio + "/DOWNLOAD/") #Para funcionar tanto no windows como unix/linux
  for chunk in  pd.read_csv(caminho_import / "brasil.csv", encoding = 'latin-1', chunksize=100000, low_memory=False):
      mylist.append(chunk)
  dados_anatel_full = pd.concat(mylist, axis= 0)
  del mylist #Deleta o Dataframe para economizar memória

  logging.debug("SUCESSO - Importação brasil.csv")

  #Testa se alguma coluna esperada não está no arquivo original e acrescenta no log
  colunas_esperadas = ['Status.state','NomeEntidade','NumFistel','NumServico','NumAto','NumEstacao','EnderecoEstacao','EndComplemento','SiglaUf','CodMunicipio','DesignacaoEmissao','Tecnologia','tipoTecnologia','meioAcesso','FreqTxMHz','FreqRxMHz','Azimute','CodTipoClasseEstacao','ClassInfraFisica','CompartilhamentoInfraFisica','CodTipoAntena','CodEquipamentoAntena','GanhoAntena','FrenteCostaAntena','AnguloMeiaPotenciaAntena','AnguloElevacao','Polarizacao','AlturaAntena','CodEquipamentoTransmissor','PotenciaTransmissorWatts','Latitude','Longitude','CodDebitoTFI','DataLicenciamento','DataPrimeiroLicenciamento','NumRede','_id','DataValidade','NumFistelAssociado','NomeEntidadeAssociado']
  for coluna in colunas_esperadas:
    if coluna not in dados_anatel_full.columns:
      logging.debug("ERRO - Coluna " + coluna + " não está presente no arquivo brasil.csv")

  #Força o tipo da variável das colunas para float e int
  campos_int = ['NumFistel','NumServico','NumAto','NumEstacao','CodMunicipio']
  campos_float = ['Azimute','FreqTxMHz','FreqRxMHz','GanhoAntena','FrenteCostaAntena','AnguloMeiaPotenciaAntena','AnguloElevacao','AlturaAntena','PotenciaTransmissorWatts','Latitude','Longitude']
  dados_anatel_full[campos_int] = dados_anatel_full[campos_int].apply(pd.to_numeric, downcast='integer', errors='coerce')
  dados_anatel_full[campos_float] = dados_anatel_full[campos_float].apply(pd.to_numeric, downcast='float', errors='coerce')

  logging.debug("SUCESSO - Conversão de formatos campos Int e Float")

  #filtra apenas as principais operadoras
  dados_anatel_filtrado = dados_anatel_full[dados_anatel_full['NomeEntidade'].isin(operadoras)]
  #Filtra apenas serviço do tipo "FB" (SMP Acesso, descarta TX)
  dados_anatel_filtrado = dados_anatel_filtrado[dados_anatel_filtrado['CodTipoClasseEstacao'] == 'FB']
  #Filtra o serviço SMP (010)
  dados_anatel_filtrado = dados_anatel_filtrado[dados_anatel_filtrado['NumServico'] == 10]
  #Filtra DesignacaoEmissao diferente de nulo e ERRO
  dados_anatel_filtrado = dados_anatel_filtrado[(dados_anatel_filtrado['DesignacaoEmissao'].notnull()) 
                                                & (dados_anatel_filtrado['DesignacaoEmissao'] != 'ERRO') ]
  #Filtra as tecnologias diferente de vazio
  dados_anatel_filtrado = dados_anatel_filtrado[dados_anatel_filtrado['Tecnologia'].notnull()]
  #Deleta o dataframe dados_anatel_full para economizar memoria
  del dados_anatel_full

  logging.debug("SUCESSO - Filtros:Operadora,CodTipoClasseEstacao,NumServico,DesignacaoEmissao")

  # Criar uma coluna chamada "OPERADORA" e popula com os nomes fantasia das operadoras
  def categoria_operadora(row):
      if row['NomeEntidade'] == 'TELEFONICA BRASIL S.A.' or row['NomeEntidade'] == 'TELEFÔNICA BRASIL S.A.':
        return 'VIVO'
      elif row['NomeEntidade'] == 'CLARO S.A.':
        return 'CLARO'
      elif row['NomeEntidade'].find('TIM') == 0:
        return 'TIM'
      elif row['NomeEntidade'] == 'OI MÓVEL S.A.' or row['NomeEntidade'] == 'OI MÓVEL S.A. - EM RECUPERAÇÃO JUDICIAL ':
        return 'OI'
      elif row['NomeEntidade'] == 'ALGAR TELECOM S/A':
        return 'ALGAR'
      elif row['NomeEntidade'] == 'SERCOMTEL S.A. TELECOMUNICAÇÕES':
        return 'SERCOMTEL'
      else:
        return row['NomeEntidade']

  dados_anatel_filtrado['OPERADORA'] = dados_anatel_filtrado.apply(lambda row: categoria_operadora(row), axis=1)

  logging.debug("SUCESSO - Criação Campo OPERADORA")

  # Condições para substituir Largura de Banda
  def categoria_largura_banda(row):
      if row['DesignacaoEmissao'][:4].find('M') != -1: #Vefica se contem a letra M (Megabyte) nos 4 primeiros digitos da Designação Emissão
        return float(re.findall(r'\d+', row['DesignacaoEmissao'][:4].replace('M','.'))[0]) #Substitui a letra M por ponto(.) e retira o numero do texto e pega o primeiro item da lista (re.findall)
      elif row['DesignacaoEmissao'][:4].find('K') != -1: #Vefica se contem a letra K (Kilobyte) nos 4 primeiros digitos da Designação Emissão
        return float(re.findall(r'\d+', row['DesignacaoEmissao'][:4].replace('K','.'))[0])/1000 #Substitui a letra M por ponto(.) e retira o numero do texto e pega o primeiro item da lista (re.findall) e divide por Mil (para transoforma em)
      else:
        return 0
  #Cria uma coluna para a largura de banda em Mbytes com base no campo Designação Emissão
  dados_anatel_filtrado['BANDA_MHZ'] = dados_anatel_filtrado.apply(lambda row: categoria_largura_banda(row), axis=1)
  #Converte a Largura de Banda em float
  dados_anatel_filtrado['BANDA_MHZ'] = pd.to_numeric(dados_anatel_filtrado['BANDA_MHZ'], downcast="float", errors='coerce')

  logging.debug("SUCESSO - Criação Campo BANDA_MHZ")

  #Cria uma coluna de "frequencia" e popula com a frequencia principal. A conta é o MAX do FreqTxMHz e FrepRxMHz, 
  # dividido por 1000, depois aplica "// e /" para chegar ao floor e multiplica por 1000 (mas antes é preciso converter os campos em float)
  dados_anatel_filtrado['FreqTxMHz'] = pd.to_numeric(dados_anatel_filtrado['FreqTxMHz'], downcast="float", errors='coerce')
  dados_anatel_filtrado['FreqRxMHz'] = pd.to_numeric(dados_anatel_filtrado['FreqRxMHz'], downcast="float", errors='coerce')
  dados_anatel_filtrado['FREQUENCIA_MHZ'] = ((dados_anatel_filtrado[['FreqTxMHz', 'FreqRxMHz']].max(axis=1) / 1000) // 0.1 / 10) * 1000

  logging.debug("SUCESSO - Criação do campo FREQUENCIA_MHZ")

  #>>>>>>>>>>>
  # Condições para substituir as FREQUANCIA_MHZ > 3000 e < 4000 para 3500 por padronização apenas
  def categoria_frequencia(row):
      if row['FREQUENCIA_MHZ']>3000 and row['FREQUENCIA_MHZ']<4000: #Verifica se a frequencia é maior que 3000MHZ e menor que 4000MHZ para converter para 3500MHZ
        return 3500
      elif row['FREQUENCIA_MHZ']>25000 and row['FREQUENCIA_MHZ']<26000:
        return 25000
      else:
        return row['FREQUENCIA_MHZ']
  # Chama o trecho acima para substituir todas variações > 3000 e <4000 por 3500MHZ
  dados_anatel_filtrado['FREQUENCIA_MHZ'] = dados_anatel_filtrado.apply(lambda row: categoria_frequencia(row), axis=1)

  logging.debug("SUCESSO - Criação Campo FREQUENCIA_MHZ")

  #Cria campo TECNO para padronizar os nomes das tecnologias
  def categoria_tecnologia(row):
      if row['Tecnologia'] == 'WCDMA' or row['Tecnologia'] == 'WDCMA' or row['Tecnologia'] == 'WCMDA' or row['Tecnologia'] == 'WCDMA ':
        return '3G'
      elif row['Tecnologia'] == 'LTE' or row['Tecnologia'] == 'LTE ':
        return '4G'
      elif row['Tecnologia'] == 'GSM' or row['Tecnologia'] == 'CDMA' or row['Tecnologia'] == 'EDGE':
        return '2G'
      elif row['Tecnologia'] == 'NR ' or row['Tecnologia'] == 'NR':
        return '5G'
      else:
        return ''
  # Chama o trecho acima para substituir as tecnologias
  dados_anatel_filtrado['TECNO'] = dados_anatel_filtrado.apply(lambda row: categoria_tecnologia(row), axis=1)

  logging.debug("SUCESSO - Criação campo TECNO")

  #Insere a data da extração/tratamento
  dados_anatel_filtrado['DATA_EXTRACAO'] = datetime.today().strftime('%Y-%m-%d')

  logging.debug("SUCESSO - Criação Campo DATA_EXTRACAO")

  #Incluir as informações por município
  caminho_apoio = Path(diretorio + "/APOIO/") #Para funcionar tanto no windows como unix/linux
  tabela_municipio = pd.read_csv(caminho_apoio / "estimativa_dou_2021.csv", encoding = 'latin-1', sep=';', low_memory=False)
  #Faz o join entre dados_anatel_filtrado e tabela_municipio pelo Codigo IBGE
  dados_anatel_filtrado = dados_anatel_filtrado.merge(tabela_municipio, left_on='CodMunicipio', right_on='COD_IBGE' ,how='left').drop(columns = ['COD_IBGE','UF'])

  logging.debug("SUCESSO - Join entre dados_anatel_filtrado e tabela_municipio")

  #RESULTADO(1)
  #Exporta o arquivo de Células (Setor/Portadora) tratado para um arquivo CSV, com REGISTROS ORIGINAIS (!!!CONTEM DUPLICIDADES!!!)
  #Codigo comentado é a versão para rodar no micro e não no colab
  dados_anatel_filtrado.to_csv(caminho_export / '1_RADAR_ANATEL_CELULA_ORIGINAL.csv', 
                                  encoding = 'latin-1', 
                                  sep = ';', 
                                  decimal=',', 
                                  index = False)

  logging.debug("SUCESSO - Export(1)-1_RADAR_ANATEL_CELULA_ORIGINAL.csv")

  ##### IMPORTANTE #####
  #No período em que este codigo foi desenvolvido, existem campos que estão duplicando os registros, são eles:
  #ClassInfraFisica, CompartilhamentoInfraFisica, _id, DataLicenciamento
  #São registros identicos com difernças nestes campos

  #TRATAMENTO(2)
  #Exporta o arquivo de Células tratado para um arquivo CSV (SEM duplicidades) os campos mais importantes
  dados_anatel_filtrado_sem_duplicidade = dados_anatel_filtrado[['DATA_EXTRACAO','Status.state','NomeEntidade','NumFistel',
                                                                'NumServico','NumAto','NumEstacao','EnderecoEstacao',
                                                                'EndComplemento','SiglaUf','CodMunicipio', 'MUNICIPIO',
                                                                'POPULACAO', 'RANGE_POP', 'CAPITAL',
                                                                'DesignacaoEmissao','Tecnologia',
                                                                'FreqTxMHz','FreqRxMHz','Azimute',
                                                                'Latitude','Longitude', 'TECNO',
                                                                'OPERADORA','BANDA_MHZ','FREQUENCIA_MHZ']].drop_duplicates()

  logging.debug("SUCESSO - Tratamento(2)")

  #RESULTADO(3)
  #Retirando duplicidade com mesmas frequencias (FreqRxMHz) e largura de banda diferente (DesignacaoEmissao), 
  #pegando o max (retirando "DesignacaoEmissao")
  #O objetivo é retirar duplicidade por problema de falha no cadastro, pois esta é uma situação que não deveria acontecer (mesma frequencia com banda diferente)
  dados_anatel_filtrado_sem_duplicidade2 = dados_anatel_filtrado_sem_duplicidade.groupby(['DATA_EXTRACAO','Status.state','NomeEntidade','NumFistel',
                                                                'NumServico','NumAto','NumEstacao','EnderecoEstacao',
                                                                'EndComplemento','SiglaUf','CodMunicipio', 'MUNICIPIO',
                                                                'POPULACAO', 'RANGE_POP', 'CAPITAL','Tecnologia',
                                                                'FreqTxMHz','FreqRxMHz','Azimute','Latitude','Longitude','TECNO','OPERADORA',
                                                                'FREQUENCIA_MHZ'], as_index=False, dropna=False)['BANDA_MHZ'].max()

  dados_anatel_filtrado_sem_duplicidade2.to_csv(caminho_export / '2_RADAR_ANATEL_CELULA_SEM_DUPLICIDADE.csv', 
                                  encoding = 'latin-1', 
                                  sep = ';', 
                                  decimal=',', 
                                  index = False)

  logging.debug("SUCESSO - Export(2)-2_RADAR_ANATEL_CELULA_SEM_DUPLICIDADE2.csv")

  #RESULTADO(3)
  #Extração Resumida ainda a nível de Estação
  dados_anatel_filtrado_sem_duplicidade_site_tecn_freq = dados_anatel_filtrado_sem_duplicidade2.groupby(['DATA_EXTRACAO','SiglaUf', 
                                                                    'NumEstacao','CodMunicipio','MUNICIPIO','POPULACAO','RANGE_POP','CAPITAL',
                                                                    'EnderecoEstacao','EndComplemento','Latitude','Longitude',
                                                                    'OPERADORA','TECNO', 
                                                                    'FREQUENCIA_MHZ'], as_index=False, dropna=False)['BANDA_MHZ'].sum()
  dados_anatel_filtrado_sem_duplicidade_site_tecn_freq.to_csv(caminho_export / '3_RADAR_ANATEL_SITE_TECNO_FREQ.csv', 
                                          encoding = 'latin-1', 
                                          sep = ';', 
                                          decimal=',', 
                                          index = False)

  logging.debug("SUCESSO - Export(3)-3_RADAR_ANATEL_SITE_TECNO_FREQ.csv")

  logging.debug("-----------  Fim do log Tratamento Mosaico  -----------")

  return True