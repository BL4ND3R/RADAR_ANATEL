### DOWNLOAD > CONSOLIDA > TRANSFORMA DADOS ESTACOES E ACESSOS ANATEL

from a_download_anatel import download
from b_consolida_arquivos import consolida
from c_trata_dados_anatel import transforma
from d_download_acessos import download_acessos
from e_trata_dados_acessos import transforma_acessos
from f_trata_estacoes_acessos import transforma_estacoes_acessos

#Executa a função que coleta os dados de ERBs da Anatel e salva em CSV no diretório DOWNLOAD do código
if download() == True:
    print("Sucesso na extração dos dados por UF do Mosaico!")
else:
    print("Erro na extração dos dados por UF do Mosaico!")

#Executa a função que concatena (union) de todos os arquivos CSV por UF em um unico CSV chamado brasil.csv no diretorio DOWNLOAD
if consolida() == True:
    print("Sucesso na consolidação dos dados Brasil!")
else:
    print("Erro na consolidação dos dados Brasil!")

#Executa a função que trata os dados do arquivo brasil.csv e salva os arquivos resultantes no dir \RESULTADO
if transforma() == True:
    print("Sucesso na transformação dos dados Brasil!")
else:
    print("Erro na transformação dos dados Brasil")

#Executa a função que coleta os dados de ACESSOS Anatel e salva em CSV no diretório DOWNLOAD_ACESSOS
if download_acessos() == True:
    print("Sucesso no Download Acessos!")
else:
    print("Erro no Dwonload Acessos")

#Executa a função que trata os dados de ACESSOS e salva os arquivos resultantes no dir \RESULTADO
if transforma_acessos() == True:
    print("Sucesso na transformação dos dados Acesso!")
else:
    print("Erro na transformação dos dados Acesso")

#Executa a função que trata os dados de ESTACOES e faz o join com ACESSOS e salva o arquivo resultante no dir \RESULTADO
if transforma_estacoes_acessos() == True:
    print("Sucesso na transformação dos dados de Estações + Acesso!")
else:
    print("Erro na transformação dos dados de Estações + Acesso")

