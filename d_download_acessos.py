### FAZ O DOWNLOAD DO ARQUIVO DE ACESSOS MÓVEL DA ANATEL E EXTRAI O ARQUIVO "Acessos_Telefonia_Movel_YYYYMM-YYYYMM.csv"
# https://www.anatel.gov.br/dadosabertos/paineis_de_dados/acessos/acessos_telefonia_movel.zip

# FRAGILIDADES DO CÓDIGO:
## Se a  Anatel mudar os arquivos , será necessária a revisão da solução

def download_acessos():

## FAZ O DOWNLOAD DOS ARQUIVOS DE ACESSOS MÓVEIS
    from selenium import webdriver #importa o navegador do Selenium
    from selenium.webdriver.common.by import By #importa a função By do Selenium, com ela é que fazemos a definição do TIPO de busca que será feito (aqui vamos utilizar XPATH)
    from selenium.webdriver.support.ui import Select #importa a função Select do Selenium, com ela é que fazemos a seleção da opção dentro do DropDown
    from selenium.webdriver.support import expected_conditions as EC #importa a função que testa as condições do elemento aparecer na página
    from selenium.common.exceptions import TimeoutException
    from selenium.webdriver.support.ui import WebDriverWait # modulo 
    import time #importa o modulo time, com ele vamos usar a função sleep para aguardar o carregamento do website da Anatel
    from zipfile import ZipFile #importa o modulo Zip para descompactar os arquivos
    import os #importa o modulo Operational System para realizar comando do DOS de arquivos e diretórios
    from datetime import datetime
    from pathlib import Path #função para tratar os caminhos para compatibilidade com dif OS
    import logging #log do código

    #Configura o log que vai ser salvo no arquivo log_data no dir \LOG
    #Apenas cria o diretorio do log caso não exista
    diretorio = os.getcwd()
    caminho_log = Path(diretorio + "/LOG/") #Para funcionar tanto no windows como unix/linux
    if not os.path.exists(caminho_log):
        os.makedirs(caminho_log)
    #Cria o log e configura (o nome do arquivo tem a data e hora da criação)
    nome_log = str(caminho_log / str(datetime.today().strftime('%Y-%m-%d_%H-%M-%S') + "_log_4_download_acessos") ) + '.txt'
    logger = logging.getLogger()
    fhandler = logging.FileHandler(filename=nome_log, mode='a')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fhandler.setFormatter(formatter)
    logger.addHandler(fhandler)
    logger.setLevel(logging.DEBUG)

    logging.debug("-----------  Início do log Download Acessos  -----------")

    # variável url com o endereço da consulta Acessos Móveis
    url = 'https://www.anatel.gov.br/dadosabertos/paineis_de_dados/acessos/acessos_telefonia_movel.zip'

    #Verifica se o diretório \DOWNLOAD_ACESSOS existe, caso não cria
    caminho_download = Path(diretorio + "/DOWNLOAD_ACESSOS/") #Para funcionar tanto no windows como unix/linux
    if not os.path.exists(caminho_download):
        os.makedirs(caminho_download)
    else:
        import shutil
        shutil.rmtree(str(Path(diretorio + "/DOWNLOAD_ACESSOS/")))
        os.makedirs(caminho_download)

    logging.debug("SUCESSO - Criação e Limpeza do diretorio \DOWNLOAD_ACESSOS")

    # opções do chrome - define o diretório de download dos arquivos zip da Anatel
    opcoes_chrome = webdriver.ChromeOptions()
    prefs = {'download.default_directory' : str(Path(diretorio + "/DOWNLOAD_ACESSOS/"))}
    opcoes_chrome.add_experimental_option('prefs', prefs)
    # opcoes_chrome.add_argument('headless')
    # opcoes_chrome.add_argument('window-size=1920x1080')
    # opcoes_chrome.add_argument("disable-gpu")

    # abre o chrome e acrescenta as opcoes
    navegador = webdriver.Chrome(chrome_options = opcoes_chrome)

    # acessa a url dentro do chrome e aguarda 5 segundos para ela carregar
    navegador.get(url)
    
    # Código para aguardar até que o download do arquivo seja concluído
    # Fica verificando se o arquivo .crdownload desaparece, pois em caso positivo o download terminou
    # ATENÇÃO!!!! Só funciona para o Chrome por conta da extenção do arquivo de download .crdownload
    seconds = 0
    dl_wait = True
    while dl_wait and seconds < 3600: # Termina o loop em 1 hora = 3600seg por segurança
        time.sleep(1)
        dl_wait = False
        for fname in os.listdir(str(Path(diretorio + "/DOWNLOAD_ACESSOS/"))):
            if fname.endswith('.crdownload'):
                dl_wait = True
        seconds += 1

    logging.debug("SUCESSO - Download Arquivo Acessos")

    ## Constroi o nome do arquivo a ser descompactado
    mescorrente = datetime.now().month
    anocorrente = datetime.now().year
    # Identifica qual é o ano do nome do arquivo
    if mescorrente == 1:
        ano = anocorrente - 1
    else:
        ano = anocorrente
    # Identifica em qual semestre está e ja monta o nome do arquivo. Se cair em janeiro, o arquivo é do ano anterior
    if (mescorrente - 1) > 6 or (mescorrente - 1) == 0:
        nome_arquivo_acessos_movel_atual =  'Acessos_Telefonia_Movel_' + str(ano) + '07-' + str(ano) + '12.csv'
    else:
        nome_arquivo_acessos_movel_atual =  'Acessos_Telefonia_Movel_' + str(ano) + '01-' + str(ano) + '06.csv'
    print(nome_arquivo_acessos_movel_atual)

    # Descompacta o arquivo zip de acessos apenas 'Acessos_Telefonia_Movel_YYYYMM-YYYYMM.csv'
    nome_arquivo_zip_acessos =  'acessos_telefonia_movel.zip'
    caminho_zip_acessos = os.path.join(Path(diretorio + "/DOWNLOAD_ACESSOS/"), nome_arquivo_zip_acessos)
    zf = ZipFile(caminho_zip_acessos , 'r')
    lista_arquivos_csv_do_zip = zf.namelist()
    # Faz um loop nos arquivos que estão dentro do zip
    for fileName in lista_arquivos_csv_do_zip:
        print(fileName)
        # verifica se o arquivo CSV do ano/mes atual está dentro do zip. Se sim, descompacta.
        if fileName == nome_arquivo_acessos_movel_atual:
            # Extrai o arquivo desejado
            zf.extract(fileName, str(Path(diretorio + "/DOWNLOAD_ACESSOS/")))
            logging.debug("SUCESSO - Descompactou arquivo: " + nome_arquivo_acessos_movel_atual)
    zf.close()
    logging.debug("-----------  Fim do log Download Acessos  -----------")

    return True