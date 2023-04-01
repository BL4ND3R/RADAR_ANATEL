### FUNÇÃO PARA FAZER A EXTRAÇÃO DADOS ANATEL POR UF e SALVAR EM CSV NA PASTA ...\DOWNLOAD\

# FRAGILIDADES DO CÓDIGO:
## Se a página Anatel mudar ou seus componentes, será necessária a revisão desta parte da solução
## Caso apareça alguma frequencia nova e seja neessária a padronização, alterar o trecho do código com: ">>>>>>>>>>>"

def download():

## FAZ O DOWNLOAD DOS ARQUIVOS
    from selenium import webdriver #importa o navegador do Selenium
    from selenium.webdriver.common.by import By #importa a função By do Selenium, com ela é que fazemos a definição do TIPO de busca que será feito (aqui vamos utilizar XPATH)
    from selenium.webdriver.support.ui import Select #importa a função Select do Selenium, com ela é que fazemos a seleção da opção dentro do DropDown
    from selenium.webdriver.support import expected_conditions as EC #importa a função que testa as condições do elemento aparecer na página
    from selenium.common.exceptions import TimeoutException
    from selenium.webdriver.support.ui import WebDriverWait # modulo 
    import time #importa o modulo time, com ele vamos usar a função sleep para aguardar o carregamento do website da Anatel
    from zipfile import ZipFile #importa o modulo Zip para descompactar os arquivos
    import os #importa o modulo Operational System para realizar comando do DOS de arquivos e diretórios
    import glob #modulo para deletar todos os arquivos de um diretório
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
    nome_log = str(caminho_log / str(datetime.today().strftime('%Y-%m-%d_%H-%M-%S') + "_log_1_download") ) + '.txt'
    logger = logging.getLogger()
    fhandler = logging.FileHandler(filename=nome_log, mode='a')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fhandler.setFormatter(formatter)
    logger.addHandler(fhandler)
    logger.setLevel(logging.DEBUG)

    logging.debug("-----------  Início do log Download Mosaico  -----------")

    # variável url com o endereço da consulta de licenciamento da Anatel
    url = "http://sistemas.anatel.gov.br/se/public/view/b/licenciamento.php?view=licenciamento"

    # Cria lista com as UFs
    ufs = ['AC','AL','AM','AP','BA','CE','DF','ES','GO','MA','MG','MS','MT','PA','PB','PE','PI','PR','RJ','RN','RO','RR','RS','SC','SE','SP','TO']

    # Cria variável primeira_vezfiltro para marcar se o filtro de pagina foi clicado uma vez
    primeira_vez_filtro = True

    #Verifica se o diretório \DOWNLOAD existe, caso não cria
    caminho_download = Path(diretorio + "/DOWNLOAD/") #Para funcionar tanto no windows como unix/linux
    if not os.path.exists(caminho_download):
        os.makedirs(caminho_download)
    else:
        # deleta todos os arquivos do diretorio DOWNLOAD
        arquivos = glob.glob(str(Path(diretorio + "/DOWNLOAD/*")))
        for f in arquivos:
            os.remove(f)

    logging.debug("SUCESSO - Criação e Limpeza do diretorio \DOWNLOAD")

    # opções do chrome - define o diretório de download dos arquivos zip da Anatel
    opcoes_chrome = webdriver.ChromeOptions()
    prefs = {'download.default_directory' : str(Path(diretorio + "/DOWNLOAD/"))}
    opcoes_chrome.add_experimental_option('prefs', prefs)
    # opcoes_chrome.add_argument('headless')
    # opcoes_chrome.add_argument('window-size=1920x1080')
    # opcoes_chrome.add_argument("disable-gpu")

    # abre o chrome e acrescenta as opcoes
    navegador = webdriver.Chrome(chrome_options = opcoes_chrome)

    # acessa a url dentro do chrome e aguarda 5 segundos para ela carregar
    navegador.get(url)
    time.sleep(5)
    delay = 3 #Tempo de espera para fazer o waiting para os elementos aparecerem na página

    logging.debug("SUCESSO - Configuracao webdriver Selenium")

    for uf in ufs:  
        # clica na opção "Filtros Adicionais"
        navegador.find_element(By.XPATH,'//*[@id="filtros_adicionais"]').click()
        
        # clica na opção DropDown "Pesquisar por:" e seleciona a opção "Estado"
        navegador.find_element(By.XPATH,'//*[@id="fa_gsearch"]').click()
        selecionar_por_estado = Select(navegador.find_element(By.XPATH,'//*[@id="fa_gsearch"]'))
        selecionar_por_estado.select_by_visible_text('Estado')

        # clica na opção DropDown "Estado:" e seleciona a sigla do estado
        navegador.find_element(By.XPATH,'//*[@id="fa_uf"]').click()
        selecionar_por_uf = Select(navegador.find_element(By.XPATH,'//*[@id="fa_uf"]'))
        selecionar_por_uf.select_by_visible_text(uf)

        # clica no botão "Enviar", mas testa se passou uma vez neste trecho, pois só pode clicar em filtrar uma vez apenas
        navegador.find_element(By.XPATH, '//*[@id="import"]').click()
        time.sleep(5)

        #### Incluido este trecho pois apenas os "Filtros Adicionais" não estava funcionando
        
        # clica em "Filtrar"
        if (primeira_vez_filtro):
            navegador.find_element(By.XPATH,'//*[@id="tblFilter"]/span[5]').click()
            navegador.implicitly_wait(5) # gives an implicit wait for 5 seconds
            primeira_vez_filtro = False
        
        # clica no combo "UF" na barra de filtros   
        navegador.find_element(By.NAME,'fc_8').click()
        navegador.implicitly_wait(5) # gives an implicit wait for 3 seconds
        selecionar_por_estado_filtrar = Select(navegador.find_element(By.NAME,'fc_8'))
        selecionar_por_estado_filtrar.select_by_visible_text(uf)
        time.sleep(5)
    
        #### fim
    
        # clica no botão "Download Filtradas" e faz o download do arquivo .zip com os registros da UF selecionada
        navegador.find_element(By.XPATH, '//*[@id="download_filtradas"]').click()
        time.sleep(5)
        
        # cria uma variavel com o nome do arquivo zip, que varia de nome para cada seleção. O codigo espera que exista apenas um arquivo por vez
        # o loop abaixo lista todos os arquivos e verificar se é do tipo .ZIP
        nome_arquivo_zip = ''
        while nome_arquivo_zip == '':
            for file in os.listdir(Path(diretorio + "/DOWNLOAD/")):
                if file.endswith(".zip"):
                    nome_arquivo_zip =  os.path.join(Path(diretorio + "/DOWNLOAD/"), file)
                    print(nome_arquivo_zip)
        
        #Descompacta o arquivo zip que vem da Anatel
        zf = ZipFile(nome_arquivo_zip , 'r')
        zf.extractall(str(Path(diretorio + "/DOWNLOAD/")))
        zf.close()

        # Pegando o nome do arquivo CSV que foi descompactado
        nome_arquivo_csv = nome_arquivo_zip[:-3] + "csv"
        print(nome_arquivo_csv)
        
        # variaveis dos caminhos do download do arquivo para conversao do nome 
        nome_atual = nome_arquivo_csv
        nome_novo = str( Path(diretorio + "/DOWNLOAD/") / (uf + '.csv') )
        
        # renomeia o arquivo CSV com o nome da UF (com proteção caso o arquivo já exista ou não exista)
        try:
            os.rename(nome_atual, nome_novo)
        except FileNotFoundError:
            print("Arquivo CSV não existe no diretório: " + nome_atual)
        except FileExistsError:
            print("Arquivo CSV já existe no diretório:" + nome_atual)

        # deleta o arquivo zip para a proxima UF (com protenção caso o arquivo não exista)
        try:
            os.remove(nome_arquivo_zip)
        except FileNotFoundError:
            print("Não foi possível apagar o arquivo a seguir, ele não existe: " + nome_atual)

        logging.debug("SUCESSO - Download da UF: " + uf)
    logging.debug("-----------  Fim do log Download Mosaico  -----------")

    return True