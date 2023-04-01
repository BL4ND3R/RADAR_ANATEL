### FUNÇÃO QUE CONSOLIDA OS ARQUIVOS "UF.csv" EM UM ÚNICO ARQUIVO CSV "brasil.csv" na pasta ...\DOWNLOAD\

# FRAGILIDADES DO CÓDIGO:
# Não foi possível mapear fragilidades até o momento

def consolida():
    import pandas as pd #importa o pandas
    import os #importa o sistema operacional para manipulação de arquivos no windows
    from datetime import date # para pegar a data e hora de hoje
    from datetime import datetime # para a data do log
    from pathlib import Path #função para tratar os caminhos para compatibilidade com dif OS
    import logging #log do código

    #Configura o log que vai ser salvo no arquivo log_data no dir \LOG
    #Apenas cria o diretorio do log caso não exista
    diretorio = os.getcwd()
    caminho_log = Path(diretorio + "/LOG/") #Para funcionar tanto no windows como unix/linux
    if not os.path.exists(caminho_log):
        os.makedirs(caminho_log)
    #Cria o log e configura (o nome do arquivo tem a data e hora da criação)
    nome_log = str(caminho_log / str(datetime.today().strftime('%Y-%m-%d_%H-%M-%S') + "_log_2_consolida") ) + '.txt'
    logger = logging.getLogger()
    fhandler = logging.FileHandler(filename=nome_log, mode='a')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fhandler.setFormatter(formatter)
    logger.addHandler(fhandler)
    logger.setLevel(logging.DEBUG)

    logging.debug("-----------  Início do log Consolida brasil.csv  -----------")

    #Lista de UFs
    ufs = ['AC','AL','AM','AP','BA','CE','DF','ES','GO','MA','MG','MS','MT','PA','PB','PE','PI','PR','RJ','RN','RO','RR','RS','SC','SE','SP','TO']

    #cria o dataframe que vai consolidar todas as UFs
    brasil = pd.DataFrame() 

    #Loop para passar por todas as UFs agregando no dataframe "brasil"
    for UF in ufs:
        df_temp = pd.read_csv(diretorio + "\\Download\\" + UF + '.csv', sep=',', encoding = 'latin-1', dtype='unicode') #abre o arquivos CSV de uma UF temporariamente
        brasil = pd.concat([df_temp , brasil]) #faz o union/concatena as UFs em um unico dataframe
        print(UF)
        logging.debug("SUCESSO - Consolidada a UF: " + UF)

    #cria uma coluna no dataframe brasil e insere a data do dia da coleta (hoje)
    today = date.today()
    d1 = today.strftime("%d/%m/%Y")    # dd/mm/YY
    print("d1 =", d1)
    brasil["data_coleta"] = d1

    #Testa se o arquivo brasil.csv existe e deleta
    if os.path.exists(diretorio + "\\Download\\brasil.csv"):
        os.remove(diretorio + "\\Download\\brasil.csv")

    # Escreve o dataframe brasil em um arquivo csv
    brasil.to_csv(diretorio + "\\Download\\brasil.csv", index = False, encoding = 'latin-1' )
    del brasil #deleta o Dataframe brasil para economizar memória

    logging.debug("-----------  Fim do log Consolida brasil.csv  -----------")
    
    return True
