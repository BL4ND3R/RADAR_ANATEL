# RADAR_ANATEL
Código Python para raspagem de dados públicos de Telefonia Móvel da Agência Nacional de Telecomunicações (ANATEL)

O código em Pyhton realiza a extração, transformação/tratamento e exportação em CSV de dados Anatel com o intuito de calcular um indicador de
de Clientes/MHz. Para a raspagem dos dados é utilizada a biblioteca SELENIUM com webdrive Chrome.

São basicamente duas fontes de dados:

1) Licenças de Estações Rádio Base (antenas de celular)
2) Número de Acessos Móveis (clientes de telefonica móvel)

Como resultado são exportados os seguintes arquivos:

  1_RADAR_ANATEL_CELULA_ORIGINAL
  2_RADAR_ANATEL_CELULA_SEM_DUPLICIDADE
  3_RADAR_ANATEL_SITE_TECNO_FREQ
  4_ACESSOS_MOVEIS_ANATEL_MES_RECENTE
  5_ACESSOS_MOVEIS_ANATEL_MES_RECENTE_POR_MUN_OPER_TECNO
  6_ESTACOES_ACESSOS_MOVEIS_MUNICIPIO_OPERADORA_TECNOLOGIA
