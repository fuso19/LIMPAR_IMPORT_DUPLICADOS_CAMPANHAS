import pandas as pd
import os
from datetime import date
import pyodbc
from collections import Counter


sql_conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server}; \
    Server=SERVER_HOSTNAME/IP; \
    Database=DATABASENAME; \
    Trusted_Connection=yes;")


def Diff_List(a, b):
    return list(set(a)-set(b))


'''
    SELECT [CampaignName]
	   ,[Description]
	   ,[CampaignID]
    FROM [pcce_awdb].[dbo].[t_Campaign]
'''
campaign_id_map = {
    "CAMP1": 5000,
    "CAMP2": 5001,
    "CAMP3": 5002,
    "CAMP4": 5003,
    "CAMP5": 5004,


data = date.today()
today = int(data.strftime('%Y%m%d'))  # data de hoje no formato YYYYMMDD
print('DATA DE HOJE: ' + today)

dir_mailing_import = 'C:/DIRETORIO'


# todos os arquivos contidos no diretório especificado
all_files = os.listdir(dir_mailing_import)
# print(all_files)

################# CRIAR LISTA DE ARQUIVOS QUE FORAM IMPORTADOS HOJE #################
arquivos_not_today = []
# cria lista de arquivos que não são de hoje
for f in all_files:
    data_file = int(f[-19:-11])
    # print('Arquivo: ' + f + '\t\t\tData:\t' + str(data_file))
    # print(data_file)
    if (data_file != today):
        arquivos_not_today.append(f)

# arquivos de hoje = todos - não_hoje
files_today = Diff_List(all_files, arquivos_not_today)

################# CRIA UM DICIONARIO DE CADA CAMPANHA COM A QUANTIDADE DE REGISTROS TOTAL #################

# percorre os arquivos de hoje para preencher os nomes das campanhas
campaign_name_registers = {}
for f1 in files_today:
    # print(f1)
    campaign_name = f1[:-20]  # nome da campanha
    campaign_name_registers[campaign_name] = 0

# percorre os arquivos de hoje para preencher a quantidade de linhas
for f2 in files_today:
    # numero de linhas em cada arquivo
    num_lines = sum(1 for line in open(dir_mailing_import + '/' + f2))
    campaign_name = f2[:-20]
    campaign_name_registers[campaign_name] += num_lines
# print(campaign_name_registers)


#################  PERCORRE O BANCO DE DADOS PARA CADA CAMPANHA E VERIFICA QUANTOS REGISTROS FORAM IMPORTADOS #################
for d in campaign_name_registers:
    # numero de registros que foram verificados nos arquivos de importacao
    max_registers = campaign_name_registers[d]

    query_verifica_qtd_import = 'SELECT DialingListID\
                                , AccountNumber\
                                , FirstName\
                                , LastName FROM [pcce_baA].[dbo].[DL_{id}_{id}]'.format(id=campaign_id_map[d])

    df_db_registros_camp = pd.read_sql(query_verifica_qtd_import, sql_conn)
    num_registros_camp_db = df_db_registros_camp.DialingListID.count()

    if (num_registros_camp_db > campaign_name_registers[d]):
        # print(d + '\tArquivo:\t' + str(max_registers))
        # print('DB:\t\t\t\t\t\t\t' + str(num_registros_camp_db))

        # Cria query para apagar as duplicatas
        query_trunkate_excess = 'DELETE FROM [pcce_baA].[dbo].[DL_{id}_{id}] WHERE DialingListID > {num_registros}'.format(
            id=campaign_id_map[d], num_registros=max_registers)
        print(query_trunkate_excess)
        # df_db_excess_camp = pd.read_sql(query_trunkate_excess, sql_conn)
        # num_registros_excess_db = df_db_excess_camp.DialingListID.count()
        # print('Excesso:\t' + str(num_registros_excess_db))
