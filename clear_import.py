import pandas as pd
import os
from datetime import date
import pyodbc
from collections import Counter


sql_conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server}; \
    Server=ATVVMCCROG1A; \
    Database=pcce_baA; \
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
    "CAMP_PRED_SAE_DIA_EVENTUAL01": 5000,
    "CAMP_PRED_SAE_DIA_SG_Ativo_G1": 5001,
    "CAMP_PRED_SAE_DIA_SG_Ativo_G21": 5002,
    "CAMP_PRED_SAE_DIA_SG_Ativo_G3": 5003,
    "CAMP_PRED_SAE_DIA_SG_Ativo_INAD1": 5004,
    "CAMP_PRED_SAE_DIA_LEMBRETE": 5005,
    "CAMP_PRED_SAE_DIA_SG_Ativos_Ouvidoria": 5006,
    "CAMP_PRED_SAE_DIA_SG_Ativo_Manutencao": 5007,
    "CAMP_PRED_ZAA_DIA_SG_PGM_FORTALEZA": 5008,
    "CAMP_PRED_SAE_DIA_SG_PGFN": 5009,
    "CAMP_PRED_PCCE_DIA_SG_PGM_FORTALEZA_ABANDONO": 5012,
    "CAMP_PRED_ZAA_DIA_SG_PGM_VR": 5013,
    "CAMP_PRED_ZAA_DIA_SG_PGE_RN_SEM_ACORDO": 5015,
    "NULL": 5017,
    "CAMP_PRED_ZAA_DIA_SG_PGM_VR_MAIOR_50K": 5018,
    "CAMP_PRED_SAE_DIA_SG_Ativo_G22": 5029,
    "CAMP_PRED_SAE_DIA_SG_Ativo_INAD2": 5030,
    "CAMP_PRED_SAE_DIA_SG_Ativo_LoteNovoG1": 5031,
    "CAMP_PRED_SAE_DIA_SG_Ativo_LoteNovoG21": 5032,
    "CAMP_PRED_SAE_DIA_SG_Ativo_LoteNovoG22": 5033,
    "CAMP_PRED_SAE_DIA_SG_Ativo_LoteNovoG3": 5034,
    "CAMP_PRED_SAE_DIA_EVENTUAL02": 5035,
    "EV": 5036,
    "CAMP_PRED_ZAA_DIA_SG_PGM_VR_EVENTUAL": 5037,
    "CAMP_PRED_ZAA_DIA_SG_PGM_VR_INAD": 5038,
    "CAMP_PRED_ZAA_DIA_SG_PGM_VR_MANUT": 5039,
    "CAMP_PRED_ZAA_DIA_SG_PGM_VR_SEM_ACORDO": 5040,
    "CAMP_PRED_ZAA_DIA_SG_PGE_RN_EVENTUAL": 5041,
    "CAMP_PRED_ZAA_DIA_SG_PGE_RN_INAD": 5042,
    "CAMP_PRED_ZAA_DIA_SG_PGE_RN_MANUT": 5043,
    "CAMP_PRED_SAE_DIA_SG_NUCLEODF1_G1": 5044,
    "CAMP_PRED_SAE_DIA_SG_NUCLEODF1_G21": 5045,
    "CAMP_PRED_SAE_DIA_SG_NUCLEODF1_G22": 5046,
    "CAMP_PRED_SAE_DIA_SG_NUCLEODF1_G3": 5047,
    "CAMP_PRED_SAE_DIA_SG_NUCLEODF1_INAD1": 5048,
    "CAMP_PRED_SAE_DIA_SG_NUCLEODF1_INAD2": 5049,
    "CAMP_PRED_SAE_DIA_SG_NUCLEODF1_Manutencao": 5050,
    "CAMP_PRED_SAE_DIA_NUCLEODF1_EVENTUAL01": 5052,
    "CAMP_PRED_SAE_DIA_NUCLEODF1_EVENTUAL02": 5053,
}

data = date.today()
today = int(data.strftime('%Y%m%d'))  # data de hoje no formato YYYYMMDD
print('DATA DE HOJE: ' + today)

#dir_mailing_import = 'C:/FTP_ATIVOS/OUTBOUND/Mailing_Import'
dir_mailing_import = 'C:/Users/admin-atelecom/Desktop/TESTE'  # diretorio para teste

# todos os arquivos contidos no diretório especificado
all_files = os.listdir(dir_mailing_import)
# print(all_files)

################# CRIAR LISTA DE ARQUIVOS QUE FORAM IMPORTADOS HOJE #################
arquivos_not_today = []
# cria lista de arquivos que não são de hoje
for f in all_files:
    data_file = int(f[-19:-11])
    #print('Arquivo: ' + f + '\t\t\tData:\t' + str(data_file))
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
        #print(d + '\tArquivo:\t' + str(max_registers))
        #print('DB:\t\t\t\t\t\t\t' + str(num_registros_camp_db))

        # Cria query para apagar as duplicatas
        query_trunkate_excess = 'DELETE FROM [pcce_baA].[dbo].[DL_{id}_{id}] WHERE DialingListID > {num_registros}'.format(
            id=campaign_id_map[d], num_registros=max_registers)
        print(query_trunkate_excess)
        #df_db_excess_camp = pd.read_sql(query_trunkate_excess, sql_conn)
        #num_registros_excess_db = df_db_excess_camp.DialingListID.count()
        #print('Excesso:\t' + str(num_registros_excess_db))

# query = ''
