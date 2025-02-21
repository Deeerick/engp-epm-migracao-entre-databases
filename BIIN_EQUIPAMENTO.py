import pandas as pd
import warnings

from utils.connection_db import connection
from utils.last_update import last_update
from utils.update_table import update_management_table

warnings.simplefilter(action='ignore', category=UserWarning)


def main():
    lista_loc_instal = ['351902', '301081', '301049', '301059', '351401', '351402', 
                        # '351901', '351701', '301010', '301083', '301018', '301019', 
                        # '301020', '301029', '301032', '301033', '301036', '301040', 
                        # '301056', '301057', '303006', '301066', '301063', '301071'
                        ]
    
    conn_sql = connection(dsn='BD_UN-BC')
    conn_tdv = connection(dsn='TDV')
    
    for loc_instal in lista_loc_instal:
        
        df_last_update = last_update(table='BIIN_EQUIPAMENTO', conn=conn_sql)
        # df_last_update = pd.to_datetime(df_last_update['ULTIMA_ATUALIZACAO']).dt.strftime('%Y-%m-%d %H:%M:%S').iloc[0]
        
        df_source_data = import_source_data(loc_instal, df_last_update)
        print(f'TDV - PLATAFORMA {loc_instal} = {len(df_source_data)} REGISTROS')
        
        df_target_data = import_target_data(loc_instal)
        print(f'SQL - PLATAFORMA {loc_instal} = {len(df_target_data)} REGISTROS')
        
        dif = len(df_target_data) - len(df_source_data)
        print(f'REGISTROS PARA CARREGAR = {dif}')
        
        df_att, df_insert = create_df_att(df_target_data, df_source_data)
        print(f'REGISTROS PARA ATUALIZAR = {len(df_att)}')
        print(f'REGISTROS PARA INSERIR = {len(df_insert)}')
        
        insert_data_db(df_att, df_insert)
        
        update_management_table(table='BIIN_EQUIPAMENTO')


def import_source_data(loc_instal, df_last_update):
    
    df_source_data = pd.DataFrame()
    conn_sql = connection(dsn='BD_UN-BC')
    conn_tdv = connection(dsn='TDV')
    
    if conn_sql is None or conn_tdv is None:
        print("Erro ao estabelecer conexão com o banco de dados.")
        return df_source_data
    
    try:
        if df_last_update.empty:
            query = f"SELECT * FROM BIIN.BIIN.VW_BIIN_EQUIPAMENTO WHERE LOIN_NM_LOCAL_INSTALACAO LIKE '{loc_instal}%';"
        else:
            last_update_str = pd.to_datetime(df_last_update['ULTIMA_ATUALIZACAO']).dt.strftime('%Y-%m-%d %H:%M:%S').iloc[0]
            query = f"SELECT * FROM BIIN.BIIN.VW_BIIN_EQUIPAMENTO WHERE LOIN_NM_LOCAL_INSTALACAO LIKE '{loc_instal}%' AND EQUI_DF_ATUALIZACAO_ODS > '{last_update_str}';"
            
        df_source_data = pd.read_sql(query, conn_tdv)
        
    except Exception as e:
        print(e)
    
    finally:
        if conn_sql:
            conn_sql.close()
        if conn_tdv:
            conn_tdv.close()

    return df_source_data


def import_target_data(loc_instal):
    
    df_target_data = pd.DataFrame()
    conn_sql = connection(dsn='BD_UN-BC')
    conn_tdv = connection(dsn='TDV')
    
    if conn_sql is None or conn_tdv is None:
        print("Erro ao estabelecer conexão com o banco de dados.")
        return df_target_data
    
    try:
        query = f"""SELECT [EQUI_CD_EQUIPAMENTO] FROM [BD_UNBCDIGITAL].[BIIN].[EQUIPAMENTO] WHERE [LOIN_NM_LOCAL_INSTALACAO] LIKE '{loc_instal}%'"""
        df_target_data = pd.read_sql(query, conn_sql)
        return df_target_data
        
    except Exception as e:
        print(e)
        df_target_data = pd.DataFrame()
        return df_target_data
        
    finally:
        if conn_sql:
            conn_sql.close()
        if conn_tdv:
            conn_tdv.close()


def create_df_att(df_target_data, df_source_data):
    
    try:
        df_common_indexes = df_target_data.index.intersection(df_source_data.index)
        df_att = df_source_data.loc[df_common_indexes]
    
        df_insert = df_source_data.drop(df_common_indexes)
        df_insert = df_insert.drop_duplicates()
        
        return df_att, df_insert
    
    except Exception as e:
        print(e)
        return pd.DataFrame(), pd.DataFrame()


def insert_data_db(df_att, df_insert):
    conn_sql = connection(dsn='BD_UN-BC')
    
    if conn_sql is None:
        print("Erro ao estabelecer conexão com o banco de dados.")
        return
    
    try:
        # Atualizar registros existentes
        for index, row in df_att.iterrows():
            query = f"""
            UPDATE [BD_UNBCDIGITAL].[BIIN].[EQUIPAMENTO]
            SET 
                [CAEQ_CD_CATEGORIA_EQUIPAMENTO] = '{row['CAEQ_CD_CATEGORIA_EQUIPAMENTO']}',
                [EQUI_DS_EQUIPAMENTO] = '{row['EQUI_DS_EQUIPAMENTO']}',
                [EQUI_TX_STATUS_SISTEMA] = '{row['EQUI_TX_STATUS_SISTEMA']}',
                [EQUI_TX_STATUS_USUARIO] = '{row['EQUI_TX_STATUS_USUARIO']}',
                [EQUI_DT_INICIO_VALIDADE] = '{row['EQUI_DT_INICIO_VALIDADE']}',
                [EQUI_DT_FIM_VALIDADE] = '{row['EQUI_DT_FIM_VALIDADE']}',
                [CLEQ_CD_CLASSE_EQUIPAMENTO] = '{row['CLEQ_CD_CLASSE_EQUIPAMENTO']}',
                [GRAU_CD_GRUPO_AUTORIZACAO] = '{row['GRAU_CD_GRUPO_AUTORIZACAO']}',
                [EQUI_MD_PESO_EQUIPAMENTO] = '{row['EQUI_MD_PESO_EQUIPAMENTO']}',
                [UNME_CD_UNIDADE_MEDIDA_PESO] = '{row['UNME_CD_UNIDADE_MEDIDA_PESO']}',
                [EQUI_TX_TAMANHO_EQUIPAMENTO] = '{row['EQUI_TX_TAMANHO_EQUIPAMENTO']}',
                [EQUI_CD_INVENTARIO_EQUIPAMENTO] = '{row['EQUI_CD_INVENTARIO_EQUIPAMENTO']}',
                [EQUI_DT_ENTRADA_SERVICO_EQUIPA] = '{row['EQUI_DT_ENTRADA_SERVICO_EQUIPA']}',
                [EQUI_VL_AQUISICAO_EQUIPAMENTO] = '{row['EQUI_VL_AQUISICAO_EQUIPAMENTO']}',
                [MOED_CD_MOEDA_AQUISICAO] = '{row['MOED_CD_MOEDA_AQUISICAO']}',
                [EQUI_DT_AQUISICAO_EQUIPAMENTO] = '{row['EQUI_DT_AQUISICAO_EQUIPAMENTO']}',
                [EQUI_TX_FABRICANTE_EQUIPAMENTO] = '{row['EQUI_TX_FABRICANTE_EQUIPAMENTO']}',
                [EQUI_TX_TIPO_EQUIPAMENTO] = '{row['EQUI_TX_TIPO_EQUIPAMENTO']}',
                [EQUI_CD_SERIE_EQUIPAMENTO_FABR] = '{row['EQUI_CD_SERIE_EQUIPAMENTO_FABR']}',
                [CESA_CD_CENTRO_SAP_LOCALIZACAO] = '{row['CESA_CD_CENTRO_SAP_LOCALIZACAO']}',
                [UNPR_CD_UNIDADE_PROCESSO] = '{row['UNPR_CD_UNIDADE_PROCESSO']}',
                [EQUI_TX_SALA] = '{row['EQUI_TX_SALA']}',
                [AROP_CD_AREA_OPERACIONAL] = '{row['AROP_CD_AREA_OPERACIONAL']}',
                [CETR_CD_CENTRO_TRABALHO] = '{row['CETR_CD_CENTRO_TRABALHO']}',
                [EQUI_IN_HISTORICO_FALHA] = '{row['EQUI_IN_HISTORICO_FALHA']}',
                [EQUI_TX_SELECAO] = '{row['EQUI_TX_SELECAO']}',
                [CECU_CD_CENTRO_CUSTO] = '{row['CECU_CD_CENTRO_CUSTO']}',
                [ARCC_CD_SAP_AREA_CONTABILIDADE] = '{row['ARCC_CD_SAP_AREA_CONTABILIDADE']}',
                [EQUI_CD_ELEMENTO_PEP] = '{row['EQUI_CD_ELEMENTO_PEP']}',
                [CESA_CD_CENTRO_SAP_PLANEJAMENT] = '{row['CESA_CD_CENTRO_SAP_PLANEJAMENT']}',
                [GRPM_CD_GRUPO_PLANEJA_MANUTENC] = '{row['GRPM_CD_GRUPO_PLANEJA_MANUTENC']}',
                [PECA_CD_PERFIL_CATALOGO] = '{row['PECA_CD_PERFIL_CATALOGO']}',
                [LOIN_NM_LOCAL_INSTALACAO] = '{row['LOIN_NM_LOCAL_INSTALACAO']}',
                [EQUI_TX_IDENTIFICACAO_TECNICA] = '{row['EQUI_TX_IDENTIFICACAO_TECNICA']}',
                [EQUI_CD_USUARIO_ULTIMA_MODIFIC] = '{row['EQUI_CD_USUARIO_ULTIMA_MODIFIC']}',
                [EQUI_DT_ULTIMA_MODIFICACAO] = '{row['EQUI_DT_ULTIMA_MODIFICACAO']}',
                [EQUI_IN_EXISTE_TEXTO_DESCRITIV] = '{row['EQUI_IN_EXISTE_TEXTO_DESCRITIV']}',
                [EQUI_DT_CRIACAO] = '{row['EQUI_DT_CRIACAO']}',
                [EQUI_CD_USUARIO_CRIACAO] = '{row['EQUI_CD_USUARIO_CRIACAO']}',
                [PAIS_SG_PAIS_PRODUTOR] = '{row['PAIS_SG_PAIS_PRODUTOR']}',
                [EQUI_TX_ANO_CONSTRUCAO] = '{row['EQUI_TX_ANO_CONSTRUCAO']}',
                [EQUI_TX_MES_CONSTRUCAO] = '{row['EQUI_TX_MES_CONSTRUCAO']}',
                [EQUI_CD_PECA_FABRICANTE] = '{row['EQUI_CD_PECA_FABRICANTE']}',
                [EQUI_TX_CIDADE] = '{row['EQUI_TX_CIDADE']}',
                [EQUI_CD_OBJETO] = '{row['EQUI_CD_OBJETO']}',
                [EQUI_DF_ATUALIZACAO_STAGING] = '{row['EQUI_DF_ATUALIZACAO_STAGING']}',
                [EQUI_DF_ATUALIZACAO_ODS] = '{row['EQUI_DF_ATUALIZACAO_ODS']}',
                [DATA_ATUALIZACAO_ODS] = '{row['DATA_ATUALIZACAO_ODS']}',
                [USUARIO_ATUALIZACAO_ODS] = '{row['USUARIO_ATUALIZACAO_ODS']}'
            WHERE [EQUI_CD_EQUIPAMENTO] = '{row['EQUI_CD_EQUIPAMENTO']}'
            """
            conn_sql.execute(query)
        
        # Inserir novos registros
        for index, row in df_insert.iterrows():
            query = f"""
            INSERT INTO [BD_UNBCDIGITAL].[BIIN].[EQUIPAMENTO] (
                [EQUI_CD_EQUIPAMENTO],
                [CAEQ_CD_CATEGORIA_EQUIPAMENTO],
                [EQUI_DS_EQUIPAMENTO],
                [EQUI_TX_STATUS_SISTEMA],
                [EQUI_TX_STATUS_USUARIO],
                [EQUI_DT_INICIO_VALIDADE],
                [EQUI_DT_FIM_VALIDADE],
                [CLEQ_CD_CLASSE_EQUIPAMENTO],
                [GRAU_CD_GRUPO_AUTORIZACAO],
                [EQUI_MD_PESO_EQUIPAMENTO],
                [UNME_CD_UNIDADE_MEDIDA_PESO],
                [EQUI_TX_TAMANHO_EQUIPAMENTO],
                [EQUI_CD_INVENTARIO_EQUIPAMENTO],
                [EQUI_DT_ENTRADA_SERVICO_EQUIPA],
                [EQUI_VL_AQUISICAO_EQUIPAMENTO],
                [MOED_CD_MOEDA_AQUISICAO],
                [EQUI_DT_AQUISICAO_EQUIPAMENTO],
                [EQUI_TX_FABRICANTE_EQUIPAMENTO],
                [EQUI_TX_TIPO_EQUIPAMENTO],
                [EQUI_CD_SERIE_EQUIPAMENTO_FABR],
                [CESA_CD_CENTRO_SAP_LOCALIZACAO],
                [UNPR_CD_UNIDADE_PROCESSO],
                [EQUI_TX_SALA],
                [AROP_CD_AREA_OPERACIONAL],
                [CETR_CD_CENTRO_TRABALHO],
                [EQUI_IN_HISTORICO_FALHA],
                [EQUI_TX_SELECAO],
                [CECU_CD_CENTRO_CUSTO],
                [ARCC_CD_SAP_AREA_CONTABILIDADE],
                [EQUI_CD_ELEMENTO_PEP],
                [CESA_CD_CENTRO_SAP_PLANEJAMENT],
                [GRPM_CD_GRUPO_PLANEJA_MANUTENC],
                [PECA_CD_PERFIL_CATALOGO],
                [LOIN_NM_LOCAL_INSTALACAO],
                [EQUI_TX_IDENTIFICACAO_TECNICA],
                [EQUI_CD_USUARIO_ULTIMA_MODIFIC],
                [EQUI_DT_ULTIMA_MODIFICACAO],
                [EQUI_IN_EXISTE_TEXTO_DESCRITIV],
                [EQUI_DT_CRIACAO],
                [EQUI_CD_USUARIO_CRIACAO],
                [PAIS_SG_PAIS_PRODUTOR],
                [EQUI_TX_ANO_CONSTRUCAO],
                [EQUI_TX_MES_CONSTRUCAO],
                [EQUI_CD_PECA_FABRICANTE],
                [EQUI_TX_CIDADE],
                [EQUI_CD_OBJETO],
                [EQUI_DF_ATUALIZACAO_STAGING],
                [EQUI_DF_ATUALIZACAO_ODS],
                [DATA_ATUALIZACAO_ODS],
                [USUARIO_ATUALIZACAO_ODS]
            ) VALUES (
                '{row['EQUI_CD_EQUIPAMENTO']}',
                '{row['CAEQ_CD_CATEGORIA_EQUIPAMENTO']}',
                '{row['EQUI_DS_EQUIPAMENTO']}',
                '{row['EQUI_TX_STATUS_SISTEMA']}',
                '{row['EQUI_TX_STATUS_USUARIO']}',
                '{row['EQUI_DT_INICIO_VALIDADE']}',
                '{row['EQUI_DT_FIM_VALIDADE']}',
                '{row['CLEQ_CD_CLASSE_EQUIPAMENTO']}',
                '{row['GRAU_CD_GRUPO_AUTORIZACAO']}',
                '{row['EQUI_MD_PESO_EQUIPAMENTO']}',
                '{row['UNME_CD_UNIDADE_MEDIDA_PESO']}',
                '{row['EQUI_TX_TAMANHO_EQUIPAMENTO']}',
                '{row['EQUI_CD_INVENTARIO_EQUIPAMENTO']}',
                '{row['EQUI_DT_ENTRADA_SERVICO_EQUIPA']}',
                '{row['EQUI_VL_AQUISICAO_EQUIPAMENTO']}',
                '{row['MOED_CD_MOEDA_AQUISICAO']}',
                '{row['EQUI_DT_AQUISICAO_EQUIPAMENTO']}',
                '{row['EQUI_TX_FABRICANTE_EQUIPAMENTO']}',
                '{row['EQUI_TX_TIPO_EQUIPAMENTO']}',
                '{row['EQUI_CD_SERIE_EQUIPAMENTO_FABR']}',
                '{row['CESA_CD_CENTRO_SAP_LOCALIZACAO']}',
                '{row['UNPR_CD_UNIDADE_PROCESSO']}',
                '{row['EQUI_TX_SALA']}',
                '{row['AROP_CD_AREA_OPERACIONAL']}',
                '{row['CETR_CD_CENTRO_TRABALHO']}',
                '{row['EQUI_IN_HISTORICO_FALHA']}',
                '{row['EQUI_TX_SELECAO']}',
                '{row['CECU_CD_CENTRO_CUSTO']}',
                '{row['ARCC_CD_SAP_AREA_CONTABILIDADE']}',
                '{row['EQUI_CD_ELEMENTO_PEP']}',
                '{row['CESA_CD_CENTRO_SAP_PLANEJAMENT']}',
                '{row['GRPM_CD_GRUPO_PLANEJA_MANUTENC']}',
                '{row['PECA_CD_PERFIL_CATALOGO']}',
                '{row['LOIN_NM_LOCAL_INSTALACAO']}',
                '{row['EQUI_TX_IDENTIFICACAO_TECNICA']}',
                '{row['EQUI_CD_USUARIO_ULTIMA_MODIFIC']}',
                '{row['EQUI_DT_ULTIMA_MODIFICACAO']}',
                '{row['EQUI_IN_EXISTE_TEXTO_DESCRITIV']}',
                '{row['EQUI_DT_CRIACAO']}',
                '{row['EQUI_CD_USUARIO_CRIACAO']}',
                '{row['PAIS_SG_PAIS_PRODUTOR']}',
                '{row['EQUI_TX_ANO_CONSTRUCAO']}',
                '{row['EQUI_TX_MES_CONSTRUCAO']}',
                '{row['EQUI_CD_PECA_FABRICANTE']}',
                '{row['EQUI_TX_CIDADE']}',
                '{row['EQUI_CD_OBJETO']}',
                '{row['EQUI_DF_ATUALIZACAO_STAGING']}',
                '{row['EQUI_DF_ATUALIZACAO_ODS']}',
                '{row['DATA_ATUALIZACAO_ODS']}',
                '{row['USUARIO_ATUALIZACAO_ODS']}'
            )
            """
            conn_sql.execute(query)
        
        conn_sql.commit()
        print("Dados inseridos/atualizados com sucesso.")
    
    except Exception as e:
        print(f"Erro ao inserir/atualizar dados: {e}")
    
    finally:
        if conn_sql:
            conn_sql.close()


if __name__ == "__main__":
    main()
