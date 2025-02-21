import pandas as pd
import warnings

from utils.conn_sql import connection
from utils.last_update import last_update

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


if __name__ == "__main__":
    main()
