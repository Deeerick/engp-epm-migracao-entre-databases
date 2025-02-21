import pandas as pd
import warnings

from tqdm import tqdm
from utils.last_update import last_update
from utils.connection_db import connection
from utils.update_table import update_management_table

warnings.simplefilter(action='ignore', category=UserWarning)


def main():
    lista_loc_instal = ['351902', '301081', '301049', '301059', '351401', '351402', 
                        '351901', '351701', '301010', '301083', '301018', '301019', 
                        '301020', '301029', '301032', '301033', '301036', '301040', 
                        '301056', '301057', '303006', '301066', '301063', '301071'
                        ]
    
    conn_sql = connection(dsn='BD_UN-BC')
    conn_tdv = connection(dsn='TDV')
    
    for loc_instal in lista_loc_instal:
        
        df_last_update = last_update(table='BIIN_EQUIPAMENTO', conn=conn_sql)
        # df_last_update = pd.to_datetime(df_last_update['ULTIMA_ATUALIZACAO']).dt.strftime('%Y-%m-%d %H:%M:%S').iloc[0]
        
        df_source_data = import_source_data(loc_instal, df_last_update)
        # print(f'TDV - PLATAFORMA {loc_instal} = {len(df_source_data)} REGISTROS')
        
        df_target_data = import_target_data(loc_instal)
        # print(f'SQL - PLATAFORMA {loc_instal} = {len(df_target_data)} REGISTROS\n')
        
        # dif = len(df_target_data) - len(df_source_data)
        # print(f'REGISTROS PARA CARREGAR = {dif}')
        
        df_att, df_insert = create_df_att(df_target_data, df_source_data)
        print(f'REGISTROS PARA UPDATE = {len(df_att)}')
        print(f'REGISTROS PARA INSERT = {len(df_insert)}\n')
        
        insert_data_db(df_insert)
        att_data_db(df_att)
        
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
        
        # Save dataframes to txt files
        # df_att.to_csv('df_att.txt', sep='\t', index=False)
        # df_insert.to_csv('df_insert.txt', sep='\t', index=False)
        
        return df_att, df_insert
    
    except Exception as e:
        print(e)
        return pd.DataFrame(), pd.DataFrame()


def insert_data_db(df_insert):
    conn_sql = connection(dsn='BD_UN-BC')
    
    query = f"INSERT INTO [BD_UNBCDIGITAL].[BIIN].[EQUIPAMENTO] VALUES "
    
    if conn_sql is None:
        print("Erro ao estabelecer conexão com o banco de dados.")
        return
    
    values_string = []  # Esta variável deve ser uma lista.
    
    try:
        count = 0
        # Adiciona a barra de progresso no loop principal
        for index, row in tqdm(df_insert.iterrows(), total=len(df_insert), desc="Inserindo dados", unit="registro"):
            values = ["'{}'".format(str(value).replace("'", "''")) if pd.notna(value) else 'NULL' for value in row]
            values_string.append(f"({', '.join(values)})")  # Adiciona as tuplas de valores à lista.
            
            if len(values_string) == 1000:
                query_values = ', '.join(values_string)
                query_full = f"{query} {query_values}"
                conn_sql.execute(query_full)
                conn_sql.commit()
                count += 1000
                values_string = []  # Limpa a lista para o próximo lote.

        # Caso restem dados a serem inseridos após o loop
        if values_string:
            query_values = ', '.join(values_string)
            query_full = f"{query} {query_values}"
            conn_sql.execute(query_full)
            conn_sql.commit()
                
    except Exception as e:
        print(e)
        conn_sql.rollback()
        
    finally:
        if values_string:
            try:
                query_values = ', '.join(values_string)
                query_full = f"{query} {query_values}"
                conn_sql.execute(query_full)
                conn_sql.commit()
                print('FINALLY INSERT')
            except Exception as e:
                print(e)
                conn_sql.rollback()
                
        conn_sql.close()


def att_data_db(df_att):
    conn_sql = connection(dsn='BD_UN-BC')
    
    try:
        df_att.to_sql('TEMP_EQUIPAMENTO', conn_sql, schema='BIIN', if_exists='replace', index=False)
        
        set_clauses = []
        
        for col in df_att.columns:
            if col == 'EQUI_CD_EQUIPAMENTO':
                continue
            set_clauses.append(f"A.{col} = B.{col}")
            
        set_clauses_str = ', '.join(set_clauses)
        
        where_conditions = f'A.EQUI_CD_EQUIPAMENTO = B.EQUI_CD_EQUIPAMENTO'
        
        query = f"""UPDATE A
                    SET {set_clauses_str}
                    FROM [BD_UNBCDIGITAL].[BIIN].[EQUIPAMENTO] A
                    INNER JOIN [BD_UNBCDIGITAL].[BIIN].[TEMP_EQUIPAMENTO] B
                    ON {where_conditions}"""
                    
        conn_sql.execute(query)
        conn_sql.commit()
        print('UPDATE OK')
        return True
        
    except Exception as e:
        print(e)
        conn_sql.rollback()
        return False
        
    finally:
        conn_sql.commit()
        conn_sql.close()


if __name__ == "__main__":
    main()
