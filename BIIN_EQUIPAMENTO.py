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
                        '301056', '301057', '303006', '301066', '301063', '301071']

    with connection(dsn='BD_UN-BC') as conn_sql, connection(dsn='TDV') as conn_tdv:
        for loc_instal in lista_loc_instal:
            df_last_update = last_update(table='BIIN_EQUIPAMENTO', conn=conn_sql)
            
            df_source_data = import_source_data(loc_instal, df_last_update, conn_tdv)
            df_target_data = import_target_data(loc_instal, conn_sql)

            df_att, df_insert = create_df_att(df_target_data, df_source_data)
            # df_att.to_csv(f'DATAFRAME ATT {loc_instal}.csv', index=False)
            # df_insert.to_csv(f'DATAFRAME INSERT {loc_instal}.csv', index=False)

            print(f'REGISTROS PARA UPDATE = {len(df_att)}')
            print(f'REGISTROS PARA INSERT = {len(df_insert)}')

            insert_data_db(df_insert, conn_sql, loc_instal)
            att_data_db(df_att, conn_sql, loc_instal)

        update_management_table(table='BIIN_EQUIPAMENTO')


def import_source_data(loc_instal, df_last_update, conn_tdv):
    df_source_data = pd.DataFrame()
    
    try:
        query = f"SELECT * FROM BIIN.BIIN.VW_BIIN_EQUIPAMENTO WHERE LOIN_NM_LOCAL_INSTALACAO LIKE '{loc_instal}%'"
        if not df_last_update.empty:
            last_update_str = df_last_update['ULTIMA_ATUALIZACAO'].max().strftime('%Y-%m-%d %H:%M:%S')
            query += f" AND EQUI_DF_ATUALIZACAO_ODS > '{last_update_str}'"
        
        df_source_data = pd.read_sql(query, conn_tdv)
    except Exception as e:
        print(e)
    
    return df_source_data


def import_target_data(loc_instal, conn_sql):
    df_target_data = pd.DataFrame()
    
    try:
        query = f"SELECT * FROM BD_UNBCDIGITAL.BIIN.EQUIPAMENTO WHERE LOIN_NM_LOCAL_INSTALACAO LIKE '{loc_instal}%'"
        df_target_data = pd.read_sql(query, conn_sql)
    except Exception as e:
        print(e)
    
    return df_target_data


def create_df_att(df_target_data, df_source_data):
    try:
        df_merge = df_source_data.merge(df_target_data, on='EQUI_CD_EQUIPAMENTO', how='left', suffixes=('', '_target'), indicator=True)
        df_att = df_merge[df_merge['_merge'] == 'both'].drop(columns=['_merge'])
        df_insert = df_merge[df_merge['_merge'] == 'left_only'].drop(columns=['_merge'])
        return df_att, df_insert
    except Exception as e:
        print(e)
        return pd.DataFrame(), pd.DataFrame()


def insert_data_db(df_insert, conn_sql, loc_instal):
    if df_insert.empty:
        print(f'SEM REGISTROS PARA INSERT EM {loc_instal}')
        return
    
    query = "INSERT INTO BD_UNBCDIGITAL.BIIN.EQUIPAMENTO VALUES "
    values_string = []
    
    try:
        for _, row in tqdm(df_insert.iterrows(), total=len(df_insert), desc="Inserindo dados", unit="registro"):
            values = [f"'{str(value).replace("'", "''")}'" if pd.notna(value) else 'NULL' for value in row]
            values_string.append(f"({', '.join(values)})")
            
            if len(values_string) == 1000:
                conn_sql.execute(query + ', '.join(values_string))
                conn_sql.commit()
                values_string = []

        if values_string:
            conn_sql.execute(query + ', '.join(values_string))
            conn_sql.commit()
            
        print(f'DADOS INSERIDOS EM {loc_instal} - {len(df_insert)}')
    except Exception as e:
        print(e)
        conn_sql.rollback()


def att_data_db(df_att, conn_sql, loc_instal):
    if df_att.empty:
        print(f'SEM REGISTROS PARA UPDATE EM {loc_instal}\n')
        return
    
    try:
        # Remove a tabela temporária se ela já existir
        drop_temp_table_query = f"IF OBJECT_ID('[BD_UNBCDIGITAL].[BIIN].[TEMP_EQUIPAMENTO]', 'U') IS NOT NULL DROP TABLE [BD_UNBCDIGITAL].[BIIN].[TEMP_EQUIPAMENTO]"
        conn_sql.execute(drop_temp_table_query)
        conn_sql.commit()

        # Cria uma tabela temporária com os dados a serem atualizados
        temp_table_name = "TEMP_EQUIPAMENTO"
        create_temp_table_query = f"""
            CREATE TABLE [BD_UNBCDIGITAL].[BIIN].[{temp_table_name}] (
                {', '.join([f'{col} NVARCHAR(MAX)' for col in df_att.columns])}
            )
        """
        conn_sql.execute(create_temp_table_query)
        conn_sql.commit()

        # Insere os dados na tabela temporária
        insert_temp_table_query = f"INSERT INTO [BD_UNBCDIGITAL].[BIIN].[{temp_table_name}] VALUES "
        values_string = []
        for _, row in tqdm(df_att.iterrows(), total=len(df_att), desc="Inserindo dados temporários", unit="registro"):
            values = [f"'{str(value).replace("'", "''")}'" if pd.notna(value) else 'NULL' for value in row]
            values_string.append(f"({', '.join(values)})")
            
            if len(values_string) == 1000:
                conn_sql.execute(insert_temp_table_query + ', '.join(values_string))
                conn_sql.commit()
                values_string = []

        if values_string:
            conn_sql.execute(insert_temp_table_query + ', '.join(values_string))
            conn_sql.commit()

        # Atualiza os registros existentes na tabela de destino
        set_clauses = ', '.join([f"A.{col} = B.{col}" for col in df_att.columns if col != 'EQUI_CD_EQUIPAMENTO'])
        update_query = f"""
            UPDATE A
            SET {set_clauses}
            FROM [BD_UNBCDIGITAL].[BIIN].[EQUIPAMENTO] A
            INNER JOIN [BD_UNBCDIGITAL].[BIIN].[{temp_table_name}] B
            ON A.EQUI_CD_EQUIPAMENTO = B.EQUI_CD_EQUIPAMENTO
        """
        conn_sql.execute(update_query)
        conn_sql.commit()

        # Remove a tabela temporária
        drop_temp_table_query = f"DROP TABLE [BD_UNBCDIGITAL].[BIIN].[{temp_table_name}]"
        conn_sql.execute(drop_temp_table_query)
        conn_sql.commit()
        
        print(f'DADOS ATUALIZADOS EM {loc_instal} - {len(df_att)}\n')
    except Exception as e:
        print(e)
        conn_sql.rollback()


if __name__ == "__main__":
    main()
