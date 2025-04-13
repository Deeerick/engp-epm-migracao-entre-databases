import re
import pandas as pd
import warnings

from sqlalchemy import create_engine
from sqlalchemy.exc import DBAPIError
from utils.connection_db import connection
from utils.update_table import update_management_table
from utils.last_update import last_update

warnings.simplefilter(action='ignore', category=UserWarning)


def main():
    # Locais de instalação a serem consultados
    lista_loc = [
        '351902', '301081', '301049', '301059', '351401', '351402',
        '351901', '351701', '301010', '301083', '301018', '301019',
        '301020', '301029', '301032', '301033', '301036', '301040',
        '301056', '301057', '303006', '301066', '301063', '301071'
    ]

    conn_sql = connection(dsn='BD_UN-BC')
    conn_tdv = connection(dsn='TDV')

    # Verificar a data da última atualização da tabela
    df_last_update = last_update(conn=conn_sql, table='BIIN_LOCAL_INSTALACAO')
    df_last_update = df_last_update['ULTIMA_ATUALIZACAO'][0]
    
    for loc in lista_loc:
        df_origem = importar_dados_origem(loc, df_last_update, conn_tdv)
        
        if df_origem is False:
            print(f"Erro ao importar dados de origem para {loc}")
            continue

        df_destino = importar_dados_destino(conn_sql, loc)

        if df_destino is False:
            print(f"Erro ao importar dados de destino para {loc}")
            continue

        df_update, df_insert = criar_df_atualizacao(df_destino, df_origem)

        if df_update is False or df_insert is False:
            print(f"Erro ao criar DataFrame de atualização para {loc}")
            continue

        inserir_dados_no_banco(df_insert, conn_sql)
        atualizar_no_banco(df_update, conn_sql)

    tabela = 'BIIN_LOCAL_INSTALACAO'
    update_management_table(tabela, conn_sql)


def importar_dados_origem(loc, df_last_update, conn_tdv):
    try:
        if df_last_update:
            
            query = f"""SELECT * 
                            FROM BIIN.BIIN.VW_BIIN_LOCAL_INSTALACAO 
                            WHERE LOIN_NM_LOCAL_INSTALACAO LIKE '{loc}%'
                            AND LOIN_DF_ATUALIZACAO_ODS > '{df_last_update}'
                        """
        else:
            query = f"""SELECT * 
                        FROM BIIN.BIIN.VW_BIIN_LOCAL_INSTALACAO 
                        WHERE LOIN_NM_LOCAL_INSTALACAO LIKE '{loc}%'
                        """

        df_origem = pd.read_sql(query, conn_tdv)

        return df_origem

    except Exception as e:
        print(f"Erro: {e}")
        return False


def importar_dados_destino(conn_sql, loc):
    try:
        query = f"""SELECT [LOIN_NM_LOCAL_INSTALACAO] 
                    FROM [BD_UNBCDIGITAL].[BIIN].[LOCAL_INSTALACAO] 
                    WHERE [LOIN_NM_LOCAL_INSTALACAO] LIKE '{loc}%'"""
                
        df_destino = pd.read_sql(query, con=conn_sql)
        return df_destino
    
    except Exception as e:
        print(f"Erro: {e}")
        return False


def criar_df_atualizacao(df_destino, df_origem):
    try:
        if not df_destino.empty and not df_origem.empty:
            indices_comuns = df_destino['LOIN_NM_LOCAL_INSTALACAO'].isin(df_origem['LOIN_NM_LOCAL_INSTALACAO'])
            df_update = df_destino[indices_comuns].copy()
            df_update = pd.merge(df_update, df_origem, on='LOIN_NM_LOCAL_INSTALACAO', suffixes=('_destino', '_origem'))
            df_insert = df_origem[~df_origem['LOIN_NM_LOCAL_INSTALACAO'].isin(df_destino['LOIN_NM_LOCAL_INSTALACAO'])]
        else:
            df_insert = df_origem.copy(deep=False)
            df_update = pd.DataFrame(columns=df_destino.columns)
            df_update = df_update.astype(df_update.dtypes)
        return df_update, df_insert
    
    except Exception as e:
        print(f"Erro: {e}")
        return False


def atualizar_destino(df_destino, df_origem, tabela_destino, conn, filtro, batch_size=500):

    agrupamento_update = []
    agrupamento_insert = []

    for index, row in df_origem.iterrows():

        cursor = conn.cursor()
        st_valor_coluna = row[filtro]

        if st_valor_coluna in df_destino:

            # Realizar update se ordem existir
            set_clauses = []

            for col in df_origem.columns:

                if row[col] != 'Null':
                    set_clauses.append(f"{col} = '{row[col]}'")
                else:
                    set_clauses.append(f"{col} = Null")

            HeadWithOrdem = set_clauses[0]
            match = re.search(r"'(.*?)'", HeadWithOrdem)
            if match:
                stOrdem = match.group(1)

            string_sqlUpdate = f"UPDATE {tabela_destino} SET {
                ', '.join(set_clauses)} WHERE {filtro} like '{stOrdem}';"

            agrupamento_update.append(string_sqlUpdate)

            if len(agrupamento_update) >= batch_size:
                cursor.execute(string_sqlUpdate)
                agrupamento_update = []

        else:

            column_names = df_origem.columns
            values = [f"'{row[col]}'" if row[col] !=
                      'Null' else 'Null' for col in column_names]
            stValues = f"({', '.join(values)})"

            # agrupamento_insert.append(string_sqlInsert)
            agrupamento_insert.append(stValues)

            if len(agrupamento_insert) >= batch_size:
                string_sqlInsert = f"INSERT INTO {tabela_destino} VALUES {
                    ', '.join(agrupamento_insert)}"
                cursor.execute(string_sqlInsert)
                agrupamento_insert = []

    if agrupamento_update:
        cursor.execute(string_sqlUpdate)

    if agrupamento_insert:
        string_sqlInsert = f"INSERT INTO {tabela_destino} VALUES {
            ', '.join(agrupamento_insert)}"
        cursor.execute(string_sqlInsert)

    conn.commit()


def inserir_dados_no_banco(df_insert, conn):

    engine = None
    st_server = 'npaa7408'
    st_database = 'bd_unbcdigital'
    conn_str = f'mssql+pyodbc://{st_server}/{st_database}?driver=ODBC+Driver+17+for+SQL+Server'

    try:
        engine = create_engine(conn_str)  # Criar engine
        df_insert.to_sql('local_instalacao', con=engine, if_exists='append', index=False, schema='biin')
        return True
    
    except DBAPIError as e:
        print(f"Erro de banco de dados: {e}")
        return False
    
    except Exception as e:
        print(f"Erro: {e}")
        return False
    
    finally:
        if engine:
            engine.dispose()


def atualizar_no_banco(df_update, conn_sql):

    engine = None

    st_server = 'npaa7408'
    st_database = 'bd_unbcdigital'

    conn_str = f'mssql+pyodbc://{st_server}/{
        st_database}?driver=ODBC+Driver+17+for+SQL+Server'

    try:
        engine = create_engine(conn_str)
        df_update.to_sql('TabTempUpdate', con=engine,
                         index=False, if_exists='replace', schema='biin')

        # Construir a cláusula SET para o UPDATE
        set_clauses = []
        for col in df_update.columns:
            set_clauses.append(f"A.{col} = B.{col}")

        set_clause_str = ", ".join(set_clauses)

        # Construir a consulta UPDATE
        update_query = f"""
                        UPDATE A
                        SET {set_clause_str}
                        FROM [BD_UNBCDIGITAL].[BIIN].[LOCAL_INSTALACAO] A
                        INNER JOIN [BD_UNBCDIGITAL].[BIIN].[TABTEMPUPDATE] B 
                        ON A.LOIN_NM_LOCAL_INSTALACAO = B.LOIN_NM_LOCAL_INSTALACAO;
                        """

        # Executar o UPDATE
        conn_sql.execute(update_query)
        return True
    
    except Exception as e:
        print(f"Erro: {e}")
        return False
    
    finally:
        # Dispose do engine para liberar recursos
        if engine:
            engine.dispose()

        # Remover a tabela temporária após o uso
        try:
            conn_sql.execute(f"DROP TABLE [BD_UNBCDIGITAL].[BIIN].[TABTEMPUPDATE]")
            conn_sql.commit()
            # print('Tabela temporária excluída com sucesso.')
        except:
            print("Não foi possível excluir a tabela temporária.")


if __name__ == '__main__':
    print('Executando BIIN_LOCAL_INSTALACAO.py...')
    main()
    print('BIIN_LOCAL_INSTALACAO.py atualizado com sucesso.')
