import re
import pandas as pd
import warnings

from datetime import datetime
from sqlalchemy import create_engine
from utils.connection_db import connection
from utils.update_table import update_management_table
from utils.last_update import last_update
from CONGELAR_NOTAS import congelar_notas

warnings.simplefilter(action='ignore', category=UserWarning)


def main():
    #locais de instalação a serem consultados
    lista_loc = [
        '351902', '301081', '301049', '301059', '351401', '351402',
        '351901', '351701', '301010', '301083', '301018', '301019',
        '301020', '301029', '301032', '301033', '301036', '301040',
        '301056', '301057', '303006', '301066', '301063', '301071',
        '2100', '2110', '2120', '2160', '2170', '2172'
    ]

    conn_sql = connection(dsn='BD_UN-BC')
    conn_tdv = connection(dsn='TDV')
    
    df_last_update = last_update(conn=conn_sql, table='BIIN_NOTA_MANUTENCAO')
    df_last_update = df_last_update['ULTIMA_ATUALIZACAO'][0]
    
    for loc in lista_loc:
        
        df_origem = importar_dados_origem(loc, df_last_update, conn_tdv)
        df_destino = importar_dados_destino(conn_sql, loc)
        df_update, df_insert = criar_dataframe_atualizacao(df_destino, df_origem)

        inserir_no_banco(df_insert, conn_sql)
        atualizar_no_banco(df_update, conn_sql)

    tabela = 'BIIN_NOTA_MANUTENCAO'
    update_management_table(tabela, conn_sql)
    
    congelar_notas(conn_sql)


def importar_dados_origem(loc, df_last_update, conn_tdv):
    try:
        if df_last_update:
            query = f"""
                    SELECT * FROM  BIIN.BIIN.VW_NOTA_MANUTENCAO WHERE LOCAL_INSTALACAO like '{loc}%'
                    AND data_hora_atualizacao_ods > '{df_last_update}'
                    """
        else:
            query = f"SELECT * FROM BIIN.BIIN.VW_NOTA_MANUTENCAO WHERE LOCAL_INSTALACAO like '{loc}%'"
                   
        df_origem = pd.read_sql(query,conn_tdv)
        print(f"Plataforma: {loc} Total_Linhas: {len(df_origem)}")

        return df_origem
    
    except:
        return False
    

def atualizar_destino(df_destino, df_origem, conn, batch_size=500):

    agrupamento_update = []
    agrupamento_insert = []

    for index, row in df_origem.iterrows():
    
        cursor = conn.cursor()
        st_valor_coluna = row['LOCAL_INSTALACAO']        

        if st_valor_coluna in df_destino:
            
            #Realizar update se ordem existir
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
            
            string_sqlUpdate = f"UPDATE [BD_UNBCDIGITAL].[BIIN].[NOTA_MANUTENCAO] SET {', '.join(set_clauses)} WHERE LOCAL_INSTALACAO LIKE '{stOrdem}';" 

            agrupamento_update.append(string_sqlUpdate)
            
            if len(agrupamento_update) >= batch_size:
                cursor.execute(string_sqlUpdate)
                agrupamento_update = []
            
        else:
            
            column_names = df_origem.columns
            values = [f"'{row[col]}'" if row[col] != 'Null' else 'Null' for col in column_names]
            stValues = f"({', '.join(values)})"
            
            #agrupamento_insert.append(string_sqlInsert)
            agrupamento_insert.append(stValues)            

            if len(agrupamento_insert) >= batch_size:
                string_sqlInsert = f"INSERT INTO [BD_UNBCDIGITAL].[BIIN].[NOTA_MANUTENCAO] VALUES {', '.join(agrupamento_insert)}"
                cursor.execute(string_sqlInsert)
                agrupamento_insert = []

    if agrupamento_update:
        cursor.execute(string_sqlUpdate)
        
    if agrupamento_insert:
        string_sqlInsert = f"INSERT INTO [BD_UNBCDIGITAL].[BIIN].[NOTA_MANUTENCAO] VALUES {', '.join(agrupamento_insert)}"
        cursor.execute(string_sqlInsert)

    conn.commit()


def importar_dados_destino(conn_sql, loc):

    try:
        query = f"""SELECT NOTA FROM [BD_UNBCDIGITAL].[BIIN].[NOTA_MANUTENCAO] WHERE LOCAL_INSTALACAO LIKE '{loc}%'"""
        df_destino = pd.read_sql(query, conn_sql)
        return df_destino
    
    except Exception as e:
        print(f"Erro: {e}")
        return False


def criar_dataframe_atualizacao(df_destino, df_origem):
    try:
        # Encontrar índices dos itens comuns
        indices_comuns = df_destino['NOTA'].isin(df_origem['NOTA'])

        # Dividir em DataFrame de atualização e inserção
        df_update = df_destino[indices_comuns].copy()
        df_update = pd.merge(df_update, df_origem, on='NOTA', suffixes=('_destino', '_origem'))

        df_insert = df_origem[~df_origem['NOTA'].isin(df_destino['NOTA'])]

        return df_update, df_insert
    
    except Exception as e:
        print(f"Erro: {e}")
        return pd.DataFrame(), pd.DataFrame()

  
def inserir_no_banco(df_insert, conn_sql, batch_size=1000):
    # Substitua 'SeuEsquema' pelo esquema da tabela, se necessário
    query = f"INSERT INTO [BD_UNBCDIGITAL].[BIIN].[NOTA_MANUTENCAO] VALUES "

    # Crie uma lista para armazenar as strings de valores
    values_strings = []

    try:
        contador = 0
        for index, row in df_insert.iterrows():
            # Use parâmetros e formate a string de valores
            values = ["'{}'".format(str(value).replace("'", "")) if pd.notna(value) else 'NULL' for value in row]
            values_string = "({})".format(', '.join(values))
            values_strings.append(values_string)

            # Se a lista atingir o tamanho do lote, execute o commit
            if len(values_strings) == batch_size:
                # Concatene as strings e execute o INSERT
                query_values = ', '.join(values_strings)
                query_full = "{} {};".format(query, query_values)
                conn_sql.execute(query_full)
                contador = contador + 1000
                
                hora_atual = datetime.now().strftime("%H:%M:%S")
                print(f"Hora atual: {hora_atual} - linha: {contador}")      

                # Limpe a lista para o próximo lote
                values_strings = []
                
    except Exception as e:
        print(f"Erro ao inserir no banco: {e}")
        conn_sql.rollback()
        
    finally:
        # Se ainda houver valores na lista, execute o commit final
        if values_strings:
            try:
                query_values = ', '.join(values_strings)
                query_full = "{} {};".format(query, query_values)
                conn_sql.execute(query_full)
            except Exception as e:
                print(f"Erro ao inserir no banco: {e}")
                conn_sql.rollback()

        conn_sql.commit()


def atualizar_no_banco(df_update, conn_sql):

    engine = None
    
    st_server = 'npaa7408'
    st_database = 'bd_unbcdigital'
    conn_str = f'mssql+pyodbc://{st_server}/{st_database}?driver=ODBC+Driver+17+for+SQL+Server'

    try:
        # Crie o engine
        engine = create_engine(conn_str)
        df_update.to_sql('TabTempUpdate', con=engine, index=False, if_exists='replace', schema='biin')

        # Construir a cláusula SET para o UPDATE
        set_clauses = []
        for col in df_update.columns:
            set_clauses.append(f"A.{col} = B.{col}")

        set_clause_str = ", ".join(set_clauses)

        # Construir a cláusula WHERE para o UPDATE
        where_condition = f"A.LOCAL_INSTALACAO = B.LOCAL_INSTALACAO"

        # Construir a consulta UPDATE
        update_query = f"""
                        UPDATE A
                        SET {set_clause_str}
                        FROM [BD_UNBCDIGITAL].[BIIN].[NOTA_MANUTENCAO] A
                        INNER JOIN [BD_UNBCDIGITAL].[biin].[TabTempUpdate] B ON {where_condition};
                        """

        # Executar o UPDATE
        conn_sql.execute(update_query)
        return True
    
    except Exception as e:
        print(f"Erro: {e}")
        return False
    
    finally:
        # Remover a tabela temporária após o uso
        try:
            conn_sql.execute(f"DROP TABLE [BD_UNBCDIGITAL].[biin].[TabTempUpdate]")
            conn_sql.commit()
        except Exception as e:
            print(e)


if __name__ == '__main__':
    print('Executando BIIN_NOTAS_MANUTENCAO.py...')
    main()
    print('BIIN_NOTAS_MANUTENCAO.py atualizado com sucesso.')
