# Programa para atualização da tabela [BD_UNBCDIGITAL].[biin].[nota_manutencao_aco] 
# Este sistema atualizará a tabela acima para, somente, as notas que contiverem na tabela [BD_UNBCDIGITAL].[biin].[nota_manutencao]
# É necessário executar o código desta forma, pois a tabela [BD_UNBCDIGITAL].[biin].[nota_manutencao_medida] não possui local de instalação.
# Não é possivel filtrar diretamente na [BD_UNBCDIGITAL].[biin].[nota_manutencao_medida]  as plataformas da UN-BC

import pandas as pd
import pyodbc
import re
import warnings

from datetime import datetime
from utils.update_table import update_management_table

warnings.simplefilter(action='ignore', category=UserWarning)


def main():

    st_tabela_destino = '[BD_UNBCDIGITAL].[biin].[texto_longo_medida]'
    st_coluna_filtro_2 = 'TELO_CD_OBJETO'    
    st_coluna_selecao = 'TELO_CD_OBJETO'

    # Locais de instalação a serem consultados
    lista_loc_instal = [
                        # '301066','301063', '351902',
                        '301081',
                        # '301049','301059','351401','351402',
                        # '351901','351701','301010','301083','301018','301019','301020','301029',
                        # '301032','301033','301036','301040','301056','301057','303006','301071'
                        ]

    for loc_instal in lista_loc_instal:

        # Conectar ao banco sql
        conn_sql = conexao_sql()

        #Verificar a data da ultima atualização da tabela
        ultima_atualizacao_tabela = ultima_atualizacao(conn_sql, st_tabela_destino)
        print(f"Última atualização: {ultima_atualizacao_tabela}")

        # Importar os dados e colocar no data frame
        df_tdv = importar_dados_origem(loc_instal, ultima_atualizacao_tabela)
        # print(df_tdv)

        # Substituir valores None e 'NaT' por 'Null', escapar aspas simples e formatar valores para inserção em SQL
        try:
            for column in df_tdv.columns:
                df_tdv[column] = df_tdv[column].apply(lambda value:
                                'Null' if value is None or str(value) == 'NaT' else value.replace("'", "''") if "'" in str(value) else value)
                
        except Exception as e:
            print(f"Erro: {e}")
            return False

        dados_filtrados = df_tdv[st_coluna_selecao]
        
        # Carregar resultado da consulta para o dataframe
        df_unbcdigital = importar_dados_destino(dados_filtrados,st_tabela_destino,conn_sql,st_coluna_filtro_2,st_coluna_selecao)

        # Exportar os dados data frame para o banco Sql st_server
        atualizar_tabela_destino(df_unbcdigital,df_tdv, st_tabela_destino, conn_sql, st_coluna_filtro_2)     

    #Atualizar Gestão Tabelas
    update_management_table(table='BIIN_TEXTO_LONGO_MEDIDA')

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def importar_dados_origem(loc_instal, ultima_atualizacao_tabela):

    try:

        # Configurações de conexão 
        conn_tdv = conexao_tdv()

        # String query para consulta
        if ultima_atualizacao_tabela:

            query = f"""
                        SELECT 
                            txtlongo.TELO_CD_TABELA_SAP,
                            txtlongo.TELO_CD_TIPO_TEXTO,
                            txtlongo.TELO_CD_OBJETO,
                            txtlongo.TELO_QN_LINHA,
                            txtlongo.TELO_TX_LINHA,
                            txtlongo.TELO_DF_ATUALIZACAO_STAGING,
                            txtlongo.TELO_DF_ATUALIZACAO_ODS
                        FROM (
                            SELECT 
                                medida.NUMERO_NOTA,
                                medida.NUMERO_INTERNO_MEDIDA,
                                medida.NUMERO_MEDIDA,
                                medida.NUMERO_INTERNO_PARTE,
                                medida.CODIGO_GRUPO_CODE_MEDIDA,
                                medida.CODIGO_CODE_MEDIDA,
                                medida.TEXTO_BREVE_MEDIDA,
                                medida.TEXTO_STATUS_SISTEMA,
                                medida.TEXTO_STATUS_USUARIO,
                                medida.NUMERO_OBJETO,
                                medida.CODIGO_PARCEIRO_FUNCAO,
                                medida.CODIGO_PARCEIRO_RESPONSAVEL,
                                medida.NUMERO_COMPONENTE_AFETADO,
                                medida.NUMERO_INTERNO_CAUSA,
                                medida.CODIGO_CATALOGO_MEDIDA,
                                medida.INDICADOR_MARCACAO_ELIMINACAO,
                                medida.INDICADOR_CLASSIFICACAO_MEDIDA,
                                medida.INDICADOR_EXISTE_TEXTO_LONGO,
                                medida.CODIGO_USUARIO_CRIACAO,
                                medida.DATA_CRIACAO,
                                medida.CODIGO_USUARIO_MODIFICACAO,
                                medida.DATA_MODIFICACAO,
                                medida.DATA_INICIO_PLANEJADA,
                                medida.DATA_CONCLUSAO_PLANEJADA,
                                medida.CODIGO_USUARIO_CONCLUSAO,
                                medida.DATA_CONCLUSAO,
                                medida.DATA_HORA_ATUALIZACAO_STAGING,
                                medida.DATA_HORA_ATUALIZACAO_ODS
                            FROM BIIN.VW_NOTA_MANUTENCAO_MEDIDA medida
                            INNER JOIN BIIN.BIIN.VW_NOTA_MANUTENCAO nota 
                                ON medida.NUMERO_NOTA = nota.NOTA
                            WHERE nota.LOCAL_INSTALACAO LIKE '{loc_instal}%'
                                AND nota.TIPO_NOTA = 'ZR'
                        ) med
                        INNER JOIN BIIN.BIIN.VW_BIIN_TEXTO_LONGO_NOTA_MANUTENCAO_MEDIDA txtlongo 
                            ON REPLACE(med.NUMERO_OBJETO, ' ', '') = REPLACE(txtlongo.TELO_CD_OBJETO, ' ', '')
                        WHERE txtlongo.TELO_DF_ATUALIZACAO_ODS > TO_DATE('{ultima_atualizacao_tabela}', 'YYYY-MM-DD HH24:MI:SS')"""
        else:
            query = f"""
                        SELECT 
                            txtlongo.TELO_CD_TABELA_SAP,
                            txtlongo.TELO_CD_TIPO_TEXTO,
                            txtlongo.TELO_CD_OBJETO,
                            txtlongo.TELO_QN_LINHA,
                            txtlongo.TELO_TX_LINHA,
                            txtlongo.TELO_DF_ATUALIZACAO_STAGING,
                            txtlongo.TELO_DF_ATUALIZACAO_ODS
                        FROM (
                            SELECT 
                                medida.NUMERO_NOTA,
                                medida.NUMERO_INTERNO_MEDIDA,
                                medida.NUMERO_MEDIDA,
                                medida.NUMERO_INTERNO_PARTE,
                                medida.CODIGO_GRUPO_CODE_MEDIDA,
                                medida.CODIGO_CODE_MEDIDA,
                                medida.TEXTO_BREVE_MEDIDA,
                                medida.TEXTO_STATUS_SISTEMA,
                                medida.TEXTO_STATUS_USUARIO,
                                medida.NUMERO_OBJETO,
                                medida.CODIGO_PARCEIRO_FUNCAO,
                                medida.CODIGO_PARCEIRO_RESPONSAVEL,
                                medida.NUMERO_COMPONENTE_AFETADO,
                                medida.NUMERO_INTERNO_CAUSA,
                                medida.CODIGO_CATALOGO_MEDIDA,
                                medida.INDICADOR_MARCACAO_ELIMINACAO,
                                medida.INDICADOR_CLASSIFICACAO_MEDIDA,
                                medida.INDICADOR_EXISTE_TEXTO_LONGO,
                                medida.CODIGO_USUARIO_CRIACAO,
                                medida.DATA_CRIACAO,
                                medida.CODIGO_USUARIO_MODIFICACAO,
                                medida.DATA_MODIFICACAO,
                                medida.DATA_INICIO_PLANEJADA,
                                medida.DATA_CONCLUSAO_PLANEJADA,
                                medida.CODIGO_USUARIO_CONCLUSAO,
                                medida.DATA_CONCLUSAO,
                                medida.DATA_HORA_ATUALIZACAO_STAGING,
                                medida.DATA_HORA_ATUALIZACAO_ODS
                            FROM BIIN.VW_NOTA_MANUTENCAO_MEDIDA medida
                            INNER JOIN BIIN.VW_NOTA_MANUTENCAO nota 
                                ON medida.NUMERO_NOTA = nota.NOTA
                            WHERE nota.LOCAL_INSTALACAO LIKE '{loc_instal}%'
                                AND nota.TIPO_NOTA = 'ZR'
                        ) med
                        INNER JOIN BIIN.BIIN.VW_BIIN_TEXTO_LONGO_NOTA_MANUTENCAO_MEDIDA txtlongo 
                            ON REPLACE(med.NUMERO_OBJETO, ' ', '') = REPLACE(txtlongo.TELO_CD_OBJETO, ' ', '')"""
            
        #Carregar consulta sql para o dataframe            
        df_tdv = pd.read_sql(query, conn_tdv)        
        print(f"TDV - {loc_instal} - Total de registros: {len(df_tdv)}")

        return df_tdv
    except Exception as e:
        print(f"Erro: {e}")
        return False

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def atualizar_tabela_destino(df_unbcdigital, df_consulta_biin, st_tabela_destino, conn, st_coluna_filtro_2, batch_size=1000):

    agrupamento_update = []
    agrupamento_insert = []
    total_rows = len(df_consulta_biin)
    counter = 0

    for index, row in df_consulta_biin.iterrows():
    
        cursor = conn.cursor()
        st_valor_coluna = row[st_coluna_filtro_2]        

        if st_valor_coluna in df_unbcdigital:
            
            # Realizar update se ordem existir
            set_clauses = []
            
            for col in df_consulta_biin.columns:

                if row[col] != 'Null':
                    set_clauses.append(f"{col} = '{row[col]}'")
                else:
                    set_clauses.append(f"{col} = Null")

            HeadWithOrdem = set_clauses[0]
            match = re.search(r"'(.*?)'", HeadWithOrdem)
            if match:
                stOrdem = match.group(1)
            
            string_sqlUpdate = f"UPDATE {st_tabela_destino} SET {', '.join(set_clauses)} WHERE {st_coluna_filtro_2} like '{stOrdem}';" 

            agrupamento_update.append(string_sqlUpdate)
            
            if len(agrupamento_update) >= batch_size:
                cursor.execute(string_sqlUpdate)
                agrupamento_update = []
            
        else:
            
            column_names = df_consulta_biin.columns
            values = [f"'{row[col]}'" if row[col] != 'Null' else 'Null' for col in column_names]
            stValues = f"({', '.join(values)})"
            
            agrupamento_insert.append(stValues)            

            if len(agrupamento_insert) >= batch_size:
                string_sqlInsert = f"INSERT INTO {st_tabela_destino} VALUES {', '.join(agrupamento_insert)}"
                cursor.execute(string_sqlInsert)
                agrupamento_insert = []

        counter += 1
        if counter % batch_size == 0:
            print(f"Processando registro {counter} de {total_rows}")

    if agrupamento_update:
        cursor.execute(string_sqlUpdate)
        
    if agrupamento_insert:
        string_sqlInsert = f"INSERT INTO {st_tabela_destino} VALUES {', '.join(agrupamento_insert)}"
        cursor.execute(string_sqlInsert)

    conn.commit()
    print(f"Processamento concluído. Total de registros processados: {total_rows}")

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def ultima_atualizacao(conn,st_tabela_destino):

    try:

        cursor = conn.cursor()
        str_sql = f"select data_atualizacao from [bd_unbcdigital].[apo].[GestaoTabelas] where tabela = '{st_tabela_destino}'"        
        cursor.execute(str_sql)
        dt_ultima_atualizacao = cursor.fetchone()
        dt_ultima_atualizacao = dt_ultima_atualizacao.data_atualizacao
        dt_ultima_atualizacao = dt_ultima_atualizacao.strftime("%Y-%m-%d %H:%M:%S") 

        return dt_ultima_atualizacao
    except:
        return False

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 
def conexao_sql():
    try:
        conn_sql = pyodbc.connect(DSN='BD_UN-BC')
        print("Conexão com o SQL realizada com sucesso")
        return conn_sql
    except Exception as e:
        print(f"Erro: {e}")
        return False

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def conexao_tdv():
    try:
        conn_tdv = pyodbc.connect(DSN='TDV')
        print("Conexão com o TDV realizada com sucesso")
        return conn_tdv
    except Exception as e:
        print(f"Erro: {e}")
        return False

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def importar_dados_destino(dados_filtrados, st_tabela_destino, conn, st_coluna_filtro_2,st_coluna_selecao, batch_size=1000):

    try:
        cursor = conn.cursor()

        # Criar uma lista para armazenar os resultados em lotes
        results_batches = []
        results_batches_total =[]

        for coluna in dados_filtrados:

            st_query_unbcdigital = f"'{str(coluna).strip()}'"
            results_batches.append(st_query_unbcdigital)            

            if len(results_batches) >= batch_size:

                # Montar a string SQL para a consulta                                
                st_query_sql = f"SELECT {st_coluna_selecao} FROM {st_tabela_destino} WHERE {st_coluna_filtro_2} IN ({', '.join(results_batches)})"
                
                # Executar a consulta SQL e adicionar os resultados ao lote
                cursor.execute(st_query_sql)
                results_batch = cursor.fetchall()
                results_batches = []
                results_batches_total.extend(results_batch)                

        if results_batches:

            # Montar a string SQL para o último lote
            st_query_sql = f"SELECT {st_coluna_selecao} FROM {st_tabela_destino} WHERE {st_coluna_filtro_2} IN ({', '.join(results_batches)})"
            
            # Executar a consulta SQL e adicionar os resultados ao lote final
            cursor.execute(st_query_sql)
            results_batch = cursor.fetchall()
            results_batches = []
            results_batches_total.extend(results_batch)

        # Criar lista com todas os dados da coluna filtrados
        df_dados_unbcdigital = []
        
        for batch in results_batches_total:
            df_dados_unbcdigital.extend(batch)

        # Fechar o cursor
        cursor.close()

        return df_dados_unbcdigital
    except Exception as e:
        print(f"Erro: {e}")
        return False

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    main()
