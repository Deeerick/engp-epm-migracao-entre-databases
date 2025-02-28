"""Programa para atualização da tabela [BD_UNBCDIGITAL].[biin].[nota_manutencao_aco] 
Este sistema atualizará a tabela acima para, somente, as notas que contiverem na tabela [BD_UNBCDIGITAL].[biin].[nota_manutencao]
É necessário executar o código desta forma, pois a tabela [BD_UNBCDIGITAL].[biin].[nota_manutencao_medida] não possui local de instalação.
Não é possivel filtrar diretamente na [BD_UNBCDIGITAL].[biin].[nota_manutencao_medida]  as plataformas da UN-BC"""

import pandas as pd
import pyodbc
import re
import warnings

from utils.last_update import last_update
from utils.update_table import update_management_table

warnings.simplefilter(action='ignore', category=UserWarning)


def main():
    st_tabela_destino = '[BD_UNBCDIGITAL].[biin].[historico_status_medida]'
    st_coluna_filtro_2 = 'HISO_CD_OBJETO'
    st_coluna_selecao = 'HISO_CD_OBJETO'

    # Locais de instalação a serem consultados
    lista_loc_instal = ['351902','301081','301049','301059','351401','351402','351901','351701',
                        '301010','301083','301018','301019','301020','301029','301032','301033',
                        '301036','301040','301056','301057','303006','301066','301063','301071']

    for loc_instal in lista_loc_instal:

        # Conectar ao banco sql
        conn_sql = conexao_sql()
        
        #Verificar a data da ultima atualização da tabela
        df_last_update = last_update(table='BIIN_HISTORICO_STATUS_MEDIDA', conn=conn_sql)
        print(f"Última atualização: {df_last_update}")

        # Importar os dados e colocar no data frame
        df_tdv = importar_dados_origem(loc_instal, df_last_update)

        # Substituir valores None e 'NaT' por 'Null', escapar aspas simples e formatar valores para inserção em SQL
        for column in df_tdv.columns:
            df_tdv[column] = df_tdv[column].apply(lambda value:
                            'Null' if value is None or str(value) == 'NaT' else value.replace("'", "''") if "'" in str(value) else value)

        dados_filtrados = df_tdv['HISO_CD_OBJETO']
        
        # Carregar resultado da consulta para o dataframe
        df_sql = importar_dados_destino(dados_filtrados, st_tabela_destino, conn_sql, st_coluna_filtro_2, st_coluna_selecao)

        # Exportar os dados data frame para o banco Sql st_server
        atualizar_tabela_destino(df_sql, df_tdv, st_tabela_destino, conn_sql, st_coluna_filtro_2)

    #Atualizar Gestão Tabelas
    update_management_table(table='BIIN_HISTORICO_STATUS_MEDIDA')

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def importar_dados_origem(loc_instal, df_last_update):

    try:

        # Configurações de conexão 
        conn_tdv = conexao_tdv()

        # String query para consulta SQL no oracle
        if df_last_update:

            query = f"""
                        SELECT 
                            historico.HISO_CD_OBJETO,
                            historico.HISO_CD_STATUS_OBJETO,
                            historico.HISO_QN_MODIFICACAO_STATUS,
                            historico.HISO_IN_TIPO_STATUS_OBJETO,
                            historico.HISO_TX_BREVE_STATUS,
                            historico.HISO_TX_COMPLETO_STATUS,
                            historico.HISO_CD_USUARIO_MODIFICACAO,
                            historico.HISO_DT_MODIFICACAO_STATUS,
                            historico.HISO_CD_TRANSACAO_SAP,
                            historico.HISO_IN_STATUS_INATIVO,
                            historico.HISO_IN_TIPO_MODIFICACAO,
                            historico.HISO_CD_TIPO_OBJETO_TEXT_LONGO,
                            historico.HISO_DF_ATUALIZACAO_STAGING,
                            historico.HISO_DF_ATUALIZACAO_ODS
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
                                medida.NUMERO_ORDEM_MANUNTECAO,
                                medida.CODIGO_CENTRO_SAP_LOCALIZACAO,
                                medida.CODIGO_LOCALIZACAO,
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
                            FROM BIIN.BIIN.VW_NOTA_MANUTENCAO_MEDIDA medida
                            INNER JOIN BIIN.BIIN.VW_NOTA_MANUTENCAO nota 
                                ON medida.NUMERO_NOTA = nota.NOTA
                            WHERE nota.LOCAL_INSTALACAO LIKE '{loc_instal}%'
                                AND nota.TIPO_NOTA = 'ZR'
                        ) med
                        INNER JOIN BIIN.BIIN.VW_BIIN_HISTORICO_STATUS_MEDIDA historico
                            ON REPLACE(med.NUMERO_OBJETO, ' ', '') = historico.HISO_CD_OBJETO
                        WHERE historico.HISO_DF_ATUALIZACAO_ODS > TO_DATE('{df_last_update}', 'YYYY-MM-DD HH24:MI:SS')"""
        else:
            query = f"""
                        SELECT 
                            historico.HISO_CD_OBJETO,
                            historico.HISO_CD_STATUS_OBJETO,
                            historico.HISO_QN_MODIFICACAO_STATUS,
                            historico.HISO_IN_TIPO_STATUS_OBJETO,
                            historico.HISO_TX_BREVE_STATUS,
                            historico.HISO_TX_COMPLETO_STATUS,
                            historico.HISO_CD_USUARIO_MODIFICACAO,
                            historico.HISO_DT_MODIFICACAO_STATUS,
                            historico.HISO_CD_TRANSACAO_SAP,
                            historico.HISO_IN_STATUS_INATIVO,
                            historico.HISO_IN_TIPO_MODIFICACAO,
                            historico.HISO_CD_TIPO_OBJETO_TEXT_LONGO,
                            historico.HISO_DF_ATUALIZACAO_STAGING,
                            historico.HISO_DF_ATUALIZACAO_ODS
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
                                medida.NUMERO_ORDEM_MANUNTECAO,
                                medida.CODIGO_CENTRO_SAP_LOCALIZACAO,
                                medida.CODIGO_LOCALIZACAO,
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
                            FROM BIIN.BIIN.VW_NOTA_MANUTENCAO_MEDIDA medida
                            INNER JOIN BIIN.BIIN.VW_NOTA_MANUTENCAO nota 
                                ON medida.NUMERO_NOTA = nota.NOTA
                            WHERE nota.LOCAL_INSTALACAO LIKE '{loc_instal}%'
                                AND nota.TIPO_NOTA = 'ZR'
                        ) med
                        INNER JOIN BIIN.BIIN.VW_BIIN_HISTORICO_STATUS_MEDIDA historico 
                            ON REPLACE(med.NUMERO_OBJETO, ' ', '') = historico.HISO_CD_OBJETO"""

        #Carregar consulta sql para o dataframe            
        df_tdv = pd.read_sql(query, conn_tdv)
        print(f"TDV - {loc_instal} - Total de registros: {len(df_tdv)}")

        return df_tdv
    except Exception as e:
        print(f"Erro: {e}")
        return False

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def atualizar_tabela_destino(df_sql, df_tdv, st_tabela_destino, conn, st_coluna_filtro_2, batch_size=250):

    agrupamento_update = []
    agrupamento_insert = []

    for index, row in df_tdv.iterrows():
    
        cursor = conn.cursor()
        st_valor_coluna = row['HISO_CD_OBJETO']

        if st_valor_coluna in df_sql:
            
            #Realizar update se ordem existir
            set_clauses = []
            
            for col in df_tdv.columns:

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
            
            column_names = df_tdv.columns
            values = [f"'{row[col]}'" if row[col] != 'Null' else 'Null' for col in column_names]
            stValues = f"({', '.join(values)})"
            
            #agrupamento_insert.append(string_sqlInsert)
            agrupamento_insert.append(stValues)            

            if len(agrupamento_insert) >= batch_size:
                string_sqlInsert = f"INSERT INTO {st_tabela_destino} VALUES {', '.join(agrupamento_insert)}"
                cursor.execute(string_sqlInsert)
                agrupamento_insert = []

    if agrupamento_update:
        cursor.execute(string_sqlUpdate)
        
    if agrupamento_insert:
        string_sqlInsert = f"INSERT INTO {st_tabela_destino} VALUES {', '.join(agrupamento_insert)}"
        cursor.execute(string_sqlInsert)

    conn.commit()

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def conexao_sql():
    try:
        conn_sql = pyodbc.connect(DSN='BD_UN-BC')
        print("Conexão com o banco de dados realizada com sucesso")
        return conn_sql
    except:
        return False

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def conexao_tdv():
    try:
        conn_tdv = pyodbc.connect(DSN='TDV')
        print("Conexão com o banco de dados realizada com sucesso")
        return conn_tdv
    except:
        return False

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def importar_dados_destino(dados_filtrados, st_tabela_destino, conn, st_coluna_filtro_2,st_coluna_selecao, batch_size=250):

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
