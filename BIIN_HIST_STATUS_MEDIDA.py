# Programa para atualização da tabela [BD_UNBCDIGITAL].[biin].[nota_manutencao_aco] 
# Este sistema atualizará a tabela acima para, somente, as notas que contiverem na tabela [BD_UNBCDIGITAL].[biin].[nota_manutencao]
# É necessário executar o código desta forma, pois a tabela [BD_UNBCDIGITAL].[biin].[nota_manutencao_medida] não possui local de instalação.
# Não é possivel filtrar diretamente na [BD_UNBCDIGITAL].[biin].[nota_manutencao_medida]  as plataformas da UN-BC

import pandas as pd
import pyodbc
import re
import getpass
import warnings

from datetime import datetime
from utils.update_table import update_management_table

warnings.simplefilter(action='ignore', category=UserWarning)


def main():

    # Início da medição de tempo para executar a consulta no oracle
    tm_inicio_tempo_total = datetime.now().time()
    st_tabela_destino = '[BD_UNBCDIGITAL].[biin].[historico_status_medida]'
    st_tabela_origem = 'BIIN.VW_BIIN_HISTORICO_STATUS_MEDIDA'
    st_coluna_filtro_1 = 'LOCAL_INSTALACAO'
    st_coluna_filtro_2 = 'HISO_CD_OBJETO'
    st_coluna_selecao = 'HISO_CD_OBJETO'

    # Locais de instalação a serem consultados
    li_local_instalacao = ['351902','301081','301049','301059','351401','351402','351901','351701',
                           '301010','301083','301018','301019','301020','301029','301032','301033',
                           '301036','301040','301056','301057','303006','301066','301063','301071']

    for st_local_instalacao in li_local_instalacao:

        # Conectar ao banco sql
        conn = conexao_sql()
        
        #Verificar a data da ultima atualização da tabela
        ultima_atualizacao_tabela = ultima_atualizacao(conn)

        # Importar os dados e colocar no data frame
        df_consulta_biin = importar_dados_origem(st_local_instalacao,ultima_atualizacao_tabela, st_coluna_filtro_1, st_tabela_origem)

        # Substituir valores None e 'NaT' por 'Null', escapar aspas simples e formatar valores para inserção em SQL
        for column in df_consulta_biin.columns:
            df_consulta_biin[column] = df_consulta_biin[column].apply(lambda value:
                            'Null' if value is None or str(value) == 'NaT' else value.replace("'", "''") if "'" in str(value) else value)

        dados_filtrados = df_consulta_biin[st_coluna_selecao]
        
        # Carregar resultado da consulta para o dataframe
        df_unbcdigital = importar_dados_destino(dados_filtrados,st_tabela_destino,conn,st_coluna_filtro_2,st_coluna_selecao)

        # Exportar os dados data frame para o banco Sql st_server
        atualizar_tabela_destino(df_unbcdigital,df_consulta_biin, st_tabela_destino, conn, st_coluna_filtro_2)

    #Atualizar Gestão Tabelas
    update_management_table(table='BIIN_HISTORICO_STATUS_MEDIDA')

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def importar_dados_origem(st_local_instalacao,ultima_atualizacao_tabela,st_coluna_filtro_1,st_tabela_origem):

    try:

        # Configurações de conexão 
        orac_conn = conexao_oracle()

        # Criar cursor a partir da conexão
        oracle_cursor = orac_conn.cursor()

        # String query para consulta SQL no oracle
        if ultima_atualizacao_tabela:

            st_query_oracle = f"select " \
                            f"historico.HISO_CD_OBJETO,historico.HISO_CD_STATUS_OBJETO,historico.HISO_QN_MODIFICACAO_STATUS," \
                            f"historico.HISO_IN_TIPO_STATUS_OBJETO,historico.HISO_TX_BREVE_STATUS,historico.HISO_TX_COMPLETO_STATUS," \
                            f"historico.HISO_CD_USUARIO_MODIFICACAO,historico.HISO_DT_MODIFICACAO_STATUS,historico.HISO_CD_TRANSACAO_SAP," \
                            f"historico.HISO_IN_STATUS_INATIVO,historico.HISO_IN_TIPO_MODIFICACAO,historico.HISO_CD_TIPO_OBJETO_TEXT_LONGO," \
                            f"historico.HISO_DF_ATUALIZACAO_STAGING,historico.HISO_DF_ATUALIZACAO_ODS " \
                            f"from ( " \
                            f"select " \
                            f"medida.NUMERO_NOTA,medida.NUMERO_INTERNO_MEDIDA,medida.NUMERO_MEDIDA,medida.NUMERO_INTERNO_PARTE," \
                            f"medida.CODIGO_GRUPO_CODE_MEDIDA,medida.CODIGO_CODE_MEDIDA,medida.TEXTO_BREVE_MEDIDA,medida.TEXTO_STATUS_SISTEMA," \
                            f"medida.TEXTO_STATUS_USUARIO,medida.NUMERO_OBJETO,medida.CODIGO_PARCEIRO_FUNCAO,medida.CODIGO_PARCEIRO_RESPONSAVEL," \
                            f"medida.NUMERO_COMPONENTE_AFETADO,medida.NUMERO_INTERNO_CAUSA,medida.CODIGO_CATALOGO_MEDIDA," \
                            f"medida.INDICADOR_MARCACAO_ELIMINACAO,medida.NUMERO_ORDEM_MANUNTECAO,medida.CODIGO_CENTRO_SAP_LOCALIZACAO," \
                            f"medida.CODIGO_LOCALIZACAO,medida.INDICADOR_CLASSIFICACAO_MEDIDA,medida.INDICADOR_EXISTE_TEXTO_LONGO," \
                            f"medida.CODIGO_USUARIO_CRIACAO,medida.DATA_CRIACAO,medida.CODIGO_USUARIO_MODIFICACAO,medida.DATA_MODIFICACAO," \
                            f"medida.DATA_INICIO_PLANEJADA,medida.DATA_CONCLUSAO_PLANEJADA,medida.CODIGO_USUARIO_CONCLUSAO," \
                            f"medida.DATA_CONCLUSAO,medida.DATA_HORA_ATUALIZACAO_STAGING,medida.DATA_HORA_ATUALIZACAO_ODS " \
                            f"from " \
                            f"BIIN.VW_NOTA_MANUTENCAO_MEDIDA medida " \
                            f"inner join BIIN.VW_NOTA_MANUTENCAO nota on medida.NUMERO_NOTA = nota.NOTA " \
                            f"where nota.LOCAL_INSTALACAO like '{st_local_instalacao}%'" \
                            f"and nota.TIPO_NOTA = 'ZR'" \
                            f") med " \
                            f"inner join " \
                            f"BIIN.VW_BIIN_HISTORICO_STATUS_MEDIDA historico " \
                            f"on replace(med.NUMERO_OBJETO, ' ','')  = historico.HISO_CD_OBJETO " \
                            f"where historico.HISO_DF_ATUALIZACAO_ODS > TO_DATE('{ultima_atualizacao_tabela}', 'YYYY-MM-DD HH24:MI:SS')"
                            #f"where txtlongo.TELO_DF_ATUALIZACAO_ODS > TO_DATE('{ultima_atualizacao_tabela}', 'YYYY-MM-DD HH24:MI:SS')"
                            #txtlongo.TELO_DF_ATUALIZACAO_ODS 
                            #f"where txtlongo.TELO_DF_ATUALIZACAO_ODS > TO_DATE('{ultima_atualizacao_tabela}', 'YYYY-MM-DD HH24:MI:SS')"
            
        else:

            st_query_oracle = f"select " \
                            f"historico.HISO_CD_OBJETO,historico.HISO_CD_STATUS_OBJETO,historico.HISO_QN_MODIFICACAO_STATUS," \
                            f"historico.HISO_IN_TIPO_STATUS_OBJETO,historico.HISO_TX_BREVE_STATUS,historico.HISO_TX_COMPLETO_STATUS," \
                            f"historico.HISO_CD_USUARIO_MODIFICACAO,historico.HISO_DT_MODIFICACAO_STATUS,historico.HISO_CD_TRANSACAO_SAP," \
                            f"historico.HISO_IN_STATUS_INATIVO,historico.HISO_IN_TIPO_MODIFICACAO,historico.HISO_CD_TIPO_OBJETO_TEXT_LONGO," \
                            f"historico.HISO_DF_ATUALIZACAO_STAGING,historico.HISO_DF_ATUALIZACAO_ODS " \
                            f"from ( " \
                            f"select " \
                            f"medida.NUMERO_NOTA,medida.NUMERO_INTERNO_MEDIDA,medida.NUMERO_MEDIDA,medida.NUMERO_INTERNO_PARTE," \
                            f"medida.CODIGO_GRUPO_CODE_MEDIDA,medida.CODIGO_CODE_MEDIDA,medida.TEXTO_BREVE_MEDIDA,medida.TEXTO_STATUS_SISTEMA," \
                            f"medida.TEXTO_STATUS_USUARIO,medida.NUMERO_OBJETO,medida.CODIGO_PARCEIRO_FUNCAO,medida.CODIGO_PARCEIRO_RESPONSAVEL," \
                            f"medida.NUMERO_COMPONENTE_AFETADO,medida.NUMERO_INTERNO_CAUSA,medida.CODIGO_CATALOGO_MEDIDA," \
                            f"medida.INDICADOR_MARCACAO_ELIMINACAO,medida.NUMERO_ORDEM_MANUNTECAO,medida.CODIGO_CENTRO_SAP_LOCALIZACAO," \
                            f"medida.CODIGO_LOCALIZACAO,medida.INDICADOR_CLASSIFICACAO_MEDIDA,medida.INDICADOR_EXISTE_TEXTO_LONGO," \
                            f"medida.CODIGO_USUARIO_CRIACAO,medida.DATA_CRIACAO,medida.CODIGO_USUARIO_MODIFICACAO,medida.DATA_MODIFICACAO," \
                            f"medida.DATA_INICIO_PLANEJADA,medida.DATA_CONCLUSAO_PLANEJADA,medida.CODIGO_USUARIO_CONCLUSAO," \
                            f"medida.DATA_CONCLUSAO,medida.DATA_HORA_ATUALIZACAO_STAGING,medida.DATA_HORA_ATUALIZACAO_ODS " \
                            f"from " \
                            f"BIIN.VW_NOTA_MANUTENCAO_MEDIDA medida " \
                            f"inner join BIIN.VW_NOTA_MANUTENCAO nota on medida.NUMERO_NOTA = nota.NOTA " \
                            f"where nota.LOCAL_INSTALACAO like '{st_local_instalacao}%'" \
                            f"and nota.TIPO_NOTA = 'ZR'" \
                            f") med " \
                            f"inner join " \
                            f"BIIN.VW_BIIN_HISTORICO_STATUS_MEDIDA historico " \
                            f"on replace(med.NUMERO_OBJETO, ' ','')  = historico.HISO_CD_OBJETO "   

        #Carregar consulta sql para o dataframe            
        df_oracle = pd.read_sql(st_query_oracle,orac_conn)        
        print(f"Plataforma: {st_local_instalacao} Total_Linhas: {len(df_oracle)}")

        return df_oracle
    except:
        return False
    
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def atualizar_tabela_destino(df_unbcdigital,df_consulta_biin, st_tabela_destino, conn, st_coluna_filtro_2, batch_size=250):

    agrupamento_update = []
    agrupamento_insert = []

    for index, row in df_consulta_biin.iterrows():
    
        cursor = conn.cursor()
        st_valor_coluna = row[st_coluna_filtro_2]        

        if st_valor_coluna in df_unbcdigital:
            
            #Realizar update se ordem existir
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

def ultima_atualizacao(conn):

    try:

        cursor = conn.cursor()
        str_sql = f"SELECT [ULTIMA_ATUALIZACAO] FROM [BD_UNBCDIGITAL].[apo].[GFM_STATUS_TABELAS] WHERE [NOME_TABELA] = 'BIIN_HISTORICO_STATUS_MEDIDA'"        
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
        return conn_sql
    except:
        return False

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def conexao_oracle():
    try:
        conn_tdv = pyodbc.connect(DSN='TDV')
        return conn_tdv
    except:
        return False

# ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def atualizar_st_tabela_gestao(st_tabela_destino,conn):

    cursor = conn.cursor()
    st_tabela_gestao = '[BD_UNBCDIGITAL].[apo].[GestaoTabelas]'
    username = getpass.getuser()
    dt_data_atualizacao= datetime.now()
    dt_data_atualizacao= dt_data_atualizacao.strftime("%Y-%m-%d %H:%M:%S")
    string_sql = f"update {st_tabela_gestao} set data_atualizacao = '{dt_data_atualizacao}', tipo_atualizacao = 'Atualizacao', usuario = '{username}' where tabela = '{st_tabela_destino}'" 
    cursor.execute(string_sql)
    conn.commit()

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
