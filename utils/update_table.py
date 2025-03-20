import getpass

from datetime import datetime


def update_management_table(table, conn_sql):
    
    try:
        date_att = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        username = getpass.getuser()
        
        query = f"""UPDATE [BD_UNBCDIGITAL].[APO].[GFM_STATUS_TABELAS] 
                SET [ULTIMA_ATUALIZACAO] = '{date_att}',
                [USUARIO] = '{username}'
                WHERE [NOME_TABELA] = '{table}'
                """
                
        conn_sql.execute(query)
        conn_sql.commit()
        
    except Exception as e:
        print(f"Erro ao atualizar a tabela {table}.\n")
        print(e)
