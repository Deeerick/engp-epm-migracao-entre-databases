import getpass

from datetime import datetime
from utils.connection_db import connection


def update_management_table(table):
    
    try:
    
        conn_sql = connection(dsn='BD_UN-BC')
        
        if conn_sql is None:
            print("Erro ao estabelecer conexão com o banco de dados.")
            return
        
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
        print(f"Erro ao atualizar a tabela {table}.")
        print(e)

    finally:
        conn_sql.close()
        print(f"Tabela {table} atualizada com sucesso.")
        return
