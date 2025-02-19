import pandas as pd


def last_update(table, conn):
    try:
        query = f"SELECT [ULTIMA_ATUALIZACAO] FROM [BD_UNBCDIGITAL].[APO].[GFM_STATUS_TABELAS] WHERE [NOME_TABELA] = '{table}'"
        df_last_update = pd.read_sql(query, conn)
        return df_last_update

    except Exception as e:
        print(e)
    return None
