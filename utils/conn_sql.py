import pyodbc

def connection(dsn):
    try:
        connection = pyodbc.connect(f'DSN={dsn}')
        return connection
        
    except Exception as e:
        print(f"Erro ao conectar ao DSN {dsn}: {e}")
    return None
