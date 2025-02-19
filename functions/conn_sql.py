import pyodbc


def conn(dsn):
    try:
        conn = pyodbc.connect(f'DSN={dsn}')
        print(f'Conexão realizada ao DSN: {dsn}')
    except Exception as e:
        print(e)
    return conn
