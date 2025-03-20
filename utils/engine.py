import sqlalchemy

from sqlalchemy import create_engine


def engine():

    # String de conexão para SQL Server
    server = 'NPAA7408'
    database = 'BD_UNBCDIGITAL'
    username = 'FLUL'
    password = 'Ta52212525'
    driver = '{ODBC Driver 17 for SQL Server}'

    # Criando a string de conexão
    conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}"

    # Criando o engine de conexão
    engine = create_engine(conn_str)

    return engine
