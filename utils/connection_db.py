import pyodbc
import time

def connection(dsn, retries=3, delay=5):
    
    for attempt in range(retries):
        try:
            connection = pyodbc.connect(f'DSN={dsn}')
            return connection
        
        except Exception as e:
            print(f"Erro ao conectar ao DSN {dsn} na tentativa {attempt + 1}: {e}")
            
            if attempt < retries - 1:
                time.sleep(delay)
    return None
