from utils.update_table import update_management_table
from utils.connection_db import connection


conn_sql = connection(dsn='BD_UN-BC')

update_management_table(table='BIIN_HISTORICO_STATUS_MEDIDA', conn_sql=conn_sql)
update_management_table(table='BIIN_TEXTO_LONGO_MEDID', conn_sql=conn_sql)

conn_sql.close()
