import pandas as pd
import warnings

from tqdm import tqdm
from utils.connection_db import connection
from utils.last_update import last_update
from utils.update_table import update_management_table

warnings.simplefilter(action='ignore', category=UserWarning)


def main():
    
    lista_loc = ['351902', '301081', '301049', '301059', '351401', '351402',
                 '351901', '351701', '301010', '301083', '301018', '301019',
                 '301020', '301029', '301032', '301033', '301036', '301040',
                 '301056', '301057', '303006', '301066', '301063', '301071']
    
    conn_sql = connection(dsn='BD_UN-BC')
    last_update_table = last_update('BIIN_HISTORICO_STATUS_MEDIDA', conn_sql)
    print(last_update_table)
    
    for loc in lista_loc:
        
        conn_tdv = connection(dsn='TDV')
        conn_sql = connection(dsn='BD_UN-BC')
        
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
            WHERE nota.LOCAL_INSTALACAO LIKE '{loc}%'
            AND nota.TIPO_NOTA = 'ZR'
        ) med
        INNER JOIN BIIN.BIIN.VW_BIIN_HISTORICO_STATUS_MEDIDA historico 
        ON REPLACE(med.NUMERO_OBJETO, ' ', '') = historico.HISO_CD_OBJETO
        WHERE historico.HISO_DF_ATUALIZACAO_ODS >= '{last_update_table['ULTIMA_ATUALIZACAO'][0]}'"""
                    
        df = pd.read_sql(query, conn_tdv)
        print(f'{loc} - {len(df)}')

        df = df.astype({
            'HISO_CD_OBJETO': 'string',
            'HISO_CD_STATUS_OBJETO': 'string',
            'HISO_QN_MODIFICACAO_STATUS': 'int',
            'HISO_IN_TIPO_STATUS_OBJETO': 'string',
            'HISO_TX_BREVE_STATUS': 'string',
            'HISO_TX_COMPLETO_STATUS': 'string',
            'HISO_CD_USUARIO_MODIFICACAO': 'string',
            'HISO_DT_MODIFICACAO_STATUS': 'datetime64[ns]',
            'HISO_CD_TRANSACAO_SAP': 'string',
            'HISO_IN_STATUS_INATIVO': 'string',
            'HISO_IN_TIPO_MODIFICACAO': 'string',
            'HISO_CD_TIPO_OBJETO_TEXT_LONGO': 'string',
            'HISO_DF_ATUALIZACAO_STAGING': 'datetime64[ns]',
            'HISO_DF_ATUALIZACAO_ODS': 'datetime64[ns]'
        })

        for index, row in tqdm(df.iterrows(), total=len(df), desc="Inserindo dados"):
            row = row.fillna('')

            conn_sql.execute(
                """
                INSERT INTO [BD_UNBCDIGITAL].[BIIN].[HISTORICO_STATUS_MEDIDA] (
                    HISO_CD_OBJETO, HISO_CD_STATUS_OBJETO, HISO_QN_MODIFICACAO_STATUS, 
                    HISO_IN_TIPO_STATUS_OBJETO, HISO_TX_BREVE_STATUS, HISO_TX_COMPLETO_STATUS, 
                    HISO_CD_USUARIO_MODIFICACAO, HISO_DT_MODIFICACAO_STATUS, HISO_CD_TRANSACAO_SAP, 
                    HISO_IN_STATUS_INATIVO, HISO_IN_TIPO_MODIFICACAO, HISO_CD_TIPO_OBJETO_TEXT_LONGO, 
                    HISO_DF_ATUALIZACAO_STAGING, HISO_DF_ATUALIZACAO_ODS
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                tuple(row)
            )

        conn_sql.commit()
        conn_sql.close()
        
    conn_sql = connection(dsn='BD_UN-BC')    
    update_management_table('BIIN_HISTORICO_STATUS_MEDIDA', conn_sql)
    conn_sql.close()


if __name__ == '__main__':
    main()
