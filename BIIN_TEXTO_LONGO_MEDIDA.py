import pandas as pd
import warnings

from tqdm import tqdm
from utils.connection_db import connection
from utils.last_update import last_update
from utils.update_table import update_management_table

warnings.simplefilter(action='ignore', category=UserWarning)


def main():
    
    lista_loc = [
        '351902', '301081', '301049', '301059', '351401', '351402',
        '351901', '351701', '301010', '301083', '301018', '301019',
        '301020', '301029', '301032', '301033', '301036', '301040',
        '301056', '301057', '303006', '301066', '301063', '301071'
    ]
    
    conn_sql = connection(dsn='BD_UN-BC')
    last_update_table = last_update('BIIN_TEXTO_LONGO_MEDIDA', conn_sql)
    print(f"Última atualizacao: {last_update_table['ULTIMA_ATUALIZACAO'][0]}")
    
    for loc in lista_loc:
        
        conn_tdv = connection(dsn='TDV')
        conn_sql = connection(dsn='BD_UN-BC')
        
        query = f"""
                    SELECT 
                        txtlongo.TELO_CD_TABELA_SAP,
                        txtlongo.TELO_CD_TIPO_TEXTO,
                        txtlongo.TELO_CD_OBJETO,
                        txtlongo.TELO_QN_LINHA,
                        txtlongo.TELO_TX_LINHA,
                        txtlongo.TELO_DF_ATUALIZACAO_STAGING,
                        txtlongo.TELO_DF_ATUALIZACAO_ODS
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
                        FROM BIIN.VW_NOTA_MANUTENCAO_MEDIDA medida
                        INNER JOIN BIIN.VW_NOTA_MANUTENCAO nota 
                            ON medida.NUMERO_NOTA = nota.NOTA
                        WHERE nota.LOCAL_INSTALACAO LIKE '{loc}%'
                            AND nota.TIPO_NOTA = 'ZR'
                    ) med
                    INNER JOIN BIIN.BIIN.VW_BIIN_TEXTO_LONGO_NOTA_MANUTENCAO_MEDIDA txtlongo 
                    ON REPLACE(med.NUMERO_OBJETO, ' ', '') = REPLACE(txtlongo.TELO_CD_OBJETO, ' ', '')
                    WHERE txtlongo.TELO_DF_ATUALIZACAO_ODS > '{last_update_table['ULTIMA_ATUALIZACAO'][0]}'"""
            
        df = pd.read_sql(query, conn_tdv)
        print(f'{loc} - {len(df)}')

        df = df.astype({
            'TELO_CD_TABELA_SAP': 'string',
            'TELO_CD_TIPO_TEXTO': 'string',
            'TELO_CD_OBJETO': 'string',
            'TELO_QN_LINHA': 'int',
            'TELO_TX_LINHA': 'string',
            'TELO_DF_ATUALIZACAO_STAGING': 'datetime64[ns]',
            'TELO_DF_ATUALIZACAO_ODS': 'datetime64[ns]'
        })

        for index, row in tqdm(df.iterrows(), total=len(df), desc="Inserindo dados"):
            row = row.fillna('')

            conn_sql.execute(
                """
                INSERT INTO [BD_UNBCDIGITAL].[BIIN].[TEXTO_LONGO_MEDIDA] (
                    [TELO_CD_TABELA_SAP], [TELO_CD_TIPO_TEXTO], [TELO_CD_OBJETO],
                    [TELO_QN_LINHA], [TELO_TX_LINHA], [TELO_DF_ATUALIZACAO_STAGING],
                    [TELO_DF_ATUALIZACAO_ODS]
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                tuple(row)
            )

        conn_sql.commit()
        conn_sql.close()
        
    conn_sql = connection(dsn='BD_UN-BC')  
    update_management_table('BIIN_TEXTO_LONGO_MEDIDA', conn_sql)
    conn_sql.close()


if __name__ == '__main__':
    main()
