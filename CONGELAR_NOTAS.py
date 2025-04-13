def congelar_notas(conn_sql):
    # Define a consulta SQL
    query = """
            -- CONGELAR NOVAS NOTAS E ACOMPANHAR MODIFICACOES NO IF
            DECLARE @maiorNota VARCHAR(20);

            -- Obtém a maior NOTA já existente
            SELECT @maiorNota = MAX(NOTA) FROM [BD_UNBCDIGITAL].[ges].[nota_manutencao_congelado];

            -- Insere apenas as notas maiores que a última registrada, convertendo para número
            INSERT INTO [BD_UNBCDIGITAL].[ges].[nota_manutencao_congelado]
            SELECT * FROM [BD_UNBCDIGITAL].[biin].[nota_manutencao]
            WHERE CAST(NOTA AS BIGINT) > CAST(@maiorNota AS BIGINT);
            """
    
    # Executa a consulta no banco de dados
    with conn_sql.cursor() as cursor:
        cursor.execute(query)
        conn_sql.commit()
        print('Novas notas congeladas com sucesso!!!')
