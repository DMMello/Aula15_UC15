import pandas as pd
import polars as pl
from datetime import datetime
import os
import gc  # Garbage Collector

ENDERECO_DADOS = r'./dados/'

try:
    print('Obtendo dados')

    inicio = datetime.now()

    # Lista para Receber os arquivos CSVs, que estão no diretório
    lista_arquivos = []
    
    # Lista temporária de Dados que serão puxados do diretório
    # os.listdir() retorna uma lista de todos os arquivos e pastas do diretório
    lista_dir_arquivos = os.listdir(ENDERECO_DADOS)

    # Pegando os arquivos CSVs do diretório
    for arquivo in lista_dir_arquivos:
        # o método endswith() verifica se o arquivo termina com a extensão .csv
        # e adiciona o arquivo à lista de arquivos
        if arquivo.endswith('.csv'):
            # método add() adiciona o arquivo à lista de arquivos
            lista_arquivos.append(arquivo)

    print(lista_arquivos)

    # Leitura dos arquivos
    # após todos os arquivos já terem sido adicionados à lista de arquivos, 
    # O código começa a ler cada um dos arquivos
    for arquivo in lista_arquivos:
        print(f'Processando arquivo {arquivo}')

        # Leitura de cada um dos dataframes
        # como estamos dentro de um For, cada arquivo é lido e adicionado ao DataFrame df_bolsa_familia
        # o dataframe df é apenas um auxiliar, que será usado para receber os 
        # dados de cada arquivo CSv a cada iteração do For
        df = pl.read_csv(ENDERECO_DADOS + arquivo, separator=';', encoding='iso-8859-1')

        # Concatenação dos Dataframes
        # Verifica se o DataFrame df_bolsa_familia já existe,
        # Se existir, acontece a concatenação, 
        # Senão existir, (que dizer que o Loop está na primeira execução), 
        # então o df_dados é atribuído a df_bolsa_familia
        if 'df_bolsa_familia' in locals():
            df_bolsa_familia = pl.concat([df_bolsa_familia, df])
        else:
            df_bolsa_familia = df
        
        # Remover df_dados após o uso para liberar memória
        del df
        
        # Prints
        print(df_bolsa_familia.head())
        # print(df_bolsa_familia.shape)
        # print(df_bolsa_familia.columns)
        # print(df_bolsa_familia.dtypes)

        print(f'Arquivo {arquivo} processados com sucesso!')
    #   Fim do For #

    # Converte a coluna 'VALOR PARCELA' para o tipo float
    df_bolsa_familia = df_bolsa_familia.with_columns(
        pl.col('VALOR PARCELA').str.replace(',', '.').cast(pl.Float64)
    )

    print('\nDados dos DataFrames concatenados com sucesso!')
    print('Incinando a gravação do arquivo Parquet...')

    # Criar arquivo Parquet
    # O arquivo parquet é um arquivo compactado que permite a leitura e escrita de dados em formato binário
    # Isso gera um arquivo de auta compactação e muito mais rápido de ler do que um arquivo CSV
    # O arquivo parquet é um formato de arquivo de dados que é comumente usado para armazenar dados em massa
    # Será salvo um arquivo .parquet dentro do diretório diretório atual, onde está o aquivo exemplo3.py
    df_bolsa_familia.write_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')    

    # Deletar df_bolsa_familia da memória
    del df_bolsa_familia
    
    # Coletar resíduos da memória
    gc.collect()

    fim = datetime.now()
    
    print(f'Tempo de execução: {fim - inicio}')
    print('Gravação do arquivo Parquet realizada com sucesso!')

except ImportError as e:
    print(f'Erro ao processar os dataframes: {e}')
