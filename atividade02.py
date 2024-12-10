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
    lista_dir_arquivos = os.listdir(ENDERECO_DADOS)

    for arquivos in lista_dir_arquivos:
        if arquivos.endswith('.csv'):
            lista_dir_arquivos.append(arquivos)
    print(lista_arquivos)

    del lista_dir_arquivos
    
except ImportError as e:
    print(f'Erro ao processar os dataframes: {e}')