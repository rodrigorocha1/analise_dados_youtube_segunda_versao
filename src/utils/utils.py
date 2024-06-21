import os
import pickle as pk
import pandas as pd
from typing import Tuple, List, Dict


def obter_categorias_youtube() -> Tuple[pd.DataFrame, List[Dict[str, int]]]:
    CAMINHO_BASE = os.getcwd()

    with open(os.path.join(CAMINHO_BASE, 'src', 'depara', 'trends', 'categoria.pkl'), 'rb') as arq:
        df_categorias = pk.load(arq)
        df_categorias['ID'] = df_categorias['ID'].astype('int32')
        df_categorias['NOME_CATEGORIA'] = df_categorias['NOME_CATEGORIA'].astype(
            'string')
        opcoes_categoria = []
        for chave, linha in df_categorias.iterrows():
            categoria_completa = f"{linha['ID']} - {linha['NOME_CATEGORIA']}"
            opcoes_categoria.append(categoria_completa)

    return df_categorias, opcoes_categoria


def trocar_cor_grafico_barra(chave: str) -> str:

    cor = {
        'TOTAL_VISUALIZACOES': '#3CBC59',
        'TOTAL_COMENTARIOS': '#FE7800',
        'TOTAL_LIKES': '#4749CA'
    }
    cor = cor[chave]

    return cor
