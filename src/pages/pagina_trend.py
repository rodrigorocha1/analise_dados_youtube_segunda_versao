try:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from datetime import date
import dash
import dash_bootstrap_components as dbc
from dash import html, dcc, callback, Output, Input
from src.dados.gerador_consulta import GeradorConsulta
from src.visualization.visualizacao import Visualizacao

from src.utils.utils import trocar_cor_grafico_barra, obter_categorias_youtube

dash.register_page(__name__, name="Analise Trends")


def gerar_layout_categoria_top_dez():
    return [
        html.H5(
            'Top 10 Categoria Populares',
            id='id_titulo_categoria_populares_trends'
        ),
        dbc.Row(
            [
                dbc.Col(
                    dbc.RadioItems(
                        inline=True,
                        value='TOTAL_VISUALIZACOES',
                        options=[
                            {
                                'label': 'Visualizações',
                                'value': 'TOTAL_VISUALIZACOES'
                            },
                            {
                                'label': 'Comentários',
                                'value': 'TOTAL_COMENTARIOS'
                            },
                            {
                                'label': 'Likes',
                                'value': 'TOTAL_LIKES'
                            },
                        ],
                        id='id_input_desempenho',
                    ),
                    lg=6
                ),
                dbc.Col(
                    dcc.DatePickerSingle(
                        date='2024-01-20',
                        display_format='DD/MM/YYYY',
                        max_date_allowed=date(2024, 1, 23),
                        min_date_allowed=date(2024, 1, 17),
                        id='id_input_data_top_dez_categoria'
                    ),
                    lg=6
                )
            ]
        ),
        html.Div(
            dcc.Graph(id='id_grafico_layout_top_dez'),
            id='id_div_grafico_layout_top_dez'
        ),

    ]


def gerar_layout_canais_populares():
    return [
        html.H5(
            'Top 10 Canais populares',
            id='id_titulo_canais_populares_trends'
        ),
        dbc.Row(
            [
                dbc.Col(
                    dbc.RadioItems(
                        inline=True,
                        value='TOTAL_VISUALIZACOES',
                        options=[
                            {
                                'label': 'Visualizações',
                                'value': 'TOTAL_VISUALIZACOES'
                            },
                            {
                                'label': 'Comentários',
                                'value': 'TOTAL_COMENTARIOS'
                            },
                            {
                                'label': 'Likes',
                                'value': 'TOTAL_LIKES'
                            },
                        ],
                        id='id_input_desempenho_canais_populares',
                        style={'fontSize': '12px'}

                    ),
                    lg=4
                ),
                dbc.Col(
                    dcc.DatePickerSingle(
                        date='2024-01-20',
                        display_format='DD/MM/YYYY',
                        max_date_allowed=date(2024, 1, 23),
                        min_date_allowed=date(2024, 1, 17),
                        id='id_input_date_canais_populares'
                    ),
                    lg=4
                ),
                dbc.Col(
                    dbc.Select(
                        options=obter_categorias_youtube()[1],
                        value=obter_categorias_youtube()[1][0],
                        id='id_select_categoria_canais_populares',
                        class_name='class_select_categoria_canais_populares'
                    ),
                    lg=4
                )
            ],
        ),
        dbc.Tabs(
            [
                dbc.Tab(


                    label='Top 10 canais mais populares',
                    tab_id='tab_id_canais_mais_populares'
                ),
                dbc.Tab(

                    label='Top 10 canais menos populares',
                    tab_id='tab_id_canais_meno_populares'
                ),
            ],
            id='id_tabs_canais_populares'
        ),
        html.Div(id='id_main_canais_populares')
    ]


def gerar_desempenho_categoria_dia():
    return [
        html.H5(
            'Desempenho Categoria dia',
            id='id_titulo_desempenho_categoria'
        ),
        dbc.Row(
            [
                dbc.Col(
                    dbc.Select(
                        options=obter_categorias_youtube()[1],
                        value=obter_categorias_youtube()[1][0],
                        id='id_select_categoria_canal_dia',
                        class_name='class_select_categoria_canal_dia'
                    ),
                    lg=4
                ),
                dbc.Col(
                    dbc.RadioItems(
                        inline=True,
                        value='TOTAL_VISUALIZACOES',
                        options=[
                            {
                                'label': 'Visualizações',
                                'value': 'TOTAL_VISUALIZACOES'
                            },
                            {
                                'label': 'Comentários',
                                'value': 'TOTAL_COMENTARIOS'
                            },
                            {
                                'label': 'Likes',
                                'value': 'TOTAL_LIKES'
                            },
                        ],
                        id='id_input_desempenho_canal_dia',
                        style={'fontSize': '14px'}
                    ),
                    lg=4
                ),
            ],
        ),
        dcc.Graph(id='id_graph_desempenho_canal_dia')
    ]


def gerar_layout_video_categoria_dia():
    return [
        html.H5(
            'TOP 10 Vídeo por categoria',
            id='id_titulo_desempenho_categoria_dia'
        ),
        dbc.Row(
            [
                dbc.Col(
                    dbc.RadioItems(
                        inline=True,
                        value='TOTAL_VISUALIZACOES',
                        options=[
                            {
                                'label': 'Visualizações',
                                'value': 'TOTAL_VISUALIZACOES'
                            },
                            {
                                'label': 'Comentários',
                                'value': 'TOTAL_COMENTARIOS'
                            },
                            {
                                'label': 'Likes',
                                'value': 'TOTAL_LIKES'
                            },
                        ],
                        id='id_input_desempenho_categoria_video_dia',
                        style={'fontSize': '12px'}

                    ),
                    lg=4
                ),
                dbc.Col(
                    dcc.DatePickerSingle(
                        date='2024-01-20',
                        display_format='DD/MM/YYYY',
                        max_date_allowed=date(2024, 1, 23),
                        min_date_allowed=date(2024, 1, 17),
                        id='id_input_date_desempenho_categoria_video_dia'
                    ),
                    lg=4
                ),
                dbc.Col(
                    dbc.Select(
                        options=obter_categorias_youtube()[1],
                        value=obter_categorias_youtube()[1][0],
                        id='id_select_desempenho_categoria_video_dia',
                        class_name='class_select_categoria_canais_populares'
                    ),
                    lg=4
                )
            ],
        ),
        html.Div(
            dcc.Graph(id='id_graph_categoria_video_populares'),
            id='id_div_graph_categoria_video_populares'
        ),
    ]


def gerar_layout_engajamento_canal():
    return [
        html.H5(
            'TOP 10 Engajamento canal',
            id='id_titulo_engajamento_dia'
        ),
        dbc.Row(
            [
                dbc.Col(
                    dcc.DatePickerSingle(
                        date='2024-01-20',
                        display_format='DD/MM/YYYY',
                        max_date_allowed=date(2024, 1, 23),
                        min_date_allowed=date(2024, 1, 17),
                        id='id_input_date_engajamento_canal'
                    ),
                    lg=6
                ),
                dbc.Col(
                    dbc.Select(
                        options=obter_categorias_youtube()[1],
                        value=obter_categorias_youtube()[1][0],
                        id='id_select_engajamento_canal',
                        class_name='class_select_engajamento_canal'
                    ),
                    lg=6
                ),

            ]
        ),
        html.Div(
            dcc.Graph(id='id_grafico_engajamento_canal'),
            id='id_div_grafico_engajamento_canal'
        )
    ]


def gerar_layout_engajamento_video():
    return [
        html.H5(
            'TOP 10 Engajamento Vídeo',
            id='id_titulo_engajamento_video'
        ),
        dbc.Row(
            [
                dbc.Col(
                    dcc.DatePickerSingle(
                        date='2024-01-20',
                        display_format='DD/MM/YYYY',
                        max_date_allowed=date(2024, 1, 23),
                        min_date_allowed=date(2024, 1, 17),
                        id='id_input_date_engajamento_video'
                    ),
                    lg=6
                ),
                dbc.Col(
                    dbc.Select(
                        options=obter_categorias_youtube()[1],
                        value=obter_categorias_youtube()[1][0],
                        id='id_select_engajamento_video',
                        class_name='class_select_engajamento_video'
                    ),
                    lg=6
                ),
                html.Div(
                    dcc.Graph(id='id_grafico_engajamento_video'),
                    id='id_div_grafico_engajamento_video'
                )
            ]
        ),
    ]


def gerar_layout_dashboard():
    return html.Div(
        [
            dbc.Row(
                [
                    dbc.Col(
                        html.Div(
                            gerar_layout_categoria_top_dez(),
                            id='id_div_primeira_coluna_primeira_linha_trend',
                            className='class_div_coluna'
                        ),
                        lg=4,
                        id='id_primeira_coluna_primeira_linha_trend',
                        class_name='class_primeira_coluna_primeira_linha_trend'
                    ),
                    dbc.Col(
                        html.Div(
                            gerar_layout_canais_populares(),
                            id='id_div_segunda_coluna_primeira_linha_trend',
                            className='class_div_coluna'
                        ),
                        lg=4,
                        id='id_segunda_coluna_primeira_linha_trend',
                        class_name='class_segunda_coluna_primeira_linha_trend'
                    ),
                    dbc.Col(
                        html.Div(
                            gerar_desempenho_categoria_dia(),
                            id='id_div_terceira_coluna_primeira_linha_trend',
                            className='class_div_coluna'
                        ),
                        lg=4,
                        id='id_terceira_coluna_primeira_linha_trend',
                        class_name='class_terceira_coluna_primeira_linha_trend'
                    ),
                ],
                id='id_primeira_linha_trend',
                class_name='class_primeira_linha_trend'
            ),
            dbc.Row(
                [
                    dbc.Col(
                        html.Div(
                            gerar_layout_video_categoria_dia(),
                            id='id_div_primeira_coluna_segunda_linha_trend',
                            className='class_div_coluna'
                        ),
                        lg=4,
                        id='id_primeira_coluna_segunda_linha_trend',
                        class_name='class_primeira_coluna_segunda_linha_trend'
                    ),
                    dbc.Col(
                        html.Div(
                            gerar_layout_engajamento_canal(),
                            id='id_div_segunda_coluna_segunda_linha_trend',
                            className='class_div_coluna'
                        ),
                        lg=4,
                        id='id_segunda_coluna_segunda_linha_trend',
                        class_name='class_segunda_coluna_segunda_linha_trend'
                    ),
                    dbc.Col(
                        html.Div(
                            gerar_layout_engajamento_video(),
                            id='id_div_terceira_coluna_segunda_linha_trend',
                            className='class_div_coluna'
                        ),
                        lg=4,
                        id='id_terceira_coluna_segunda_linha_trend',
                        class_name='class_terceira_coluna_segunda_linha_trend'
                    ),
                ],
                id='id_segunda_linha_trend',
                class_name='class_segunda_linha_trend'
            )
        ],
        id='id_main_page_trend',
        className='class_name_trend'
    )


@callback(
    Output('id_grafico_layout_top_dez', 'figure'),
    Input('id_input_desempenho', 'value'),
    Input('id_input_data_top_dez_categoria', 'date')
)
def obter_top_dez_categoria(desempenho: str, data: str):
    colunas = ['data_extracao', 'ID_CATEGORIA', 'TURNO_EXTRACAO',
               'INDICE_TURNO_EXTRACAO', 'ID_VIDEO', desempenho]
    nome_arquivo = 'dados_tratado_estatisticas_trends.parquet'
    gerador_consulta = GeradorConsulta(arquivo=nome_arquivo, colunas=colunas)
    dataframe = gerador_consulta.gerar_df_categorias_populares(
        data=data, metrica=desempenho)
    visualizacao = Visualizacao(df_resultado=dataframe)
    cor = trocar_cor_grafico_barra(chave=desempenho)

    fig = visualizacao.gerar_grafico_de_barras(
        coluna_x='TOTAL_MAX',
        coluna_y='NOME_CATEGORIA',
        orientation='h',
        height=len(dataframe) * 100,
        largura=600,
        color=cor,
        texto_posicao='auto',
        category_orders={
            'NOME_CATEGORIA': dataframe['NOME_CATEGORIA'].tolist()
        },
        tickvals_y=False,

    )
    return fig


@callback(
    Output('id_main_canais_populares', 'children'),
    Input('id_select_categoria_canais_populares', 'value'),
    Input('id_input_date_canais_populares', 'date'),
    Input('id_input_desempenho_canais_populares', 'value'),
    Input('id_tabs_canais_populares', 'active_tab')

)
def obter_top_dez_canais_populares(categoria: str, data: str, desempenho: str, tabs: str):
    categoria = categoria.split('-')[0]
    colunas = ['data_extracao', 'ID_CATEGORIA', 'ID_CANAL', 'NM_CANAL',
               'ID_VIDEO', 'TURNO_EXTRACAO', 'INDICE_TURNO_EXTRACAO', desempenho]
    nome_arquivo = 'dados_tratado_estatisticas_trends.parquet'
    gerador_consulta = GeradorConsulta(arquivo=nome_arquivo, colunas=colunas)

    if tabs == "tab_id_canais_mais_populares":
        flag_asc_desc = True
        dataframe = gerador_consulta.gerar_df_canais_populares(
            data=data,
            id_categoria=categoria,
            metrica=desempenho,
            flag_asc_desc=flag_asc_desc
        )

        print('tamanho df', len(dataframe))
        cor = trocar_cor_grafico_barra(chave=desempenho)
        visualizacao = Visualizacao(df_resultado=dataframe)
        fig = visualizacao.gerar_grafico_de_barras(
            coluna_x='TOTAL_MAX',
            coluna_y='NM_CANAL',
            orientation='h',
            height=len(dataframe) * 100,
            largura=600,
            color=cor,
            texto_posicao='auto',
            category_orders={
                'NM_CANAL': dataframe['NM_CANAL'].tolist()
            },
            tickvals_y=False,
        )
        return dcc.Graph(figure=fig)
    else:
        flag_asc_desc = False
        dataframe = gerador_consulta.gerar_df_canais_populares(
            data=data,
            id_categoria=categoria,
            metrica=desempenho,
            flag_asc_desc=flag_asc_desc
        )

        cor = trocar_cor_grafico_barra(chave=desempenho)
        visualizacao = Visualizacao(df_resultado=dataframe)
        fig = visualizacao.gerar_grafico_de_barras(
            coluna_x='TOTAL_MAX',
            coluna_y='NM_CANAL',
            orientation='h',
            height=len(dataframe) * 100,
            largura=600,
            color=cor,
            texto_posicao='auto',
            tickvals_y=False,
        )
        return dcc.Graph(figure=fig)


@callback(
    Output('id_graph_desempenho_canal_dia', 'figure'),
    Input('id_select_categoria_canal_dia', 'value'),
    Input('id_input_desempenho_canal_dia', 'value')
)
def obter_categoria_dia(categoria: str, desempenho: str):
    colunas = ['data_extracao', 'ID_CATEGORIA', 'ID_CANAL', 'NM_CANAL',
               'ID_VIDEO', 'TURNO_EXTRACAO', 'INDICE_TURNO_EXTRACAO', desempenho]
    nome_arquivo = 'dados_tratado_estatisticas_trends.parquet'
    categoria = categoria.split('-')[0]
    gerador_consulta = GeradorConsulta(arquivo=nome_arquivo, colunas=colunas)
    dataframe = gerador_consulta.gerar_df_categorias_populares_dia(
        categoria=categoria, metrica=desempenho)
    visualizacao = Visualizacao(df_resultado=dataframe)
    fig = visualizacao.gerar_grafico_linha(
        coluna_x='data_extracao',
        coluna_y='TOTAL_MAX',
        altura_grafico=400,
        largura_grafico=600,
        color=None
    )
    return fig


@callback(
    Output('id_graph_categoria_video_populares', 'figure'),
    Input('id_input_date_desempenho_categoria_video_dia', 'date'),
    Input('id_input_desempenho_categoria_video_dia', 'value'),
    Input('id_select_desempenho_categoria_video_dia', 'value')
)
def obter_categoria_video_dia_top_dez(data: str, desempenho: str, categoria: str):
    categoria = categoria.split('-')[0]
    nome_arquivo = 'dados_tratado_estatisticas_trends.parquet'
    colunas = ['data_extracao', 'ID_CATEGORIA', 'TURNO_EXTRACAO',
               'INDICE_TURNO_EXTRACAO', 'ID_VIDEO', 'TITULO_VIDEO', desempenho]

    gerador_consulta = GeradorConsulta(arquivo=nome_arquivo, colunas=colunas)
    dataframe = gerador_consulta.gerar_df_categoria_video_dia(
        data=data,
        categoria=categoria.split(' - ')[0],
        metrica=desempenho
    )

    visualizacao = Visualizacao(df_resultado=dataframe)
    fig = visualizacao.gerar_grafico_de_barras(
        coluna_x='TOTAL_MAX',
        coluna_y='ID_VIDEO',
        category_orders={
            'ID_VIDEO': dataframe['ID_VIDEO'].tolist()
        },
        texto_posicao='auto',
        largura=600,
        height=len(dataframe) * 100,
        orientation='h',
        tickvals_y=False
    )
    return fig


@callback(
    Output('id_grafico_engajamento_canal', 'figure'),
    Input('id_input_date_engajamento_canal', 'date'),
    Input('id_select_engajamento_canal', 'value'),
)
def gerar_engajamento_canal(data: str, categoria: str):
    categoria = categoria.split('-')[0]
    nome_arquivo = 'dados_tratado_estatisticas_trends.parquet'
    colunas = ['data_extracao', 'ID_CATEGORIA', 'INDICE_TURNO_EXTRACAO', 'ID_CANAL',
               'NM_CANAL', 'TITULO_VIDEO', 'TOTAL_VISUALIZACOES', 'TOTAL_LIKES', 'TOTAL_COMENTARIOS']

    gerador_consulta = GeradorConsulta(arquivo=nome_arquivo, colunas=colunas)
    dataframe = gerador_consulta.gerar_df_engajamento_canal(
        data=data,
        categoria=categoria
    )
    visualizacao = Visualizacao(df_resultado=dataframe)
    fig = visualizacao.gerar_grafico_de_barras(
        coluna_x='TAXA_ENGAJAMENTO',
        tickvals_y=False,
        height=len(dataframe) * 100,
        coluna_y='ID_CANAL',
        orientation='h',
        category_orders={
            'ID_CANAL': dataframe['ID_CANAL'].tolist()
        },
        texto_posicao='inside'

    )

    return fig


@callback(
    Output('id_grafico_engajamento_video', 'figure'),
    Input('id_input_date_engajamento_video', 'date'),
    Input('id_select_engajamento_video', 'value')
)
def gerar_engajamento_video(data: str, categoria: str):
    categoria = categoria.split('-')[0]
    nome_arquivo = 'dados_tratado_estatisticas_trends.parquet'
    colunas = ['data_extracao', 'ID_CATEGORIA', 'INDICE_TURNO_EXTRACAO', 'NM_CANAL',
               'ID_VIDEO',  'TITULO_VIDEO', 'TOTAL_VISUALIZACOES', 'TOTAL_LIKES', 'TOTAL_COMENTARIOS']
    gerador_consulta = GeradorConsulta(arquivo=nome_arquivo, colunas=colunas)
    dataframe = gerador_consulta.gerar_df_engajamento_video(
        data=data, categoria=categoria)
    visualizacao = Visualizacao(df_resultado=dataframe)
    fig = visualizacao.gerar_grafico_de_barras(
        coluna_x='TAXA_ENGAJAMENTO',
        coluna_y='ID_VIDEO',
        orientation='h',
        tickvals_y=False,
        height=len(dataframe) * 100,
        category_orders={
            'ID_VIDEO': dataframe['ID_VIDEO'].tolist()
        },
        texto_posicao='inside'
    )
    return fig


layout = gerar_layout_dashboard()
