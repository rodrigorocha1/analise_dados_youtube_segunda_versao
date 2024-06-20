try:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from datetime import date
from typing import List
import dash
import dash_bootstrap_components as dbc
from dash import html, dcc, callback, Output, Input
from dash.exceptions import PreventUpdate
from src.dados.gerador_consulta import GeradorConsulta
from src.visualization.visualizacao import Visualizacao
from src.dados.depara import Depara
from src.utils.utils import trocar_cor_grafico_barra

dash.register_page(__name__, name="Analise Assunto", path='/')


def gerar_comparacao():
    return [
        html.H5('Comparação desempenho vídeo',
                id='id_titulo_comparacao_video'),
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
        dcc.Graph(id='id_grafico_comparacao_video')
    ]


def gerar_layout_videos_publicados():
    return [
        html.H5(
            'Frequência dos vídeos públicados',
            id='id_titulo_video_publicado'
        ),
        dcc.Graph('id_video_publicado')
    ]


def gerar_top_dez_desempenho():
    return [
        html.H5('Teste', id='id_titulo_top_dez',
                className='class_titulo_grafico'),
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
                        id='id_input_top_dez',
                        className='class_input_select'
                    ), lg=6
                ),
                dbc.Col(
                    dcc.DatePickerSingle(
                        date='2024-01-20',
                        display_format='DD/MM/YYYY',
                        max_date_allowed=date(2024, 1, 23),
                        min_date_allowed=date(2024, 1, 17),
                        id='id_input_data_top_dez'
                    ),
                    lg=6
                ),
            ]
        ),
        html.Div(
            dcc.Graph(id='id_grafico_top_dez'),
            id='id_div_grafico_top_dez'
        ),
    ]


def gerar_layout_desempenho_canal_dia():
    return (
        [
            html.H5('Desempenho canal por dia (Likes/ Comentários/ Visualizações)',
                    className='class_titulo_grafico'),
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Select(
                            id='id_select_canal_desempenho',

                            className='class_input_canal',
                            style={'backgroundColor': 'black',
                                   'color': 'white'},
                            placeholder='Escolha o Canal'
                        ),
                        lg=6
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
                            id='id_input_desempenho_canal',
                            className='class_input_select'
                        ),
                        lg=6
                    )
                ],
            ),
            dcc.Graph(id='id_grafico_desempenho_canal_dia')
        ]
    )


def gerar_layout_desempenho_video():
    return [
        html.H5(
            'Desempenho Vídeo',
            id='id_titulo_desempenho_video',
            className='class_titulo_grafico'
        ),
        dbc.Form(
            [
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
                    id='id_input_desempenho_video',
                    className='class_input_select'
                ),
                dbc.Select(
                    id='id_desempenho_video',
                    style={
                        'backgroundColor': 'black', 'color': 'white',
                    },
                )
            ],
            id='id_form_group_video',
            class_name='class_form_group_data'

        ),



        dcc.Graph(id='id_grafico_desempenho_video')
    ]


def gerar_layout_engajamento():
    return [
        html.H5(
            'TOP 10 Análise engajamento DIA',
            id='id_titulo_engajamento',
            className='class_titulo_grafico'
        ),
        dbc.Form(
            [
                html.P('Selecione a data: ',
                       className='class_titulo_grafico'),
                dcc.DatePickerSingle(
                    date='2024-01-20',
                    display_format='DD/MM/YYYY',
                    max_date_allowed=date(2024, 1, 23),
                    min_date_allowed=date(2024, 1, 17),
                    id='id_input_data_top_dez_engaj_dia',
                    style={
                        'background-color': 'red'
                    }
                )
            ],
            id='id_form_group_data',
            class_name='class_form_group_data'

        ),
        html.Div(id='id_div_tabela_top_dez_engaj')
    ]


def gerar_layout_popularidade_tags():
    return [
        html.H5(
            'Análise popularidade tags',
            id='id_titulo_popularidade',
            className='class_titulo_grafico_tabela'
        ),
        html.Div(id='id_div_tabela_top_dez_popularidade_tags')
    ]


def gerar_layout_duracao_video_engajamento():
    return [
        html.H5(
            'Duração Vídeo X Engajamento ',
            id='id_titulo_popularidade',
            className='class_titulo_grafico_tabela'
        ),
        html.Div(id='id_div_layout_engajamento')
    ]


def gerar_layout_analise_palavra_chave_titulos():
    return [
        html.H5(
            'Análise de palavras Chaves em títulos',
            id='id_titulo_palavra_chave',
            className='class_titulo_grafico_tabela'
        ),
        html.Div(id='id_div_tabela_popularidade_titulo')
    ]


def gerar_layout_dashboard():
    return html.Div(
        [
            dbc.Row(
                [
                    html.Div(
                        [
                            html.H5(
                                'Escolha o assunto de Análise:',
                                id='id_titulo_pprimeira_linha',
                                className='class_titulo_primeira_linha'
                            ),
                            dbc.RadioItems(
                                options=[
                                    {
                                        'label': 'Cites Skylines',
                                        'value': 'assunto_cities_skylines'
                                    },
                                    {
                                        'label': 'Linux',
                                        'value': 'assunto_linux'
                                    },
                                    {
                                        'label': 'Power BI',
                                        'value': 'assunto_power_bi'
                                    },
                                    {
                                        'label': 'Python AND dados',
                                        'value': 'assunto_python_and_dados'
                                    },
                                    {
                                        'label': 'Cities Skylines 2',
                                        'value': 'assunto_cities_skylines_2'
                                    },
                                    {
                                        'label': 'Linux Gamming',
                                        'value': 'assunto_linux_gamming'
                                    },
                                    {
                                        'label': 'Gensim Impact',
                                        'value': 'assunto_genshin_impact'
                                    },
                                    {
                                        'label': 'Zelda',
                                        'value': 'assunto_zelda'
                                    }
                                ],
                                inline=True,
                                id='id_select_assunto',
                                value='assunto_cities_skylines'
                            )
                        ],


                        id='id_div_primeira_coluna_label_dashboard',
                        className='class_div_primeira_linha_primeira_coluna_label_dashboard'
                    )
                ],
                id='id_input_primeira_linha_dashboard',
                className='class_primeira_linha_dashboad'
            ),

            dbc.Row(
                [
                    dbc.Col(
                        html.Div(
                            gerar_comparacao(),
                            id='id_div_segunda_linha_primeira_coluna_label_dashboard',
                            className='class_div_coluna'
                        ),
                        lg=4,
                        id='id_segunda_linha_segunda_coluna_label_dashboard',
                        className='class_segunda_linha_primeira_coluna_label_dashboard'
                    ),
                    dbc.Col(
                        html.Div(
                            gerar_layout_videos_publicados(),
                            id='id_div_segunda_linha_segunda_coluna_input_dashboard',
                            className='class_div_coluna'

                        ),
                        lg=4,
                        id='id_segunda_linha_segunda_coluna_input_dashboard',
                        className='class_segunda_linha_segunda_coluna_input_dashboard'
                    ),
                    dbc.Col(
                        html.Div(
                            gerar_top_dez_desempenho(),
                            id='id_div_segunda_linha_terceira_coluna_input_dashboard',
                            className='class_div_coluna'

                        ),
                        lg=4,
                        id='id_segunda_linha_terceira_coluna_input_dashboard',
                        className='class_segunda_linha_terceira_coluna_input_dashboard'
                    ),
                ],
                id='id_segunda_linha_input_dashboard',
                className='class_segunda_linha_name_dashboad'
            ),
            dbc.Row(
                [
                    dbc.Col(
                        html.Div(
                            gerar_layout_desempenho_canal_dia(),
                            id='id_div_terceira_linha_primeira_coluna_dashboard',
                            className='class_div_coluna'
                        ),
                        lg=4,
                        id='id_terceira_linha_primeira_coluna_dashboard',
                        className='class_terceira_linha_primeira_coluna_dashboard'
                    ),
                    dbc.Col(
                        html.Div(
                            gerar_layout_desempenho_video(),
                            id='id_div_terceira_linha_segunda_coluna_dashboard',
                            className='class_div_coluna'
                        ),
                        lg=4,
                        id='id_terceira_linha_segunda_coluna_dashboard',
                        className='class_terceira_linha_segunda_coluna_dashboard'
                    ),
                    dbc.Col(
                        html.Div(
                            gerar_layout_analise_palavra_chave_titulos(),
                            id='id_div_quarta_linha_terceira_coluna_dashboard',
                            className='class_div_coluna'
                        ),
                        id='id_quarta_linha_terceira_coluna_dashboard',
                        class_name='class_quarta_linha_terceira_coluna_dashboard',
                        lg=4
                    ),
                ],
                id='id_terceira_linha_dashboard',
                class_name='class_terceira_linha_dashboard'
            ),
            dbc.Row(
                [
                    dbc.Col(
                        html.Div(
                            gerar_layout_engajamento(),
                            id='id_div_quarta_linha_primeira_coluna_dashboard',
                            className='class_div_coluna'
                        ),
                        id='id_quarta_linha_primeira_coluna_dashboard',
                        class_name='class_quarta_linha_primeira_coluna_dashboard',
                        lg=8
                    ),
                    dbc.Col(
                        html.Div(
                            gerar_layout_popularidade_tags(),
                            id='id_div_quarta_linha_segunda_coluna_dashboard',
                            className='class_div_coluna'
                        ),
                        id='id_quarta_linha_segunda_coluna_dashboard',
                        class_name='class_quarta_linha_segunda_coluna_dashboard',
                        lg=4
                    ),



                ],
                id='id_quarta_linha_dashboard',
                class_name='class_quarta_linha_dashboard'
            ),
        ],
        id='id_main_page_dashboard',
        className='class_name_dashboard'
    )


@callback(
    Output('id_grafico_comparacao_video', 'figure'),
    Input('id_select_assunto', 'value'),
    Input('id_input_desempenho', 'value')
)
def gerar_desempenho(assunto: str, desempenho: str):
    nome_arquivo = 'dados_tratado_estatisticas_gerais.parquet'
    columns = ['ASSUNTO', 'data_extracao', 'ID_VIDEO',
               desempenho, 'TURNO_EXTRACAO', 'INDICE_TURNO_EXTRACAO']
    gerador_consulta = GeradorConsulta(arquivo=nome_arquivo, colunas=columns)

    dataframe, top_dez_max, top_dez_min, valor_maximo, valor_minimo = gerador_consulta.gerar_desempenho_dia(
        assunto=assunto, coluna_analise=desempenho)
    max_value = dataframe['TOTAL_MAX_DIA'].max()
    dia_pico = dataframe[dataframe['TOTAL_MAX_DIA']
                         == max_value]['data_extracao'].dt.strftime('%d/%m/%Y').values[0]

    if desempenho == 'TOTAL_VISUALIZACOES':
        texto = f'O maior pico de <b>vizualições</b><br>foi no dia <b>{dia_pico}</b>'
    elif desempenho == 'TOTAL_LIKES':
        texto = f'O maior pico de <b>likes</b><br>foi no dia <br>{dia_pico}</b>'
    else:
        texto = f'O maior pico de <b>comentários</b><br>foi no dia <b>{dia_pico}</b>'

    visualizacao = Visualizacao(df_resultado=dataframe)
    tickfont = '%d/%m/%Y',

    hovertemplate = '<b>DATA</b>: %{x}<br><b>Total Visualizações dia:</br> %{y}'
    cor = trocar_cor_grafico_barra(desempenho)

    fig = visualizacao.gerar_grafico_de_barras(
        coluna_x='data_extracao',
        coluna_y='TOTAL_MAX_DIA',
        valor_maximo=valor_maximo,
        valor_minimo=valor_minimo,
        text_anotation=texto,
        tickfont=tickfont,
        orientation='v',
        hovertemplate=hovertemplate,
        height=450,
        color=cor,
        largura=600
    )

    return fig


@callback(
    Output('id_video_publicado', 'figure'),
    Input('id_select_assunto', 'value'),
)
def gerar_publicacao_video(assunto: str):
    nome_arqruivo = 'dados_tratado_estatisticas_gerais.parquet'
    columns = ['DATA_PUBLICACAO', 'ASSUNTO', 'ID_VIDEO']
    gerador_consulta = GeradorConsulta(arquivo=nome_arqruivo, colunas=columns)
    dataframe = gerador_consulta.gerar_publicacao_video(assunto=assunto)
    total_max = dataframe['TOTAL_VIDEOS_PUBLICADOS'].max()

    dia_publicacao = dataframe[dataframe['TOTAL_VIDEOS_PUBLICADOS']
                               == total_max]['DIA_PUBLICACAO'].values[0]

    texto = f'O dia que teve o maior pico<br>de publicação de vídeos foi <b>{dia_publicacao}</b>'

    visualizacao = Visualizacao(df_resultado=dataframe)
    fig = visualizacao.gerar_grafico_de_barras(
        coluna_x='DIA_PUBLICACAO',
        text_anotation=texto,
        coluna_y='TOTAL_VIDEOS_PUBLICADOS',
        valor_maximo=None,
        valor_minimo=None,
        orientation='v',
        tickfont=None,
        hovertemplate='<b>Dia Publicação:</b> %{x}<b><br>Total Vídeos Públicados: %{y}',
        height=440
    )
    return fig


@callback(
    Output('id_grafico_top_dez', 'figure'),
    Output('id_titulo_top_dez', 'children'),
    Input('id_select_assunto', 'value'),
    Input('id_input_data_top_dez', 'date'),
    Input('id_input_top_dez', 'value')

)
def gerar_top_dez(assunto: str, data: str, metricas: str):

    columns = ['data_extracao', 'ASSUNTO', 'ID_VIDEO',
               metricas, 'TURNO_EXTRACAO', 'INDICE_TURNO_EXTRACAO']
    nome_arqruivo = 'dados_tratado_estatisticas_gerais.parquet'
    gerador_consulta = GeradorConsulta(colunas=columns, arquivo=nome_arqruivo)
    dataframe = gerador_consulta.gerar_top_dez(
        assunto=assunto, data=data, metrica=metricas)
    visualizacao = Visualizacao(df_resultado=dataframe)
    if metricas == 'TOTAL_VISUALIZACOES':
        texto_template = '<b>Total Visualizações:</b> %{x}<b><br>Vídeo %{y}'
    elif metricas == 'TOTAL_LIKES':
        texto_template = '<b>Total Likes:</b> %{x}<b><br>Vídeo %{y}'
    else:
        texto_template = '<b>Total Comentários:</b> %{x}<b><br>Vídeo %{y}'

    cor = trocar_cor_grafico_barra(metricas)

    fig = visualizacao.gerar_grafico_de_barras(
        coluna_x='TOTAL',
        coluna_y='ID_VIDEO',
        valor_maximo=None,
        valor_minimo=None,
        text_anotation='',
        orientation='h',
        tickfont=None,
        hovertemplate=texto_template,
        category_orders={'ID_VIDEO': dataframe['ID_VIDEO'].tolist()},
        height=600,
        largura=550,
        texto_posicao='auto',
        color=cor,
        tickvals_y=False



    )

    metricas_titulos = {
        'TOTAL_VISUALIZACOES': 'TOP 10 vídeos com mais visualizações',
        'TOTAL_COMENTARIOS': 'TOP 10 vídeos com mais comentários',
        'TOTAL_LIKES': 'TOP 10 vídeos com mais likes'
    }

    titulo = metricas_titulos.get(metricas)
    return fig, titulo


@callback(
    Output('id_select_canal_desempenho', 'options'),
    Output('id_select_canal_desempenho', 'value'),
    Input('id_select_assunto', 'value')
)
def gerar_input_assunto_canal(assunto: str):
    # nome_arquivo = 'inputs_assunto_canal_cript.pkl'
    nome_arquivo = 'inputs_assunto_canal_rev.pkl'
    path_pasta = 'outros'
    depara = Depara(nm_arquivo=nome_arquivo, path_pasta=path_pasta)
    inputs_canal = depara.abrir_picke(param_filtro=assunto)

    return inputs_canal, inputs_canal[5]['label']


@callback(
    Output('id_grafico_desempenho_canal_dia', 'figure'),
    Input('id_select_canal_desempenho', 'value'),
    Input('id_input_desempenho_canal', 'value')
)
def gerar_desempenho_canal_dia(canal: str | List, metrica: str):

    nome_arqruivo = 'dados_tratado_estatisticas_gerais.parquet'
    colunas = ['ID_CANAL', 'NM_CANAL', 'data_extracao', 'ID_VIDEO',
               'TURNO_EXTRACAO', metrica, 'INDICE_TURNO_EXTRACAO']
    gerador_consulta = GeradorConsulta(arquivo=nome_arqruivo, colunas=colunas)
    dataframe = gerador_consulta.gerar_desempenho_canal(
        id_canal=canal, metrica=metrica)
    visualizacao = Visualizacao(df_resultado=dataframe)
    fig = visualizacao.gerar_grafico_linha(
        coluna_x='data_extracao', coluna_y='TOTAL_DIA', color='ID_CANAL', altura_grafico=400)

    return fig


@callback(
    Output('id_desempenho_video', 'options'),
    Output('id_desempenho_video', 'value'),
    Input('id_select_assunto', 'value')
)
def gerar_input_canal_video(assunto: str):

    if assunto is None:
        raise PreventUpdate
    # canal = 'UCMqGy4xIIGs01ZVcBv0B8Cw'

    nome_arquivo = 'dic_inputs_video_revelado.pkl'
    path_pasta = 'outros'
    depara = Depara(nm_arquivo=nome_arquivo, path_pasta=path_pasta)
    inputs_video = depara.abrir_picke(param_filtro=assunto)

    return inputs_video, inputs_video[5]['label']


@callback(
    Output('id_grafico_desempenho_video', 'figure'),
    Input('id_desempenho_video', 'value'),
    Input('id_input_desempenho_video', 'value')
)
def gerar_desempenho_video(video: str | List, metrica: str):

    # metrica = 'TOTAL_LIKES'
    nome_arqruivo = 'dados_tratado_estatisticas_gerais.parquet'
    colunas = ['data_extracao', 'ID_VIDEO', 'TITULO_VIDEO',
               'TURNO_EXTRACAO', metrica, 'INDICE_TURNO_EXTRACAO']
    gerador_consulta = GeradorConsulta(arquivo=nome_arqruivo, colunas=colunas)
    dataframe = gerador_consulta.gerar_desempenho_video(
        id_video=video, metrica=metrica)
    visualizacao = Visualizacao(df_resultado=dataframe)

    fig = visualizacao.gerar_grafico_linha(
        coluna_x='data_extracao',
        coluna_y='TOTAL_DIA',
        color='ID_VIDEO',
        altura_grafico=400
    )
    return fig


@callback(
    Output('id_div_tabela_top_dez_engaj', 'children'),
    Input('id_select_assunto', 'value'),
    Input('id_input_data_top_dez_engaj_dia', 'date'),
)
def gerar_top_dez_engajamento(assunto: str, data: str):

    colunas = ['data_extracao', 'ASSUNTO', 'ID_VIDEO', 'TURNO_EXTRACAO', 'TOTAL_LIKES',
               'TOTAL_COMENTARIOS', 'TOTAL_VISUALIZACOES', 'INDICE_TURNO_EXTRACAO', 'TOTAL_FAVORITOS']
    nome_arqruivo = 'dados_tratado_estatisticas_gerais.parquet'
    gerador_consulta = GeradorConsulta(arquivo=nome_arqruivo, colunas=colunas)
    dataframe = gerador_consulta.gerar_top_dez_engagamento(
        assunto=assunto, data=data)

    visualizacao = Visualizacao(df_resultado=dataframe)
    fig = visualizacao.gerar_tabela(id_tabela='id_tabela_top_dez_engaj')
    return fig


@callback(
    Output('id_div_tabela_top_dez_popularidade_tags', 'children'),
    Input('id_select_assunto', 'value'),
)
def gerar_popularidade_tags(assunto: str):
    colunas = ['data_extracao', 'ASSUNTO', 'ID_VIDEO', 'TITULO_VIDEO', 'TAGS', 'TOTAL_VISUALIZACOES',
               'TOTAL_LIKES', 'TOTAL_COMENTARIOS', 'ID_CANAL', 'NM_CANAL', 'TURNO_EXTRACAO', 'INDICE_TURNO_EXTRACAO']
    nome_arqruivo = 'dados_tratado_estatisticas_gerais.parquet'
    gerador_consulta = GeradorConsulta(arquivo=nome_arqruivo, colunas=colunas)
    dataframe = gerador_consulta.gerar_dataframe_popularidade_tags(
        assunto=assunto)
    visualizacao = Visualizacao(df_resultado=dataframe)
    fig = visualizacao.gerar_tabela(id_tabela='id_tabela_popularidade_tags')
    return fig


@callback(
    Output('id_div_tabela_popularidade_titulo', 'children'),
    Input('id_select_assunto', 'value'),
)
def gerar_popularidade_titulo(assunto: str):
    colunas = ['data_extracao', 'ASSUNTO', 'ID_VIDEO',
               'TITULO_VIDEO', 'ID_CANAL', 'NM_CANAL']
    nome_arqruivo = 'dados_tratado_estatisticas_gerais.parquet'
    gerador_consulta = GeradorConsulta(arquivo=nome_arqruivo, colunas=colunas)
    dataframe = gerador_consulta.gerar_popularidade_titulo(assunto=assunto)
    visualizacao = Visualizacao(df_resultado=dataframe)
    fig = visualizacao.gerar_tabela(id_tabela='id_tabela_popularidade_titulo')
    return fig


layout = gerar_layout_dashboard()
