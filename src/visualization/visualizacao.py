import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, List
from dash.dash_table import DataTable
import plotly.graph_objs as go


class Visualizacao:
    def __init__(self, df_resultado: pd.DataFrame) -> None:
        """Init da classe

        Args:
            df_resultado (pd.DataFrame): recebe um dataframe do pandas
        """
        self.__df_resultado = df_resultado
        self.__cor_base_um = '#21242D'
        self.__cor_legenda = 'white'
        self.__cor_data = '#1A1C1E'

    def gerar_grafico_de_barras(
            self,
            coluna_x: str,
            coluna_y: str,
            orientation: str,
            height: int = 400,
            valor_maximo: float = None,
            valor_minimo: float = None,
            text_anotation: str = None,
            color: str = '#C552C9',
            tickfont: str = None,
            hovertemplate: str = None,
            text_update_traces: str = None,
            category_orders: Dict = None,
            tickvals_y: bool = True,
            largura: int = None,
            texto_posicao: str = 'outside'

    ):
        # if color:

        #     param_color = self.__df_resultado[coluna_y].apply(
        #         lambda x: 'blue' if x == valor_minimo else ('green' if x == valor_maximo else 'red'))
        # else:
        #     param_color = None

        fig = px.bar(
            self.__df_resultado,
            x=coluna_x,
            y=coluna_y,
            text_auto='0',
            orientation=orientation,
            category_orders=category_orders,
            height=height,
            width=largura
        )
        if tickvals_y:
            yaxis_config = dict(
                title='',
                tickvals=[],
                showgrid=False,
                showline=False,
            )
        else:
            yaxis_config = dict(
                title='',
                showgrid=False,
                showline=False,
            )

        fig.update_layout(

            font=dict(
                color=self.__cor_legenda
            ),
            xaxis_tickformat='%d/%m/%Y',
            paper_bgcolor=self.__cor_base_um,
            plot_bgcolor=self.__cor_base_um,
            xaxis=dict(
                title='',
                showgrid=False,
                showline=False,
                tickfont=dict(
                    size=11,
                )
            ),
            yaxis=yaxis_config,
            showlegend=False,


        )
        if text_anotation is not None:
            margem = 0.1

# Ajuste a coordenada y da anotação para evitar sobreposição com as barras
            y_annot = (height) + margem
            fig.add_annotation(
                text=text_anotation,
                xref="paper",
                yref="paper",
                axref='x',
                ayref='y',
                x=0.1,
                y=1.15,
                ax=8,
                ay=y_annot,
                showarrow=False,
                font=dict(
                    size=14
                )
            )

        fig.update_traces(
            hovertemplate=hovertemplate,
            text=text_update_traces,
            textposition=texto_posicao,
            textfont_color='white',
            hoverlabel=dict(
                font=dict(color='white')

            ),

        )
        for trace in fig.data:
            if isinstance(trace, go.Bar):
                trace.marker.color = color
        return fig

    def gerar_grafico_linha(self, coluna_x: str, coluna_y: str, color: str, altura_grafico: int, largura_grafico=None):
        fig = px.line(self.__df_resultado, x=coluna_x, width=largura_grafico,
                      y=coluna_y, color=color, height=altura_grafico, text=coluna_y, markers=True)
        fig.update_layout(

            font=dict(
                color=self.__cor_legenda
            ),
            xaxis_tickformat='%d/%m/%Y',
            paper_bgcolor=self.__cor_base_um,
            plot_bgcolor=self.__cor_base_um,
            xaxis=dict(
                title='',
                showgrid=False,
                showline=False,
                tickfont=dict(
                    size=11,
                )
            ),
            yaxis=dict(
                title='',
                tickvals=[],
                showgrid=False,
                showline=False,
            ),
            showlegend=False,


        )
        fig.update_traces(
            textposition='top center'
        )
        return fig

    def gerar_tabela(self, id_tabela: str):
        tabela = DataTable(
            self.__df_resultado.to_dict('records'),
            [
                {
                    "name": i,
                    "id": i
                }
                for i in self.__df_resultado.columns
            ],
            style_data={
                'color': 'white',
                'text-align': 'center',
                'background-color': self.__cor_data,
                'border': '1px solid gray',
                'margin-botton': '20px'
            },
            style_header={
                'color': 'white',
                'text-align': 'center',
                'font-weight': 'bold',
                'border': '1px solid gray',
                'margin-botton': '20px',
                'background-color': self.__cor_base_um
            },
            id=id_tabela,
            style_as_list_view=True
        )
        return tabela
