import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import dash_bootstrap_components as dbc

# Criação do app Dash
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Layout com as tabs e o espaço para o gráfico
app.layout = html.Div([
    dbc.Tabs(
        [
            dbc.Tab(

                label='Desempenho Likes',
                tab_id='id_tab_likes'
            ),
            dbc.Tab(

                label='Desempenho Comentários',
                tab_id='id_tab_comentarios'
            ),
            dbc.Tab(
                label='Desempenho Visualizações',
                tab_id='id_tab_visualizacoes'
            ),
        ],
        id='tabs',
        active_tab='id_tab_likes'  # Tab ativa por padrão
    ),
    html.Div(id='grafico-selecionado')
])

# Callback para atualizar o gráfico de acordo com o tab selecionado


@app.callback(
    Output('grafico-selecionado', 'children'),
    [Input('tabs', 'active_tab')]
)
def render_content(tab):
    if tab == 'id_tab_likes':
        # Aqui você pode substituir esta lógica com seus próprios dados e gráficos
        fig = px.bar(x=[1, 2, 3], y=[4, 5, 6], labels={
                     'x': 'X-axis', 'y': 'Y-axis'}, title='Likes')
        return dcc.Graph(figure=fig)
    elif tab == 'id_tab_comentarios':
        # Lógica para o gráfico de comentários
        fig = px.line(x=[1, 2, 3], y=[10, 15, 12], labels={
                      'x': 'X-axis', 'y': 'Y-axis'}, title='Comentários')
        return dcc.Graph(figure=fig)
    elif tab == 'id_tab_visualizacoes':
        # Lógica para o gráfico de visualizações
        fig = px.scatter(x=[1, 2, 3], y=[50, 30, 45], labels={
                         'x': 'X-axis', 'y': 'Y-axis'}, title='Visualizações')
        return dcc.Graph(figure=fig)
    else:
        return 'Tab não reconhecida'


if __name__ == '__main__':
    app.run_server(debug=True)
