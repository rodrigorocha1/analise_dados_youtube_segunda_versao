import dash
from dash import dcc, html, Input, Output
from dash.dependencies import Input, Output


# Inicialize sua aplicação Dash
app = dash.Dash(__name__)

# Layout da aplicação
app.layout = html.Div([
    dcc.Dropdown(
        id='id_select_assunto',
        options=[
            {'label': 'Assunto 1', 'value': 'assunto1'},
            {'label': 'Assunto 2', 'value': 'assunto2'},
            {'label': 'Assunto 3', 'value': 'assunto3'}
        ],
        value='assunto1',
        placeholder='Escolha o Assunto'
    ),
    dcc.Dropdown(
        id='id_select_canal_desempenho',
        multi=True,
        className='class_input_canal',
        style={'backgroundColor': 'black', 'color': 'white'},
        placeholder='Escolha o Canal'
    )
])

# Callback para gerar as opções e valor inicial do dropdown do canal


@app.callback(
    Output('id_select_canal_desempenho', 'options'),
    Output('id_select_canal_desempenho', 'value'),
    Input('id_select_assunto', 'value')
)
def gerar_input_assunto_canal(assunto: str):
    # Simule a leitura dos inputs do seu arquivo ou de onde você os obtém
    inputs_canal = [{'label': 'Canal 1', 'value': 'canal1'}, {
        'label': 'Canal 2', 'value': 'canal2'}, {'label': 'Canal 3', 'value': 'canal3'}]
    # Retorna as opções e o valor inicial
    return inputs_canal, inputs_canal[0]['value']


# Roda a aplicação
if __name__ == '__main__':
    app.run_server(debug=True, port=8070)
