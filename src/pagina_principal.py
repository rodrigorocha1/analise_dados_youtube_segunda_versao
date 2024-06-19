from dash import Dash, dash
import dash
import dash_bootstrap_components as dbc


app = Dash(
    __name__,
    use_pages=True,
    external_stylesheets=[dbc.themes.BOOTSTRAP]
)
app.config['suppress_callback_exceptions'] = True


def gerar_layout_principal():
    return dbc.Container(
        [
            dbc.Row(
                dbc.NavbarSimple(
                    children=[
                        dbc.NavLink(
                            pagina['name'],
                            href=pagina['relative_path'],
                            className='nav_custon',
                        ) for pagina in dash.page_registry.values()
                    ],
                    brand='Dashboard Youtube',
                    className='class_nav_brand',
                    brand_href='#',
                    color='#21242D',
                    dark=True
                ),
                id='id_container_pages',
                className='class_containrer_pages'
            ),
            dbc.Row(
                [
                    dash.page_container
                ],
                id='id_segunda_linha_main',
                className='class_segunda_linha_main'
            )
        ],
        class_name='class_main_container',
        id='id_main_container',
        fluid=True,
    )


app.layout = gerar_layout_principal()
if __name__ == '__main__':
    app.run(debug=True)
