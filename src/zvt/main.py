import dash_bootstrap_components as dbc
from dash import dcc, html
from dash.dependencies import Input, Output
import logging

from zvt.ui import zvt_app
from zvt.ui.apps import factor_app

logger = logging.getLogger(__name__)

# Sidebar styling
SIDEBAR_STYLE = {
    "position": "fixed",
    "top": 0,
    "left": 0,
    "bottom": 0,
    "width": "16rem",
    "padding": "2rem 1rem",
    "backgroundColor": "#222222",
}

# Content styling
CONTENT_STYLE = {
    "marginLeft": "18rem",
    "marginRight": "2rem",
    "padding": "2rem 1rem",
}

def serve_layout():
    sidebar = html.Div(
        [
            html.P("ZVT 量化平台", className="lead", style={"color": "darkgray", "textAlign": "center"}),
            dbc.Nav(
                [
                    dbc.NavLink("因子分析", href="/", active="exact"),
                ],
                vertical=True,
                pills=True,
            ),
        ],
        style=SIDEBAR_STYLE,
    )

    content = html.Div(
        id="page-content",
        style=CONTENT_STYLE,
        children=factor_app.factor_layout()
    )

    layout = html.Div(
        [
            dcc.Location(id="url"),
            sidebar,
            content
        ]
    )
    return layout

zvt_app.layout = serve_layout

# We can add routing callbacks here later if we add more pages
@zvt_app.callback(
    Output("page-content", "children"),
    [Input("url", "pathname")]
)
def render_page_content(pathname):
    if pathname == "/" or pathname == "/factor":
        return factor_app.factor_layout()
    # Add more routes here as the app grows
    return html.Div(
        [
            html.H1("404: 未找到页面", className="text-danger"),
            html.Hr(),
            html.P(f"路径 {pathname} 无法识别..."),
        ],
        className="p-3 bg-light rounded-3",
    )

def main():
    zvt_app.title = "ZVT Quant"
    # Ensure layout is function so it's generated on load
    zvt_app.layout = serve_layout
    zvt_app.run_server(debug=True, host="0.0.0.0", port=8050)

if __name__ == "__main__":
    main()
