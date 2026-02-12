import dash
import dash_bootstrap_components as dbc
from dash import html


def create_app() -> dash.Dash:
    """Create and configure the Dash app instance."""
    app = dash.Dash(
        __name__,
        external_stylesheets=[dbc.themes.CYBORG]
    )
    app.layout = dbc.Container(
        [
            html.H1("Longevity Biotech Insights", className="mt-4"),
            html.P("Welcome to the LBI dashboard")
        ],
        fluid=True,
    )
    return app


def main() -> None:
    """Entrypoint for running the dev server."""
    app = create_app()
    app.run(debug=True)


if __name__ == "__main__":
    main()
