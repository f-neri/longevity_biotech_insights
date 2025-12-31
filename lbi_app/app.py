import dash
from dash import html


def create_app() -> dash.Dash:
    """Create and configure the Dash app instance."""
    app = dash.Dash(__name__)
    app.layout = html.Div(
        [
            html.H1("Longevity Biotech Insights"),
            html.P("Welcome to the LBI dashboard"),
        ]
    )
    return app


def main() -> None:
    """Entrypoint for running the dev server."""
    app = create_app()
    app.run(debug=True)


if __name__ == "__main__":
    main()
