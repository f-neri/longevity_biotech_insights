from __future__ import annotations

from pathlib import Path

import dash
import dash_bootstrap_components as dbc
from dash import dcc, html
import pandas as pd

from lbi_app.viz.plots import category_bar_figure, companies_founded_over_time_figure

# lbi_app/app.py -> repo root
REPO_ROOT = Path(__file__).resolve().parents[1]
CLEAN_PATH = REPO_ROOT / "data" / "companies_clean.csv"


def load_snapshot() -> pd.DataFrame:
    """Load the latest cleaned snapshot used by the dashboard."""
    return pd.read_csv(CLEAN_PATH)


def create_app() -> dash.Dash:
    """Create and configure the Dash app instance."""
    df = load_snapshot()
    fig_categories = category_bar_figure(df, top_n=10)
    fig_founded_over_time = companies_founded_over_time_figure(df)

    app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG])

    app._favicon = "favicon.ico"

    app.layout = dbc.Container(
        [
            html.Img(
                src="/assets/LBI_white_text_yellow_bulb.svg",
                style={
                    "maxWidth": "600px",
                    "width": "100%",
                    "height": "auto",
                    "marginTop": "1rem",
                    "marginBottom": "1rem",
                },
            ),

            dcc.Markdown(
                """
                Longevity Biotech Insights is a dashboard showcasing information
                about the aging/longevity biotech industry.

                This website was created and is maintained by [Francesco Neri](https://f-neri.github.io/). All data is sourced from [AgingBiotech.info](https://agingbiotech.info/companies/),
                a website created and mantained by [Karl Pfleger](https://www.linkedin.com/in/karl-r-pfleger/).
                """,
            ),
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    dcc.Graph(
                                        id="founded-over-time",
                                        figure=fig_founded_over_time,
                                        config={"displayModeBar": False},
                                    ),
                                ]
                            ),
                        ),
                        xs=12,
                        md=6,
                    ),
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    dcc.Graph(
                                        id="category-bar",
                                        figure=fig_categories,
                                        config={"displayModeBar": False},
                                    ),
                                ]
                            ),
                        ),
                        xs=12,
                        md=6,
                    )
                ],
                className="mt-3 g-3",
            ),
        ],
        fluid=True,
        className="pb-4",
    )

    return app


def main() -> None:
    """Entrypoint for running the dev server."""
    app = create_app()
    app.run(debug=True)


if __name__ == "__main__":
    main()
