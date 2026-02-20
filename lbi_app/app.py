from __future__ import annotations

import os
from pathlib import Path
from importlib import metadata

import dash
import dash_bootstrap_components as dbc
from dash import dcc, html
import pandas as pd

from lbi_app.viz.plots import category_bar_figure, companies_founded_over_time_figure

REPO_ROOT = Path(__file__).resolve().parents[1]
CLEAN_PATH = REPO_ROOT / "data" / "companies_clean.csv"


def load_snapshot() -> pd.DataFrame:
    """Load the latest cleaned snapshot used by the dashboard."""
    return pd.read_csv(CLEAN_PATH)

def get_app_version() -> str:
    try:
        return metadata.version("longevity-biotech-insights")
    except metadata.PackageNotFoundError:
        # Fallback for edge cases (e.g., running without installation)
        return "dev"
    
version = get_app_version()

def create_app() -> dash.Dash:
    """Create and configure the Dash app instance."""
    df = load_snapshot()
    fig_categories = category_bar_figure(df, top_n=10)
    fig_founded_over_time = companies_founded_over_time_figure(df)

    app = dash.Dash(
        __name__,
        external_stylesheets=[dbc.themes.CYBORG],
        title="Longevity Biotech Insights",
    )

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
            html.Footer(
                dbc.Container(
                    [
                        dcc.Markdown(
                            f"Longevity Biotech Insights · v{version} ·"
                            " Created and maintained by [Francesco Neri](https://f-neri.github.io/) ·"
                            " Raw data: [AgingBiotech.info](https://agingbiotech.info/companies/) "
                            " by [Karl Pfleger](https://www.linkedin.com/in/karl-r-pfleger/)",
                            className="text-center text-muted small",
                        )
                    ],
                    className="text-center py-3",
                )
            ),
        ],
        fluid=True,
        className="pb-4",
    )

    return app


def main() -> None:
    """Entrypoint for running the dev server."""
    app = create_app()
    port = int(os.environ.get("PORT", "8050"))
    app.run(host="0.0.0.0", port=port, debug=True)


if __name__ == "__main__":
    main()
