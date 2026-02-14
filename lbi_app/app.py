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
    fig_categories = category_bar_figure(df, top_n=15)
    fig_founded_over_time = companies_founded_over_time_figure(df)

    app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG])

    app.layout = dbc.Container(
        [
            html.H1("Longevity Biotech Insights", className="mt-4"),
            html.P(
                [
                    "Longevity Biotech Insights is a dashboard showcasing"
                    " information about aging/longevity-focused"
                    " biotechnology companies. All data is sourced from"
                    " [AgingBiotech.info](https://agingbiotech.info/companies/)"
                    " a website created and mantained by [Karl Pfleger](https://www.google.com/url?q=https://www.linkedin.com/in/karl-r-pfleger/&sa=D&source=editors&ust=1771032486980320&usg=AOvVaw0pIucVyWHaS9pL_3fDxO8N).",
                ],
                className="text-muted",
            ),
            dbc.Card(
                dbc.CardBody(
                    [
                        html.H4("Longevity Biotech Companies Over Time", className="card-title"),
                        dcc.Graph(
                            id="founded-over-time",
                            figure=fig_founded_over_time,
                            config={"displayModeBar": False},
                        ),
                    ]
                ),
                className="mt-3",
            ),
            dbc.Card(
                dbc.CardBody(
                    [
                        html.H4("Companies per Category", className="card-title"),
                        dcc.Graph(
                            id="category-bar",
                            figure=fig_categories,
                            config={"displayModeBar": False},
                        ),
                    ]
                ),
                className="mt-3",
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
