from __future__ import annotations

import json
import os
from pathlib import Path
from importlib import metadata

import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, ctx, dcc, html, no_update
from flask import Response
import pandas as pd

from lbi_app.viz.plots import (
    category_polar_bar_figure,
    clinical_stage_bar_figure,
    companies_founded_over_time_figure,
    geo_map_figure,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
# match the format used by the pipeline/load step
CLEAN_PATH = REPO_ROOT / "data" / "companies_clean.parquet"
DETAIL_LOOKUPS_PATH = REPO_ROOT / "data" / "detail_lookups.json"


def load_snapshot() -> pd.DataFrame:
    """
    Load the latest cleaned snapshot used by the dashboard.
    """
    return pd.read_parquet(CLEAN_PATH)


def load_detail_lookups() -> dict[str, dict[str, list[dict[str, str]]]]:
    """
    Load precomputed detail lookups (stage, year, category, country).
    These are computed during the ETL pipeline and cached here.
    """
    if not DETAIL_LOOKUPS_PATH.exists():
        raise FileNotFoundError(
            f"Detail lookups not found at {DETAIL_LOOKUPS_PATH}. "
            "Run 'lbi-update' to generate them via the ETL pipeline."
        )
    with open(DETAIL_LOOKUPS_PATH) as f:
        return json.load(f)


def _format_year_founded(value: object) -> str:
    if pd.isna(value):
        return "N/A"

    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.notna(timestamp):
        return str(timestamp.year)

    return str(value)


def _format_list_cell(value: object) -> str:
    if isinstance(value, (list, tuple, set)):
        items = [str(item).strip() for item in value if pd.notna(item) and str(item).strip()]
        return ", ".join(items) if items else "N/A"

    if hasattr(value, "tolist") and not isinstance(value, (str, bytes)):
        converted = value.tolist()
        if isinstance(converted, list):
            items = [str(item).strip() for item in converted if pd.notna(item) and str(item).strip()]
            return ", ".join(items) if items else "N/A"

    if pd.isna(value):
        return "N/A"

    text = str(value).strip()
    return text or "N/A"


def build_detail_modal_body(title: str, rows: list[dict[str, str]]) -> list:
    header_cells = [
        html.Th(
            label,
            style={
                "position": "sticky",
                "top": 0,
                "backgroundColor": "#1a1a1a",
                "padding": "0.4rem 0.6rem",
                "textAlign": "left",
                "borderBottom": "1px solid #444444",
                "whiteSpace": "nowrap",
            },
        )
        for label in ["Company", "Year Founded", "Category", "Clinical Stage", "Location"]
    ]

    body_rows = [
        html.Tr(
            [
                html.Td(row["Company"], style={"padding": "0.35rem 0.6rem", "verticalAlign": "top", "whiteSpace": "nowrap"}),
                html.Td(row["Year Founded"], style={"padding": "0.35rem 0.6rem", "verticalAlign": "top", "whiteSpace": "nowrap"}),
                html.Td(row["Category"], style={"padding": "0.35rem 0.6rem", "verticalAlign": "top"}),
                html.Td(row["Clinical Stage"], style={"padding": "0.35rem 0.6rem", "verticalAlign": "top", "whiteSpace": "nowrap"}),
                html.Td(row["Location"], style={"padding": "0.35rem 0.6rem", "verticalAlign": "top"}),
            ]
        )
        for row in rows
    ]

    return [
        dbc.ModalHeader(dbc.ModalTitle(title)),
        dbc.ModalBody(
            html.Div(
                html.Table(
                    [html.Thead(html.Tr(header_cells)), html.Tbody(body_rows)],
                    style={
                        "width": "100%",
                        "borderCollapse": "separate",
                        "borderSpacing": 0,
                        "fontSize": "0.9rem",
                    },
                ),
                style={"overflowX": "auto", "overflowY": "auto", "maxHeight": "65vh"},
            ),
        ),
    ]


def format_detail_modal_title(filter_label: str, selected_value: str, count: int) -> str:
    company_label = "company" if count == 1 else "companies"
    return f"{filter_label}: {selected_value} ({count} {company_label})"


def get_trace_name(fig: object, curve_number: object) -> str | None:
    if not isinstance(curve_number, int):
        return None

    data = getattr(fig, "data", None)
    if data is None or curve_number < 0 or curve_number >= len(data):
        return None

    return getattr(data[curve_number], "name", None)

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
    lookups = load_detail_lookups()
    stage_details = lookups["stage_details"]
    year_details = lookups["year_details"]
    category_details = lookups["category_details"]
    country_details = lookups["country_details"]
    fig_categories = category_polar_bar_figure(df, top_n=10)
    fig_founded_over_time = companies_founded_over_time_figure(df)
    fig_clinical_stage = clinical_stage_bar_figure(df)
    fig_geo = geo_map_figure(df)

    app = dash.Dash(
        __name__,
        external_stylesheets=[dbc.themes.CYBORG],
        title="Longevity Biotech Insights",
    )

    app._favicon = "favicon.ico"

    app.index_string = """
<!DOCTYPE html>
<html lang="en">
    <head>
        {%metas%}
        <title>{%title%}</title>
        <meta name="description" content="Longevity Biotech Insights: interactive dashboard tracking 250+ aging and longevity biotech companies worldwide. Explore companies by founding year, research category (senescence, proteostasis, epigenetics, metabolism and more), clinical stage, and geography.">
        <meta name="robots" content="index, follow">
        <link rel="canonical" href="https://francescon-longevity-biotech-insights.hf.space/">
        <meta property="og:type" content="website">
        <meta property="og:title" content="Longevity Biotech Insights">
        <meta property="og:description" content="Interactive dashboard tracking 250+ aging and longevity biotech companies worldwide by research category, clinical stage, and country.">
        <meta property="og:url" content="https://francescon-longevity-biotech-insights.hf.space/">
        <meta property="og:image" content="https://francescon-longevity-biotech-insights.hf.space/assets/favicon.png">
        <meta name="twitter:card" content="summary">
        <meta name="twitter:title" content="Longevity Biotech Insights">
        <meta name="twitter:description" content="Interactive dashboard tracking 250+ aging and longevity biotech companies worldwide by research category, clinical stage, and country.">
        <script type="application/ld+json">
        {
          "@context": "https://schema.org",
          "@type": "Dataset",
          "name": "Longevity Biotech Insights",
          "description": "A curated dataset and interactive dashboard of aging and longevity biotech companies worldwide, categorised by research focus, clinical stage and geography. Data sourced from AgingBiotech.info.",
          "url": "https://francescon-longevity-biotech-insights.hf.space/",
          "creator": {"@type": "Person", "name": "Francesco Neri", "url": "https://f-neri.github.io/"},
          "isBasedOn": {"@type": "Dataset", "name": "AgingBiotech.info Companies", "url": "https://agingbiotech.info/companies/"},
          "keywords": ["longevity", "aging", "biotech", "anti-aging", "senescence", "proteostasis", "epigenetics", "metabolism", "stem cells", "lifespan", "healthspan", "clinical stage", "drug development"]
        }
        </script>
        {%favicon%}
        {%css%}
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
"""

    @app.server.route("/robots.txt")
    def robots_txt():
        content = (
            "User-agent: *\n"
            "Allow: /\n"
            "Sitemap: https://francescon-longevity-biotech-insights.hf.space/sitemap.xml\n"
        )
        return Response(content, mimetype="text/plain")

    @app.server.route("/sitemap.xml")
    def sitemap_xml():
        content = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
            '  <url>\n'
            '    <loc>https://francescon-longevity-biotech-insights.hf.space/</loc>\n'
            '    <changefreq>weekly</changefreq>\n'
            '    <priority>1.0</priority>\n'
            '  </url>\n'
            '</urlset>\n'
        )
        return Response(content, mimetype="application/xml")

    @app.server.route("/google7d669b47886d2e7c.html")
    def google_site_verification():
        return Response(
            "google-site-verification: google7d669b47886d2e7c.html",
            mimetype="text/html",
        )

    app.layout = dbc.Container(
        [
            html.Img(
                src="/assets/LBI_white_text_yellow_bulb.svg",
                style={
                    "maxWidth": "800px",
                    "width": "100%",
                    "height": "auto",
                    "display": "block",
                    "marginTop": "1rem",
                    "marginBottom": "1rem",
                    "marginLeft": "auto",
                    "marginRight": "auto",
                },
            ),
            dcc.Markdown(
                """
                **Longevity Biotech Insights** is an interactive dashboard tracking the global
                aging/longevity biotechnology industry. It covers 250+ companies working
                on extending healthy human lifespan.

                Explore companies by the year they were founded, their primary research
                category, current clinical development stage (pre-clinical through commercial),
                and geographic location across more than 30 countries.
                Click any chart to see a detailed company breakdown.
                Data sourced from [AgingBiotech.info/companies](https://agingbiotech.info/companies/).
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
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    dcc.Graph(
                                        id="clinical-stage-bar",
                                        figure=fig_clinical_stage,
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
                                        id="geo-map",
                                        figure=fig_geo,
                                        config={"displayModeBar": False},
                                    ),
                                ]
                            ),
                        ),
                        xs=12,
                        md=6,
                    ),
                ],
                className="mt-3 g-3",
            ),
            dbc.Modal(
                id="detail-modal",
                is_open=False,
                size="xl",
                scrollable=True,
                children=[],
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

    @app.callback(
        Output("detail-modal", "is_open"),
        Output("detail-modal", "children"),
        Output("clinical-stage-bar", "clickData"),
        Output("founded-over-time", "clickData"),
        Output("category-bar", "clickData"),
        Output("geo-map", "clickData"),
        Input("clinical-stage-bar", "clickData"),
        Input("founded-over-time", "clickData"),
        Input("category-bar", "clickData"),
        Input("geo-map", "clickData"),
        prevent_initial_call=True,
    )
    def handle_chart_click(stage_click, time_click, cat_click, geo_click):
        triggered = ctx.triggered_id
        no_changes = (False, no_update, no_update, no_update, no_update, no_update)
        filter_labels = {
            "clinical-stage-bar": "Clinical Stage",
            "founded-over-time": "Year Founded",
            "category-bar": "Category",
            "geo-map": "Location",
        }

        click_data_map = {
            "clinical-stage-bar": stage_click,
            "founded-over-time": time_click,
            "category-bar": cat_click,
            "geo-map": geo_click,
        }
        click_data = click_data_map.get(triggered)
        if not click_data or not click_data.get("points"):
            return no_changes

        point = click_data["points"][0]

        if triggered == "clinical-stage-bar":
            key = str(point.get("x", "")).strip()
            rows = stage_details.get(key)
        elif triggered == "founded-over-time":
            trace_name = get_trace_name(fig_founded_over_time, point.get("curveNumber"))
            if trace_name != "New":
                return no_changes
            key = str(int(float(point.get("x", 0))))
            rows = year_details.get(key)
        elif triggered == "category-bar":
            key = str(point.get("theta", "")).strip()
            rows = category_details.get(key)
        elif triggered == "geo-map":
            key = str(point.get("location", "")).strip()
            rows = country_details.get(key)
        else:
            return no_changes

        if not key or not rows:
            return no_changes

        resets = dict.fromkeys(
            ["clinical-stage-bar", "founded-over-time", "category-bar", "geo-map"],
            no_update,
        )
        resets[triggered] = None

        return (
            True,
            build_detail_modal_body(
                format_detail_modal_title(filter_labels[triggered], key, len(rows)),
                rows,
            ),
            resets["clinical-stage-bar"],
            resets["founded-over-time"],
            resets["category-bar"],
            resets["geo-map"],
        )

    return app


def main() -> None:
    """Entrypoint for running the dev server."""
    app = create_app()
    port = int(os.environ.get("PORT", "8050"))
    app.run(host="0.0.0.0", port=port, debug=True)


if __name__ == "__main__":
    main()
