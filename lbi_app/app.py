from __future__ import annotations

import os
from pathlib import Path
from importlib import metadata

import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, State, ctx, dcc, html, no_update, dash_table
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


def load_snapshot() -> pd.DataFrame:
    """
    Load the latest cleaned snapshot used by the dashboard.
    """
    return pd.read_parquet(CLEAN_PATH)


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
    def _display_location(value: object) -> str:
        text = str(value).strip() if value is not None else ""
        if not text:
            return "N/A"
        parts = [part.strip() for part in text.split(",")]
        normalized = ["N/A" if part == "Unknown" else part for part in parts if part]
        return ", ".join(normalized) if normalized else "N/A"

    table_rows = [
        {
            "Company": row.get("Company", "N/A"),
            "Year Founded": row.get("Year Founded", "N/A"),
            "Category": row.get("Category", "N/A"),
            "Clinical Stage": row.get("Clinical Stage", "N/A"),
            "Location": _display_location(row.get("Location")),
        }
        for row in rows
    ]

    columns = [
        {"name": "Company", "id": "Company"},
        {"name": "Year Founded", "id": "Year Founded"},
        {"name": "Category", "id": "Category"},
        {"name": "Clinical Stage", "id": "Clinical Stage"},
        {"name": "Location", "id": "Location"},
    ]

    # Ensure header labels do not clip by deriving a minimum width from label length.
    header_width_rules = [
        {
            "if": {"column_id": col["id"]},
            "minWidth": f"{max(len(col['name']) + 3, 10)}ch",
        }
        for col in columns
    ]

    return [
        dbc.ModalHeader(dbc.ModalTitle(title, style={"whiteSpace": "pre-line"})),
        dbc.ModalBody(
            html.Div(
                dash_table.DataTable(
                    id="detail-modal-table",
                    columns=columns,
                    data=table_rows,
                    sort_action="native",
                    sort_mode="multi",
                    cell_selectable=False,
                    page_action="none",
                    fill_width=False,
                    style_as_list_view=True,
                    style_table={
                        "minWidth": "100%",
                        "overflowX": "auto",
                    },
                    style_header={
                        "backgroundColor": "#1a1a1a",
                        "color": "#adafae",
                        "fontWeight": "600",
                        "padding": "0.4rem 0.6rem",
                        "textAlign": "left",
                        "borderBottom": "1px solid #444444",
                        "whiteSpace": "nowrap",
                    },
                    style_cell={
                        "backgroundColor": "rgba(0,0,0,0)",
                        "color": "#adafae",
                        "padding": "0.35rem 0.6rem",
                        "fontSize": "0.9rem",
                        "border": "none",
                        "textAlign": "left",
                        "verticalAlign": "top",
                        "whiteSpace": "normal",
                        "height": "auto",
                    },
                    style_cell_conditional=[
                        *header_width_rules,
                        {"if": {"column_id": "Company"}, "whiteSpace": "nowrap"},
                        {"if": {"column_id": "Year Founded"}, "whiteSpace": "nowrap"},
                        {"if": {"column_id": "Clinical Stage"}, "whiteSpace": "nowrap"},
                    ],
                    fixed_rows={"headers": True},
                ),
                style={"overflowX": "auto", "overflowY": "auto", "maxHeight": "65vh"},
            ),
        ),
    ]


def format_detail_modal_title(filter_label: str, selected_value: str, count: int) -> str:
    company_label = "company" if count == 1 else "companies"
    return f"{filter_label}: {selected_value}\n({count} {company_label})"


def _build_detail_rows(df_subset: pd.DataFrame) -> list[dict[str, str]]:
    """Format filtered company rows for the detail modal table."""
    if df_subset.empty:
        return []

    ordered = df_subset.copy()
    if "full overall score" in ordered.columns:
        ordered = ordered.sort_values("full overall score", ascending=False)

    rows: list[dict[str, str]] = []
    for _, row in ordered.iterrows():
        stage_val = row.get("latest clinical stage")
        stage_text = "N/A" if pd.isna(stage_val) else str(stage_val).strip() or "N/A"

        rows.append(
            {
                "Company": str(row.get("Company", "N/A")).strip() or "N/A",
                "Year Founded": _format_year_founded(row.get("year founded")),
                "Category": _format_list_cell(row.get("categories")),
                "Clinical Stage": stage_text,
                "Location": _format_list_cell(row.get("geo_country")),
            }
        )

    return rows


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

FILTER_YEAR_MIN = 2000

def _as_items(value: object) -> list[str]:
    """Normalize scalar/list-like cell values into a clean string list."""
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(v).strip() for v in value if pd.notna(v) and str(v).strip()]
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if hasattr(value, "tolist") and not isinstance(value, (bytes, bytearray)):
        converted = value.tolist()
        if isinstance(converted, list):
            return [str(v).strip() for v in converted if pd.notna(v) and str(v).strip()]
        if pd.notna(converted):
            text = str(converted).strip()
            return [text] if text else []
        return []
    if pd.isna(value):
        return []
    text = str(value).strip()
    return [text] if text else []


def _apply_df_filters(
    df: pd.DataFrame,
    year_range: list[int],
    categories: list[str] | None,
    stages: list[str] | None,
    countries: list[str] | None,
) -> pd.DataFrame:
    """Return a filtered copy of df based on dashboard filter selections."""

    mask = pd.Series(True, index=df.index)
    years = pd.to_datetime(df["year founded"], errors="coerce").dt.year
    # Apply year filtering only when user narrows the slider.
    # At full default span, keep all rows (including unknown/current-year values).
    full_default_year_span = (
        year_range[0] <= FILTER_YEAR_MIN
        and year_range[1] >= (pd.Timestamp.today().year - 1)
    )
    if not full_default_year_span:
        mask &= (years >= year_range[0]) & (years <= year_range[1])
    if categories:
        def _has_cat(cats: object) -> bool:
            return any(c in categories for c in _as_items(cats))
        mask &= df["categories"].apply(_has_cat)
    if stages:
        mask &= df["latest clinical stage"].isin(stages)
    if countries:
        def _has_country(geo_list: object) -> bool:
            return any(
                (entry.split(" - ")[0] if " - " in entry else entry) in countries
                for entry in _as_items(geo_list)
                if entry != "Unknown"
            )
        mask &= df["geo_country"].apply(_has_country)
    return df[mask]

def graph_loader(graph_id: str, figure: object | None = None) -> dcc.Loading:
    """Return a graph wrapped in a consistent loading spinner."""
    return dcc.Loading(
        type="circle",
        color="#2a9fd6",
        delay_show=150,
        target_components={graph_id: "figure"},
        custom_spinner=html.Div(
            [
                html.Div(className="lbi-spinner-ring"),
                html.Img(
                    src="/assets/logo.svg",
                    className="lbi-spinner-logo",
                ),
            ],
            className="lbi-spinner-container",
        ),
        children=dcc.Graph(
            id=graph_id,
            figure=figure if figure is not None else {},
            config={"displayModeBar": False},
        ),
    )

def create_app() -> dash.Dash:
    """Create and configure the Dash app instance."""
    df = load_snapshot()
    
    # --- Filter options ------------------------------------------------------
    filter_year_max = pd.Timestamp.today().year - 1
    all_categories = sorted(
        cat
        for cat in df["categories"].explode().dropna().unique()
        if isinstance(cat, str)
    )
    _stage_order = [
        "Pre-Clinical", "Phase 1", "Phase 2", "Phase 3", "Pre-Commercial", "Commercial",
    ]
    _present_stages = set(df["latest clinical stage"].dropna().astype(str).unique())
    all_stages = [s for s in _stage_order if s in _present_stages]
    all_countries = sorted(
        c
        for c in (
            df["geo_country"].explode().dropna()
            .apply(lambda x: x.split(" - ")[0] if isinstance(x, str) else None)
            .dropna()
            .unique()
        )
        if c != "Unknown"
    )
    # -------------------------------------------------------------------------

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
            dcc.Store(id="initial-load-trigger", data=True),
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
                Use the Filter button to refine the dashboard by year founded range, category,
                clinical stage, or country.
                Click any chart to see a detailed company breakdown.
                Data sourced from [AgingBiotech.info/companies](https://agingbiotech.info/companies/).
                """,
            ),
            dbc.Row(
                dbc.Col(
                    dbc.Button(
                        "Filter Charts",
                        id="filter-open-btn",
                        color="primary",
                        size="md",
                        className="filter-launch-btn",
                    ),
                    className="d-flex justify-content-center",
                ),
                className="filter-launch-row",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    graph_loader("founded-over-time")
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
                                    graph_loader("category-bar")
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
                                    graph_loader("clinical-stage-bar")
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
                                    graph_loader("geo-map")
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
                id="filter-modal",
                is_open=False,
                size="lg",
                children=[
                    dbc.ModalHeader(dbc.ModalTitle("Filter Charts")),
                    dbc.ModalBody(
                        [
                            dbc.Label("Year Founded"),
                            dcc.RangeSlider(
                                id="filter-year-range",
                                min=FILTER_YEAR_MIN,
                                max=filter_year_max,
                                step=1,
                                value=[FILTER_YEAR_MIN, filter_year_max],
                                marks={
                                    y: str(y)
                                    for y in range(FILTER_YEAR_MIN, filter_year_max + 1, 5)
                                },
                                tooltip={"placement": "bottom", "always_visible": True},
                                className="mb-4",
                            ),
                            dbc.Label("Category", className="mt-2"),
                            dcc.Dropdown(
                                id="filter-category",
                                options=[{"label": c, "value": c} for c in all_categories],
                                multi=True,
                                placeholder="All categories",
                                className="mb-3",
                            ),
                            dbc.Label("Clinical Stage", className="mt-2"),
                            dcc.Dropdown(
                                id="filter-stage",
                                options=[{"label": s, "value": s} for s in all_stages],
                                multi=True,
                                placeholder="All stages",
                                className="mb-3",
                            ),
                            dbc.Label("Country", className="mt-2"),
                            dcc.Dropdown(
                                id="filter-country",
                                options=[{"label": c, "value": c} for c in all_countries],
                                multi=True,
                                placeholder="All countries",
                                className="mb-3",
                            ),
                        ],
                        className="filter-modal-body",
                    ),
                    dbc.ModalFooter(
                        [
                            dbc.Button(
                                "Reset",
                                id="filter-reset-btn",
                                color="secondary",
                                outline=True,
                                className="me-auto filter-reset-btn",
                            ),
                            dbc.Button("Apply", id="filter-apply-btn", color="primary"),
                        ]
                    ),
                ],
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

    # --- Chart click callback for detail table modal ------------------------------------------------
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
        State("filter-year-range", "value"),
        State("filter-category", "value"),
        State("filter-stage", "value"),
        State("filter-country", "value"),
        prevent_initial_call=True,
    )
    def handle_chart_click(
        stage_click,
        time_click,
        cat_click,
        geo_click,
        year_range,
        sel_cats,
        sel_stages,
        sel_countries,
    ):
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

        safe_year = year_range if year_range else [FILTER_YEAR_MIN, filter_year_max]
        filtered_df = _apply_df_filters(df, safe_year, sel_cats, sel_stages, sel_countries)

        point = click_data["points"][0]

        if triggered == "clinical-stage-bar":
            key = str(point.get("x", "")).strip()
            stage_series = filtered_df["latest clinical stage"].astype("string").str.strip()
            rows_df = filtered_df[stage_series == key]
        elif triggered == "founded-over-time":
            if point.get("curveNumber") != 1:
                return no_changes
            try:
                year_int = int(float(point.get("x", 0)))
            except (TypeError, ValueError):
                return no_changes
            key = str(year_int)
            founded_years = pd.to_datetime(filtered_df["year founded"], errors="coerce").dt.year
            rows_df = filtered_df[founded_years == year_int]
        elif triggered == "category-bar":
            key = str(point.get("theta", "")).strip()
            rows_df = filtered_df[filtered_df["categories"].apply(lambda cats: key in _as_items(cats))]
        elif triggered == "geo-map":
            key = str(point.get("location", "")).strip()
            def _has_country_key(geo_list: object) -> bool:
                return any(
                    (entry.split(" - ")[0] if " - " in entry else entry) == key
                    for entry in _as_items(geo_list)
                    if entry != "Unknown"
                )

            rows_df = filtered_df[filtered_df["geo_country"].apply(_has_country_key)]
        else:
            return no_changes

        rows = _build_detail_rows(rows_df)

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

    # --- Filter modal callback ------------------------------------------------
    @app.callback(
        Output("filter-modal", "is_open"),
        Input("filter-open-btn", "n_clicks"),
        Input("filter-apply-btn", "n_clicks"),
        Input("filter-reset-btn", "n_clicks"),
        State("filter-modal", "is_open"),
        prevent_initial_call=True,
    )
    def toggle_filter_modal(open_clicks, apply_clicks, reset_clicks, is_open):
        triggered = ctx.triggered_id

        if triggered == "filter-open-btn":
            return True

        if triggered in ("filter-apply-btn", "filter-reset-btn"):
            return False

        return is_open
    
    # --- Figures callback ------------------------------------------------
    @app.callback(
        Output("founded-over-time", "figure"),
        Output("category-bar", "figure"),
        Output("clinical-stage-bar", "figure"),
        Output("geo-map", "figure"),
        Output("filter-year-range", "value"),
        Output("filter-category", "value"),
        Output("filter-stage", "value"),
        Output("filter-country", "value"),
        Input("initial-load-trigger", "data"),
        Input("filter-apply-btn", "n_clicks"),
        Input("filter-reset-btn", "n_clicks"),
        State("filter-year-range", "value"),
        State("filter-category", "value"),
        State("filter-stage", "value"),
        State("filter-country", "value"),
        prevent_initial_call=False,
    )
    def update_figures(
        initial_load, apply_clicks, reset_clicks,
        year_range, sel_cats, sel_stages, sel_countries,
    ):
        triggered = ctx.triggered_id
        _no = no_update

        if triggered in (None, "initial-load-trigger"):
            return (
                companies_founded_over_time_figure(df),
                category_polar_bar_figure(df, top_n=10),
                clinical_stage_bar_figure(df),
                geo_map_figure(df),
                [FILTER_YEAR_MIN, filter_year_max],
                [],
                [],
                [],
            )

        safe_year = year_range if year_range else [FILTER_YEAR_MIN, filter_year_max]

        if triggered == "filter-apply-btn":
            filtered_df = _apply_df_filters(df, safe_year, sel_cats, sel_stages, sel_countries)

            cat_df = filtered_df.copy()
            if sel_cats:
                def _constrain_cats(cats: object) -> list[str] | None:
                    items = _as_items(cats)
                    selected = [c for c in items if c in sel_cats]
                    return selected if selected else None
                cat_df["categories"] = cat_df["categories"].apply(_constrain_cats)

            geo_df = filtered_df.copy()
            if sel_countries:
                def _constrain_countries(geo_list: object) -> list[str]:
                    items = _as_items(geo_list)
                    selected = []
                    for c in items:
                        if c != "Unknown":
                            base = c.split(" - ")[0] if " - " in c else c
                            if base in sel_countries:
                                selected.append(c)
                    return selected

                geo_df["geo_country"] = geo_df["geo_country"].apply(_constrain_countries)
                geo_df = geo_df[
                    geo_df["geo_country"].apply(lambda x: isinstance(x, list) and len(x) > 0)
                ]

            return (
                companies_founded_over_time_figure(filtered_df, min_year=safe_year[0], max_year=safe_year[1]),
                category_polar_bar_figure(cat_df, top_n=10),
                clinical_stage_bar_figure(filtered_df),
                geo_map_figure(geo_df),
                _no,
                _no,
                _no,
                _no,
            )

        if triggered == "filter-reset-btn":
            return (
                companies_founded_over_time_figure(df),
                category_polar_bar_figure(df, top_n=10),
                clinical_stage_bar_figure(df),
                geo_map_figure(df),
                [FILTER_YEAR_MIN, filter_year_max],
                [],
                [],
                [],
            )

        return (_no, _no, _no, _no, _no, _no, _no, _no)
    
    return app


def main() -> None:
    """Entrypoint for running the dev server."""
    app = create_app()
    port = int(os.environ.get("PORT", "8050"))
    app.run(host="0.0.0.0", port=port, debug=True)


if __name__ == "__main__":
    main()
