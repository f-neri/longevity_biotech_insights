from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio


# ----------------------------
# Theme + Plotly template
# ----------------------------

CYBORG = {
    "bg": "#060606",
    "fg": "#adafae",
    "grid": "rgba(255,255,255,0.1)",
    "border": "#dee2e6",
    "primary": "#2a9fd6",
    "secondary": "#555555",
    "success": "#77b300",
    "info": "#9933cc",
    "warning": "#ff8800",
    "danger": "#cc0000",
}

PLOTLY_COLORWAY = [
    CYBORG["primary"],
    CYBORG["info"],
    CYBORG["success"],
    CYBORG["warning"],
    CYBORG["danger"],
    CYBORG["secondary"],
]


def register_lbi_template(*, name: str = "lbi_cyborg") -> str:
    """
    Register (and set) the default Plotly template for the LBI app.
    """
    template = go.layout.Template()

    template.layout = dict(
        template="plotly_dark",
        colorway=PLOTLY_COLORWAY,
        paper_bgcolor="rgba(0,0,0,0)",  # let dbc.Card background show through
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=CYBORG["fg"]),
        margin=dict(l=60, r=60, t=60, b=60),
        legend=dict(
            y=0.5,
            yanchor="middle",
            xanchor="left",
        ),
        xaxis=dict(
            gridcolor=CYBORG["grid"],
            zerolinecolor=CYBORG["grid"],
            linecolor=CYBORG["secondary"],
        ),
        yaxis=dict(
            gridcolor=CYBORG["grid"],
            zerolinecolor=CYBORG["grid"],
            linecolor=CYBORG["secondary"],
        ),
    )

    pio.templates[name] = template
    pio.templates.default = name
    return name


# Register + set default template immediately when this module is imported
register_lbi_template()


# ----------------------------
# Category counts and bar chart
# ----------------------------

def category_counts(df: pd.DataFrame) -> pd.DataFrame:
    counts = (
        df.assign(category=df["categories"].fillna("NA"))
        .assign(category=lambda d: d["categories"].astype(str))
        .assign(category=lambda d: d["categories"].str.split(r"\s*,\s*"))
        .explode("categories")
        .assign(category=lambda d: d["categories"].str.strip())
        .query("category != ''")
        .groupby("categories", as_index=False)
        .size()
        .rename(columns={"size": "n_companies"})
        .sort_values("n_companies", ascending=False)
    )
    return counts


def category_bar_figure(df: pd.DataFrame, top_n: int = 10) -> go.Figure:
    counts = category_counts(df).head(top_n)

    fig = px.bar(
        counts,
        x="categories",
        y="n_companies",
        title=f"Companies per Category (Top {top_n})",
        labels={
            "categories": "Category",
            "n_companies": "Company Number",
        },
    )

    fig.update_layout(
        xaxis=dict(
            title=None,
            tickangle=+45,
        ),
    )
    return fig


# ----------------------------
# Companies founded over time
# ----------------------------

def companies_founded_over_time_figure(
    df: pd.DataFrame,
    *,
    year_col: str = "year founded",
    min_year: int = 1999,
    max_year: int | None = None,
) -> go.Figure:
    """
    Bar = number of companies founded in each year
    Line = cumulative companies founded up to that year
    Both plotted on the same y-axis.
    """
    if year_col not in df.columns:
        raise KeyError(f"Column not found: {year_col}")

    if max_year is None:
        max_year = pd.Timestamp.today().year

    years = (
        df[[year_col]]
        .rename(columns={year_col: "year"})
        .assign(year=lambda x: x["year"].dt.year)
        .dropna(subset=["year"])
        .assign(year=lambda x: x["year"].astype(int))
        .query("year >= @min_year and year <= @max_year")
    )

    counts = (
        years.groupby("year", as_index=False)
        .size()
        .rename(columns={"size": "new_companies"})
        .sort_values("year")
        .assign(cumulative_companies=lambda x: x["new_companies"].cumsum())
    )

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=counts["year"],
            y=counts["cumulative_companies"],
            name="Total",
            mode="lines+markers",
        )
    )

    fig.add_trace(
        go.Bar(
            x=counts["year"],
            y=counts["new_companies"],
            name="New",
        )
    )

    fig.update_layout(
        title="Companies Founded over Time",
        xaxis_title="Year",
        yaxis_title="Company Number",
    )

    return fig