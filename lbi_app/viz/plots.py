from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Base layout and styling for all figures

BASE_LAYOUT = dict(
    template="plotly_dark",
    # margin=dict(l=40, r=20, t=60, b=120),
    # height=600,
)

def apply_base_layout(fig):
    fig.update_layout(**BASE_LAYOUT)
    return fig


# Category counts and bar chart

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


def category_bar_figure(df: pd.DataFrame, top_n: int = 20) -> go.Figure:
    counts = category_counts(df).head(top_n)

    fig = px.bar(
        counts,
        x="categories",
        y="n_companies",
        title=f"Companies per Category (Top {top_n})",
    )

    return apply_base_layout(fig)

# Companies founded over time

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
        .assign(year=lambda x: pd.to_numeric(x["year"], errors="coerce"))
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

    # Bars: new companies
    fig.add_trace(
        go.Bar(
            x=counts["year"],
            y=counts["new_companies"],
            name="Companies founded",
        )
    )

    # Line: cumulative (same y-axis)
    fig.add_trace(
        go.Scatter(
            x=counts["year"],
            y=counts["cumulative_companies"],
            name="Cumulative companies",
            mode="lines+markers",
        )
    )

    # Make sure the axis range fits the cumulative max
    y_max = counts["cumulative_companies"].max()

    fig.update_layout(
        title="Companies founded over time",
        xaxis_title="Year founded",
        yaxis=dict(
            title="Number of Companies",
            fixedrange=False, # allow zoom/pan
        ),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        margin=dict(l=60, r=40, t=70, b=60),
    )

    return apply_base_layout(fig)