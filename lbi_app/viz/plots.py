from __future__ import annotations

import pandas as pd
import math
import plotly.graph_objects as go
import plotly.io as pio
import plotly.colors as pc


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
        hoverlabel=dict(
            align="left",
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
    """
    Count companies per category from a list-valued 'categories' column.
    Also includes a list of top companies (by score) for each category.
    """
    # ensure required columns exist
    if "Company" not in df.columns:
        raise KeyError("Column not found: Company")
    if "full overall score" not in df.columns:
        raise KeyError("Column not found: full overall score")
    
    # explode categories and keep company name and score
    expanded = (
        df[df["categories"].notna()][["categories", "Company", "full overall score"]]
        .explode("categories")
    )
    
    # sort by category then descending score
    expanded = expanded.sort_values(["categories", "full overall score"], ascending=[True, False])
    
    # helper to build top-10 company list
    def _top_companies(names: pd.Series) -> str:
        lst = [str(n) for n in names.dropna().tolist()]
        if len(lst) > 10:
            return "- " + "<br>- ".join(lst[:10]) + "<br>..."
        return "- " + "<br>- ".join(lst)
    
    counts = (
        expanded.groupby("categories", as_index=False)
        .agg(
            n_companies=("Company", "count"),
            company_list=("Company", _top_companies),
        )
        .sort_values("n_companies", ascending=False)
    )
    return counts


def category_polar_bar_figure(df: pd.DataFrame, top_n: int = 10) -> go.Figure:
    counts = category_counts(df).head(top_n)

    fig = go.Figure()

    categories = counts["categories"].astype(str).tolist()
    values = counts["n_companies"].astype(float).tolist()
    company_lists = counts["company_list"].tolist()

    max_r = max(values) if values else 0.0
    blue_scale = [
        [0.00, "#0a1f44"],
        [0.20, "#123b73"],
        [0.45, "#1f6db2"],
        [0.70, "#35a3dc"],
        [1.00, "#8fe9ff"],
    ]

    if max_r > 0:
        # Stack thin radial slices to emulate a smooth vertical gradient inside each wedge.
        n_steps = min(64, max(24, int(max_r * 2)))
        step = max_r / n_steps

        for i in range(n_steps):
            lower = i * step
            upper = (i + 1) * step

            theta_slice: list[str] = []
            r_slice: list[float] = []

            for theta, value in zip(categories, values):
                height = min(value, upper) - lower
                if height <= 0:
                    continue
                theta_slice.append(theta)
                r_slice.append(height)

            if not theta_slice:
                continue

            color_ratio = upper / max_r
            slice_color = pc.sample_colorscale(blue_scale, [color_ratio])[0]

            fig.add_trace(
                go.Barpolar(
                    theta=theta_slice,
                    r=r_slice,
                    base=[lower] * len(theta_slice),
                    marker=dict(
                        color=[slice_color] * len(theta_slice),
                        line=dict(color="rgba(0,0,0,0)", width=0),
                    ),
                    hoverinfo="skip",
                    showlegend=False,
                )
            )

    # Add an invisible bar trace on top to capture hover events and show company lists.
    fig.add_trace(
        go.Barpolar(
            theta=categories,
            r=values,
            customdata=company_lists,
            marker=dict(
                color="rgba(0,0,0,0)",
                line=dict(color=CYBORG["bg"], width=1.2),
            ),
            hovertemplate=(
                "<b>%{theta}</b><br>%{r} Companies<br><br>"
                "%{customdata}<extra></extra>"
            ),
        )
    )

    fig.update_layout(
        title=f"Companies per Category (Top {top_n})",
        polar=dict(
            bgcolor="rgba(0,0,0,0)",
            angularaxis=dict(
                direction="clockwise",
                rotation=90 - 360 / (2 * top_n),
                gridcolor=CYBORG["grid"],
                linecolor=CYBORG["secondary"],
            ),
            radialaxis=dict(
                gridcolor=CYBORG["grid"],
                linecolor=CYBORG["secondary"],
                tickfont=dict(color=CYBORG["fg"]),
                angle=90,
                tickangle=90,
                side="counterclockwise",
            ),
        ),
        showlegend=False,
    )

    return fig


# ----------------------------
# Companies founded over time
# ----------------------------

def companies_founded_over_time_figure(
    df: pd.DataFrame,
    *,
    year_col: str = "year founded",
    min_year: int = 2000,
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
        max_year = pd.Timestamp.today().year - 1  # default to last year to avoid partial data for current year

    # ensure columns we need exist
    if "Company" not in df.columns:
        raise KeyError("Column not found: Company")
    if "full overall score" not in df.columns:
        raise KeyError("Column not found: full overall score")

    # grab year, company, and score so that we can sort by score later
    years = (
        df[[year_col, "Company", "full overall score"]]
        .rename(columns={year_col: "year"})
        .assign(year=lambda x: x["year"].dt.year)
        .dropna(subset=["year"])
        .assign(year=lambda x: x["year"].astype(int))
        .query("year >= @min_year and year <= @max_year")
    )

    # sort by year then descending score so the top names appear first
    years = years.sort_values(["year", "full overall score"], ascending=[True, False])

    # helper for producing truncated list of companies
    def _company_list(names: pd.Series) -> str:
        lst = [str(n) for n in names.dropna().tolist()]
        if len(lst) > 5:
            return "- " + "<br>- ".join(lst[:5]) + "<br>..."
        return "- " + "<br>- ".join(lst)

    # build counts and also collect names for hover
    counts = (
        years.groupby("year", as_index=False)
        .agg(
            new_companies=("Company", "count"),
            company_list=("Company", _company_list),
        )
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
            hovertemplate="Year: %{x}<br>Tot Companies: %{y}<extra></extra>",
        )
    )

    fig.add_trace(
        go.Bar(
            x=counts["year"],
            y=counts["new_companies"],
            name="New",
            customdata=counts["company_list"],
            hovertemplate=(
                "Year: %{x}<br>New Companies: %{y}<br><br>"
                "%{customdata}<extra></extra>"
            ),
        )
    )

    fig.update_layout(
        title="Companies Founded over Time",
        xaxis_title="Year",
        yaxis_title="Company Number",
    )

    return fig


def clinical_stage_bar_figure(df: pd.DataFrame) -> go.Figure:
    if "Company" not in df.columns:
        raise KeyError("Column not found: Company")
    if "latest clinical stage" not in df.columns:
        raise KeyError("Column not found: latest clinical stage")

    def _top_companies(names: pd.Series) -> str:
        lst = [str(n) for n in names.dropna().tolist()]
        if len(lst) > 5:
            return "- " + "<br>- ".join(lst[:5]) + "<br>..."
        return "- " + "<br>- ".join(lst)

    stage_df = df.dropna(subset=["latest clinical stage", "Company"]).copy()
    if "full overall score" in stage_df.columns:
        stage_df = stage_df.sort_values(
            ["latest clinical stage", "full overall score"], ascending=[True, False]
        )

    counts = (
        stage_df.groupby("latest clinical stage", as_index=False, observed=True)
        .agg(
            n_companies=("Company", "count"),
            company_list=("Company", _top_companies),
        )
    )

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=counts["latest clinical stage"],
            y=counts["n_companies"],
            customdata=counts["company_list"],
            hovertemplate=(
                "<b>%{x}</b><br>%{y} Companies<br><br>"
                "%{customdata}<extra></extra>"
            ),
        )
    )

    fig.update_layout(
        title="Companies per Clinical Stage",
        xaxis=dict(
            title=None,
            tickangle=45,
            automargin=True,
        ),
        yaxis=dict(
            title="Company Number",
        ),
    )

    return fig


# ----------------------------
# Companies by location (choropleth map)
# ----------------------------

def geo_country_counts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate geo_country into country-level counts for the choropleth map.
    """
    if "Company" not in df.columns:
        raise KeyError("Column not found: Company")
    if "geo_country" not in df.columns:
        raise KeyError("Column not found: geo_country")
    if "full overall score" not in df.columns:
        raise KeyError("Column not found: full overall score")

    sorted_df = df.copy().sort_values("full overall score", ascending=False)

    sorted_df = sorted_df.explode("geo_country").dropna(subset=["geo_country"])

    sorted_df = sorted_df.drop_duplicates(subset=["Company", "geo_country"], keep="first")

    def _top_companies(names: pd.Series) -> str:
        lst = [str(n) for n in names.dropna().tolist()]
        if len(lst) > 10:
            return "- " + "<br>- ".join(lst[:10]) + "<br>..."
        return "- " + "<br>- ".join(lst)

    agg = (
        sorted_df[sorted_df["geo_country"] != "Unknown"]
        .groupby("geo_country", as_index=False, sort=False)
        .agg(
            n_companies=("Company", "count"),
            company_list=("Company", _top_companies),
        )
        .sort_values("n_companies", ascending=False)
    )

    def _hover(row: pd.Series) -> str:
        country_label = row["geo_country"]
        company_label = "company" if row["n_companies"] == 1 else "companies"
        lines = [
            f"<b>{country_label}</b>",
            f"{row['n_companies']} {company_label}<br><br>{row['company_list']}",
        ]
        return "<br>".join(lines)

    agg["hover_text"] = agg.apply(_hover, axis=1)
    
    return agg


def geo_map_figure(df: pd.DataFrame) -> go.Figure:
    counts = geo_country_counts(df).copy()

    # Log scaling improves contrast for long-tail country counts (e.g., 176 vs 3).
    counts["z_color"] = counts["n_companies"].astype(float).map(math.log1p)

    preferred_cols = [c for c in ["Company", "geo_country", "n_companies", "z_color"] if c in counts.columns]
    remaining_cols = [c for c in counts.columns if c not in preferred_cols]
    counts = counts[preferred_cols + remaining_cols]

    max_count = int(counts["n_companies"].max()) if not counts.empty else 1
    zmax = math.log1p(max_count) if max_count > 0 else 1.0

    base_ticks = [1, 3, 10, 30, 100, 300, 1000, 3000, 10000]
    tick_counts = [v for v in base_ticks if v <= max_count]
    # if max_count not in tick_counts:
    #     tick_counts.append(max_count)
    if not tick_counts:
        tick_counts = [1]

    tickvals = [math.log1p(v) for v in tick_counts]
    ticktext = [str(v) for v in tick_counts]

    geo_colorscale = [
        [0.00, "#081320"],
        [0.08, "#0d2540"],
        [0.18, "#143c63"],
        [0.32, "#1a5a86"],
        [0.50, "#237aa8"],
        [0.68, "#2f9ac2"],
        [0.84, "#4dbcdc"],
        [1.00, "#82e4f5"],
    ]

    fig = go.Figure(
        go.Choropleth(
            locations=counts["geo_country"],
            locationmode="country names",
            z=counts["z_color"],
            zmin=0,
            zmax=zmax,
            text=counts["hover_text"],
            hovertemplate="%{text}<extra></extra>",
            colorscale=geo_colorscale,
            colorbar=dict(
                tickvals=tickvals,
                ticktext=ticktext,
                tickfont=dict(color=CYBORG["border"]),
                bgcolor="rgba(0,0,0,0)",
                outlinewidth=1.5,
                len=0.5,
                thickness=10,
                y=0.5,
                x=0.99
            ),
            marker_line_color=CYBORG["border"],
            marker_line_width=0.3,
        )
    )

    fig.update_layout(
        title=dict(text="Companies by Country", pad=dict(t=0, b=-50)),
        margin=dict(l=0, r=0, b=0), # default t=60 margin needed to prevent title cutoff
        geo=dict(
            showframe=False,
            showcoastlines=True,
            coastlinecolor=CYBORG["secondary"],
            showland=True,
            landcolor="#111111",
            showocean=True,
            oceancolor="#060606",
            showlakes=False,
            projection_type="natural earth",
            bgcolor="rgba(0,0,0,0)",
        ),
    )

    return fig