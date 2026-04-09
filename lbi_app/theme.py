from __future__ import annotations

# Shared color tokens for the app and charts.
CYBORG = {
    "bg": "#060606",
    "surface": "#1a1a1a",
    "fg": "#adafae",
    "fg_weak": "#c7cdd3",
    "muted": "#6c757d",
    "grid": "rgba(255,255,255,0.1)",
    "border": "#dee2e6",
    "table_divider": "#444444",
    "primary": "#2a9fd6",
    "secondary": "#555555",
    "success": "#77b300",
    "info": "#9933cc",
    "warning": "#ff8800",
    "danger": "#cc0000",
    "accent_light": "#8fe9ff",
    "primary_hover": "#35a8dd",
    "neutral_hover": "#9aa4ad",
    "table_header_hover_bg": "#252525",
    "white": "#ffffff",
    "black": "#000000",
}

PLOTLY_COLORWAY = [
    CYBORG["primary"],
    CYBORG["info"],
    CYBORG["success"],
    CYBORG["warning"],
    CYBORG["danger"],
    CYBORG["secondary"],
]

POLAR_BLUE_SCALE = [
    [0.00, "#0a1f44"],
    [0.20, "#123b73"],
    [0.45, "#1f6db2"],
    [0.70, "#35a3dc"],
    [1.00, "#8fe9ff"],
]

GEO_COLOR_SCALE = [
    [0.00, "#081320"],
    [0.08, "#0d2540"],
    [0.18, "#143c63"],
    [0.32, "#1a5a86"],
    [0.50, "#237aa8"],
    [0.68, "#2f9ac2"],
    [0.84, "#4dbcdc"],
    [1.00, "#82e4f5"],
]

MAP_LAND_COLOR = "#111111"

_CSS_VAR_KEY_MAP = {
    "bg": "--lbi-bg",
    "surface": "--lbi-surface",
    "fg": "--lbi-fg",
    "fg_weak": "--lbi-fg-weak",
    "muted": "--lbi-muted",
    "secondary": "--lbi-secondary",
    "primary": "--lbi-primary",
    "accent_light": "--lbi-accent-light",
    "table_divider": "--lbi-table-divider",
    "primary_hover": "--lbi-primary-hover",
    "neutral_hover": "--lbi-neutral-hover",
    "table_header_hover_bg": "--lbi-table-header-hover-bg",
    "white": "--lbi-white",
    "black": "--lbi-black",
}


def build_css_root_block() -> str:
    """Generate a :root block so CSS variables are sourced from Python theme tokens."""
    lines = [":root {"]
    for token, css_var in _CSS_VAR_KEY_MAP.items():
        lines.append(f"  {css_var}: {CYBORG[token]};")
    lines.append("}")
    return "\n".join(lines)
