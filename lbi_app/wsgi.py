from __future__ import annotations

import os

from lbi_app.app import create_app

# Create the Dash app once at import time (gunicorn will import this module)
_dash_app = create_app()

# Gunicorn looks for a WSGI callable named `server`
# Dash exposes the underlying Flask server at `.server`
server = _dash_app.server


def main() -> None:
    """
    Optional: run locally
    """
    port = int(os.environ.get("PORT", "7860"))
    _dash_app.run(host="0.0.0.0", port=port, debug=False)


if __name__ == "__main__":
    main()