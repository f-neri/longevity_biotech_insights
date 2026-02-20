FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Optional but commonly needed for building wheels; safe to keep
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
  && rm -rf /var/lib/apt/lists/*

# Install the package (and its deps) via pyproject.toml
# Copy metadata first for better caching
COPY pyproject.toml README.md LICENSE /app/

RUN pip install --no-cache-dir -U pip \
 && pip install --no-cache-dir .

# Now copy the rest of the repo (data/, lbi_app/, etc.)
COPY . /app

# Hugging Face Spaces provides PORT; 7860 is the common default
ENV PORT=7860
EXPOSE 7860

# Serve the Dash app via gunicorn using your WSGI server
CMD ["bash", "-lc", "gunicorn -w 2 -b 0.0.0.0:${PORT} lbi_app.wsgi:server"]