FROM python:3.12-slim

# Prevent Python from creating .pyc files and force immediate log output.
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the application directory inside the container.
WORKDIR /app

# Install runtime dependencies needed by psycopg and health checks.
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        curl \
        libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy the dependency list first so Docker can cache installation.
COPY requirements.txt .

# Install the dashboard's Python dependencies.
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy the dashboard source code into the image.
COPY . .

# Document the port used by Streamlit.
EXPOSE 8501

# Confirm that the Streamlit server is responding.
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Start the Streamlit dashboard.
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true", "--browser.gatherUsageStats=false"]