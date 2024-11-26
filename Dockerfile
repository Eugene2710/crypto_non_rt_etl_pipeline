# First stage: Builder
FROM python:3.11-slim AS builder

# Set environment variables for Poetry
ENV POETRY_VERSION=1.6.1
ENV POETRY_HOME=/opt/poetry
ENV POETRY_NO_INTERACTION=1
ENV PATH="$POETRY_HOME/bin:$PATH"

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        curl && \
    rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# COPY pyproject.toml and poetry.lock, used to poetry export -> not necessary
COPY pyproject.toml poetry.lock ./

# Install dependencies
RUN poetry install --no-root

# Export dependencies to requirements.txt
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes

# Second Stage: Final Image
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Install runtime dependencies, including postgresql-client
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        libpq-dev \
        postgresql-client && \
    rm -rf /var/lib/apt/lists/*

# Copy the requirements.txt from the builder stage
COPY --from=builder requirements.txt .

# Install dependencies using pip
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code - Not necessary
COPY src/ ./src/
COPY alembic.ini ./
COPY database_management/ ./database_management/
COPY client/ ./client/
COPY .env/ ./.env

# Default command (can be overridden)
CMD ["streamlit", "run", "client/streamlit_app.py"]