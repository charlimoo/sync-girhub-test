# Dockerfile

# --- Stage 1: Build Stage ---
# Use a full Python image to build our dependencies. This stage includes
# build tools that we don't need in our final, lightweight image.
FROM python:3.11-slim as builder

# Set the working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Install system dependencies required for building Python packages.
# Specifically, unixodbc-dev and build-essential are needed for pyodbc.
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc unixodbc-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies into a temporary location
COPY requirements.txt .
RUN pip wheel --no-cache-dir --wheel-dir=/app/wheels -r requirements.txt


# --- Stage 2: Final Stage ---
# Use a lightweight Python image for the final production image.
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Install the Microsoft ODBC Driver for SQL Server. This is critical.
# This part is for Debian-based systems (like the python:3.11-slim image).
# If you use a different base image, you'll need to adjust this.
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl gnupg unixodbc && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    rm -rf /var/lib/apt/lists/*

# Copy the pre-built wheels from the builder stage
COPY --from=builder /app/wheels /wheels

# Install the Python dependencies from the wheels without needing build tools
RUN pip install --no-cache /wheels/*

# Copy the application source code into the final image
COPY . .

# Expose the port the app runs on
EXPOSE 5000

# Set the entrypoint for the container.
# We use Gunicorn as a production-grade WSGI server.
# First, add gunicorn to your requirements.txt
# Command to run the application
# We increase the timeout to handle potentially long-running requests or job triggers.
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "2", "--threads", "4", "--timeout", "120", "run:app"]