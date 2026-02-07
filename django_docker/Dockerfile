
FROM python:3.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

## Prevents Python from writing .pyc files to disk
ENV PYTHONDONTWRITEBYTECODE=1
## Ensures that Python output is sent straight to terminal (e.g., your container log) without being buffered first
ENV PYTHONUNBUFFERED=1 

# Create the app directory
RUN mkdir /app

COPY requirements.txt /app/ 

WORKDIR /app

RUN uv pip install --no-cache-dir -r requirements.txt --system

COPY . /app/

EXPOSE 8080

RUN chmod +x  /app/entrypoint.sh
# Start the application using Gunicorn
CMD ["/app/entrypoint.sh"]




