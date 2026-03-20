FROM python:3.11-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./

HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=3 \
    CMD python -c "\
import psycopg2, os; \
pg_dsn = os.getenv('PG_DSN', 'host=postgres port=5432 dbname=thelook user=admin password=admin123'); \
conn = psycopg2.connect(pg_dsn); conn.close(); \
print('Dependencies healthy')" || exit 1

CMD ["python", "-u", "-m", "src.generator"]
