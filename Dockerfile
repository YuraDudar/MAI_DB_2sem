FROM postgres:latest

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    build-essential \
    postgresql-contrib \
    postgresql-server-dev-$PG_MAJOR \
    && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/pgbigm/pg_bigm.git /tmp/pg_bigm \
    && cd /tmp/pg_bigm \
    # Используем USE_PGXS=1 для сборки как расширения
    && make USE_PGXS=1 install \
    # Очищаем временные файлы
    && cd / \
    && rm -rf /tmp/pg_bigm