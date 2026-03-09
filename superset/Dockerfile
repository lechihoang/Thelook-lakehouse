FROM apache/superset:3.1.3

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
      gcc pkg-config default-libmysqlclient-dev \
    && pip install --no-cache-dir mysqlclient "trino[sqlalchemy]" \
    && apt-get purge -y gcc && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

COPY superset-init.sh /superset-init.sh
RUN chmod +x /superset-init.sh

USER superset
ENTRYPOINT ["/superset-init.sh"]
