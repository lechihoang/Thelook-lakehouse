FROM apache/superset:latest

USER root

RUN pip install --no-cache-dir mysqlclient trino[sqlalchemy]

ENV ADMIN_USERNAME=admin
ENV ADMIN_EMAIL=admin@tpcds.com
ENV ADMIN_PASSWORD=admin123

COPY superset-init.sh /superset-init.sh
RUN chmod +x /superset-init.sh

USER superset
ENTRYPOINT ["/superset-init.sh"]
