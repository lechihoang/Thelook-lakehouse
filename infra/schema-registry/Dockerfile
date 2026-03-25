FROM confluentinc/cp-schema-registry:7.5.0

USER root
RUN dnf install -y --setopt=install_weak_deps=False curl jq && dnf clean -y all

COPY schemas/ /opt/schemas/
COPY init-schemas.sh /opt/init-schemas.sh
COPY entrypoint.sh /opt/entrypoint.sh

RUN chmod +x /opt/init-schemas.sh /opt/entrypoint.sh

USER appuser

ENTRYPOINT ["/opt/entrypoint.sh"]
