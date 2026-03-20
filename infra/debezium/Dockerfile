FROM debezium/connect:3.0.0.Final

USER root

RUN microdnf install -y unzip && \
    curl -fsSL https://d1i4t75tc5dwf.cloudfront.net/confluent-8.0.0.zip -o /tmp/confluent.zip && \
    unzip -q /tmp/confluent.zip -d /tmp/confluent && \
    cp -r /tmp/confluent/confluent-8.0.0/share/confluent-hub-client/lib/* /kafka/connect/ && \
    cp -r /tmp/confluent/confluent-8.0.0/share/confluent-hub-client/components/confluentinc-kafka-connect-avro-converter/lib/* /kafka/connect/ && \
    rm -rf /tmp/confluent /tmp/confluent.zip && \
    microdnf clean all

COPY conf /kafka/connectors
COPY start-with-connectors.sh /usr/local/bin/start-with-connectors.sh

RUN chmod +x /usr/local/bin/start-with-connectors.sh && \
    chown -R kafka:kafka /kafka/connectors /usr/local/bin/start-with-connectors.sh

USER kafka

ENTRYPOINT ["/usr/local/bin/start-with-connectors.sh"]
