# WIP: image to be used for the Kafka producer
# Needs to be tested in the Azure AKS cluster
FROM amsterdam/python:3.8-buster

WORKDIR /app

RUN pip install amsterdam-schema-tools[kafka]

COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

ENTRYPOINT ["/app/docker-entrypoint.sh"]

USER datapunt
