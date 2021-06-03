Kafka consumer for GOB event data
==================================

This consumer docker image needs some environment variables as defined in the docker-compose.yml


DATABASE_URL
Connection to the reference database (a postgres database).

SCHEMA_URL
Location where amsterdam schema definitions can be obtained.
For production this should be: https://schemas.data.amsterdam.nl/datasets/


BOOTSTRAP_SERVERS
Location of kafka-proxy ([acc|tst|dev].kafka.data.amsterdam.nl:<kafka-port>)

TOPICS
Topics that need to be consumed.

GROUP_ID
Kafka group id

AUTO_OFFSET_RESET
Kafka setting indicating where to start reading from the topics


Todo
====

- More fine-grained configuration for events, associated schemas and topic ids.
- Put docker command in a python or bash script.
