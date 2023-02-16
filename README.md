kafka-to-timescaledb-ew
================

Kafka to TimescaleDB export-worker.

## Docker compose template

```yaml
version: "2"

services:
  kafka-to-influxdb-ew-0:
    image: ghcr.io/senergy-platform/kafka-to-timescaledb-ew:prod
    environment:
      CONF_LOGGER_LEVEL:
      CONF_GET_DATA_TIMEOUT:
      CONF_GET_DATA_LIMIT:
      CONF_PAGE_SIZE:
      CONF_KAFKA_METADATA_BROKER_LIST:
      CONF_KAFKA_ID_POSTFIX:
      CONF_KAFKA_DATA_CLIENT_SUBSCRIBE_INTERVAL:
      CONF_KAFKA_DATA_CLIENT_KAFKA_MSG_ERR_IGNORE:
      CONF_KAFKA_DATA_CONSUMER_GROUP_ID:
      CONF_KAFKA_DATA_CONSUMER_AUTO_OFFSET_RESET:
      CONF_KAFKA_DATA_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY:
      CONF_KAFKA_FILTER_CLIENT_FILTER_TOPIC:
      CONF_KAFKA_FILTER_CLIENT_POLL_TIMEOUT:
      CONF_KAFKA_FILTER_CLIENT_SYNC_DELAY:
      CONF_KAFKA_FILTER_CLIENT_TIME_FORMAT:
      CONF_KAFKA_FILTER_CLIENT_UTC:
      CONF_KAFKA_FILTER_CONSUMER_GROUP_ID: 'kafka-to-timescaledb-ew-0'
      CONF_KAFKA_METRICS_PRODUCER_CLIENT_ID: 'kafka-to-timescaledb-ew-0'
      CONF_WATCHDOG_MONITOR_DELAY:
      CONF_WATCHDOG_START_DELAY:
      CONF_TIMESCALEDB_HOST:
      CONF_TIMESCALEDB_PORT:
      CONF_TIMESCALEDB_USERNAME:
      CONF_TIMESCALEDB_PASSWORD:
      CONF_TIMESCALEDB_DATABASE:
      CONF_TIMESCALEDB_DISTRIBUTED_HYPERTABLES:
      CONF_TIMESCALEDB_HYPERTABLE_REPLICATION_FACTOR:
      CONF_TABLE_MANAGER_TIMEOUT:
      CONF_TABLE_MANAGER_RETRIES:
      CONF_TABLE_MANAGER_RETRY_DELAY:
      CONF_TABLE_MANAGER_METRICS:

  kafka-to-influxdb-ew-1:
    image: ghcr.io/senergy-platform/kafka-to-timescaledb-ew:prod
    environment:
      CONF_LOGGER_LEVEL:
      CONF_GET_DATA_TIMEOUT:
      CONF_GET_DATA_LIMIT:
      CONF_PAGE_SIZE:
      CONF_KAFKA_METADATA_BROKER_LIST:
      CONF_KAFKA_ID_POSTFIX:
      CONF_KAFKA_DATA_CLIENT_SUBSCRIBE_INTERVAL:
      CONF_KAFKA_DATA_CLIENT_KAFKA_MSG_ERR_IGNORE:
      CONF_KAFKA_DATA_CONSUMER_GROUP_ID:
      CONF_KAFKA_DATA_CONSUMER_AUTO_OFFSET_RESET:
      CONF_KAFKA_DATA_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY:
      CONF_KAFKA_FILTER_CLIENT_FILTER_TOPIC:
      CONF_KAFKA_FILTER_CLIENT_POLL_TIMEOUT:
      CONF_KAFKA_FILTER_CLIENT_SYNC_DELAY:
      CONF_KAFKA_FILTER_CLIENT_TIME_FORMAT:
      CONF_KAFKA_FILTER_CLIENT_UTC:
      CONF_KAFKA_FILTER_CONSUMER_GROUP_ID: 'kafka-to-timescaledb-ew-1'
      CONF_KAFKA_METRICS_PRODUCER_CLIENT_ID: 'kafka-to-timescaledb-ew-1'
      CONF_WATCHDOG_MONITOR_DELAY:
      CONF_WATCHDOG_START_DELAY:
      CONF_TIMESCALEDB_HOST:
      CONF_TIMESCALEDB_PORT:
      CONF_TIMESCALEDB_USERNAME:
      CONF_TIMESCALEDB_PASSWORD:
      CONF_TIMESCALEDB_DATABASE:
      CONF_TIMESCALEDB_DISTRIBUTED_HYPERTABLES:
      CONF_TIMESCALEDB_HYPERTABLE_REPLICATION_FACTOR:
      CONF_TABLE_MANAGER_TIMEOUT:
      CONF_TABLE_MANAGER_RETRIES:
      CONF_TABLE_MANAGER_RETRY_DELAY:
      CONF_TABLE_MANAGER_METRICS:
```

## Kubernetes deployment template

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kafka-to-influxdb-ew
spec:
  selector:
    matchLabels:
      app: export-worker
  replicas: 2
  template:
    metadata:
      labels:
        app: export-worker
    spec:
      containers:
        - name: export-worker
          image: ghcr.io/senergy-platform/kafka-to-timescaledb-ew:prod
          imagePullPolicy: Always
          env:
            - name: CONF_LOGGER_LEVEL
              value: 
            - name: CONF_GET_DATA_TIMEOUT
              value: 
            - name: CONF_GET_DATA_LIMIT
              value: 
            - name: CONF_PAGE_SIZE
              value: 
            - name: CONF_KAFKA_METADATA_BROKER_LIST
              value: 
            - name: CONF_KAFKA_ID_POSTFIX
              value: 
            - name: CONF_KAFKA_DATA_CLIENT_SUBSCRIBE_INTERVAL
              value: 
            - name: CONF_KAFKA_DATA_CLIENT_KAFKA_MSG_ERR_IGNORE
              value: 
            - name: CONF_KAFKA_DATA_CONSUMER_GROUP_ID
              value: 
            - name: CONF_KAFKA_DATA_CONSUMER_AUTO_OFFSET_RESET
              value: 
            - name: CONF_KAFKA_DATA_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY
              value: 
            - name: CONF_KAFKA_FILTER_CLIENT_FILTER_TOPIC
              value: 
            - name: CONF_KAFKA_FILTER_CLIENT_POLL_TIMEOUT
              value: 
            - name: CONF_KAFKA_FILTER_CLIENT_SYNC_DELAY
              value: 
            - name: CONF_KAFKA_FILTER_CLIENT_TIME_FORMAT
              value: 
            - name: CONF_KAFKA_FILTER_CLIENT_UTC
              value: 
            - name: CONF_KAFKA_FILTER_CONSUMER_GROUP_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: CONF_KAFKA_METRICS_PRODUCER_CLIENT_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: CONF_WATCHDOG_MONITOR_DELAY
              value: 
            - name: CONF_WATCHDOG_START_DELAY
              value: 
            - name: CONF_TIMESCALEDB_HOST
              value: 
            - name: CONF_TIMESCALEDB_PORT
              value: 
            - name: CONF_TIMESCALEDB_USERNAME
              value: 
            - name: CONF_TIMESCALEDB_PASSWORD
              value: 
            - name: CONF_TIMESCALEDB_DATABASE
              value: 
            - name: CONF_TIMESCALEDB_DISTRIBUTED_HYPERTABLES
              value: 
            - name: CONF_TIMESCALEDB_HYPERTABLE_REPLICATION_FACTOR
              value: 
            - name: CONF_TABLE_MANAGER_TIMEOUT
              value: 
            - name: CONF_TABLE_MANAGER_RETRIES
              value: 
            - name: CONF_TABLE_MANAGER_RETRY_DELAY
              value: 
            - name: CONF_TABLE_MANAGER_METRICS
              value: 
      restartPolicy: Always
```