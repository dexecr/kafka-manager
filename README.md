# Kafka Manager

Simple api to manage Kafka.

Use the next command to build docker with native image

```
./gradlew dockerBuildNative
```

## API

API docs: [kafka-manager-docs.dexecr.com](http://kafka-manager-docs.dexecr.com])


## Docker

Use `docker run -p 8080:8080 dexecr/kafka-manager-api --bootstrap.servers=<kafka_host:kafka_port>` to run kafka manager
for specific kafka host