# Batch Processing with Kafka Streams

This project demonstrates how to perform batch processing of Kafka records using Spring Cloud Stream and Kafka Streams.

## Modules

The project is divided into three modules:

*   **`common-dtos`**: This module contains the common Data Transfer Objects (DTOs) used by the producer and consumer.
    *   `DataRecord`: Represents a data record sent by the producer.
    *   `MetadataRecord`: Represents a metadata record sent by the producer, which contains the total number of data records.
*   **`client`**: This is a Spring Boot application that acts as a Kafka producer. It exposes a REST endpoint `/produce/{producerId}` that generates and sends a batch of `DataRecord`s and a final `MetadataRecord` to a Kafka topic.
*   **`batch-processing-kafka-streams`**: This is a Spring Boot application that consumes the records from the Kafka topic, groups them by `producerId` using a `KTable`, and processes them in a batch once the `MetadataRecord` is received and the record count matches.

## Docker Compose

The `docker-compose.yaml` file sets up the following services:

*   **`kafka`**: A single-node Kafka broker running in KRaft mode.
*   **`akhq`**: A web-based GUI for managing and monitoring the Kafka cluster. It is available at `http://localhost:8082`.

## How to Run

1.  Start the Kafka broker:
    ```bash
    docker-compose up -d
    ```
2.  Start the `batch-processing-kafka-streams` application.
3.  Start the `client` application.
4.  Send a request to the `client` application to produce data:
    ```bash
    curl http://localhost:8080/produce/producer-1
    ```
5.  Check the logs of the `batch-processing-kafka-streams` application to see the batch processing in action.

## Design

The `client` application sends a number of `DataRecord` messages to the `messageTopic` topic. Each message has the `producerId` as the key. After sending all the data records, it sends a `MetadataRecord` with the `recordType` header set to `metadata`. This metadata record contains the total number of records sent by that producer.

The `batch-processing-kafka-streams` application uses a `KStream` to consume messages from the `messageTopic` topic. It then does the following:

1.  It filters the incoming stream into two separate streams: one for `DataRecord`s and one for `MetadataRecord`s.
2.  The `DataRecord` stream is grouped by key (which is the `producerId`) and aggregated into a `KTable`. The `KTable` stores a list of all `DataRecord`s received for each `producerId`.
3.  The `MetadataRecord` stream is then joined with the `KTable` of data records.
4.  When a `MetadataRecord` is received, the join is triggered. The application then checks if the number of records in the `KTable` matches the `totalRecords` in the `MetadataRecord`.
5.  If the counts match, it means all the records for that producer have been received, and the application can then process them as a batch.
6.  If the counts do not match, it means that not all records have been received yet, and the application waits for more records to arrive.