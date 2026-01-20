# Batch Processing with Kafka Streams

This project demonstrates a common batch processing pattern using Kafka Streams. It consists of a producer client that sends a series of data records followed by a metadata record to a Kafka topic. A Kafka Streams application then processes these records, grouping them by a producer ID and ensuring all data for a given batch is received before processing, with automatic state cleanup.

## Project Architecture

The project is a multi-module Gradle project with the following structure:

*   `common-dtos`: A Java library containing the shared data transfer objects (DTOs) used by the other modules.
*   `client`: A Spring Boot application that acts as a Kafka producer, sending data and metadata records to a Kafka topic via a REST endpoint.
*   `batch-processing-kafka-streams`: A Spring Boot application that contains the Kafka Streams topology for processing the records.

### Data Models

The `common-dtos` module defines the following data models:

*   **`BaseRecord`**: An interface that serves as a common parent for `DataRecord` and `MetadataRecord`. It uses Jackson annotations (`@JsonTypeInfo` and `@JsonSubTypes`) to handle polymorphism during JSON serialization and deserialization.
*   **`DataRecord`**: Represents a data record. It contains a `producerId` to identify the batch and a `payload` which is a string.
*   **`MetadataRecord`**: Represents the metadata for a batch. It contains the `producerId` and the `totalRecords` count for that batch.

## How it Works

The process is as follows:

1.  **Data Production**: The `client` application's `DataProducerController` exposes a REST endpoint at `/produce/{producerId}/{count}`. When this endpoint is called, the client sends a specified number of `DataRecord` messages to the `jsonMessageTopic` Kafka topic. The `producerId` is used as the key for the Kafka messages. After sending all data records, it sends a final `MetadataRecord` which includes the `producerId` and the total count of `DataRecord`s sent.

2.  **Data Consumption & Processing**: The `batch-processing-kafka-streams` application's `RecordProcessor` defines the Kafka Streams topology.
    *   It consumes the `jsonMessageTopic` as a `KStream<String, BaseRecord>`.
    *   The stream is split into two branches based on the record type (`DataRecord` or `MetadataRecord`).
    *   The `DataRecord` stream is grouped by key (`producerId`) and aggregated into a `KTable<Windowed<String>, List<DataRecord>>` using **Session Windows**. This `KTable` is materialized into a `SessionStore` (which is a type of `WindowStore`) with a defined retention period, ensuring automatic cleanup of old sessions.
    *   The `KTable` is converted back to a `KStream` (where each record represents a closed or updated session window) and its key is remapped to `String`.
    *   This stream is then joined with the `metadataStream` using a **stream-stream join** with a `JoinWindows`.

3.  **Batch Completion**: When a `MetadataRecord` arrives and joins with a closed session window containing the aggregated `DataRecord`s:
    *   The application checks if the number of aggregated `DataRecord`s in the `KTable` matches the `totalRecords` count from the `MetadataRecord`.
    *   If the counts match, it signifies a complete batch, and the application logs that it can proceed with processing the collected data.
    *   If the counts do not match, it logs a warning indicating a mismatch.

## Interactive Queries

This application also demonstrates the interactive query feature of Kafka Streams. The `StateQueryController` exposes a REST endpoint at `/data/{id}` that allows you to query the `data-store` `SessionStore` for a given `producerId`.

To avoid returning redundant or outdated information, the query service is designed to retrieve **only the latest session** available in the store for the specified `producerId`. This allows you to inspect the most recently aggregated data for a batch.

## Prerequisites

- Java 25 or higher
- Docker

## How to Run

1.  **Start Kafka:**
    Open a terminal in the root directory of the project and run:
    ```bash
    docker-compose up -d
    ```
    This will start a Kafka broker and the AKHQ Kafka UI. The Kafka broker will be available at `localhost:9092`, and AKHQ at `http://localhost:8082`.

2.  **Build the Project:**
    ```bash
    ./gradlew build
    ```

3.  **Run the Applications:**
    Open two separate terminals.

    *   In the first terminal, run the Kafka Streams processing application:
        ```bash
        ./gradlew :batch-processing-kafka-streams:bootRun
        ```
    *   In the second terminal, run the client application:
        ```bash
        ./gradlew :client:bootRun
        ```

## How to Use

Once both applications are running, you can trigger the data production by sending a GET request to the client's endpoint. You can use a tool like `curl` or your web browser.

Replace `producer-1` with any identifier you want to use for the batch, and `10` with the number of records to generate.

```bash
curl http://localhost:7070/produce/producer-1/10
```

This will trigger the `client` to send 10 `DataRecord` messages and 1 `MetadataRecord` to the `jsonMessageTopic`.

In the terminal where the `batch-processing-kafka-streams` application is running, you will see logs indicating that the messages are being processed and when the complete batch is received.

### Querying the State Store

To query the state store for the aggregated data (the latest session), you can send a GET request to the following endpoint:

```bash
curl http://localhost:7071/data/producer-1
```

This will return the list of `DataRecord`s from the latest session that has been aggregated for the `producer-1` batch. Note that the `batch-processing-kafka-streams` application runs on port `7071`.

## Kafka Topics

*   **`jsonMessageTopic`**: This is the main topic used for communication. The `client` produces `DataRecord` and `MetadataRecord` messages to this topic, and the `batch-processing-kafka-streams` application consumes from it.
