# Batch Processing with Kafka Streams

This project demonstrates a common batch processing pattern using Kafka Streams. It consists of a producer client that sends a series of data records followed by a metadata record to a Kafka topic. A Kafka Streams application then processes these records, grouping them by a producer ID and ensuring all data for a given batch is received before processing, with automatic state cleanup.

The project now supports two implementations:
1.  **JSON**: Using standard POJOs and JSON serialization.
2.  **Protobuf**: Using Protocol Buffers for efficient serialization and schema evolution, integrated with Confluent Schema Registry.

## Project Architecture

The project is a multi-module Gradle project with the following structure:

*   `common-dtos`: A Java library containing the shared data transfer objects (DTOs) and Protobuf definitions used by the other modules.
*   `client`: A Spring Boot application that acts as a Kafka producer, sending data and metadata records to Kafka topics via REST endpoints.
*   `kafka-streams-json`: A Spring Boot application that contains the Kafka Streams topology for processing JSON records.
*   `kafka-streams-protobuf`: A Spring Boot application that contains the Kafka Streams topology for processing Protobuf records.

### Data Models

*   **JSON**: Uses `DataRecord`, `MetadataRecord`, and `BatchRecord` POJOs.
*   **Protobuf**: Uses compiled Protobuf classes (`ProtoBufRecord`, `DataRecord`, `MetadataRecord`, `BatchRecord`) generated from `.proto` files in `common-dtos`.

## How it Works

The core logic is similar for both implementations:

1.  **Data Production**:
    *   The `client` sends a stream of `DataRecord`s for a specific `producerId`.
    *   Once all data is sent, it sends a `MetadataRecord` containing the `totalRecords` count.
    *   **JSON Topic**: `jsonMessageTopic`
    *   **Protobuf Topic**: `protoMessageTopic`

2.  **Stream Processing**:
    *   **Session Windows**: Data records are grouped by `producerId` into session windows (default 2 minutes inactivity gap).
    *   **Aggregation**: Records within a session are aggregated into a `BatchRecord`.
    *   **Join**: The aggregated stream is joined with the `MetadataRecord` stream (default 4 minutes join window).
    *   **Verification**: The processor compares the count of aggregated records with the `totalRecords` in the metadata.
    *   **Completion**: If counts match, the batch is marked complete and an external API call is simulated.

## Prerequisites

- Java 25 or higher
- Docker

## How to Run

1.  **Start Infrastructure:**
    Open a terminal in the root directory and run:
    ```bash
    docker-compose up -d
    ```
    This starts:
    *   **Kafka**: `localhost:9092`
    *   **Schema Registry**: `localhost:8081`
    *   **AKHQ**: `http://localhost:8082`

2.  **Build the Project:**
    ```bash
    ./gradlew build
    ```

3.  **Run the Applications:**
    Open separate terminals for each application you want to run.

    *   **Client (Producer)**:
        ```bash
        ./gradlew :client:bootRun
        ```
        Port: `7070`

    *   **JSON Processor**:
        ```bash
        ./gradlew :kafka-streams-json:bootRun
        ```
        Port: `7071`

    *   **Protobuf Processor**:
        ```bash
        ./gradlew :kafka-streams-protobuf:bootRun
        ```
        Port: `7072`

## How to Use

### 1. JSON Implementation

**Produce Data:**
Send 10 records for `producer-json-1`.
```bash
curl http://localhost:7070/produce/producer-json-1/10
```

**Query State Store (Interactive Query):**
Check the aggregated data in the JSON processor.
```bash
curl http://localhost:7071/data/producer-json-1
```

### 2. Protobuf Implementation

**Produce Data:**
Send 10 records for `producer-proto-1`.
```bash
curl http://localhost:7070/produce/protobuf/producer-proto-1/10
```

**Query State Store (Interactive Query):**
Check the aggregated data in the Protobuf processor.
```bash
curl http://localhost:7072/data/producer-proto-1
```

## Kafka Topics

*   **`jsonMessageTopic`**: For JSON implementation.
*   **`protoMessageTopic`**: For Protobuf implementation.