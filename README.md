# Batch Processing with Kafka Streams

This project demonstrates a batch processing pattern using Kafka Streams. It consists of a producer client that sends a series of data records followed by a metadata record to a Kafka topic. A Kafka Streams application then processes these records, grouping them by a producer ID and ensuring all data for a given batch is received before processing.

## How it Works

The process is as follows:

1.  **Data Production:** A client sends a specified number of `DataRecord` messages to a Kafka topic. Each message contains a `producerId`. After sending all data records, it sends a final `MetadataRecord` which includes the `producerId` and the total count of `DataRecord`s sent.

2.  **Data Consumption & Processing:** A Kafka Streams application consumes the records from the topic.
    *   It separates the incoming stream into two: one for `DataRecord`s and one for `MetadataRecord`s.
    *   `DataRecord`s are aggregated into a list, grouped by `producerId`, and stored in a `KTable`.
    *   The `MetadataRecord` stream is joined with this `KTable` on the `producerId`.

3.  **Batch Completion:** When a `MetadataRecord` is received, the join is triggered. The application checks if the number of aggregated `DataRecord`s in the `KTable` matches the `totalRecords` count from the `MetadataRecord`.
    *   If the counts match, it signifies a complete batch, and the application logs that it can proceed with processing the collected data.
    *   If the counts do not match, it logs a warning indicating a mismatch.

## Modules

The project is structured into three modules:

*   `common-dtos`: Contains the Plain Old Java Objects (POJOs) for `DataRecord` and `MetadataRecord` that are shared between the client and the processing application.
*   `client`: A Spring Boot application with a REST endpoint that acts as the Kafka producer, sending the data and metadata records.
*   `batch-processing-kafka-streams`: A Spring Cloud Stream application that contains the Kafka Streams topology for processing the records.

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

Replace `producer-1` with any identifier you want to use for the batch.

```bash
curl http://localhost:8080/produce/producer-1
```

This will trigger the `client` to send 10 `DataRecord` messages and 1 `MetadataRecord` to the `jsonMessageTopic`.

In the terminal where the `batch-processing-kafka-streams` application is running, you will see logs indicating that the messages are being processed and when the complete batch is received.

## Kafka Topics

*   **`jsonMessageTopic`**: This is the main topic used for communication. The `client` produces `DataRecord` and `MetadataRecord` messages to this topic, and the `batch-processing-kafka-streams` application consumes from it.
