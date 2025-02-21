# DE Project 5

## Spark Streaming + DWH + Message Broker

#### Note 1: project is part of Yandex.Practicum Data Engineer Education
#### Note 2: non-reprodusible , code only.

### Stack: Spark Streaming, Kafka


## Description
The goal was to develop a **streaming data pipeline** for restaurant promotions. The pipeline processes real-time data and stores it in both a data bus and a local storage in the required format.

### Event Flow:
1. A stream of messages about restaurant promotions is **generated in Kafka** (simulated using a script in the `producer` directory).
2. The **Spark Streaming application** reads this data stream.
3. The data undergoes:
   - **Deduplication**
   - **Enrichment** (by fetching user subscription data from a database)
   - **Formatting** into the required structure.
4. The **processed data is written** back in streaming mode:
   - **To Kafka** (in another topic).
   - **To a local database**.

---

## Repository Structure
The repository files are structured to facilitate evaluation and feedback. Please follow this structure to ensure alignment between tasks and solutions.

Inside the `src` directory, you will find:
- `/src/producer` - Scripts for generating the promotion data stream in Kafka.
- `/src/scripts` - Scripts implementing the **streaming application**.

Within `/src/scripts`, there is a file named **`credentials.yaml`**, which contains necessary connection details (sanitized for security purposes).

---

## Technical Setup
This project was developed as part of **Yandex.Practicum** training inside a **preconfigured Docker container**, deployed on a **virtual machine** within the same **intranet** as the data bus.

### Main Components:
1. **Kafka** – Message broker with two topics:
   - One for reading incoming promotion data.
   - One for storing processed output data.
2. **Docker + PostgreSQL** – A local database for storing processed data.
3. **Spark & Spark Streaming** – To handle both **static and streaming data**.

---

## How to Run the Project
1. Start the Kafka broker.
2. Run the producer script (`producer`) to generate streaming data.
3. Execute the Spark Streaming application to process and store the data.
4. Validate the processed results in both Kafka and PostgreSQL.

---

## Notes
- Ensure all dependencies are installed.
- Make sure `credentials.yaml` is configured correctly.
- Adjust configurations as needed for different environments.