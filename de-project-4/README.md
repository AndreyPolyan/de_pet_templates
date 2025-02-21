# DE Project 4

## HDFS + Spark Batch proccesing with Airflow orchestration

#### Note 1: project is part of Yandex.Practicum Data Engineer Education
#### Note 2: non-reproducible, code only.

### Stack: HDFS, Spark, Airflow

## DataMart Creation for Messaging Application

## Project Objective
The goal of this project is to process events from a messaging application and create three main reports:
1. **User Summary Report (`dmart_users`)**:
   - This report provides a consolidated view of users' latest information, including their last known location and travel data.
   - This is updated via a FULL REFRESH and only contains the most recent data.

2. **Event Statistics Report (`dmart_zones`)**:
   - Aggregated statistics of events broken down by month, week, and event type.
   - Tracks metrics such as the number of messages, reactions, subscriptions, and unique users.

3. **User Connection Recommendations (`dmart_reccomendations`)**:
   - Recommends potential user connections based on shared subscriptions to the same channels.
   - Excludes users who have interacted with each other directly.
   - Ensures that the physical distance between users (based on the latest known locations) does not exceed 1 km.
   
## Project Structure

### Directories and Files
- **`cities/`**:
  - Contains data related to cities used for geolocation in the project.
  - **`geo.csv`**: A CSV file with city coordinates and identifiers. Used for cross-referencing user locations.

- **`src/`**:
  - The source code for the project, including PySpark scripts and Airflow DAGs.

  #### `src/dags/`:
  - **`dags_dmarts.py`**:
    - Contains the Airflow DAG that orchestrates the generation of all three reports (`dmart_users`, event statistics, and `dmart_reccomendations`).
    - Manages dependencies between tasks and ensures periodic execution.

  #### `src/scripts/`:
  - **`dmart_reccomendations.py`**:
    - Generates the recommendations for user connections.
    - Cross-references user subscriptions and geolocation data to find close matches within 1 km.

  - **`dmart_users.py`**:
    - Creates the user summary report.
    - Combines data on users' last known locations and travel history.

  - **`dmart_zones.py`**:
    - Generates event statistics reports broken down by time (month, week) and event type.
    - Provides insights into user activity levels in specific zones.

### Key Libraries and Frameworks
- **PySpark**:
  - Used for processing large-scale data in a distributed environment.
- **Airflow**:
  - Handles workflow orchestration and automation of the ETL pipelines.
- **Hadoop**:
  - Provides the underlying file system (HDFS) for storing input and output data.

### Usage Instructions
1. Prepare the `geo.csv` file in the `cities` directory with accurate city information.
2. Place event data in the appropriate HDFS path.
3. Execute the Airflow DAG (`dags_dmarts.py`) to generate the reports.

### Outputs
- Processed reports are stored in the output directory specified in the scripts:
  - `dmart_users`: User summary.
  - `dmart_zones`: Event statistics.
  - `dmart_reccomendations`: User connection recommendations.

---

For further details, refer to the respective script files for in-depth logic and implementations.