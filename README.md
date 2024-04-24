### Producer README

#### Introduction
The producer script (`producer.py`) is designed to preprocess data sourced from a JSON file and send the processed data to a Kafka topic. This README offers an insight into the functionality of the producer script along with guidance on how to utilize it effectively.

#### Prerequisites
Before running the producer script, ensure that the following prerequisites are fulfilled:
- Your system has Python 3.x installed.
- The Confluent Kafka Python library (`confluent_kafka`) is accessible.
- A Kafka broker is operational and running on `localhost:9092`.
- You have a JSON file containing the data to be processed.

#### Usage
1. **Configuration**: Customize the configuration parameters within the script to match your environment:
   - `input_json_file`: Path to the input JSON file containing the data.
   - Kafka Configuration:
     - `bootstrap.servers`: Kafka broker address (e.g., `'localhost:9092'`).
     - `client.id`: A unique identifier for the producer client.
   - Specify the Kafka topic where the data will be sent (`topic`).
   - Adjust the `batch_size` and `batch_interval` parameters as required.

2. **Running the Script**:
   Execute the producer script using the provided command:
   ```
   python producer.py
   ```
   The script processes the data from the JSON file, performs preprocessing, and then sends the preprocessed data to the specified Kafka topic.

3. **Output**:
   During execution, the script provides updates regarding the batches processed and messages sent.

#### Example
```python
python producer.py
```

### Note
- Ensure that the Kafka broker is operational and configured to accept connections before executing the producer script.
- Modify the Kafka topic name within the producer script to match the topic expected by the consumer script.

### Consumer README

#### Introduction
The consumer script (`consumer.py`) is tasked with consuming preprocessed data from a Kafka topic, applying various algorithms for processing, and storing the results in a MongoDB database. This README outlines the functionality of the consumer script and provides instructions for its usage.

#### Prerequisites
Before executing the consumer script, make sure the following prerequisites are met:
- Python 3.x is installed on your system.
- The Confluent Kafka Python library (`confluent_kafka`) is installed.
- A MongoDB server is running on `localhost:27017`.
- Necessary Python libraries for algorithm implementations (e.g., `mlxtend`) are installed.

#### Usage
1. **Configuration**: Update the configuration parameters within the script to match your environment:
   - Kafka Configuration:
     - `bootstrap.servers`: Kafka broker address (e.g., `'localhost:9092'`).
     - `group.id`: Consumer group ID.
     - `auto.offset.reset`: Offset reset policy (e.g., `'earliest'`).
   - MongoDB Configuration:
     - `mongodb://localhost:27017/`: MongoDB connection URI.
     - Database and Collection Names (`db`, `collection`).

2. **Running the Script**:
   Execute the consumer script using the provided command:
   ```
   python consumer.py
   ```
   The script consumes preprocessed data from the specified Kafka topic, processes it using Apriori, PCY, and SON algorithms, and stores the results in a MongoDB database.

3. **Output**:
   During execution, the script prints insights derived from frequent itemsets obtained using Apriori, PCY, and SON algorithms.

#### Example
```python
python consumer.py
```

### Note
- Ensure that the Kafka producer is actively running and producing data to the specified topic before executing the consumer script.
- Adjust the Kafka topic name in the consumer script to match the topic produced by the producer.
- The Apriori, PCY, and SON algorithms are implemented in separate files (`apriori_algorithm.py`, `pcy_son_algorithms.py`).
