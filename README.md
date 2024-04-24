### Producer README

#### Introduction
The producer script (`producer.py`) is responsible for preprocessing data from a JSON file and producing the preprocessed data to a Kafka topic. This README provides an overview of the producer script and instructions for its usage.

#### Prerequisites
Before running the producer script, ensure that you have the following prerequisites installed:
- Python 3.x
- Confluent Kafka Python library (`confluent_kafka`)
- Kafka broker running on `localhost:9092`
- A JSON file containing the data to be processed

#### Usage
1. **Configuration**: Update the configuration parameters in the script according to your environment:
   - `input_json_file`: Path to the input JSON file containing the data to be processed.
   - `bootstrap.servers`: Kafka broker address (e.g., `'localhost:9092'`).
   - `batch_size`: Number of messages to be processed in each batch.
   - `batch_interval`: Time interval (in seconds) between each batch processing.

2. **Running the Script**:
   Execute the producer script using the following command:
   ```
   python producer.py
   ```
   The script will preprocess the data from the JSON file and produce the preprocessed data to the specified Kafka topic.

3. **Output**:
   During execution, the script will display information about the batches processed and messages sent.

#### Example
```python
python producer.py
```

### Consumer README

#### Introduction
The consumer script (`consumer.py`) is responsible for consuming preprocessed data from a Kafka topic, processing it using various algorithms, and storing the results in a MongoDB database. This README provides an overview of the consumer script and instructions for its usage.

#### Prerequisites
Before running the consumer script, ensure that you have the following prerequisites installed:
- Python 3.x
- Confluent Kafka Python library (`confluent_kafka`)
- MongoDB server running on `localhost:27017`
- Necessary Python libraries for algorithm implementations (e.g., `mlxtend`)

#### Usage
1. **Configuration**: Update the configuration parameters in the script according to your environment:
   - Kafka configuration:
     - `bootstrap.servers`: Kafka broker address (e.g., `'localhost:9092'`).
     - `group.id`: Consumer group ID.
     - `auto.offset.reset`: Offset reset policy (e.g., `'earliest'`).
   - MongoDB configuration:
     - `mongodb://localhost:27017/`: MongoDB connection URI.
     - Database name and collection name (`db`, `collection`).

2. **Running the Script**:
   Execute the consumer script using the following command:
   ```
   python consumer.py
   ```
   The script will consume preprocessed data from the specified Kafka topic, process it using Apriori, PCY, and SON algorithms, and store the results in a MongoDB database.

3. **Output**:
   During execution, the script will print insights from frequent itemsets obtained using Apriori, PCY, and SON algorithms.

#### Example
```python
python consumer.py
```

### Note
- Make sure that the Kafka producer is running and producing data to the specified topic before executing the consumer script.
- Adjust the Kafka topic name in the consumer script to match the topic produced by the producer.
