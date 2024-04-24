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
- The Apriori, PCY, and SON algorithms are implemented in separate files (`apriori_algorithm.py`, `pcy_son_algorithms.py`).
