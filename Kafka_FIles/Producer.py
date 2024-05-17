from kafka import KafkaProducer
import json
import time

# Kafka broker settings
bootstrap_servers = 'localhost:9092'
topic = 'topic1'

# Create Kafka Producer instance
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Read preprocessed data from JSON file and publish to Kafka topic
def produce_data():
    with open('Preprocessed_Amazon_Meta.json', 'r') as file:
        for line in file:
            data = json.loads(line)
            producer.send(topic, value=data)
            print("Message sent:", data)  # Print message sent
            time.sleep(4)  # Optional: add delay to control the rate of data production
 
    producer.flush()  # Ensure all messages are sent

if __name__ == '__main__':
    produce_data()
