from confluent_kafka import Consumer, KafkaError
import json
import pymongo

def preprocess_data(data):
    # Preprocessing steps go here
    # Example: Cleaning and formatting the data
    preprocessed_data = []
    for item in data:
        # Example preprocessing steps
        cleaned_description = item.get('description', '').replace('<br>', ' ').replace('<b>', '').replace('</b>', '')
        preprocessed_item = {
            'asin': item.get('asin', ''),
            'title': item.get('title', ''),
            'description': cleaned_description,
            'price': float(item.get('price', '').strip('$')) if item.get('price') else None,
            'brand': item.get('brand', ''),
            'categories': item.get('category', []),
            'images': item.get('image', []),
            'features': item.get('feature', [])
            # Add more fields as needed
        }
        preprocessed_data.append(preprocessed_item)
    return preprocessed_data

def save_to_mongodb(data):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["amazon_data"]
    collection = db["preprocessed_data"]
    collection.insert_many(data)

def consume_data(topic):
    # Consumer configuration
    consumer_config = {
        'bootstrap.servers': 'localhost:9092',  # Kafka broker address
        'group.id': 'consumer_group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            # Process the message data
            process_message(msg.value())
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def process_message(message):
    # Preprocess the message
    preprocessed_data = preprocess_data(json.loads(message))

    # Save preprocessed data to MongoDB
    save_to_mongodb(preprocessed_data)

def main():
    topic = 'as1'
    consume_data(topic)

if __name__ == "__main__":
    main()
