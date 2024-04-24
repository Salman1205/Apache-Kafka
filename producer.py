from confluent_kafka import Producer
import json
import time

def preprocess_batch(batch):
    preprocessed_batch = []
    for data in batch:
        # Extracting relevant fields
        asin = data.get('asin', '')
        title = data.get('title', '')
        description = data.get('description', [])
        price = data.get('price', '')
        brand = data.get('brand', '')
        categories = data.get('category', [])
        images = data.get('image', [])
        features = data.get('feature', [])
        
        # Cleaning data
        # For demonstration, let's assume cleaning involves removing HTML tags from the description
        if isinstance(description, list):
            # Joining list elements into a single string
            description = ' '.join(description)
        cleaned_description = description.replace('<br>', ' ').replace('<b>', '').replace('</b>', '')
        
        # Convert price to float if it's a valid number
        try:
            price = float(price.strip('$'))
        except ValueError:
            price = None
        
        # Adding preprocessed data to the batch
        preprocessed_data = {
            'asin': asin,
            'title': title,
            'description': cleaned_description,
            'price': price,
            'brand': brand,
            'categories': categories,
            'images': images,
            'features': features
            # Add more fields as needed
        }
        preprocessed_batch.append(preprocessed_data)
    
    return preprocessed_batch

def load_json_file(input_json_file):
    with open(input_json_file, 'r') as file:
        # Read each line from the file
        lines = file.readlines()
    
    data = []
    for line in lines:
        # Load JSON data from each line and append to the list
        data.append(json.loads(line))
    
    return data

def produce_data(producer, topic, data, batch_size, batch_interval):
    total_batches = 0
    total_messages_sent = 0

    while data:
        batch = []
        for _ in range(batch_size):
            if not data:
                break
            batch.append(data.pop(0))

        if batch:
            # Preprocess the batch
            preprocessed_batch = preprocess_batch(batch)

            for message in preprocessed_batch:
                # Display the preprocessed message before sending
                print("Preprocessed Message:")
                print(json.dumps(message, indent=4))  # Pretty print the message
                print("=" * 30)

                # Produce message to Kafka topic
                producer.produce(topic, json.dumps(message).encode('utf-8'))
                producer.flush()
                total_messages_sent += 1

            total_batches += 1
            print(f"Batch {total_batches} sent with {len(preprocessed_batch)} messages.")
        else:
            print("No more data to process. Exiting...")

        time.sleep(batch_interval)

    print(f"Total batches processed: {total_batches}")  
    print(f"Total messages sent: {total_messages_sent}")


def main():
    # Specify the path to the input JSON file
    input_json_file = '/home/user/Downloads/maryam/ass3/Sampled_Amazon_Meta.json'

    # Kafka producer configuration
    producer_config = {
        'bootstrap.servers': 'localhost:9092',  # Kafka broker address
        'client.id': 'producer'
    }
    
    # Create Kafka producer
    producer = Producer(producer_config)
    
    # Specify Kafka topic to produce data to
    topic = 'topic-1'
    
    # Specify batch size and batch interval (in seconds)
    batch_size = 10
    batch_interval = 10
    
    # Load data from the input JSON file
    data = load_json_file(input_json_file)
    
    # Produce data to Kafka
    produce_data(producer, topic, data, batch_size, batch_interval)

if __name__ == "__main__":
    main()
from confluent_kafka import Producer
import json
import time

def preprocess_batch(batch):
    preprocessed_batch = []
    for data in batch:
        # Extracting relevant fields
        asin = data.get('asin', '')
        title = data.get('title', '')
        description = data.get('description', [])
        price = data.get('price', '')
        brand = data.get('brand', '')
        categories = data.get('category', [])
        images = data.get('image', [])
        features = data.get('feature', [])
        
        # Cleaning data
        # For demonstration, let's assume cleaning involves removing HTML tags from the description
        if isinstance(description, list):
            # Joining list elements into a single string
            description = ' '.join(description)
        cleaned_description = description.replace('<br>', ' ').replace('<b>', '').replace('</b>', '')
        
        # Convert price to float if it's a valid number
        try:
            price = float(price.strip('$'))
        except ValueError:
            price = None
        
        # Adding preprocessed data to the batch
        preprocessed_data = {
            'asin': asin,
            'title': title,
            'description': cleaned_description,
            'price': price,
            'brand': brand,
            'categories': categories,
            'images': images,
            'features': features
            # Add more fields as needed
        }
        preprocessed_batch.append(preprocessed_data)
    
    return preprocessed_batch

def load_json_file(input_json_file):
    with open(input_json_file, 'r') as file:
        # Read each line from the file
        lines = file.readlines()
    
    data = []
    for line in lines:
        # Load JSON data from each line and append to the list
        data.append(json.loads(line))
    
    return data

def produce_data(producer, topic, data, batch_size, batch_interval):
    total_batches = 0
    total_messages_sent = 0

    while data:
        batch = []
        for _ in range(batch_size):
            if not data:
                break
            batch.append(data.pop(0))

        if batch:
            # Preprocess the batch
            preprocessed_batch = preprocess_batch(batch)

            for message in preprocessed_batch:
                # Display the preprocessed message before sending
                print("Preprocessed Message:")
                print(json.dumps(message, indent=4))  # Pretty print the message
                print("=" * 30)

                # Produce message to Kafka topic
                producer.produce(topic, json.dumps(message).encode('utf-8'))
                producer.flush()
                total_messages_sent += 1

            total_batches += 1
            print(f"Batch {total_batches} sent with {len(preprocessed_batch)} messages.")
        else:
            print("No more data to process. Exiting...")

        time.sleep(batch_interval)

    print(f"Total batches processed: {total_batches}")  
    print(f"Total messages sent: {total_messages_sent}")


def main():
    # Specify the path to the input JSON file
    input_json_file = '/home/user/Downloads/maryam/ass3/Sampled_Amazon_Meta.json'

    # Kafka producer configuration
    producer_config = {
        'bootstrap.servers': 'localhost:9092',  # Kafka broker address
        'client.id': 'producer'
    }
    
    # Create Kafka producer
    producer = Producer(producer_config)
    
    # Specify Kafka topic to produce data to
    topic = 'topic-1'
    
    # Specify batch size and batch interval (in seconds)
    batch_size = 10
    batch_interval = 10
    
    # Load data from the input JSON file
    data = load_json_file(input_json_file)
    
    # Produce data to Kafka
    produce_data(producer, topic, data, batch_size, batch_interval)

if __name__ == "__main__":
    main()
