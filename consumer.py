from confluent_kafka import Consumer, KafkaError
import json
import time
import pandas as pd
import pymongo
from mlxtend.frequent_patterns import apriori
from collections import defaultdict

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

def consume_data(topic):
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

    # Run Apriori algorithm
    frequent_itemsets_apriori = generate_itemsets_apriori(preprocessed_data)

    # Run PCY algorithm
    frequent_itemsets_pcy = generate_itemsets_pcy(preprocessed_data)

    # Run SON algorithm
    frequent_itemsets_son = generate_itemsets_son(preprocessed_data)

    # Print insights from frequent itemsets (modify as needed)
    print("Frequent Itemsets (Apriori):")
    print(frequent_itemsets_apriori)

    print("Frequent Itemsets (PCY):")
    print(frequent_itemsets_pcy)

    print("Frequent Itemsets (SON):")
    print(frequent_itemsets_son)

def generate_itemsets_apriori(data):
    # Generate transactions
    transactions = generate_transactions(data)

    # Apply Apriori algorithm
    frequent_itemsets = apriori(transactions, min_support=0.1, use_colnames=True)

    return frequent_itemsets

def generate_itemsets_pcy(data):
    # Generate transactions
    transactions = generate_transactions(data)

    # Apply PCY algorithm
    frequent_itemsets = pcy(transactions, min_support=0.1)

    return frequent_itemsets

def generate_itemsets_son(data):
    # Generate transactions
    transactions = generate_transactions(data)

    # Apply SON algorithm
    frequent_itemsets = son(transactions, min_support=0.1)

    return frequent_itemsets

def generate_transactions(data):
    transactions = []
    for item in data:
        transaction = [item.get('brand', ''), item.get('categories', [])]  # Customize as needed
        transactions.append(transaction)
    return transactions

def save_to_mongodb(data):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["amazon_data"]
    collection = db["preprocessed_data"]
    collection.insert_many(data)

def pcy(transactions, min_support):
    # Create a hash table for the first pass
    hash_table = defaultdict(int)
    for transaction in transactions:
        for item in transaction:
            hash_table[item] += 1

    # Filter frequent items using the hash table
    frequent_items = {item for item, count in hash_table.items() if count >= min_support * len(transactions)}

    # Create a bitmap for the second pass
    bitmap = [0] * len(frequent_items)
    index_map = {item: i for i, item in enumerate(frequent_items)}
    for transaction in transactions:
        for item in transaction:
            if item in frequent_items:
                bitmap[index_map[item]] += 1

    # Apply the PCY algorithm to find frequent itemsets
    frequent_itemsets = []
    for i, itemset in enumerate(frequent_items):
        if bitmap[i] >= min_support:
            frequent_itemsets.append({itemset})

    return frequent_itemsets

def son(transactions, min_support):
    # SON algorithm implementation goes here
    # This is just a placeholder
    return []

def main():
    topic = 'as1'
    consume_data(topic)

if __name__ == "__main__":
    main()

