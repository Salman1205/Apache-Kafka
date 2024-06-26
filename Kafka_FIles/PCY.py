from kafka import KafkaConsumer
import json
from collections import defaultdict
from itertools import combinations
from pymongo import MongoClient

# Kafka broker settings
bootstrap_servers = 'localhost:9092'
topic = 'topic1'

# MongoDB settings
mongo_uri = "mongodb://localhost:27017/"
mongo_db_name = "db"
mongo_collection_name = "f_pairs"

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client[mongo_db_name]
collection = db[mongo_collection_name]

# Create Kafka Consumer instance
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Function to extract itemsets from messages
def extract_itemsets(message):
    itemset = set(message.get('also_buy', []))
    return itemset

# Function to generate candidate pairs using PCY algorithm
def generate_candidate_pairs(transactions, hash_table, min_support):
    candidate_pairs = defaultdict(int)
    
    # First pass: Count occurrences of single items
    single_item_counts = defaultdict(int)
    for transaction in transactions:
        for item in transaction:
            single_item_counts[item] += 1
    
    # Second pass: Count occurrences of pairs and filter based on hash table
    for transaction in transactions:
        for pair in combinations(transaction, 2):
            if hash_table[hash(pair) % len(hash_table)] > min_support:
                candidate_pairs[pair] += 1
    
    return candidate_pairs

# Main function to consume messages and apply PCY algorithm
def consume_data(window_size=5, min_support=2, hash_table_size=1000):
    transactions_window = []
    transactions_all = []
    
    # Initialize hash table
    hash_table = [0] * hash_table_size
    
    for message in consumer:
        data = message.value
        itemset = extract_itemsets(data)
        transactions_window.append(itemset)
        transactions_all.append(itemset)
        
        if len(transactions_window) == window_size:
            # Generate candidate pairs using PCY algorithm
            candidate_pairs = generate_candidate_pairs(transactions_window, hash_table, min_support)
            
            # Save frequent pairs to MongoDB
            collection.insert_one({"frequent_pairs": {str(pair): count for pair, count in candidate_pairs.items() if count >= min_support}})
            
            # Print frequent pairs
            print("Frequent pairs saved to MongoDB.")
            
            # Slide the window
            transactions_window.pop(0)
        
        # Update hash table
        for pair in combinations(itemset, 2):
            hash_table[hash(pair) % len(hash_table)] += 1
    
    # Apply PCY algorithm on all transactions at the end
    candidate_pairs = generate_candidate_pairs(transactions_all, hash_table, min_support)
    collection.insert_one({"frequent_pairs": {str(pair): count for pair, count in candidate_pairs.items() if count >= min_support}})
    print("Frequent pairs for all transactions saved to MongoDB.")

if __name__ == '__main__':
    consume_data()
