
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
mongo_db_name = "db4"
mongo_collection_name = "f_itemsets"

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

# Function to generate frequent itemsets using Apriori algorithm
def apriori(transactions, min_support):
    candidate_itemsets = defaultdict(int)
    for transaction in transactions:
        for item in transaction:
            candidate_itemsets[item] += 1
    
    frequent_itemsets = {item: support for item, support in candidate_itemsets.items() if support >= min_support}
    
    # Save frequent itemsets to MongoDB
    collection.insert_one({"frequent_itemsets": frequent_itemsets})
    
    print("Frequent itemsets saved to MongoDB.")
    
    k = 2
    while frequent_itemsets:
        candidate_itemsets = defaultdict(int)
        for transaction in transactions:
            for itemset in combinations(transaction, k):
                if all(subset in frequent_itemsets for subset in combinations(itemset, k - 1)):
                    candidate_itemsets[itemset] += 1
        
        frequent_itemsets = {itemset: support for itemset, support in candidate_itemsets.items() if support >= min_support}
        
        collection.insert_one({"frequent_itemsets": frequent_itemsets})
        
        print(f"Frequent {k}-itemsets saved to MongoDB.")
        
        k += 1

# Main function to consume messages and apply Apriori algorithm with sliding window
def consume_data(window_size=5, min_support=2):
    transactions_window = []
    transactions_all = []
    
    for message in consumer:
        data = message.value
        itemset = extract_itemsets(data)
        transactions_window.append(itemset)
        transactions_all.append(itemset)
        
        if len(transactions_window) == window_size:
            # Apply Apriori algorithm on the current window
            apriori(transactions_window, min_support)
            
            # Slide the window
            transactions_window.pop(0)
    
    if transactions_window:
        apriori(transactions_window, min_support)
    
    print("Applying Apriori on all transactions:")
    apriori(transactions_all, min_support)

if __name__ == '__main__':
    consume_data()
