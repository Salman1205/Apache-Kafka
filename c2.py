import pandas as pd
from mlxtend.frequent_patterns import apriori

def generate_itemsets_apriori(data):
    # Generate transactions
    transactions = generate_transactions(data)

    # Apply Apriori algorithm
    frequent_itemsets = apriori(transactions, min_support=0.1, use_colnames=True)

    return frequent_itemsets

def generate_transactions(data):
    transactions = []
    for item in data:
        transaction = [item.get('brand', ''), item.get('categories', [])]  # Customize as needed
        transactions.append(transaction)
    return transactions

