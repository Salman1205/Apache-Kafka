#!/usr/bin/env python3

import sys
import json
from collections import defaultdict

def mapper():
    frequent_items_local = defaultdict(int)

    # Read data from standard input
    for line in sys.stdin:
        try:
            data = json.loads(line.strip())
            # Process data and generate local frequent itemsets
            for item in data.get('also_buy', []):
                frequent_items_local[item] += 1
        except Exception as e:
            # Skip invalid JSON data
            continue

    # Emit local frequent itemsets to the reducer
    for item, count in frequent_items_local.items():
        print('%s\t%d' % (item, count))

if __name__ == "__main__":
    mapper()
