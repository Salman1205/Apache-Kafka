#!/usr/bin/env python3

import sys
from collections import defaultdict

def reducer():
    frequent_items_global = defaultdict(int)

    # Read mapper output from standard input
    for line in sys.stdin:
        try:
            item, count = line.strip().split('\t')
            count = int(count)
            # Combine local frequent itemsets to generate global frequent itemsets
            frequent_items_global[item] += count
        except ValueError:
            continue

    # Emit global frequent itemsets
    for item, count in frequent_items_global.items():
        print('%s\t%d' % (item, count))

if __name__ == "__main__":
    reducer()
