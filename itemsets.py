"""
Author: Lingzhe Teng
USC ID: 8550242127
Date:   Sep. 8, 2015
"""
import MapReduce
import sys
import re

"""
Problem 2 (frequent itemsets)

Run Command
python lingzhe_teng_itemsets.py ./data/transactions.json


The format of this file is based on "wordcount.py"
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # transaction: input value, sorted by their item numbers in the ascending orde
    transaction = record
    
    setlen = len(transaction)
    if setlen >= 2:
      for i in range(0, setlen):
        for j in range(i+1, setlen):
          itemset = [transaction[i], transaction[j]]
          mr.emit_intermediate(str(itemset), [itemset, 1])

def reducer(key, list_of_values):
    # key: itemset in string version
    # value: tuple for itemset and 1

    total = 0
    result = [];
    for v in list_of_values:
      result = v[0]
      total += v[1]

    if total >= 100:
      #mr.emit((key, total))
      mr.emit(result)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  # for test
  # inputdata = open("./data/transactions.json")
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
