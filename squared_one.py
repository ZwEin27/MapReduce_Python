import MapReduce
import sys
import re

"""
Problem 3 (squaring matrix, one-phase)

Run Command
python squared_one.py ./data/matrix.json


The format of this file is based on "wordcount.py"
This program is to compute its square (A^2),
and we assume the matric is 5x5


| 63 45 93 32 49 |   | 63 45 93 32 49 |   | x x x x x |
| 33 xx xx 26 95 |   | 33 xx xx 26 95 |   | x x x x x |
| 25 11 xx 60 89 | x | 25 11 xx 60 89 | = | x x x x x |
| 24 79 24 47 18 |   | 24 79 24 47 18 |   | x x x x x |
| 07 98 96 27 xx |   | 07 98 96 27 xx |   | x x x x x |


"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    cell = record

    value = cell[2]

    # A[i, j]   L x M
    i = cell[0]
    j = cell[1]
    for idx in range(0, 5):
      mr.emit_intermediate(str((i,idx)), ['A', i, j, value]);
    
    # B[j, k]   M x N
    j = cell[0]
    k = cell[1]

    for idx in range(0, 5):
      mr.emit_intermediate(str((idx,k)), ['B', j, k, value]);

    # C[i, k]   L x N

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    #print key
    #print  list_of_values
    ht = {}
    flag = [0]*5
    for v in list_of_values:
      symbol = v[0]
      value = v[3]

      idx = 0;
      if symbol == 'A':
        idx = v[2]
        ht.setdefault(idx, 1)
        ht[idx] *= value
      elif symbol == 'B':
        idx = v[1]
        ht.setdefault(idx, 1)
        ht[idx] *= value
      flag[idx] += 1;

    #print ht
    total = 0;
    for k,v in ht.items():
      if flag[k] >= 2:
        total += v;

    mr.emit((int(key[1]), int(key[4]), total))
    

# Do not modify below this line
# =============================
if __name__ == '__main__':
  # for test
  # inputdata = open("./data/matrix.json")
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
