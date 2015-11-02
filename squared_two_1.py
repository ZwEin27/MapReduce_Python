import MapReduce
import sys
import re

"""
Problem 4 (squaring matrix, two-phase)

Run Command
python squared_two_1.py ./data/matrix.json


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
    mr.emit_intermediate(j, ['A', i, value]);
    
    # B[j, k]   M x N
    j = cell[0]
    k = cell[1]
    mr.emit_intermediate(j, ['B', k, value]);

    # C[i, k]   L x N

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    #print key
    #print  list_of_values
    list_A = []
    list_B = []
    
    for v in list_of_values:
      symbol = v[0]
      ik = v[1]
      value = v[2]

      idx = 0;
      if symbol == 'A':
        list_A.append([ik, value])
      elif symbol == 'B':
        list_B.append([ik, value])

    # print key
    # print "A", list_A
    # print "B", list_B

    lenA = len(list_A)
    lenB = len(list_B)

    for i in range(lenA):
      A = list_A[i]
      for k in range(lenB):
        B = list_B[k]
        mr.emit((A[0], B[0], A[1]*B[1]))
    
# sys.stdout = open("./lingzhe_teng_testdata_squared_two_2.json", "w")

# Do not modify below this line
# =============================
if __name__ == '__main__':
  # for test
  # inputdata = open("./data/matrix.json")
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
