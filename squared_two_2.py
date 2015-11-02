
import MapReduce
import sys
import re

"""
Problem 4 (squaring matrix, two-phase)

Run Command
python squared_two_1.py ./data/matrix.json | python squared_two_2.py


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
    i = cell[0]
    k = cell[1]
    value = cell[2]

    mr.emit_intermediate(str((i, k)), value);

    # C[i, k]   L x N

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts

    # print key
    # print list_of_values

    total = 0
    for v in list_of_values:
      total += v
    mr.emit((int(key[1]), int(key[4]), total))
    

# Do not modify below this line
# =============================
if __name__ == '__main__':
  # for test
  inputdata = open("./lingzhe_teng_testdata_squared_two_2.json")
  # inputdata.write(sys.stdin.read())
  # inputdata = open(sys.argv[1])

  # inputdata = sys.stdin.readlines()
  mr.execute(inputdata, mapper, reducer)
