
import MapReduce
import sys
import re

"""
Problem 1 (tf-df)

Run Command
python tf_df.py ./data/books.json

The format of this file is based on "wordcount.py"
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    value = record[1]
    tokens = re.findall("[A-Za-z0-9_]+", value)  # also can use: "\w+"
    
    for w in tokens:
      mr.emit_intermediate(w.lower(), key)

def reducer(key, list_of_values):
    # key: token (word)
    # value: list of document id

    ht = {}
    result = []
    result.append(key)
    for v in list_of_values:
      ht.setdefault(v, 0)
      ht[v] += 1;
    result.append(len(ht))

    tf = [];
    for k,v in ht.items():
      tf.append([k,v]);

    # tf.sort();
    # the order of tf is a little bit different from that of tf_df_output

    result.append(tf);
    mr.emit(result)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  # for test
  # inputdata = open("./data/books.json")
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
