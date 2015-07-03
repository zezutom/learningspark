"""SimpleApp.py"""
#
# Counts the number of words in the given file and saves the result into an output directory.
#
# How to Run:
#
# $SPARK_HOME/bin/spark-submit \
# --master local[4] \
# src/main/python/WordCount.py README.md
#
#!/usr/bin/python

import sys
import os.path
from pyspark import SparkContext
from operator import add

if len(sys.argv) == 0: raise Exception("Please provide a file to parse")

file = sys.argv[1]

if not os.path.isfile(file): raise Exception("No such file: %s" % file)

sc = SparkContext("local", "Word Count")
data = sc.textFile(file)
wc = data.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(add)
wc.saveAsTextFile("wc_out.txt")