"""SimpleApp.py"""
#
# Counts the number of words in the given file and saves the result into an output directory.
#
# How to Run:
#
# $SPARK_HOME/bin/spark-submit \
# --master local[4] \
# src/main/python/app/word_count_app.py src/main/resources/README.md
#
#!/usr/bin/python

import sys
import os.path

from pyspark import SparkContext
from word_count import WordCount


if len(sys.argv) == 0: raise Exception("Please provide a file to parse")

f = sys.argv[1]
if not os.path.isfile(f): raise Exception("No such file: %s" % f)

sc = SparkContext("local", "WordCount App")
data = sc.textFile(f)

WordCount().wc(data).saveAsTextFile("wc_out.txt")