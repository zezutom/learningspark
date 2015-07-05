"""SimpleApp.py"""
#
# Counts the number of words in the given file and saves the result into an output directory.
#
# How to Run:
#
# $SPARK_HOME/bin/spark-submit \
# --master local[4] \
# src/main/python/org/zezutom/learningspark/word_count_app.py
#
#!/usr/bin/python

import sys

from pyspark import SparkContext
from word_count import WordCount


if len(sys.argv) == 0: raise Exception("Please provide a file to parse")
sc = SparkContext("local", "WordCount App")
WordCount(sc).wc(sys.argv[1]).saveAsTextFile("wc_out.txt")