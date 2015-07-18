"""SimpleApp.py"""
#
# Counts the number of words in the given file and saves the result into an output directory.
#
# How to Run:
#
# $SPARK_HOME/bin/spark-submit \
# --master local[4] \
# src/main/python/app/word_count_and_join_app.py
#
#!/usr/bin/python

import sys
import os.path

from pyspark import SparkContext
from word_count_and_join import WordCountAndJoin

if not os.path.isfile(f): raise Exception("No such file: %s" % f)
data = self.sc.textFile(f)

sc = SparkContext("local", "WordCount and Join App")
print WordCountAndJoin(sc).wc_join('Spark', read_file('src/main/resources/README.md'), read_file('src/main/resources/CHANGES.txt'))

def read_file(self, f):
    if not os.path.isfile(f): raise Exception("No such file: %s" % f)
    return sc.textFile(f)