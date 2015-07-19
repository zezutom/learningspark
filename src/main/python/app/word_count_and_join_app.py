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


def read_file(f):
    if not os.path.isfile(f): raise Exception("No such file: %s" % f)
    return sc.textFile(f)

sc = SparkContext("local", "WordCount and Join App")
print WordCountAndJoin().wc_join('Spark', read_file(u'src/main/resources/README.md'), read_file(u'src/main/resources/CHANGES.txt'))

