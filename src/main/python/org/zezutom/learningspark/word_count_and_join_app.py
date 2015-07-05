"""SimpleApp.py"""
#
# Counts the number of words in the given file and saves the result into an output directory.
#
# How to Run:
#
# $SPARK_HOME/bin/spark-submit \
# --master local[4] \
# src/main/python/org/zezutom/learningspark/word_count_and_join_app.py
#
#!/usr/bin/python

import sys

from pyspark import SparkContext
from word_count_and_join import WordCountAndJoin


sc = SparkContext("local", "WordCount and Join App")
print WordCountAndJoin(sc).wc_join('Spark', 'src/main/resources/README.md', 'src/main/resources/CHANGES.txt')