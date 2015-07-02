"""SimpleApp.py"""
#
# A simple app demo.
#
# How to Run:
#
# $SPARK_HOME/bin/spark-submit \
# --master local[4] \
# src/main/python/SimpleApp.py README.md
#
#!/usr/bin/python

import sys
import os.path
from pyspark import SparkContext

if len(sys.argv) == 0: raise Exception("Please provide a file to parse")

logFile = sys.argv[1]

if not os.path.isfile(logFile): raise Exception("No such file: %s" % logFile)


sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print "Lines with a: %i, lines with b: %i" % (numAs, numBs)