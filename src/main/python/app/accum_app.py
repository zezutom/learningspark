"""AccumApp.py"""
#
# A simple app demo.
#
# How to Run:
#
# $SPARK_HOME/bin/spark-submit \
# --master local[4] \
# src/main/python/app/accum_app.py
#
#!/usr/bin/python

import sys
import os.path

from pyspark import SparkContext
from accum import Accum

sc = SparkContext("local", "Accumulator App")
accum = sc.accumulator(0)
print Accum().sum(sc.parallelize([1, 2, 3, 4]), accum)
