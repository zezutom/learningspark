"""SimpleApp.py"""
#
# Counts the number of words in the given file and saves the result into an output directory.
#
# How to Run:
#
# $SPARK_HOME/bin/spark-submit \
# src/main/python/app/streaming/network_word_count_app.py
#
# Then, any lines typed in the terminal running the Netcat server will be counted and printed on screen every second:
#
#  $ nc -lk 9999
#  hello world
#
# Prints:
#   -------------------------------------------
#   Time: 2015-07-30 18:50:50
#   -------------------------------------------
#   (u'hello', 1)
#   (u'world', 1)

#!/usr/bin/python

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from network_word_count import NetworkWordCount

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "Network WordCount App")
ssc = StreamingContext(sc, 1)

# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 9999)

# Count each word in each batch
wordCounts = NetworkWordCount().wc(lines)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()

# Start the computation
ssc.start()

# Wait for the computation to terminate
ssc.awaitTermination()