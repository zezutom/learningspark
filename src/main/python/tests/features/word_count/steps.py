from lettuce import *
from nose.tools import assert_equals
from app.word_count import WordCount

## https://gist.github.com/ksonj/e2057deb28a44789fbb0
def add_pyspark_path():
    """
    Add PySpark to the PYTHONPATH
    Thanks go to this project: https://github.com/holdenk/sparklingpandas
    """
    import sys
    import os
    try:
        sys.path.append(os.path.join(os.environ['SPARK_HOME'], "python"))
        sys.path.append(os.path.join(os.environ['SPARK_HOME'],
                                     "python","lib","py4j-0.8.2.1-src.zip"))
    except KeyError:
        print "SPARK_HOME not set"
        sys.exit(1)

add_pyspark_path() # Now we can import pyspark
from pyspark import SparkContext


@step(u'I am using Apache Spark')
def init_spark_context(step):
    print ('Obtaining Spark context...')
    world.sc = SparkContext("local", "WordCount Test")

@step(u'I am using the WordCount')
def select_word_count(step):
    print ('Trying to use WordCount...')
    world.worldCount = WordCount()

@step(u'I provide no content')
def given_i_provide_no_content(step):
    lines = []
    world.result = world.worldCount.wc(world.sc.parallelize(lines)).collect()

@step(u'I should see no result')
def result(step):
    actual_result = world.result
    assert_equals([], actual_result)