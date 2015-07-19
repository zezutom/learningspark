from lettuce import *
from nose.tools import assert_equals
from app.word_count_and_join import WordCountAndJoin

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
    if not hasattr(world, 'sc'):
        world.sc = SparkContext("local", "WordCountAndJoin Test")

@step(u'I am using WordCountAndJoin')
def init_spark_context(step):
    print ('Obtaining WordCountAndJoin...')
    if not hasattr(world, 'wordCountAndJoin'):
        world.wordCountAndJoin = WordCountAndJoin()

@step(u'I provide two different texts about "a spark" and "Apache Spark"')
def given_i_provide_two_different_texts(step):
    lines1 = [
        u'Apache Spark is a fast and general-purpose cluster computing system.',
        u'It also supports a rich set of higher-level tools including Spark SQL and Spark Streaming.'
    ]
    lines2 = [
        u'Spark (fire), a small glowing particle or ember.',
        u'In pyrotechnics, iron filings and metal alloys such as magnalium may be used to create sparks.',
        u'Scorched linen was commonly used as tinder to catch the spark and start ',
        u'the fire but producing a good spark could take much time.'
    ]
    world.sum_result = world.wordCountAndJoin.wc_join(u'Spark', world.sc.parallelize(lines1), world.sc.parallelize(lines2))

@step(u'I should see the expected word count per text')
def sum_result(step):
    actual_result = world.sum_result
    assert_equals([(u'spark', (3, 3))], actual_result)
