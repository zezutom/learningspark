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
    if not hasattr(world, 'sc'):
        world.sc = SparkContext("local", "WordCount Test")

@step(u'I am using WordCount')
def init_word_count(step):
    print ('Obtaining WordCount...')
    if not hasattr(world, 'wordCount'):
        world.wordCount = WordCount()

@step(u'I provide no content')
def given_i_provide_no_content(step):
    lines = []
    world.empty_result = world.wordCount.wc(world.sc.parallelize(lines)).collect()

@step(u'I should see no result')
def empty_result(step):
    actual_result = world.empty_result
    assert_equals([], actual_result)

@step(u'I provide Robert Frost\'s famous quote')
def given_i_provide_the_quote(step):
    lines = [
        u'Two roads diverged in a wood, and I,',
        u'I took the one less traveled by',
        u'And that has made all the difference.'
    ]
    world.quote_result = world.wordCount.wc(world.sc.parallelize(lines)).collect()

@step(u'I should see the expected word count')
def quote_result(step):
    actual_result = world.quote_result
    assert_equals([
        (u'a', 1), (u'all', 1), (u'and', 2), (u'by', 1), (u'difference', 1), (u'diverged', 1),
        (u'has', 1), (u'i', 2), (u'in', 1), (u'less', 1), (u'made', 1), (u'one', 1),
        (u'roads', 1), (u'that', 1), (u'the' ,2), (u'took', 1), (u'traveled' ,1), (u'two', 1), (u'wood', 1)
    ], actual_result)