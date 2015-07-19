from lettuce import *
from nose.tools import assert_equals
from app.accum import Accum

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
        world.sc = SparkContext("local", "Accumulator Test")

@step(u'I am using Accumulator')
def init_accumulator(step):
    print ('Obtaining Spark context...')
    if not hasattr(world, 'accum'):
        world.accum = Accum()
        world.acc_zero = world.sc.accumulator(0)

@step(u'I provide an empty set')
def given_i_provide_empty_set(step):
    world.sum_empty = world.accum.sum(world.sc.parallelize([]), world.acc_zero)

@step(u'The resulting sum should be zero')
def the_sum_should_be_zero(step):
    assert_equals(0, world.sum_empty)

@step(u'I provide a sequence of 1 to 4')
def given_i_provide_nonempty_set(step):
    world.sum = world.accum.sum(world.sc.parallelize([1, 2, 3, 4]), world.acc_zero)

@step(u'The resulting sum should be ten')
def the_sum_should_be_ten(step):
    assert_equals(10, world.sum)