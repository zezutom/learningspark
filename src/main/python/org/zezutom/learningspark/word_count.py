import os.path
from operator import add


class WordCount:
    def __init__(self, sc):
        self.sc = sc

    def wc(self, f):
        if not os.path.isfile(f): raise Exception("No such file: %s" % f)
        data = self.sc.textFile(f)
        return data.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(add)