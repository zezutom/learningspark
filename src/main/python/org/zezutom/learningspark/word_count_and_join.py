import os.path
from operator import add


class WordCountAndJoin:
    def __init__(self, sc):
        self.sc = sc

    def wc_join(self, keyword, f1, f2):
        wc_out1 = self.wc(keyword, f1)
        wc_out2 = self.wc(keyword, f2)
        return wc_out1.join(wc_out2).collect()

    def wc(self, keyword, f):
        if not os.path.isfile(f): raise Exception("No such file: %s" % f)
        data = self.sc.textFile(f)
        return data.flatMap(lambda x: x.split(' ')).filter(lambda x: x == keyword).map(lambda x: (x, 1)).reduceByKey(add)