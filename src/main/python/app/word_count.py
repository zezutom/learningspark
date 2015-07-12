from operator import add


class WordCount:
    def wc(self, data):
        return data.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(add)