from operator import add


class WordCountAndJoin:
    def wc_join(self, keyword, data1, data2):
        wc_out1 = self.wc(keyword, data1)
        wc_out2 = self.wc(keyword, data2)
        return wc_out1.join(wc_out2).collect()

    def wc(self, keyword, data):
        # Split on any white character
        # Only count words as non-empty strings
        # Remove punctuation and make all words lowercase
        # Sort alphabetically
        return data.flatMap(lambda x: x.split()) \
            .filter(lambda x: x) \
            .map(lambda x: x.replace(',', '').replace('.', '').lower()) \
            .filter(lambda x: x == keyword.lower()) \
            .map(lambda x: (x, 1)).reduceByKey(add)