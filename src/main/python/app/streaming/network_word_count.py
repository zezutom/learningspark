from operator import add


class NetworkWordCount:
    def wc(self, data):
        # Split on any white character
        # Only count words as non-empty strings
        # Remove punctuation and make all words lowercase
        # Sort alphabetically
        return data.flatMap(lambda x: x.split()) \
            .filter(lambda x: x) \
            .map(lambda x: x.replace(',', '').replace('.', '').lower())\
            .map(lambda x: (x, 1)).reduceByKey(add)