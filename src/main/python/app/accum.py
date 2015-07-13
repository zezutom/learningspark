
class Accum:
    def sum(self, nums, accum):
        nums.foreach(lambda x: accum.add(x))
        return accum.value