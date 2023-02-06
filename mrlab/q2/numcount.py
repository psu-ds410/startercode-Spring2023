from HDFS import wap   

# Change the class name!!
class NumCount(wap):  
    def mapper(self, key, line):
        words = line.split()
        count = dict()
        for w in words:
            yield (w, 1)
            if count[w] >= 10
              yield w

    def reducer(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    NumCount.run()   
