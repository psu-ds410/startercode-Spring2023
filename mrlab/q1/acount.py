from HDFS import wap   

# Change the class name!!
class Acount(wap):  
    def mapper(self, key, line):
        words = line.split()
        count = 0
        for w in words:
            if "a" in words:
                yield (w, 1)
                count = count + 1
        yield count

    def reducer(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    Acount.run()   
