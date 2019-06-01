from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

def loadMovieNames():
    movieNames = {}
    with open ("/Users/avinashtadi/Documents/workspace/SparkCourse/ml-100k/u.ITEM",encoding='utf-8',errors='replace') as f:
        for line in f:
            field = line.split('|')
            movieNames[int (field[0])] = field[1]
        return movieNames

lines = sc.textFile("/Users/avinashtadi/Documents/workspace/SparkCourse/ml-100k/u.data")
namedict = sc.broadcast(loadMovieNames())

movies = lines.map(lambda x: (int(x.split()[1]),1))

flip = movies.reduceByKey(lambda x,y: x+y )

result = flip.map(lambda a : (a[1],a[0]) )

tresults = result.sortByKey()


fresults = tresults.map(lambda cm : (namedict.value[cm[1]] , cm[0]))

mresults = fresults.collect()

for mresult in mresults:
    print(mresult[0],mresult[1])
    
