from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("/Users/avinashtadi/Documents/workspace/SparkCourse/ml-100k/u.data")
movies = lines.map(lambda x: (x.split()[1],1))

flip = movies.reduceByKey(lambda x,y: x+y )

result = flip.map(lambda a : (a[1],a[0]) )

print (result)
tresults=result.sortByKey().collect()
for tresult in tresults:
    print(tresult[1],tresult[0])
    
