from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("/Users/avinashtadi/Documents/workspace/SparkCourse/fakefriends.csv")
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda a: (a, 1)).reduceByKey(lambda c, d: (c[0] + d[0], c[1] + d[1]))
averagesByAge = totalsByAge.mapValues(lambda c: c[0] / c[1])
results = averagesByAge.collect()
for result in results:
    print(result)
