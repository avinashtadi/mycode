import re
from pyspark import SparkConf, SparkContext

def parsedata(text):
    line = text.split(',')
    cus = int(line[0])
    cost = float(line[2])
    return (cus,cost)

conf = SparkConf().setMaster("local").setAppName("CustomerSum")
sc = SparkContext(conf = conf)

input = sc.textFile("/Users/avinashtadi/Documents/workspace/SparkCourse/customer-orders.csv")
cuskv = input.map(lambda x: parsedata(x))

cussum = cuskv.reduceByKey(lambda x, y: x + y)
print (cussum)
transpose = cussum.map(lambda x : (x[1],x[0]))
tresults = transpose.sortByKey() 
results = tresults.collect()
for result in results:
   print(result[1],result[0])
