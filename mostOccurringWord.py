#This program returns top five most occurring words in a file 
#with their frequency...


from pyspark import SparkContext

sc = SparkContext("local[*]")

fileRDD = sc.textFile('../../tmp/syslog.log')

wordRDD = fileRDD.flatMap(lambda x: x.split(" "))

pairRDD = wordRDD.filter(lambda x:not x.isspace()).map(lambda x: (x,1))

wordCountRDD = pairRDD.reduceByKey(lambda x,y : x+y)

countAsKeyRDD = wordCountRDD.map(lambda (x,y) : (y,x))

#group by key, just that there many be many key with same freq.
#sort by key False - sorts in descending.
sortedRDD = countAsKeyRDD.groupByKey().sortByKey(False)

#prints top 5 most occurring words with their frequency
result = sortedRDD.take(5)

for k, li in result:
   print(k) 
   for p in li: print p
