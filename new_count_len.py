import findspark
findspark.init()

import pyspark
import sys

if len(sys.argv) != 3:
	raise Exception("Exactly 2 arguments are required: <inputUri> <OutputUri>")

inputUri = sys.argv[1]
outputUri = sys.argv[2]

def myMapFunc(x): #takes an input, provides an output string
	val = len(x)
	return(val, 1)
def myReduceFunc(v1, v2): #Merge two values with a common key - operation must be assoc and commut
	return v1 + v2

sc = pyspark.SparkContext()
print('Spark Context initialized')
#testFile --> take the address of a text file. return it as a hadoop dataset of strings
lines = sc.textFile(sys.argv[1])

#for element in lines.collect():
#    print(element)
#flatmap --> apply a function to each element of the dataset, then flatten the result
#words = lines.flatMap(lambda line: line.split())

wordCounts = lines.map(myMapFunc).reduceByKey(myReduceFunc)
print("Operations complete")

res = {}
for element, element2 in wordCounts.collect():
	res[element] = element2

print(res)
#wordCounts.saveAsTextFile(sys.argv[2])
#print("Output as text file")
