import findspark
findspark.init()
import pyspark
import sys
import ast
import json
if len(sys.argv) != 3:
        raise Exception("Exactly 2 arguments are required: <wordlist> <weights>")
wordlist = sys.argv[1]
weights = sys.argv[2]
# wordlist = ['the']
# weights = {'a':3}
# print(weights)
# weights = json.loads(weights)
#ast.literal_eval(weights)
# print(type(weights))
def mapper(sentence): #takes a sentence as input, provides an output numerical value
        score = 0
        for letter in sentence:
           #  print(weights)
            if letter in weights.keys():
                score = score + weights[letter]
        # print(score)
        return score
def reducer(targetWord): #Merge two values with a common key - operation must be assoc and commut
        currentMax = 0
        currentMaxSent = ""
        for element in lines.collect():
            if targetWord in element:
                tmp = mapper(element)
                if tmp > currentMax:
                    currentMax = tmp
                    currentMaxSent = element
        return currentMaxSent
sc = pyspark.SparkContext()
print('Spark Context initialized')
#testFile --> take the address of a text file. return it as a hadoop dataset of strings
lines = sc.textFile('War_and_Peace.txt')
toReturn = []
for w in wordlist:
    toReturn.append(reducer(w))
print(toReturn)
