from pyspark import SparkContext
from numpy import array
from math import sqrt

from pyspark.mllib.clustering import KMeans, KMeansModel

logFile = "/home/edward/spark-2.0.2-bin-hadoop2.7/README.md"  # Should be some file on your system
sc = SparkContext("local", "User clustering")
logData = sc.textFile(logFile).cache()

repeat_diners = sc.textFile("repeat_diners.csv") # .map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda line: (line[0],line[1])).collect()
restaurants = sc.textFile("restaurants.csv") #.map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda line: (line[0],line[1])).collect()
parsedData = repeat_diners.map(lambda line: array([float(x) for x in line.split(',')]))

# Build the model (cluster the data)
clusters = KMeans.train(parsedData, 2, maxIterations=10,
                        runs=10, initializationMode="random")

# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))

# Save and load model
clusters.save(sc, "/home/edward/uni/resdiary/src/KMeansModel")
sameModel = KMeansModel.load(sc, "/home/edward/uni/resdiary/src/KMeansMod")