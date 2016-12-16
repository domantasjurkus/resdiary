from pyspark import SparkContext
from data import read, filter_outliers

sc = SparkContext('local', 'Recommendation Engine')
filter_outliers(sc, read(sc, 'Booking.csv'))
sc.stop()
