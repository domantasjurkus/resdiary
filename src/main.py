from pyspark import SparkContext
from data import read, filter_outliers
from recommenders import ALS

sc = SparkContext('local', 'Recommendation Engine')
ALS.generate_recommendations(sc, filter_outliers(sc, read(sc, 'Booking.csv')))
sc.stop()
