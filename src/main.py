from pyspark import SparkContext
from data import read, filter_outliers
from evaluator import evaluate
from recommenders import initial

sc = SparkContext('local', 'Recommendation Engine')
print '{:.3f}% of the recommendations were good'.format(evaluate(
    sc, initial.generate_recommendations,
    filter_outliers(sc, read(sc, 'Booking.csv'))))
sc.stop()
