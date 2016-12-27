import data
from pyspark import SparkContext
from evaluator import evaluate
from recommenders import initial

sc = SparkContext('local', 'Recommendation Engine')
bookings = data.read(sc, 'Booking.csv')

# data for the front end
data.write('Recommendations.csv', initial.generate_recommendations(sc, bookings))

# evaluation
print '{:.3f}% of the recommendations were good'.format(evaluate(
    sc, initial.generate_recommendations,
    data.filter_outliers(sc, bookings)))

sc.stop()
