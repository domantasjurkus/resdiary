import data
from pyspark import SparkContext
from evaluator import evaluate
from recommenders import initial, ALS

sc = SparkContext('local', 'Recommendation Engine')
bookings = data.filter_outliers(sc, data.read(sc, 'Booking.csv'))

# data for the front end
data.write('Recommendations.csv', ALS.generate_recommendations(sc, bookings))

# evaluation
for name, algorithm in [('Initial', initial), ('ALS', ALS)]:
    print '{}: {:.3f}%'.format(
        name, evaluate(sc, algorithm.generate_recommendations, bookings))

sc.stop()
