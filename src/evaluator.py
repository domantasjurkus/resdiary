from pyspark.sql import SQLContext
from collections import defaultdict
from sets import Set
from data import Data
from config import Config

class Evaluator(object):

    def __init__(self, spark, algorithm config=Config):
        super(Evaluator, self).__init__(spark, algorithm)
        self.config = config      

    def calculate_mse(actual, predictions):
        actual = actual.rdd.map(lambda r: ((r[0], r[1]), r[2]))
        rates_and_preds = actual.join(predictions.rdd)
        return rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean()




# def evaluate(model, bookings, config=Config):
#     '''Takes a SparkContext instance, a recommendation model and a DataFrame of
#     bookings, feeds the earlier half of the data (for each user) to the
#     algorithm and returns a ratio right/total, where 'right' is the number of
#     recommended restaurants that were later visited and 'total' is the total
#     number of recommendations.'''
#     data = Data(spark, config)
#     # filter out half of the data to partial_data and store the remaining
#     # restaurant IDs
#     answers = defaultdict(Set)
#     # read all the data and separate by diner id
#     bookings = defaultdict(list)
#     for booking in bookings.collect():
#         bookings[booking['Diner Id']].append(booking)
#     partial_data = []
#     for personal_bookings in bookings.values():
#         num_bookings = len(personal_bookings)
#         if num_bookings < 2:
#             # not enough bookings to use this user for evaluation
#             continue
        
#         first_test_index = int(num_bookings * Config.get("DEFAULT", "training_percent", float))
#         if first_test_index == num_bookings-1:
#             first_test_index -= 1

#         # sort by visit time
#         personal_bookings.sort(key=lambda r: r['Visit Time'])
#         # the first half of the data is used for training/predictions
#         for booking in personal_bookings[:first_test_index]:
#             partial_data.append(booking)
#         # store restaurant IDs from the remaining rows
#         for booking in personal_bookings[first_test_index + 1:]:
#             answers[booking['Diner Id']].add(booking['Restaurant Id'])

#     # create a DataFrame of training data, call the algorithm, evaluate the
#     # results
#     partial_data = spark.parallelize(partial_data)
#     model.train(SQLContext(spark).createDataFrame(partial_data,
#                                                   schema=bookings.schema))
#     recommendations = model.predict(data.nearby_restaurants(bookings))

#     # Edge case - consider returning None
#     if not recommendations:
#         return 0.0

#     right = 0
#     total = 0
#     for recommendation in recommendations.collect():
#         total += 1
#         if recommendation['restaurantID'] in answers[recommendation['userID']]:
#             right += 1
#     return float(right) / total
