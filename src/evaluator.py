from collections import defaultdict
from math import sqrt
from sets import Set

from pyspark.sql import SQLContext
from pyspark.mllib.recommendation import  MatrixFactorizationModel, ALS  

from base import Base
from config import Config
from data import Data

class Evaluator(Base):

    def __init__(self, spark, algorithm, config=Config):
        super(Evaluator, self).__init__(spark)
        self.config = config
        self.algorithm = algorithm

    def mse_evaluation(self, bookings):
        bookings = Data(self.spark).get_bookings_with_score(bookings)
        training_percentage = Config.get('DEFAULT', 'training_percentage', float)
        data, test_ratings = bookings.randomSplit([training_percentage,
                                                   1 - training_percentage])
        testdata = test_ratings.rdd.map(lambda r: (r[0], r[1]))
        self.algorithm.train(data)

        predictions = self.algorithm.predict(testdata)
        mse = self.calculate_mse(test_ratings, predictions)
        print "Mean Squared Error for {} recommender: {:.3f}".format(
            self.algorithm.get_algorithm_name(), mse)
        print "Root Mean Squared Error for {} recommender: {:.3f}".format(
            self.algorithm.get_algorithm_name(), sqrt(mse))

    def calculate_mse(self, actual, predictions):
        actual = actual.rdd.map(lambda r: ((r[0], r[1]), r[2]))
        rates_and_preds = actual.join(predictions.rdd.map(lambda r: ((r[0], r[1]), r[2])))
        return rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean()

    def right_total_evaluation(self, bookings_data):
        '''Takes a SparkContext instance, a recommendation model and a DataFrame of
        bookings, feeds the earlier half of the data (for each user) to the
        algorithm and returns a ratio right/total, where 'right' is the number of
        recommended restaurants that were later visited and 'total' is the total
        number of recommendations.'''
        model = self.algorithm
        data = Data(self.spark, self.config)
        # filter out half of the data to partial_data and store the remaining
        # restaurant IDs
        answers = defaultdict(Set)
        # read all the data and separate by diner id
        bookings = defaultdict(list)

        for booking in bookings_data.collect():
            bookings[booking['Diner Id']].append(booking)
        partial_data = []
        for personal_bookings in bookings.values():
            num_bookings = len(personal_bookings)
            if num_bookings < 2:
                # not enough bookings to use this user for evaluation
                continue
            
            first_test_index = int(num_bookings * Config.get("DEFAULT",
                                                             "training_percentage",
                                                             float))
            if first_test_index == num_bookings-1:
                first_test_index -= 1

            # sort by visit time
            personal_bookings.sort(key=lambda r: r['Visit Time'])
            # the first half of the data is used for training/predictions
            for booking in personal_bookings[:first_test_index]:
                partial_data.append(booking)
            # store restaurant IDs from the remaining rows
            for booking in personal_bookings[first_test_index + 1:]:
                answers[booking['Diner Id']].add(booking['Restaurant Id'])

        # create a DataFrame of training data, call the algorithm, evaluate the
        # results
        partial_data = self.spark.parallelize(partial_data)
        model.train(SQLContext(self.spark).createDataFrame(partial_data,
                                                      schema=bookings_data.schema))
        recommendations = model.predict(data.nearby_restaurants(bookings_data))

        # Edge case - consider returning None
        if not recommendations:
            return 0.0

        right = 0
        total = 0
        for recommendation in recommendations.collect():
            total += 1
            if recommendation['restaurantID'] in answers[recommendation['userID']]:
                right += 1
        return float(right) / total

    def evaluate(self, bookings):
        from recommenders import System
        
        if isinstance(self.algorithm, System):
            print '{}: {:.3f}%'.format(self.algorithm.get_algorithm_name(),
                                       self.right_total_evaluation(bookings))
        else:
            self.mse_evaluation(bookings)
