from collections import Counter, defaultdict
import heapq

from pyspark.mllib.recommendation import ALS as SparkALS
from pyspark.sql import SQLContext
from pyspark.sql.dataframe import DataFrame

from base import Base
from data import Data

class Recommender(Base):
    '''All recommenders should extend this class. Enforces a consistent
    interface.'''

    def train(self, data):
        """Takes a DataFrame of bookings. Doesn't return anything."""
        raise NotImplementedError("Don't use this class, extend it")

    def predict(self, data):
        '''Takes an RDD list of (userID, restaurantID) pairs and returns a
        DataFrame with the schema:
        Recommendation(userID, restaurantID, score).'''
        raise NotImplementedError("Don't use this class, extend it")

class System(Recommender):
    '''Combines other recommenders to issue the final recommendations for each user'''

    def __init__(self, spark):
        super(System, self).__init__(spark)
        # initialize all the recommenders
        self.recommenders = {'als': ALS(self.spark),
                             'implicit': ImplicitALS(self.spark)}
        # TODO: load the coefficients from a config file
        self.coefficients = {'als': 1, 'implicit': 1}
        self.recommendations_per_user = 3 # TODO: ditto

    def train(self, data):
        for recommender in self.recommenders.values():
            recommender.train(data)

    def predict(self, data):
        # tally up the scores for each (user, restaurant) pair multiplied by the
        # coefficients
        scores = defaultdict(lambda: defaultdict(int))
        for name, model in self.recommenders.iteritems():
            for row in model.predict(data).collect():
                partial_score = self.coefficients[name] * row['score']
                scores[row['userID']][row['restaurantID']] += partial_score

        # for each user find the top self.recommendations_per_user restaurants
        recommendations = {}
        for user, reviews in scores.iteritems():
            recommendations[user] = heapq.nlargest(
                self.recommendations_per_user, reviews.iteritems(),
                key=lambda r: r[1])
        # put the recommendations into an appropriate format
        top_recommendations = [(user, restaurant)
                               for user, restaurants in recommendations.items()
                               for restaurant, rating in restaurants]
        schema = ['userID', 'restaurantID']
        return SQLContext(self.spark).createDataFrame(top_recommendations,
                                                      schema)

class ALS(Recommender):
    '''Generates recommendations based on review data.'''

    def train(self, bookings):
        if not isinstance(bookings, DataFrame):
            raise TypeError('Recommender requires a DataFrame')
        data = Data(self.spark)
        ratings = data.get_bookings_with_score(bookings)
        self.model = SparkALS.train(ratings, 12, 10, 0.1)

    def predict(self, data):
        if data.isEmpty():
            raise ValueError('RDD is empty')
        predictions = self.model.predictAll(data)
        schema = ['userID', 'restaurantID', 'score']
        return SQLContext(self.spark).createDataFrame(predictions, schema)

class ImplicitALS(Recommender):
    '''Generates recommendations based on how many times a diner visited a
    restaurant.'''

    def train(self, bookings):
        # calculate how many times a diner visited each restaurant
        data = defaultdict(Counter)
        for booking in bookings.collect():
            data[booking['Diner Id']][booking['Restaurant Id']] += 1

        # transform that data into an RDD and train the model
        data = [(diner, restaurant, score) for diner, counter in data.items()
                for restaurant, score in counter.iteritems()]
        self.model = SparkALS.trainImplicit(self.spark.parallelize(data), 12, 10,
                                            alpha=0.01)

    def predict(self, data):
        predictions = self.model.predictAll(data)
        schema = ['userID', 'restaurantID', 'score']
        return SQLContext(self.spark).createDataFrame(predictions, schema)
