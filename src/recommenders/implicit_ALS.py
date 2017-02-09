from collections import Counter, defaultdict
from pyspark.mllib.recommendation import ALS, Rating
from pyspark.sql import SQLContext
from recommenders.recommender import Recommender

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
        self.model = ALS.trainImplicit(self.spark.parallelize(data), 12, 10,
                                       alpha=0.01)

    def predict(self, data):
        predictions = self.model.predictAll(data)
        schema = ['userID', 'restaurantID', 'score']
        return SQLContext(self.spark).createDataFrame(predictions, schema)
