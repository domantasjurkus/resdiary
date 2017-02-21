from collections import Counter, defaultdict
import heapq
import itertools
import os.path,shutil

from pyspark.mllib.recommendation import  MatrixFactorizationModel,ALS as SparkALS
from pyspark.sql import SQLContext
from pyspark.sql.dataframe import DataFrame

from base import Base
from data import Data
from evaluator import evaluate
from config import Config

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

    def learn_hyperparameters(self, data):
        '''Takes a DataFrame of bookings and uses the evaluator to learn optimal
        values for all the hyperparameters.'''
        raise NotImplementedError("Don't use this class, extend it")

class System(Recommender):
    '''Combines other recommenders to issue the final recommendations for each
    user.'''

    def __init__(self, spark):
        super(System, self).__init__(spark)
        # initialize all the recommenders
        self.recommenders = {
            'als': ALS(self.spark),
            'implicit': ImplicitALS(self.spark)
        }

        self.coefficients = {
            'als': Config.get("ALS", "weight"),
            'implicit': Config.get("ImplicitALS", "weight")
        }
        self.recommendations_per_user = Config.get("Default", "recs_per_user")

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

    def learn_hyperparameters(self, data):
        recommenders = self.recommenders.keys()
        best_evaluation = 0
        # for each combination of coefficients that we are considering
        # (the last coefficient is calculated as
        # range_stop_value - sum(coefficients))
        top = 3 # TODO: transfer the range values into the config
        combinations = itertools.product(range(1, top),
                                         repeat=len(recommenders) - 1)
        for coefficients in map(lambda c: c + (top - sum(c),), combinations):
            # apply the coefficients
            for i, recommender in enumerate(recommenders):
                self.coefficients[recommender] = coefficients[i]
            # keep track of the best coefficients
            evaluation = evaluate(self.spark, self, data)
            if evaluation > best_evaluation:
                best_evaluation = evaluation
                best_coefficients = coefficients
        print best_coefficients # TODO: write them to a config file

class ALS(Recommender):
    '''Generates recommendations based on review data.'''

    def train(self, bookings, save):
        if not isinstance(bookings, DataFrame):
            raise TypeError('Recommender requires a DataFrame')
        data = Data(self.spark)
        ratings = data.get_bookings_with_score(bookings)

        r = Config.get("ALS", "rank")
        i = Config.get("ALS", "iterations")
        l = Config.get("ALS", "lambda", float)
        self.spark.setCheckpointDir("./checkpoints/")
        model_location = "models/ALS/{rank}-{iterations}-{alpha}".format(rank=r,iterations=i,alpha=l)

        if os.path.isdir(model_location) and save.lower() != 'true':
            self.model =  MatrixFactorizationModel.load(self.spark, model_location)
        else:
            self.model = SparkALS.train(ratings, r, i, l)
            if os.path.isdir(model_location):
                shutil.rmtree(model_location)
            self.model.save(self.spark, model_location)

    def predict(self, data, location):
        if data.isEmpty():
            raise ValueError('RDD is empty')

        customer_id =data.filter(lambda r: (location in r[4])).map(lambda r: (r[0])).distinct()
        print "Customers: " + str(customer_id.count())
        restaurant_id = data.filter(lambda r: (location in r[4])).map(lambda r: (r[1])).distinct()
        print "Restaurants: " + str(restaurant_id.count())
        data = customer_id.cartesian(restaurant_id)
        print "Recommendations to be generated: " + str(data.count())
        predictions = self.model.predictAll(data)
        schema = ['userID', 'restaurantID', 'score']
        return SQLContext(self.spark).createDataFrame(predictions, schema)

class ImplicitALS(Recommender):
    '''Generates recommendations based on how many times a diner visited a
    restaurant.'''

    def train(self, bookings, save):
        # calculate how many times a diner visited each restaurant
        data = defaultdict(Counter)
        for booking in bookings.collect():
            data[booking['Diner Id']][booking['Restaurant Id']] += 1

        # transform that data into an RDD and train the model
        data = [(diner, restaurant, score) for diner, counter in data.items()
                for restaurant, score in counter.iteritems()]

        r = Config.get("ImplicitALS", "rank")
        i = Config.get("ImplicitALS", "iterations")
        a = Config.get("ImplicitALS", "alpha", float)
        self.spark.setCheckpointDir("./checkpoints/")
        model_location = "models/ImplicitALS/{rank}-{iterations}-{alpha}".format(rank=r,iterations=i,alpha=a)

        if os.path.isdir(model_location) and save.lower() != 'true':
            self.model =  MatrixFactorizationModel.load(self.spark, model_location)
        else:
            self.model = SparkALS.trainImplicit(self.spark.parallelize(data), r, i, alpha=a)
            shutil.rmtree(model_location)
            self.model.save(self.spark, model_location)
        

    def predict(self, data):
        predictions = self.model.predictAll(data)
        schema = ['userID', 'restaurantID', 'score']
        return SQLContext(self.spark).createDataFrame(predictions, schema)
