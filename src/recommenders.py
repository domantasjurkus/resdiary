from abc import ABCMeta, abstractmethod
from collections import Counter, defaultdict
import heapq
from itertools import product, starmap
import os.path
import shutil,sys
import csv

from pyspark.mllib.recommendation import  MatrixFactorizationModel, ALS as SparkALS
from pyspark.sql import SQLContext
from pyspark.sql.dataframe import DataFrame

from base import Base
from data import Data
from evaluator import *
from config import Config

class Recommender(Base):
    '''All recommenders should extend this class. Enforces a consistent
    interface.'''

    __metaclass__ = ABCMeta

    @abstractmethod
    def train(self, data, load=False):
        """Takes a DataFrame of bookings. Doesn't return anything."""
        raise NotImplementedError("Don't use this class, extend it")

    @abstractmethod
    def predict(self, data):
        '''Takes an RDD list of (userID, restaurantID) pairs and returns a
        DataFrame with the schema:
        Recommendation(userID, restaurantID, score).'''
        raise NotImplementedError("Don't use this class, extend it")

    def learn_hyperparameters(self, data, save=True):
        '''Takes a DataFrame of bookings and learns optimal values for all the
        hyperparameters.'''
        raise NotImplementedError("Don't use this class, extend it")

class System(Recommender):
    '''Combines other recommenders to issue the final recommendations for each
    user.'''

    def __init__(self, spark):
        super(System, self).__init__(spark)
        recommenders = Config.get_recommenders()
        self.recommenders = dict((name, eval(name)(self.spark))
                                 for name in recommenders)
        self.weights = dict((name, Config.get(name, 'weight'))
                                 for name in recommenders)
        self.recommendations_per_user = Config.get("System", "recs_per_user")

    def train(self, data, load=False):
        for name, recommender in self.recommenders.iteritems():
            recommender.train(data)
            
    def predict(self, data):
        # tally up the scores for each (user, restaurant) pair multiplied by the
        # weights
        scores = defaultdict(lambda: defaultdict(int))
        for name, model in self.recommenders.iteritems():
            for row in model.predict(data).collect():
                partial_score = self.weights[name] * row['score']
                scores[row['userID']][row['restaurantID']] += partial_score

        # for each user find the top self.recommendations_per_user restaurants
        recommendations = {}
        for user, reviews in scores.iteritems():
            recommendations[user] = heapq.nlargest(
                self.recommendations_per_user, reviews.iteritems(),
                key=lambda r: r[1])

        # put the recommendations into an appropriate format
        top_recommendations = [(user, restaurant, float(score))
                               for user, restaurants in recommendations.items()
                               for restaurant, score in restaurants]
        return SQLContext(self.spark).createDataFrame(top_recommendations,
                                                      Config.get_schema())

    def learn_hyperparameters(self, data, save=True):
        recommenders = self.recommenders.keys()
        best_evaluation = 0
        maximum_weight = Config.get('System', 'maximum_weight')
        # for each combination of weights that we are considering
        for weights in self.generate_weights(maximum_weight):
            # apply the weights
            for i, recommender in enumerate(recommenders):
                self.weights[recommender] = weights[i]
            # keep track of the best weights
            evaluation = evaluate(self.spark, self, data)
            if evaluation > best_evaluation:
                best_evaluation = evaluation
                best_weights = weights
        # write the new combination to the config file
        if save == True:
            Config.set_weights(best_weights)

    def generate_weights(self, maximum_weight):
        '''Takes a maximum weight and generates all possible combinations of
        weights from the range [0, maximum_weight], removing redundant
        options.'''
        num_recommenders = len(self.recommenders)
        weights = set(product(range(maximum_weight + 1),
                              repeat=num_recommenders))
        # remove unnecessary duplication
        # (e.g., remove (2, 2, 2) if we already have (1, 1, 1))
        weights.discard(tuple([0] * num_recommenders))
        for k in range(2, maximum_weight + 1):
            weights -= set([tuple(map(lambda c: k * c, t)) for t in weights])
        return weights

class ALS(Recommender):

    __metaclass__ = ABCMeta
    
    def train(self, bookings, parameters=None, load=False):
        recommender_name = type(self).__name__
        if load:
            self.model = self.load_model(recommender_name)
        else:
            self.model = self.create_model(bookings, self.get_parameters(recommender_name))
            model_location = self.get_model_location(recommender_name)
            if os.path.isdir(model_location):
                shutil.rmtree(model_location)
            self.model.save(self.spark, model_location)

    @abstractmethod
    def create_model(self, bookings, parameters):
        '''Takes a DataFrame of bookings and a dictionary of parameters
        Returns a trained model.'''
        raise NotImplementedError("Each subclass should extend this")

    def load_model(self, recommender_name, parameters=None):
        ''' Takes recommender's name, loads an existing model and returns it.
        If the model does not exist it returns None'''
        model_location = self.get_model_location(recommender_name)

        if os.path.isdir(model_location):
            self.model  = MatrixFactorizationModel.load(self.spark, model_location) 
            return self.model 
        else:
            return None

    def predict(self, data):
        return SQLContext(self.spark).createDataFrame(
            self.model.predictAll(data), Config.get_schema())

    def get_parameters(self, recommender_name):
        return  dict((name, Config.get(recommender_name, name, t))
                              for name, t in [('rank', int),
                                              ('iterations', int),
                                              ('lambda', float)])

    def get_model_location(self,recommender_name):
        return "models/{}/".format(recommender_name) + '-'.join(
            map(str, self.get_parameters(recommender_name).values()))
        
    # float range function
    def frange(self,x, y, jump):
      while x < y:
        yield x
        x += jump

    def learn_hyperparameters(self, bookings): # pragma: no cover
        bookings = Data(self.spark).get_bookings_with_score(bookings)
        data, test_ratings = bookings.randomSplit([0.8, 0.2])
        testdata = test_ratings.rdd.map(lambda r: (r[0], r[1]))
        best_mse = float('inf')
        parameter_names = ['rank', 'iterations', 'lambda']
        types = [int, int, float]
        range_values = [(Config.get('DEFAULT', 'min_' + parameter, t),
                         Config.get('DEFAULT', 'max_' + parameter, t),
                         Config.get('DEFAULT', parameter + '_step', t))
                        for parameter, t in zip(parameter_names, types)]
        for parameters in map(lambda v: dict(zip(parameter_names, v)),
                              product(*range_values)):
            self.train(data, parameters)
            predictions = self.predict(testdata)
            mse = calculate_mse(test_ratings, predictions)
            if mse < best_mse:
                best_parameters = parameters
                best_mse = mse
        if best_mse < float('inf'):
            Config.set_hyperparameters(type(self).__name__, best_parameters)

class ExplicitALS(ALS):
    '''Generates recommendations based on review data.'''

    def train(self, bookings, load=False):
        if not isinstance(bookings, DataFrame):
            raise TypeError('Recommender requires a DataFrame')

        super(ExplicitALS, self).train(
            Data(self.spark).get_bookings_with_score(bookings), load=load)

    def create_model(self, bookings, parameters):
        return SparkALS.train(bookings, parameters['rank'],
                              parameters['iterations'], parameters['lambda'])

class ImplicitALS(ALS):
    '''Generates recommendations based on how many times a diner visited a
    restaurant.'''

    def train(self, bookings, load=False):
        if not isinstance(bookings, DataFrame):
            raise TypeError('Recommender requires a DataFrame')

        # calculate how many times a diner visited each restaurant
        data = defaultdict(Counter)
        for booking in bookings.collect():
            data[booking['Diner Id']][booking['Restaurant Id']] += 1
        # transform that data into an RDD and train the model
        data = [(diner, restaurant, score) for diner, counter in data.items()
                for restaurant, score in counter.iteritems()]
                
        super(ImplicitALS, self).train(self.spark.parallelize(data), load=load)

    def create_model(self, bookings, parameters):
        return SparkALS.trainImplicit(bookings, parameters['rank'],
                                      parameters['iterations'],
                                      alpha=parameters['lambda'],
                                      nonnegative=True)

class CuisineType(Recommender):
    '''Generates recommendations based on a diner's prefered cuisine types.'''
    
    def train(self, bookings, load=False):
        minimum_like_score = Config.get('CuisineType', 'minimum_score')

        # stores a set of cuisines for each restaurant
        self.restaurant_cuisine = defaultdict(set)
        filename = os.path.join(Config.get('DEFAULT', 'data_dir', str),
                                Config.get('CuisineType', 'filename', str))
        for entry in Data(self.spark).read(filename).collect():
            restaurant = entry['RestaurantId']
            self.restaurant_cuisine[restaurant].add(entry['CuisineTypeId'])

        # what cuisine types does this diner like?
        self.liked_cuisine = defaultdict(set)
        for booking in bookings.collect():
            diner = booking['Diner Id']
            restaurant = booking['Restaurant Id']
            score = booking['Review Score']
            if score >= minimum_like_score:
                self.liked_cuisine[diner] |= self.restaurant_cuisine[restaurant]

    def predict(self, diners_restaurants):
        # score is the number of cuisines that the diner and the restaurant
        # have in common
        diners_restaurants = diners_restaurants.collect()
        recommendations = [(diner, restaurant,
                            len(self.restaurant_cuisine[restaurant] &
                                self.liked_cuisine[diner]))
                           for diner, restaurant in diners_restaurants]
        recommendations =  self.spark.parallelize(recommendations)
        return SQLContext(self.spark).createDataFrame(
           recommendations, Config.get_schema())

class PricePoint(Recommender):
    '''For each diner, generates recommendations based on the average price
    point of all the visited restaurants.'''
    
    def train(self, bookings, load=False):
        # record the price point of each restaurant
        self.restaurant_price_point = {}
        filename = os.path.join(Config.get('DEFAULT', 'data_dir', str),
                                Config.get('DEFAULT', 'restaurant_file', str))
        for entry in Data(self.spark).read(filename).collect():
            price_point = entry['PricePoint']
            if price_point is not None:
                self.restaurant_price_point[entry['RestaurantId']] = price_point

        # record the price points of all restaurants visited by each diner
        diner_price_points = defaultdict(list)
        for booking in bookings.collect():
            diner = booking['Diner Id']
            restaurant = booking['Restaurant Id']
            score = booking['Review Score']
            if restaurant in self.restaurant_price_point:
                diner_price_points[diner].append(
                    self.restaurant_price_point[restaurant])

        # stores the average price point of each diner's visited restaurants
        self.diner_average_price_point = dict(
            (diner, sum(diner_price_points[diner]) /
             len(diner_price_points[diner])) for diner in diner_price_points)

        # calculate averages if possible, otherwise resort to default price
        # point value in the config file
        default_price_point = Config.get('PricePoint', 'default_price_point')
        self.restaurant_default_price_point = (
            sum(self.restaurant_price_point.values()) /
            len(self.restaurant_price_point.values())
            if self.restaurant_price_point else default_price_point)
        self.diner_default_price_point = (
            sum(self.diner_average_price_point.values()) /
            len(self.diner_average_price_point.values())
            if self.diner_average_price_point else default_price_point)

    def predict(self, diners_restaurants):
        diners_restaurants = diners_restaurants.collect()
        recommendations = []
        for diner, restaurant in diners_restaurants:
            restaurant_price_point = self.restaurant_price_point.get(
                restaurant, self.restaurant_default_price_point)
            diner_price_point = self.diner_average_price_point.get(
                diner, self.diner_default_price_point)
            score = (Config.get('PricePoint', 'maximum_price_point') -
                     abs(restaurant_price_point - diner_price_point))
            recommendations.append((diner, restaurant, score))

        return SQLContext(self.spark).createDataFrame(
            self.spark.parallelize(recommendations), Config.get_schema())
