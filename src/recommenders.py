from abc import ABCMeta, abstractmethod
from collections import Counter, defaultdict
import heapq
import itertools
import os.path
import shutil
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

    def location_filtering(self,data):
        data_transform = Data(self.spark)
        data_dir  = Config.get("DEFAULT", "data_dir", str)
        lat_diff  = Config.get("DEFAULT", "lat_diff", float)
        long_diff = Config.get("DEFAULT", "long_diff", float)
        recommendations = []
        existing_pairs = {}
        restaurants  = data_transform.read(data_dir + "uk_restaurants.csv")
        restaurants_info = data_transform.get_restaurants_info(restaurants)
        restaurants = self.spark.parallelize([( row['RestaurantId'],
                                        row['Lat'],row['Lon'],row['PricePoint']) for row in
                                       restaurants.collect()])
        nearby_restaurants = {}
        restaurants = restaurants.collect()
        for current_restaurant in restaurants:
            nearby_restaurants[current_restaurant[0]] = []
            for r in restaurants:
                if (abs(abs(current_restaurant[1]) - abs(r[1])) < lat_diff ) and (abs(abs(current_restaurant[2]) - abs(r[2])) < long_diff):
                    nearby_restaurants[current_restaurant[0]].append(r[0])

        for pair in data.collect():
            if not existing_pairs.get(pair[0],None):
                existing_pairs[pair[0]] = []
            current_restaurant = restaurants_info.get(pair[1],None)
            if current_restaurant:
                for restaurant in nearby_restaurants[pair[1]]:
                    if restaurant not in existing_pairs[pair[0]]:
                        recommendations.append((pair[0],restaurant))
                        existing_pairs[pair[0]].append(restaurant)
        return self.spark.parallelize(recommendations)

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
        top_recommendations = [(user, restaurant)
                               for user, restaurants in recommendations.items()
                               for restaurant, rating in restaurants]
        schema = ['userID', 'restaurantID']
        return SQLContext(self.spark).createDataFrame(top_recommendations,
                                                      schema)

    def learn_hyperparameters(self, data, save=True):
        '''Takes a DataFrame of bookings and uses the evaluator to learn optimal
        values for all the hyperparameters.'''
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
        weights = set(itertools.product(range(maximum_weight + 1),
                                        repeat=num_recommenders))
        # remove unnecessary duplication
        # (e.g., remove (2, 2, 2) if we already have (1, 1, 1))
        weights.discard(tuple([0] * num_recommenders))
        for k in range(2, maximum_weight + 1):
            weights -= set([tuple(map(lambda c: k * c, t)) for t in weights])
        return weights

class ALS(Recommender):
    
    def train(self, bookings, load=False):
        recommender_name = str(type(self).__name__)
        r = Config.get(recommender_name, "rank")
        i = Config.get(recommender_name, "iterations")
        l = Config.get(recommender_name, "lambda", float)
        self.spark.setCheckpointDir("./checkpoints/")
        
        model_location = "models/{name}/{rank}-{iterations}-{alpha}".format(
            name=recommender_name, rank=r, iterations=i, alpha=l)

        model_exists = os.path.isdir(model_location)
        if load and model_exists:
            self.model = MatrixFactorizationModel.load(self.spark, model_location)
        else:
            if str(recommender_name) == "ExplicitALS":
                self.model = SparkALS.train(bookings, r, i, l)
            elif str(recommender_name) == "ImplicitALS":
                self.model = SparkALS.trainImplicit(self.spark.parallelize(bookings), r,
                                                    i, alpha=l, nonnegative=True)
            if model_exists:
                shutil.rmtree(model_location)
            self.model.save(self.spark, model_location)

    def predict(self, data):
        recommendations = super(ALS, self).location_filtering(data)
        predictions = self.model.predictAll(recommendations)
        schema = ['userID', 'restaurantID', 'score']
        return SQLContext(self.spark).createDataFrame(predictions, schema)

    # Float range function
    def frange(self,x, y, jump):
      while x < y:
        yield x
        x += jump

    def learn_hyperparameters(self,bookings): # pragma: no cover
        self.spark.setCheckpointDir("./checkpoints/")
        recommender_name = str(type(self).__name__)
        results = open('result.csv', 'a')
        csv_writer = csv.writer(results,delimiter=',')
        data_transform = Data(self.spark)
        bookings = data_transform.get_bookings_with_score(bookings)
        data, test_ratings = bookings.randomSplit([0.8,0.2])
        testdata = test_ratings.map(lambda r: (r[0],r[1]))
        best_model = []

        for rank in range(5,100):
            for iterations in range(5,100):
                for alpha in self.frange(0.01,1.00,0.01):
                    model_location = "models/{name}/{rank}-{iterations}-{alpha}".format(
                        name=recommender_name, rank=rank, iterations=iterations, alpha=alpha)
                    model_exists = os.path.isdir(model_location)

                    if str(recommender_name) == "ExplicitALS":
                        self.model = SparkALS.train(data, rank, iterations, alpha)
                    elif str(recommender_name) == "ImplicitALS":
                        self.model = SparkALS.trainImplicit(self.spark.parallelize(data), rank,
                                                            iterations, alpha=alpha, nonnegative=True)
                    if model_exists:
                        shutil.rmtree(model_location)
                    self.model.save(self.spark, model_location)

                    predictions = self.model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
                    mse = calculate_mse(test_ratings,predictions)
                    print rank,iterations,alpha,mse
                    csv_writer.writerow([rank,iterations,alpha,mse])
                    if len(best_model) == 0:
                        best_model.append([rank,iterations,alpha,mse])
                    elif best_model[0][3] > mse:
                        best_model[0][0]  = rank
                        best_model[0][1]  = iterations
                        best_model[0][2]  = alpha
                        best_model[0][3]  = mse
        csv_writer.writerow(best_model[0])
        print "The best model is: rank=" + str(best_model[0][0]) + ", iterations=" + str(best_model[0][1]) + ", alpha="  +str(best_model[0][2]) + ", mse=" + str(best_model[0][3])

class ExplicitALS(ALS):
    '''Generates recommendations based on review data.'''

    def train(self, bookings, load=False):
        if not isinstance(bookings, DataFrame):
            raise TypeError('Recommender requires a DataFrame')

        data = Data(self.spark)
        ratings = data.get_bookings_with_score(bookings)
        super(ExplicitALS, self).train(ratings,load)

    def predict(self, data):
        return super(ExplicitALS, self).predict(data)

    def learn_hyperparameters(self,data):
        return super(ExplicitALS, self).learn_hyperparameters(data)

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
                
        super(ImplicitALS, self).train(data,load)

    def predict(self, data):
        return super(ImplicitALS, self).predict(data)

    def learn_hyperparameters(self,bookings):
        return super(ImplicitALS, self).learn_hyperparameters(data)
        

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
        diners_restaurants = super(CuisineType, self).location_filtering(diners_restaurants).collect()
        recommendations = [(diner, restaurant,
                            len(self.restaurant_cuisine[restaurant] &
                                self.liked_cuisine[diner]))
                           for diner, restaurant in diners_restaurants]
        recommendations =  self.spark.parallelize(recommendations).filter(lambda r: r[2] != 0)
        return SQLContext(self.spark).createDataFrame(
           recommendations, Config.get_schema())

class PricePoint(Recommender):
    '''For each diner, generates recommendations based on the average price
    point of all the visited restaurants.'''
    
    def train(self, bookings, load=False):
        # record the price point of each restaurant
        self.restaurant_price_point = {}
        filename = os.path.join(Config.get('DEFAULT', 'data_dir', str),
                                Config.get('PricePoint', 'filename', str))
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
        diners_restaurants = super(PricePoint, self).location_filtering(diners_restaurants).collect()
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
