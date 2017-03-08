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

    def learn_hyperparameters(self, data):
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

    def location_filtering(self,data):
        data_transform = Data(self.spark)
        data_dir = Config.get_string("DEFAULT", "data_dir")
        lat_diff =  Config.get("DEFAULT", "lat_diff",type=float)
        long_diff = Config.get("DEFAULT", "long_diff",type=float)
        recommendations = []

        restaurants  = data_transform.read(data_dir + "Restaurant.csv")
        restaurants_info = data_transform.get_restaurants_info(restaurants)
        restaurants = self.spark.parallelize([( row['RestaurantId'],
                                        row['Lat'],row['Lon']) for row in
                                       restaurants.collect()])
        for pair in data.collect():
            current_restaurant = restaurants_info.get(pair[1],None)
            if current_restaurant:
                temp_restaurants = restaurants.filter(lambda r:(
                    (abs(current_restaurant[0]) - abs(r[1]) < lat_diff )) and
                    (abs(current_restaurant[1]) - abs(r[2]) < long_diff)) 
                temp_recs = self.spark.parallelize([pair[0]]).cartesian(temp_restaurants.map(lambda r: r[0]))
                recommendations.extend(temp_recs.collect())

        return self.spark.parallelize(recommendations)

    def predict(self, data):
        recommendations = self.location_filtering(data).distinct()
        predictions = self.model.predictAll(recommendations)
        schema = ['userID', 'restaurantID', 'score']
        return SQLContext(self.spark).createDataFrame(predictions, schema)

    # Float range function
    def frange(self,x, y, jump):
      while x < y:
        yield x
        x += jump

    def learn_hyperparameters(self,bookings):
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
        

class ContentBased(Recommender):
    '''Generates recommendations based on a diner's prefered cuisine types.'''
    
    def train(self, bookings, load=False):
        data_transform = Data(self.spark)
        restaurants = data_transform.read("data/Restaurant.csv")
        restaurantCuisines = data_transform.read("data/RestaurantCuisineTypes.csv")
        cuisineTypes = data_transform.read("data/CuisineTypes.csv")

        self.restaurantCuisine = {}
        self.visited = {}
        
        for entry in restaurantCuisines.collect():
            restaurant = entry['RestaurantId']
            cuisineType = entry['CuisineTypeId']
            self.restaurantCuisine.setdefault(restaurant, [])
            if cuisineType not in self.restaurantCuisine[restaurant]:
                self.restaurantCuisine[restaurant].append(cuisineType)

        self.likedCuisine = {}
        MINIMUM_LIKE_SCORE = 4 #the minimum review score from a booking
        #for a restaurant's cuisine to be considered liked
        
        for booking in bookings.collect():
            diner = booking['Diner Id']
            restaurant = booking['Restaurant Id']
            score = booking['Review Score']
            self.visited.setdefault(diner, [])
            if restaurant not in self.visited[diner]:
                self.visited[diner].append(restaurant)
            self.likedCuisine.setdefault(diner, [])
            if score >= MINIMUM_LIKE_SCORE and self.restaurantCuisine.get(restaurant, None):
                for cuisine in self.restaurantCuisine[restaurant]:
                    if cuisine not in self.likedCuisine[diner]:
                        self.likedCuisine[diner].append(cuisine)

    def predict(self, users_restaurants):
        # TODO: use the parameter
        data = defaultdict()
        recommendations = {}
        for diner in self.likedCuisine:
            recommendations.setdefault(diner, [])
            for restaurant in self.restaurantCuisine:
                if restaurant not in self.visited[diner]:
                    score = 0
                    for cuisine in self.restaurantCuisine[restaurant]:
                        if cuisine in self.likedCuisine[diner]:
                            score += 1
                    if score > 0:
                        recommendations[diner].append((restaurant, score))

        schema = ['userID', 'restaurantID', 'score']
        return SQLContext(self.spark).createDataFrame(self.spark.parallelize(
            [(diner, restaurant, score) for diner in recommendations
             for restaurant, score in recommendations[diner]]), schema)

class PricePoint(Recommender):
    '''Generates recommendations based on a diner's prefered cuisine types.'''
    
    def train(self, bookings, load=False):
        data_transform = Data(self.spark)
        restaurants = data_transform.read("data/Restaurant.csv")

        self.restaurantPricePoint = {}
        self.visited = {}
        self.userPricePoints = {}
        
        for entry in restaurants.collect():           
            if entry['PricePoint'] is not None:
				self.restaurantPricePoint.setdefault(entry['RestaurantId'], [])
				self.restaurantPricePoint[entry['RestaurantId']].append(entry['PricePoint'])

        MINIMUM_LIKE_SCORE = 4 #the minimum review score from a booking
        #for a restaurant's cuisine to be considered liked
        
        for booking in bookings.collect():
            diner = booking['Diner Id']
            restaurant = booking['Restaurant Id']
            score = booking['Review Score']
            self.visited.setdefault(diner, [])
            if restaurant not in self.visited[diner]:
                self.visited[diner].append(restaurant)
            self.userPricePoints.setdefault(diner, [])
            if self.restaurantPricePoint.has_key(restaurant):
                self.userPricePoints[diner].append(self.restaurantPricePoint[restaurant])

                                                              
    def predict(self, users_restaurants):
        # TODO: use the parameter
        data = defaultdict()
        recommendations = {}
        for diner in self.userPricePoints:
			recommendations.setdefault(diner, [])
			userAveragePricePoint = 0
			counter = 0
			for pricePoint in self.userPricePoints[diner]:
				if pricePoint is not None :
					userAveragePricePoint += pricePoint[0]
					counter += 1
			if counter>0:
				userAveragePricePoint = userAveragePricePoint/counter
			for restaurant in self.restaurantPricePoint:
				recommendations[diner].append((restaurant, 5 - abs(self.restaurantPricePoint[restaurant][0]-userAveragePricePoint)))

        schema = ['userID', 'restaurantID', 'score']
        return SQLContext(self.spark).createDataFrame(self.spark.parallelize(
            [(diner, restaurant, score) for diner in recommendations
             for restaurant, score in recommendations[diner]]), schema)
