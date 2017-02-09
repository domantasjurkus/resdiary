from pyspark.mllib.recommendation import ALS as SparkALS
from pyspark.sql import SQLContext
from pyspark.sql.dataframe import DataFrame
from data import Data
from recommenders.recommender import Recommender

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
