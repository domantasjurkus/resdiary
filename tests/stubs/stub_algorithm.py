from pyspark.sql import SQLContext
from recommenders import Recommender

class StubRecommender(Recommender):

    def train(self, data):
        pass

    def predict(self, data):
        # "you're going to LOVE every restaurant"
        predictions = data.map(lambda r: (r[0], r[1], 5))
        schema = ['userID', 'restaurantID', 'score']
        return SQLContext(self.spark).createDataFrame(predictions, schema)
