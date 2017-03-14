from pyspark.sql import SQLContext
from src.recommenders import Recommender

# Existing name from config
class StubCuisineType(Recommender):

    def train(self, data):
        pass

    def predict(self, dataframe):
        # "you're going to LOVE every restaurant"
        predictions = map(lambda r: (r[0], r[1], 5), dataframe.collect())
        schema = ['userID', 'restaurantID', 'score']
        return SQLContext(self.spark).createDataFrame(predictions, schema)
