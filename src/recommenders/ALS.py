from data import *
from pyspark.mllib.recommendation import ALS

def generate_recommendations(spark, bookings):
    ratings = get_bookings_with_score(spark, bookings)
    model = ALS.train(ratings, 10, 10)
    predictions = model.predictAll(ratings.map(lambda p: (p[0], p[1])))
    return SQLContext(spark).createDataFrame(
        predictions.map(lambda r: (r[0], r[1])), ['userID', 'restaurantID'])
