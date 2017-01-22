from data import *
from pyspark.mllib.recommendation import ALS

def generate_recommendations(spark, bookings):
    ratings = get_bookings_with_score(spark, bookings)
    ratings, test_ratings = ratings.randomSplit([0.9,0.1])
    model = ALS.train(ratings, 12, 10,0.1)
    testdata = test_ratings.map(lambda p: (p[0], p[1]))
    predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
    return SQLContext(spark).createDataFrame(
        predictions.map(lambda r: (r[0][0],r[0][1],r[1])), ['userID', 'restaurantID','score'])