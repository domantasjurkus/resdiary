from data import *
from pyspark.mllib.recommendation import ALS
from pyspark.sql.dataframe import DataFrame

def generate_recommendations(spark, bookings):
    if not isinstance(bookings, DataFrame):
        raise TypeError('Recommender requires a DataFrame')

    ratings = get_bookings_with_score(spark, bookings)
    ratings, test_ratings = ratings.randomSplit([0.99,0.01])
    ratings.cache()
    model = ALS.train(ratings, 60, 10,1.0)
    testdata = test_ratings.map(lambda p: (p[0], p[1]))
    # testdata = ratings.map(lambda p: (p[0], p[1]))
    predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
    ratesAndPreds = test_ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
    MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
    print("Mean Squared Error = " + str(MSE))

    if testdata.isEmpty():
        raise ValueError('RDD is empty')
    predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
    return SQLContext(spark).createDataFrame(
        predictions.map(lambda r: (r[0][0],r[0][1],r[1])), ['userID', 'restaurantID','score'])
