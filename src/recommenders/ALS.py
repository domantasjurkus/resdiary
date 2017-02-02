from data import *
from pyspark.mllib.recommendation import ALS
from pyspark.sql.dataframe import DataFrame

def generate_recommendations(spark, bookings):
    '''Generates recommendations based on review data.'''
    if not isinstance(bookings, DataFrame):
        raise TypeError('Recommender requires a DataFrame')
    ratings = get_bookings_with_score(spark, bookings)
    model = ALS.train(ratings, 12, 10, 0.1)
    testdata = ratings.map(lambda p: (p[0], p[1]))
    if testdata.isEmpty():
        raise ValueError('RDD is empty')
    predictions = model.predictAll(testdata)
    return SQLContext(spark).createDataFrame(predictions, ['userID',
                                                           'restaurantID',
                                                           'score'])
