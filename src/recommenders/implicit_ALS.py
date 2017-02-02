from collections import Counter, defaultdict
from pyspark.mllib.recommendation import ALS, Rating
from pyspark.sql import SQLContext

def generate_recommendations(spark, bookings):
    '''Generates recommendations based on how many times a diner visited a
    restaurant.'''
    # calculate how many times a diner visited each restaurant
    data = defaultdict(Counter)
    for booking in bookings.collect():
        data[booking['Diner Id']][booking['Restaurant Id']] += 1

    # transform that data into an RDD
    rdd = spark.parallelize([(diner, restaurant, score)
                             for diner, counter in data.items()
                             for restaurant, score in counter.iteritems()])
    # train and predict
    model = ALS.trainImplicit(rdd, 12, 10, alpha=0.01)
    predictions = model.predictAll(rdd.map(lambda r: (r[0], r[1])))
    return SQLContext(spark).createDataFrame(predictions, ['userID',
                                                           'restaurantID',
                                                           'score'])
