from pyspark.mllib.recommendation import ALS
from pyspark.sql import SQLContext
from pyspark.sql.dataframe import DataFrame
from data import Data

def train_model(spark, bookings):
    if not isinstance(bookings, DataFrame):
        raise TypeError('Recommender requires a DataFrame')

    data = Data(spark)
    ratings = data.get_bookings_with_score( bookings)
    ratings, test_ratings = ratings.randomSplit([0.8,0.2])
    ratings.cache()
    spark.setCheckpointDir("./checkpoints/")
    model = ALS.train(ratings, 12, 30,0.45)
    return model

    
def validate_results(model):
    data = Data(spark)
    ratings = data.get_bookings_with_score( bookings)
    ratings, test_ratings = ratings.randomSplit([0.9,0.1])
    testdata = test_ratings.map(lambda p: (p[0], p[1]))
    # testdata = ratings.map(lambda p: (p[0], p[1]))
    predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
    ratesAndPreds = test_ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
    MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
    print("Mean Squared Error = " + str(MSE))

    if testdata.isEmpty():
        raise ValueError('RDD is empty')
    predictions = model.predictAll(testdata)
    return SQLContext(spark).createDataFrame(predictions, ['userID',
                                                           'restaurantID',
                                                           'score'])