import os
from collections import Counter
from pyspark.sql import SQLContext
from pyspark.mllib.recommendation import Rating

def read(spark, filename):
    '''Takes a SparkContext instance and a filename and returns a DataFrame
    containing the parsed CSV file from the data/ directory.'''
    return SQLContext(spark).read.csv(filename,
                                      header=True, inferSchema=True,
                                      nullValue='NULL')

def write(filename, df):
    '''Takes a filename and a DataFrame and writes the contents of the DataFrame
    to the specified CSV file.'''
    df.toPandas().to_csv(os.path.join('data', filename), index=False)

def get_bookings_with_score(spark, data):
    '''Takes a SparkContext instance and a DataFrame of bookings and returns an
    RDD of Rating objects constructed from bookings that have non-null review
    scores.'''
    return spark.parallelize([Rating(row['Diner Id'], row['Restaurant Id'],
                                     row['Review Score'])
                              for row in data.collect() if row['Review Score']])

def filter_outliers(spark, df):
    '''Takes a SparkContext instance and a DataFrame of bookings and returns a
    new DataFrame without diners that have suspiciously frequent reservations.
    Frequency is defined as
    number_of_reservations / (last_visit - first_visit + 1),
    where visit times are expressed in seconds.'''
    # collect relevant data
    first_visit = {}
    last_visit = {}
    count = Counter()
    for row in df.collect():
        diner_id = row['Diner Id']
        visit_time = row['Visit Time']
        first_visit[diner_id] = (min(first_visit[diner_id], visit_time)
                                 if diner_id in first_visit else visit_time)
        last_visit[diner_id] = (max(last_visit[diner_id], visit_time)
                                if diner_id in last_visit else visit_time)
        count[diner_id] += 1

    # calculate frequencies
    frequencies = {}
    for diner_id in count:
        # +1 just to avoid division by 0
        frequencies[diner_id] = (count[diner_id] /
                                 ((last_visit[diner_id] -
                                   first_visit[diner_id]).seconds + 1.0))

    # filter out diners that have multiple reservations with abnormally high
    # frequency
    stats = spark.parallelize(frequencies.values()).stats()
    return SQLContext(spark).createDataFrame(
        df.rdd.filter(lambda row: count[row['Diner Id']] == 1 or 
                      frequencies[row['Diner Id']] <=
                      stats.mean() + 2 * stats.stdev()), schema=df.schema) 
