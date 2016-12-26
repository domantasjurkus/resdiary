from pyspark.sql import SQLContext
from collections import defaultdict
from sets import Set

def evaluate(spark, algorithm, bookings_data):
    '''Takes a SparkContext instance, an algorithm and a DataFrame of bookings,
    feeds the earlier half of the data (for each user) to the algorithm and
    returns a ratio right/total, where 'right' is the number of recommended
    restaurants that were later visited and 'total' is the total number of
    recommendations.'''
    # filter out half of the data to partial_data and store the remaining
    # restaurant IDs
    answers = defaultdict(Set)
    # read all the data and separate by diner id
    bookings = defaultdict(list)
    for booking in bookings_data.collect():
        bookings[booking['Diner Id']].append(booking)
    partial_data = []
    for personal_bookings in bookings.values():
        num_bookings = len(personal_bookings)
        if num_bookings < 2:
            # not enough bookings to use this user for evaluation
            continue
        first_test_index = num_bookings / 2
        # sort by visit time
        personal_bookings.sort(key=lambda r: r['Visit Time'])
        # the first half of the data is used for training/predictions
        for booking in personal_bookings[:first_test_index]:
            partial_data.append(booking)
        # store restaurant IDs from the remaining rows
        for booking in personal_bookings[first_test_index + 1:]:
            answers[booking['Diner Id']].add(booking['Restaurant Id'])

    # create a DataFrame of training data, call the algorithm, evaluate the
    # results
    recommendations = algorithm(spark, SQLContext(spark).createDataFrame(
        spark.parallelize(partial_data), schema=bookings_data.schema))
    right = 0
    total = 0
    for recommendation in recommendations.collect():
        total += 1
        if recommendation['restaurantID'] in answers[recommendation['userID']]:
            right += 1
    return float(right) / total
