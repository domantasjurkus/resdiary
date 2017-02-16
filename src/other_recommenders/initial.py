from collections import defaultdict
from sets import Set
from pyspark.sql import SQLContext
from pyspark.sql.dataframe import DataFrame

def generate_recommendations(spark, bookings):
    if not isinstance(bookings, DataFrame):
        raise TypeError('Recommender requires a DataFrame')
    '''Takes a SparkContext instance and a DataFrame of bookings and returns a
    DataFrame of recommendations.'''
    # extract all the relevant information from the bookings DataFrame
    visited = {} # [userID] [restaurants visited by said user]
    res = {} # [restaurantID] [booked UserIDs]
    # [restaurantID] [userIDs who have left a review of a booking for this
    #                 restaurant]
    hasreview = {}
    for booking in bookings.collect():
        diner = booking['Diner Id']
        restaurant = booking['Restaurant Id']
        review = booking['Review Score']
        visited.setdefault(diner, [])
        res.setdefault(restaurant, [])
        hasreview.setdefault(restaurant, [])
        if restaurant not in visited[diner]:
            visited[diner].append(restaurant)
        if diner not in res[restaurant]:
            res[restaurant].append(diner)
        if diner not in hasreview[restaurant] and review:
            hasreview[restaurant].append([diner, review])

    sim = {} # [userID] [userIDs of people who have visited same restaurants]
    for i in res:
        j = 0
        while j < len(res[i]):
            k = 0
            while k<len(res[i]):
                sim.setdefault(res[i][j], [])
                if res[i][k] not in sim[res[i][j]] and res[i][k] != res[i][j]:
                    sim[res[i][j]].append(res[i][k])
                k += 1
            j += 1

    # [userID] ([other userID], [similarity score with that user]) - similarity
    # score - represents differences in review scores => less = more similar
    closest = {}
    for i in hasreview:
        j = 0
        while j < len(hasreview[i]):
            if len(hasreview[i][j]) > 0:
                k = 0
                while k < len(hasreview[i]):
                    if len(hasreview[i][k]) > 0:
                        if hasreview[i][k][0] != hasreview[i][j][0]:
                            score = abs(round(float(hasreview[i][j][1]) -
                                              float(hasreview[i][k][1]), 1))
                            if (hasreview[i][j][0] not in closest or
                                score < closest[hasreview[i][j][0]][1]):
                                closest[hasreview[i][j][0]] = (
                                    hasreview[i][k][0], score)
                    k += 1
            j += 1

    # generate a list of recommended restaurants for each diner
    recommendations = defaultdict(Set)
    for i in closest:
        for k in visited.get(closest[i][0]):
            recommendations[i].add(k)
    return SQLContext(spark).createDataFrame(spark.parallelize(
        [(diner, restaurant) for diner in recommendations
         for restaurant in recommendations[diner]]), ['userID', 'restaurantID'])
