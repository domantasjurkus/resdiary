from pyspark.sql import SQLContext
from data import read

def generate_recommendations(spark, bookings):
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

    score = {}
    # [userID] [[other userID],[similarity score with that user]] - similarity
    # score - represents differences in review scores => less = more similar
    for i in hasreview:
        j = 0
        while j < len(hasreview[i]):
            if len(hasreview[i][j]) > 0:
                k = 0
                while k < len(hasreview[i]):
                    if len(hasreview[i][k]) > 0:
                        if hasreview[i][k][0] != hasreview[i][j][0]:
                            score.setdefault(hasreview[i][j][0], [])
                            score[hasreview[i][j][0]].append(
                                [hasreview[i][k][0],
                                 abs(round(float(hasreview[i][j][1]) -
                                           float(hasreview[i][k][1]), 1))])
                    k += 1
            j += 1

    # generate a list of recommendations
    recommendations = []
    for i in score:
        minResult = 100
        j = 0
        while j < len(score[i]):
            if score[i][j][1] < minResult:
                minResult = score[i][j][1]
                mostSimUser = score[i][j][0]
            j += 1

            # gives output in appropriate format
            for k in visited.get(mostSimUser):
                recommendations.append((i, k))
    return SQLContext(spark).createDataFrame(spark.parallelize(recommendations),
                                             ['userID', 'restaurantID'])
