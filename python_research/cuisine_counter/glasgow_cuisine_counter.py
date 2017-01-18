import csv
import math

def main():

    cuisineDict = {}
    restaurantDict = {}

    with open( '../data/CuisineTypes.csv', 'rb' ) as f:
        reader = csv.reader(f)

        next( reader, None )
        for row in reader:
            cuisineDict[int(row[1])] = 0
            restaurantDict[int(row[1])] = row[0]

    with open( '../data/RestaurantCuisineTypes.csv', 'rb' ) as f:
        reader = csv.reader(f)

        next( reader, None )
        for row in reader:
            cuisineDict[int(row[1])] += 1

    sortedOccurences = sorted(cuisineDict.values(), reverse=True)
    sortedTypes = sorted(cuisineDict, key=cuisineDict.get, reverse=True)

    i = 0

    for cuisine in sortedTypes:
        print restaurantDict[cuisine] + " has " + str(sortedOccurences[i]) + " occurences."
        i += 1
if __name__=="__main__":
    main()
