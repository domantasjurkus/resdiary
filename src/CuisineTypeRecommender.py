import csv

#bookings format : [dinerId] [RestaurantName] [RestaurantID] [Time] [NumberOfPeople] [ReviewScore]

with open('Booking.csv', 'rb') as b:
    reader = csv.reader(b)
    bookings = list(reader)



#restaurants format : [RestaurantID] [RestaurantName] [Town] [PricePoint] [Lat] [Lon]

with open('Restaurant.csv', 'rb') as r:
    reader = csv.reader(r)
    restaurants = list(reader)



#restaurant cuisine type format : [RestaurantID] [CuisineTypeID]

with open('RestaurantCuisineTypes.csv', 'rb') as c:
    reader = csv.reader(c)
    cuisines = list(reader)



#cuisineType format : [CuisineTypeID] [CuisineType Name]

with open('CuisineTypes.csv', 'rb') as r:
    reader = csv.reader(r)
    cuisineTypes = list(reader)



# dictionary with restaurant IDs as keys and cuisine types as values
# [restaurantID] [cuisine types]

restaurantCuisine ={}
restaurantCounter = 1
while restaurantCounter<len(restaurants):
	restaurantCuisine.setdefault(restaurants[restaurantCounter][0], [])
	for i in cuisines:
		if i[0]==restaurants[restaurantCounter][0]:			
			restaurantCuisine[i[0]].append(i[1])		
	restaurantCounter=restaurantCounter+1



# dictionary with userIDs as keys and visited restaurantIDs as values
# [userID] [restaurants visited by said user]

visited ={}
bookingId = 0

while bookingId<len(bookings):
    visited.setdefault(bookings[bookingId][0], [])
    if bookings[bookingId][2] not in visited[bookings[bookingId][0]]:
        visited[bookings[bookingId][0]].append((bookings[bookingId][2],bookings[bookingId][5]))
    bookingId=bookingId+1



# Recommendation based on cuisine type from the user's reviews
# [userID] [liked cuisineTypeIDs]

userCuisine = {}
for user in visited:
	userCuisine.setdefault(user,[])
	for restaurant in visited[user]:
		for resCounter in restaurantCuisine:			
			if (restaurant[0] == resCounter) and (restaurant[1]>=4) and (resCounter[1] not in userCuisine[user]):
				userCuisine[user].append(resCounter[1])


recommendations = {}
for user in userCuisine:
	recommendations.setdefault(user, [])
	for j in userCuisine[user]:
		for restaurant in restaurantCuisine:
			if (restaurant not in visited[user]) and (j in restaurantCuisine[restaurant]):
				recommendations[user].append(restaurant)


with open('recommendations.csv', 'wb') as csvfile:
 	recommendationWriter = csv.writer(csvfile, delimiter=',',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
	for i in recommendations:
		for j in recommendations[i]:
			line = []
			line.append(i)
			line.append(j)
			line.append(5) #dummy score value
			recommendationWriter.writerow(line)

				

