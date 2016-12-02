import csv
with open('Booking.csv', 'rb') as b:
    reader = csv.reader(b)
    bookings = list(reader)
print "Bookings : ", len(bookings)

#bookings format : [dinerId] [RestaurantName] [RestaurantID] [Time] [NumberOfPeople] [ReviewScore]

with open('Restaurant.csv', 'rb') as r:
    reader = csv.reader(r)
    restaurants = list(reader)
print "Restaurants : ", len(restaurants)

#restaurants format : [RestaurantID] [RestaurantName] [Town] [PricePoint] [Lat] [Lon]


# (key,value) = (userID,visitedRestaurantIDs)
visited ={}
i = 1

while i<len(bookings):
    visited.setdefault(bookings[i][0], [])
    if bookings[i][2] not in visited[bookings[i][0]]:
        visited[bookings[i][0]].append(bookings[i][2])
    i=i+1

#in format "id [restaurantIDs]"
#for i in visited:
#    print i, visited[i]




# (key,valie) = (restaurantID,bookedUserIDs)
res = {}
i = 1

while i<len(bookings):
    res.setdefault(bookings[i][2], [])
    if bookings[i][0] not in res[bookings[i][2]]:
        res[bookings[i][2]].append(bookings[i][0])
    i=i+1

sim = {}


i = 0

for i in res:
    j=0
    while j<len(res[i]):
        k = 0
        while k<len(res[i]):
            sim.setdefault(res[i][j], [])
            if res[i][k] not in sim[res[i][j]]:
                sim[res[i][j]].append(res[i][k])
            k=k+1
        j=j+1
    print i

print sim
