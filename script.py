import csv
with open('data/Booking.csv', 'rb') as b:
    reader = csv.reader(b)
    bookings = list(reader)

#bookings format : [dinerId] [RestaurantName] [RestaurantID] [Time] [NumberOfPeople] [ReviewScore]



with open('data/Restaurant.csv', 'rb') as r:
    reader = csv.reader(r)
    restaurants = list(reader)

#restaurants format : [RestaurantID] [RestaurantName] [Town] [PricePoint] [Lat] [Lon]




# [userID] [restaurants visited by said user]
visited ={}
i = 1

while i<len(bookings):
    visited.setdefault(bookings[i][0], [])
    if bookings[i][2] not in visited[bookings[i][0]]:
        visited[bookings[i][0]].append(bookings[i][2])
    i=i+1



# [restaurantID] [booked UserIDs]
res = {}
i = 1

while i<len(bookings):
    res.setdefault(bookings[i][2], [])
    if bookings[i][0] not in res[bookings[i][2]]:
        res[bookings[i][2]].append(bookings[i][0])
    i=i+1




# [userID] [userIDs of people who have visited same restaurants]
sim = {}
i = 0

for i in res:
    j=0
    while j<len(res[i]):
        k = 0
        while k<len(res[i]):
            sim.setdefault(res[i][j], [])
            if res[i][k] not in sim[res[i][j]] and res[i][k]!=res[i][j]:
                sim[res[i][j]].append(res[i][k])
            k=k+1
        j=j+1





hasreview = {}
score = {}
# [restaurantID] [userIDs who have left a review of a booking for this restaurant]

i=1                 
while i<len(bookings):
    hasreview.setdefault(bookings[i][2], [])
    if bookings[i][0] not in hasreview[bookings[i][2]] and bookings[i][5]!="NULL":
        hasreview[bookings[i][2]].append([bookings[i][0],bookings[i][5]])        
    i=i+1    





score = {}
# [userID] [[other userID],[similarity score with that user]] - similarity score - represents differences in review scores => less = more similar

for i in hasreview:
    j = 0
    while j < len(hasreview[i]):
        if len(hasreview[i][j])>0:
            k=0
            while k < len(hasreview[i]):
                if len(hasreview[i][k])>0:
                    if hasreview[i][k][0]!=hasreview[i][j][0]:
                        score.setdefault(hasreview[i][j][0], [])
                        score[hasreview[i][j][0]].append([hasreview[i][k][0],abs(round(float(hasreview[i][j][1])-float(hasreview[i][k][1]),1))])
                        k=k+1
                    else:
                        k=k+1
                else:
                    k=k+1        
            j=j+1
        else:
            j=j+1



recom = {}
#[userID] is most similar to [most similar userID]

# write recommendations of the type ([userID],[restaurantID]) into a file called recommendations.csv

with open('data/Recommendations.csv', 'wb') as csvfile:
    fieldnames = ['userID', 'restaurantID']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    for i in score:
        minResult = 100
        j = 0
        while j < len(score[i]):
            if score[i][j][1] < minResult:
                minResult = score[i][j][1]
                mostSimUser = score[i][j][0]
            j=j+1

        # gives output in appropriate format
        for k in visited.get(mostSimUser):
            writer.writerow({'userID':i,'restaurantID':k})

    

