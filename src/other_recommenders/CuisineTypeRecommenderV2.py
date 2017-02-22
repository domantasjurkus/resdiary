class ContentBased(Recommender):
    '''Generates recommendations based on a diner's prefered cuisine
    types.'''
    
    def train(self, bookings):
        dummyCode = 0
        
    def predict(self, data):
        
        restaurants = read(self,"Restaurants.csv")
        restaurantCuisines = read(self,"RestaurantCuisineTypes.csv")
        cuisineTypes = read(self,"CuisineTypes.csv")

        restaurantCuisine ={}
        visited = {}
        
        for entry in restaurantCuisines.collect():
            restaurant = entry['RestaurantId']
            cuisineType = entry['CuisineTypeId']
            restaurantCuisine.setdefault(restaurant, [])
            if cuisineType not in restaurantCuisine[restaurant]:
                restaurantCuisine[restaurant].append(cuisineType)

        likedCuisine = {}
        MINIMUM_LIKE_SCORE = 4 #the minimum review score from a booking
                                #for a restaurant's cuisine to be considered liked
        
        for booking in bookings.collect():
            diner = booking['Diner Id']
            restaurant = booking['Restaurant Id']
            score = booking['Review Score']
            visited.setdefault(diner, [])
            if restaurant not in visited[diner]:
                visited[diner].append(restaurant)
            likedCuisine.setdefault(diner, [])
            if score >= MINIMUM_LIKE_SCORE:
                for cuisine in restaurantCiusine[restaurant]:
                    if cuisine not in likedCuisine[diner]:
                        likedCuisine[diner].append(cuisine)       

        data = defaultdict()
        recommendations = {}
        for diner in likedCuisine:
            recommendations.setdefault(diner, [])
            for restaurant in restaurantCuisine:
                if restaurant not in visited[diner]:
                    score = 0
                    for cuisine in restaurantCuisine[restaurant]:
                        if cuisine in likedCuisine[diner]:
                            score++                    
                    recommendations[diner].append((restaurant,score))

        schema = ['userID', 'restaurantID', 'score']
        return SQLContext(spark).createDataFrame(spark.parallelize(
            [(diner,restaurant,score) for diner in recommendations
             for restaurant,score in recommendations[diner]]), schema)
