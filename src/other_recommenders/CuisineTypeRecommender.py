class ContentBased(Recommender):
    '''Generates recommendations based on a diner's prefered cuisine
    types.'''
    
    def train(self, bookings, save):
        if not isinstance(bookings, DataFrame):
            raise TypeError('Recommender requires a DataFrame')
        
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

        data = []
        recommendations = {}
        
        for diner in likedCuisine:
            recommendations.setdefault(diner, [])
            for restaurant in restaurantCuisine:
                if restaurant not in visited[diner]:
                    score = 0
                    for cuisine in restaurantCuisine[restaurant]:
                        if cuisine in likedCuisine[diner]:
                            score+=1                   
                    recommendations[diner].append((restaurant,score))
        
        data = [(diner,restaurant,score) for diner in recommendations
                for restaurant,score in recommendations[diner]]

        r = Config.get("ContentBased", "rank")
        i = Config.get("ContentBased", "iterations")
        l = Config.get("ContentBased", "lambda", float)
        self.spark.setCheckpointDir("./checkpoints/")
        model_location = "models/ContentBased/{rank}-{iterations}-{alpha}".format(rank=r,iterations=i,alpha=l)

        if os.path.isdir(model_location) and save.lower() != 'true':
            self.model =  MatrixFactorizationModel.load(self.spark, model_location)
        else:
            self.model = SparkContentBased.train(ratings, r, i, l)
            if os.path.isdir(model_location):
                shutil.rmtree(model_location)
            self.model.save(self.spark, model_location)
            
    
    def predict(self, data):
        predictions = self.model.predictAll(data)
        schema = ['userID', 'restaurantID', 'score']
        return SQLContext(self.spark).createDataFrame(predictions, schema)
