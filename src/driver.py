from pyspark import SparkContext
from collaborative_filtering.model import *
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

def collaborative_filtering_model(data):
	rank = 10
	num_iterations = 10
	model = ALS.train(data,rank,num_iterations)
	return model

sc = SparkContext('local', 'Recommendation Engine')
ratings = sc.parallelize(get_bookings_with_score(read_csv(sc,'/home/edward/Downloads/Booking.csv')))
ratings, test_ratings = ratings.randomSplit([0.7,0.3])
data = sc.parallelize(get_data_array(read_csv(sc,'/home/edward/Downloads/Booking.csv')))
model = collaborative_filtering_model(ratings)

testdata = test_ratings.map(lambda p: (p[0], p[1]))
predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
print predictions.collect()
with open('predictions_all.txt','w') as pred_file:
	for row in predictions.collect():
		pred_file.write(str(row))
ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
print("Mean Squared Error = " + str(MSE))

# Save and load model
# model.save(sc, "tmp/myCollaborativeFilter")

sc.stop()