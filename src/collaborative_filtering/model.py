from pyspark.sql import SQLContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

def read_csv(spark_context,filename):
	return SQLContext(spark_context).read.csv(filename, header=True, inferSchema=True, nullValue='NULL')

def get_data_array(data):
	data_array = []
	for row in data.collect():
		data_array.append([row['Diner Id'], row['Restaurant Name'], row['Restaurant Id'], 
			row['Visit Time'], row['Covers'], row['Review Score']])
	return data_array

def get_bookings_with_score(data):
	data_array = []
	for row in data.collect():
		if row['Review Score'] != None:
			data_array.append(Rating(row['Diner Id'], row['Restaurant Id'], row['Review Score']))
	return data_array
