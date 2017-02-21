import os, sys
import argparse
from data import Data

def execute_algorithm(algorithm_name,filename,output_file,save):
	bookings = data.get_bookings(filename)
	algorithm = algorithms[algorithm_name.lower()](sc)
	if 'als' in algorithm_name.lower():
		algorithm.train(bookings,save)
	else:
		algorithm.train(bookings)
	predictions = algorithm.predict(data.available_restaurants(bookings))
	data.write(output_file, predictions)
	
def evaluate_algorithm(algorithm_name, filename):
	bookings = data.get_bookings(filename)
	algorithm = algorithms[algorithm_name.lower()](sc)
	print '{}: {:.3f}%'.format(algorithm_name, evaluate(sc, algorithm, bookings))

def train_algorithm(algorithm_name, filename):
	bookings = data.get_bookings(filename)
	algorithm = algorithms[algorithm_name.lower()](sc)
	algorithm.learn_hyperparameters(bookings)

def parse_arguments():
	parser = argparse.ArgumentParser(
				description='Executes an algorithm on the selected data. The '
				+ 'algorithm is usually a recommendation algorithm.')
	parser.add_argument('--alg', type=str, help='Algorithm name')
	parser.add_argument('--data', type=str, help='Path to the input data.')
	parser.add_argument('--out', type=str, help='Path to output file with recommednations.')
	parser.add_argument('--save', type=str, help='Forcing to overwrite existing model.')
	args = parser.parse_args()

	if not args.out:
		args.out = './recommendations.csv'
	return args

if __name__ == "__main__":
	args = parse_arguments()

	if args.alg is None or args.data is None: # pragma: no cover
		print "\nUsage: python main.py --alg=<intial, ALS> --data=data/Booking.csv --out=/home/user/recommendations.csv\n"
		sys.exit()

	# Import only if arguments were provided
	from pyspark import SparkContext,SparkConf
	from evaluator import evaluate
	from recommenders import *
	sparkConf = SparkConf().set('spark.files.overwrite','True')
	sc = SparkContext('local','Recommendation engine', conf=sparkConf)
	sc.setLogLevel("ERROR")
	data = Data(sc)
	algorithms = {"als": ALS, "implicit": ImplicitALS, "system": System}

	execute_algorithm(args.alg,args.data,args.out,args.save)
	#evaluate_algorithm(args.alg, args.data)
	#train_algorithm(args.alg, args.data)
	sc.stop()
