import os, sys
import argparse
from data import Data

def execute_algorithm(algorithm, filename,output_file):
	bookings = data.get_bookings(filename)
	algorithm = algorithms[algorithm.lower()](sc)
	algorithm.train(bookings)
	predictions = algorithm.predict(data.available_restaurants(bookings))
	with open(output_file,'w') as csv_file:
		data.write(output_file, predictions)
	csv_file.close()

def evaluate_algorithm(algorithm_name, filename):
	bookings = data.get_bookings(filename)
	algorithm = algorithms[algorithm_name.lower()](sc)
	print '{}: {:.3f}%'.format(algorithm_name, evaluate(sc, algorithm, bookings))

def train_algorithm(algorithm_name, filename):
	bookings = data.get_bookings(filename)
	algorithm = algorithms[algorithm_name.lower()](sc)
	algorithm.learn_hyperparameters(bookings)

if __name__ == "__main__":
	parser = argparse.ArgumentParser(
				description='Executes an algorithm on the selected data. The '
				+ 'algorithm is usually a recommendation algorithm.')
	parser.add_argument('--alg', type=str, help='Algorithm name')
	parser.add_argument('--data', type=str, help='Path to the input data.')
	parser.add_argument('--out', type=str, help='Path to output file with recommednations.')
	args = parser.parse_args()

	if args.alg is None or args.data is None: # pragma: no cover
		print "\nUsage: python main.py --alg=<intial, ALS> --data=data/Booking.csv\n"
		sys.exit()

	# Import only if arguments were provided
	from pyspark import SparkContext
	from evaluator import evaluate
	from recommenders import *

	sc = SparkContext('local', 'Recommendation Engine')
	sc.setLogLevel("ERROR")
	data = Data(sc)
	algorithms = {"als": ALS, "implicit": ImplicitALS, "system": System}

	execute_algorithm(args.alg, args.data,args.out)
	#evaluate_algorithm(args.alg, args.data)
	#train_algorithm(args.alg, args.data)
	sc.stop()
