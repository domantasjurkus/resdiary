import os, sys
import argparse
from data import Data

def execute_algorithm(algorithm_name,filename,output_file,save,location):
	bookings = data.read(filename)
	algorithm = algorithms[algorithm_name.lower()](sc)
	predictions = []
	if 'als' in algorithm_name.lower():
		algorithm.train(bookings,save)
		predictions = algorithm.predict(data.get_bookings_full(bookings),location)
	else:
		algorithm.train(bookings)
		predictions = algorithm.predict(data.get_bookings_with_score(bookings))
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
	parser.add_argument('--location', type=str, help='Predict for a specific location.')
	args = parser.parse_args()

	if not args.out:
		args.out = './recommendations.csv'
	if not args.save:
		args.save = 'False'
	return args

if __name__ == "__main__":
	args = parse_arguments()

	if args.alg is None or args.data is None or args.location is None: # pragma: no cover
		print "\nUsage: python main.py --alg=ALS --location=Glasgow ALS> --data=data/Booking.csv --out=/home/user/recommendations.csv\n"
		sys.exit()

	# Import only if arguments were provided
	from pyspark import SparkContext
	from evaluator import evaluate
	from recommenders import *

	sc = SparkContext('local','Recommendation engine')
	sc.setLogLevel("ERROR")
	data = Data(sc)
	algorithms = {"als": ALS, "implicit": ImplicitALS, "system": System,"cb":ContentBased}

	execute_algorithm(args.alg,args.data,args.out,args.save,args.location)
	#evaluate_algorithm(args.alg, args.data)
	#train_algorithm(args.alg, args.data)
	sc.stop()
