import os, sys
import argparse
from data import Data
from config import Config

def execute_algorithm(args):
	bookings = data.get_bookings(args['data'])
	algorithm = algorithms[args['alg'].lower()](sc)
	algorithm.train(bookings, load=args['load'])
	predictions = algorithm.predict(data.nearby_restaurants(bookings))
	data.write(args['out'], predictions)
	
def evaluate_algorithm(args):
	bookings = data.get_bookings(args['data'])
	algorithm = algorithms[args['alg'].lower()]

	# Evaluating any other algorithm doesn't make sense because the main
	# evaluator checks how many of the recommendations are later visited,
	# but all recommenders except System return all the input pairs as
	# recommendations (with scores added)
	if algorithm not in [ExplicitALS, ImplicitALS, System]:
	        raise ValueError('This algorithm cannot be evaluated')

	Evaluator(sc, algorithm(sc)).evaluate(bookings)

def train_algorithm(args):
	bookings = data.get_bookings(args['data'])
	algorithm = algorithms[args['alg'].lower()](sc)
	algorithm.learn_hyperparameters(bookings)

def parse_arguments():
	parser = argparse.ArgumentParser(
		description='Executes an algorithm on the selected data. The ' +
		'algorithm is usually a recommendation algorithm.')
	parser.add_argument('--alg',  type=str, help='Algorithm name')
	parser.add_argument('--data', type=str, help='Path to the input data.')
	parser.add_argument('--out',  type=str, help='Path to output file with recommednations.')
	parser.add_argument('--load', type=str, help='Load the model of it exists.')
	parser.add_argument('--func', type=str, help='Select between executing, evaluating and training the algorithm.')
	args = parser.parse_args()

	if not args.out:
		args.out = './recommendations.csv'
		args.load = args.load is not None

	if not args.func:
		args.func = 'execute'
	args.func = eval(args.func+'_algorithm')

	return args

if __name__ == "__main__":
	args = parse_arguments()

	if args.alg is None or args.data is None: # pragma: no cover
		print "\nUsage: python main.py\n--alg=[ExplicitALS, ImplicitALS, CuisineType, PricePoint, system]\n"\
			"--data=<bookings.csv>\n--out=<recommendations.csv>\n" \
			"--func=[execute, evaluate, train]\n"
		sys.exit()

	# Import only if arguments were provided
	from pyspark import SparkContext
	from evaluator import *
	from recommenders import *

	sc = SparkContext('local[*]','Recommendation engine')
	sc.setLogLevel("ERROR")
	# Checkpoints necessary for training models with many iterations.
	sc.setCheckpointDir("./checkpoints/")
	data = Data(sc)

	algorithms = dict([(s.lower(), eval(s)) for s in Config.rcfg.sections()])

	# Invoke selected function
	args.func(vars(args))

	sc.stop()
