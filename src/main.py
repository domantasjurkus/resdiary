import os, sys
import argparse
from data import Data
from config import Config

def execute_algorithm(args):
	bookings = data.get_bookings(args['data'])
	algorithm = algorithms[args['alg'].lower()](sc)
	predictions = []
	algorithm.train(bookings, args['load'])
	predictions = algorithm.predict(data.available_restaurants(bookings))
	data.write(args['out'], predictions)
	
def evaluate_algorithm(args):
	bookings = data.get_bookings(args['data'])
	algorithm = algorithms[args['alg'].lower()](sc)
	print '{}: {:.3f}%'.format(args['alg'], evaluate(sc, algorithm, bookings))

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
	parser.add_argument('--load', type=str, help='Load the previously trained models.')
	parser.add_argument('--func', type=str, help='Select between executing, evaluating and training the algorithm.')
	parser.add_argument('--learn', type=str, help='Learn algorithm hyper parameters instead of predicting results.')
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
	from evaluator import evaluate
	from recommenders import *

	sc = SparkContext('local','Recommendation engine')
	sc.setLogLevel("ERROR")
	data = Data(sc)

	algorithms = dict([(s.lower(), eval(s)) for s in Config.rcfg.sections()])

	# Invoke selected function
	args.func(vars(args))

	sc.stop()
