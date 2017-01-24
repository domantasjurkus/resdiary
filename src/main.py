import os, sys
import data
import argparse

def get_bookings(filename):
	bookings = data.filter_outliers(sc, data.read(sc, filename))
	return bookings


def execute_algorithm(algorithm,filename):
	filename = get_absolute_path(filename)
	bookings = get_bookings(filename)
	data.write('Recommendations.csv', algorithms[algorithm.lower()].generate_recommendations(sc, bookings))


# evaluation
def evaluate_algorithm(algorithm,filename):
	bookings = get_bookings(filename)
	print '{}: {:.3f}%'.format(algorithm, evaluate(sc, algorithms[algorithm.lower()].generate_recommendations, bookings))


def get_absolute_path(filename):
	# If path starts with /, assume it is already absolute
	if filename[0] == "/":
		return filename
	return os.path.dirname(__file__)+"/"+filename


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Executes an algorithm on the selected data. The algorithm is usually a recommendation algorithm.')
	parser.add_argument('--alg', type=str,help='Algorithm name')
	parser.add_argument('--data', type=str,help='The data file.')
	args = parser.parse_args()

	if args.alg is None or args.data is None:
		print "\nusage: spark-submit main.py --alg=[intial,ALS] --data=[abs/relative path]\n"
		sys.exit()

	# Import only if arguments were provided
	from pyspark import SparkContext
	from evaluator import evaluate
	from recommenders import initial, ALS

	sc = SparkContext('local', 'Recommendation Engine')
	algorithms = {"als":ALS,"initial":initial}

	execute_algorithm(args.alg,args.data)
	evaluate_algorithm(args.alg,args.data)
	sc.stop()
