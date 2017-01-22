import data
import argparse
from pyspark import SparkContext
from evaluator import evaluate
from recommenders import initial, ALS

sc = SparkContext('local', 'Recommendation Engine')
algorithms = {"als":ALS,"initial":initial}

def get_bookings(filename):
	bookings = data.filter_outliers(sc, data.read(sc, filename))
	return bookings

def execute_algorithm(algorithm,filename):
	bookings = get_bookings(filename)
	data.write('Recommendations.csv', algorithms[algorithm.lower()].generate_recommendations(sc, bookings))

# evaluation
def evaluate_algorithm(algorithm,filename):
	bookings = get_bookings(filename)
	print '{}: {:.3f}%'.format(algorithm, evaluate(sc, algorithms[algorithm.lower()].generate_recommendations, bookings))


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Executes an algorithm on the selected data. The algorithm is usually a recommendation algorithm.')
	parser.add_argument('--alg', type=str,help='Algorithm name')
	parser.add_argument('--data', type=str,help='The data file.')
	args = parser.parse_args()

	execute_algorithm(args.alg,args.data)
	evaluate_algorithm(args.alg,args.data)
	sc.stop()
