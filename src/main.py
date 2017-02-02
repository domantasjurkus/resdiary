import os, sys
import data
import argparse

def execute_algorithm(algorithm, filename):
	bookings = data.get_bookings(sc, filename)
        algorithm = algorithms[algorithm.lower()]
	data.write('Recommendations.csv',
                   algorithm.generate_recommendations(sc, bookings))

def evaluate_algorithm(algorithm_name, filename):
	bookings = data.get_bookings(sc, filename)
        algorithm = algorithms[algorithm_name.lower()].generate_recommendations
	print '{}: {:.3f}%'.format(algorithm_name,
                                   evaluate(sc, algorithm, bookings))

if __name__ == "__main__":
	parser = argparse.ArgumentParser(
                description='Executes an algorithm on the selected data. The '
                + 'algorithm is usually a recommendation algorithm.')
        parser.add_argument('--alg', type=str, help='Algorithm name')
	parser.add_argument('--data', type=str, help='The data file.')
	args = parser.parse_args()

	if args.alg is None or args.data is None:
		print "\nUsage: spark-submit src/main.py --alg=<intial, ALS> --data=Booking.csv\n"
		sys.exit()

	# Import only if arguments were provided
	from pyspark import SparkContext
	from evaluator import evaluate
	from recommenders import initial, ALS, ALS_2, implicit_ALS

	sc = SparkContext('local', 'Recommendation Engine')
	sc.setLogLevel("ERROR")
	algorithms = {"als": ALS, "als2": ALS_2, "initial": initial,
                      "implicit": implicit_ALS}

	execute_algorithm(args.alg, args.data)
	evaluate_algorithm(args.alg, args.data)
	sc.stop()
