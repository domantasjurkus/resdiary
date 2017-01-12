import unittest
import sys, os
from pyspark import SparkContext
from pyspark.sql import SQLContext
from stubs import stub_algorithm

# Append the source directory to the system path
# so that we can import the files for testing
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
import evaluator

class EvaluatorTest(unittest.TestCase):

	# Set up fixtures that last for all test cases
	@classmethod
	def setUpClass(self):
		self.sc = SparkContext()
		self.data = SQLContext(self.sc).read.csv(
	        'tests/stubs/StubBookings.txt',
	        header=True,
	        inferSchema=True,
	        nullValue='NULL'
	    )

	def test_no_recommendations(self):
		score = evaluator.evaluate(
			self.sc,
			stub_algorithm.get_recommendations,
			self.data
		)

		# Data set should be too small to make any recommendations
		self.assertTrue(score==0.0);

	@classmethod
	def tearDownClass(self):
		del self.sc
		del self.data


def main():
	unittest.main()

if __name__ == "__main__":
	main()