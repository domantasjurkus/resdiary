import unittest
import sys, os
from pyspark.sql import SQLContext
from stubs import stub_algorithm
from test_superclass import *

# Append the source directory to the system path
# so that we can import the files for testing
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
import evaluator

class EvaluatorTest(BaseTestCase):

	# Set up fixtures that last for all test cases
	@classmethod
	def setUpClass(self):
		self.data = SQLContext(self.sc).read.csv(
	        os.path.dirname(__file__)+'/stubs/StubBookings.txt',
	        header=True,
	        inferSchema=True,
	        nullValue='NULL'
	    )


	def test_no_recommendations(self):
		score = evaluator.evaluate(
			self.sc,
			stub_algorithm.StubRecommender,
			self.data
		)

		# Data set should be too small to make any recommendations
		self.assertTrue(score==0.0);


	@classmethod
	def tearDownClass(self):
		del self.data
