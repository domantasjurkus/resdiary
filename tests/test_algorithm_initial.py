import unittest
import sys, os
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.dataframe import DataFrame
from stubs import stub_algorithm
from test_superclass import *

# Append the source directory to the system path
# so that we can import the files for testing
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src/recommenders'))
import initial

class InitialAlgorithmTest(BaseTestCase):

	@classmethod
	def setUpClass(self):
		self.data = SQLContext(self.sc).read.csv(
	        os.path.dirname(__file__)+'/stubs/StubBookings.txt',
	        header=True,
	        inferSchema=True,
	        nullValue='NULL'
	    )

	def test_main(self):
		res = initial.generate_recommendations(self.sc, self.data)

		# Check that the propper class is returned
		self.assertIsInstance(res, DataFrame)

	@classmethod
	def tearDownClass(self):
		del self.data