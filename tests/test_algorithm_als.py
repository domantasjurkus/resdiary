import unittest
import sys, os
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.dataframe import DataFrame

from stubs import stub_algorithm
from test_superclass import BaseTestCase
from src.recommenders import ALS as algorithm
from src.data import Data

class ALSAlgorithmTest(BaseTestCase):

	@classmethod
	def setUpClass(self):
		self.bookings = SQLContext(self.sc).read.csv(
			os.path.dirname(__file__)+'/stubs/StubBookings.txt',
			header=True,
			inferSchema=True,
			nullValue='NULL'
		)

		# Instantiate alg.model
		self.alg = algorithm(self.sc)
		self.alg.train(self.bookings)


	def test_main(self):	
		# Check that an exception is thrown if wrong argument provided
		self.assertRaises(TypeError, self.alg.train, ['foo', 'bar'])

		# Check for detection of empty RDD
		self.assertRaises(ValueError, self.alg.predict, self.sc.parallelize([]))


	@classmethod
	def tearDownClass(self):
		del self.bookings
