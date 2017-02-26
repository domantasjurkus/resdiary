import unittest
import sys, os
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.dataframe import DataFrame

from test_superclass import BaseTestCase
from src.recommenders import ALS as algorithm
from src.data import Data

class ALSAlgorithmTest(BaseTestCase):

	@classmethod
	def setUpClass(self):

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
		del self.alg
