import unittest

from pyspark.sql.dataframe import DataFrame

from base import BaseTestCase
from src.recommenders import System as algorithm
from stubs.stub_algorithm import StubCuisineType
from src.data import Data

class SystemAlgorithmTest(BaseTestCase):

	@classmethod
	def setUpClass(self):
		self.alg = algorithm(self.sc)
		self.data = Data(self.sc)

		# Replace all other recommenders with a
		# stub that likes every restaurant
		# This stub must be defined in default.cfg
		self.alg.recommenders = { "CuisineType": StubCuisineType(self.sc) }


	def test_main(self):
		# Check that predictions match interface
		self.assertIsInstance(self.alg.predict(self.bookings), DataFrame)

		# Check for detection of empty RDD
		self.assertRaises(ValueError, self.alg.predict, self.sc.parallelize([]))

		# Trigger hyperparameter learning without saving output
		self.alg.learn_hyperparameters(self.bookings, False)


	@classmethod
	def tearDownClass(self):
		del self.alg
		del self.data