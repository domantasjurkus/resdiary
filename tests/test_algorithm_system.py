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
		self.maximum_weight = 2

		# Replace all other recommenders with a
		# stub that likes every restaurant
		# This stub must be defined in default.cfg
		self.alg.recommenders = { "CuisineType": StubCuisineType(self.sc) }


	def test01_predict(self):
		# Check that predictions match interface
		self.assertIsInstance(self.alg.predict(self.bookings), DataFrame)

		# Check for detection of empty RDD
		self.assertRaises(ValueError, self.alg.predict, self.sc.parallelize([]))


	def test02_weights(self):
		# Temporarily change recommender count
		temp = self.alg.recommenders
		self.alg.recommenders = {"a":"a", "b":"b", "c":"c"}

		weights = self.alg.generate_weights(self.maximum_weight)

		# All coefficients are in [0, maximum_weight]
		for tpl in weights:
			for val in tpl:
				self.assertTrue(0<=val and val<=self.maximum_weight)

		# Tuples that are integer multiples of other tuples are not present
		# TODO: consider using vector collinearity

		# Bring back stub recommender
		self.alg.recommenders = temp


	def test03_learn(self):
		# Trigger hyperparameter learning without saving output
		self.alg.learn_hyperparameters(self.bookings, False)


	@classmethod
	def tearDownClass(self):
		del self.alg
		del self.data