from unittest import TestCase

from pyspark.sql.dataframe import DataFrame

from src.data import Data
from src.recommenders import System

from base import BaseTestCase
from stubs.stub_algorithm import StubCuisineType
from stubs.stub_config import StubConfig

def collinear(v1, v2):
        '''Are the two vectors collinear? I.e., is one a constant multiple of
        the other?'''
	if len(v1) != len(v2):
		return False
        # Avoid division by 0 by adding 1
	arr = set((v1[i] + 1.0) / (v2[i] + 1.0) for i in range(len(v1)))
	return len(arr) == 1


class SystemAlgorithmTest(TestCase, BaseTestCase):

	def setUp(self):
		self.alg = System(self.sc, config=StubConfig)
		self.data = Data(self.sc, config=StubConfig)
		self.maximum_weight = StubConfig.get('System', 'maximum_weight')

		# Replace all other recommenders with a
		# stub that likes every restaurant
		# This stub must be defined in default.cfg
		self.alg.recommenders = {'CuisineType': StubCuisineType(self.sc)}
                self.alg.weights = {'CuisineType': 1}

	def test01_predict(self):
                self.alg.train(self.bookings)
		# Check that predictions match interface
		self.assertIsInstance(self.alg.predict(
							  self.data.nearby_restaurants(self.bookings)), DataFrame)

		# Check for detection of empty RDD
		self.assertRaises(ValueError, self.alg.predict, self.sc.parallelize([]))

	def test02_weights(self):
		self.alg.recommenders = {"a":"a", "b":"b", "c":"c"}

		weights = self.alg.generate_weights(self.maximum_weight)

		for tpl in weights:
			# All coefficients are in [0, maximum_weight]
			for val in tpl:
				self.assertTrue(0 <= val <= self.maximum_weight)

			# Ensure tuples that are integer multiples
			# of other tuples are not present
			for tpl2 in weights:
				if tpl != tpl2:
					self.assertFalse(collinear(tpl, tpl2))

	def test03_learn(self):
		self.alg.config.set_weights((1, 1, 1, 1))
		self.alg.learn_hyperparameters(self.bookings)

		# After learning, at least one recommender should have a
		# non-zero weight, i.e., the sum has to be positive
		self.assertTrue(sum(self.alg.config.get_weights()) > 0)

	def tearDown(self):
		del self.alg
		del self.data
