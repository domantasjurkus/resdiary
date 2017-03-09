import unittest

from pyspark.mllib.recommendation import MatrixFactorizationModel, ALS

from src.recommenders import ExplicitALS as algorithm
from base import BaseTestCase
from src.data import Data

# Base test class for ALS algorithms
class ALSTest(BaseTestCase):

	@classmethod
	def setUpClass(self):
		# Instantiate alg.model
		self.alg = algorithm(self.sc)
		self.data = Data(self.sc)


	def test01_train(self):
		# Check that an exception is thrown if wrong argument provided
		self.assertRaises(TypeError, self.alg.train, ['foo', 'bar'])
		self.alg.train(self.bookings)

		# Training should produce a model attribute
		self.assertTrue(hasattr(self.alg, 'model'))
		self.assertTrue(type(self.alg.model) in [MatrixFactorizationModel, ALS])


	def test02_location(self):
		# Ensure there are locations returned
		self.assertTrue(self.alg.location_filtering(
			self.data.available_restaurants(self.bookings)).count() > 0)


	def test03_predict(self):
		self.alg.predict(self.data.available_restaurants(self.bookings))

	# Learning hyperparameters is currently very costly
	#def test04_hyperparameters(self):
	#	pass


	@classmethod
	def tearDownClass(self):
		del self.alg
