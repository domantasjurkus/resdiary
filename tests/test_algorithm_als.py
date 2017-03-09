import unittest

from pyspark.mllib.recommendation import MatrixFactorizationModel, ALS

from test_superclass import BaseTestCase
from src.recommenders import ExplicitALS as algorithm
from src.data import Data

# Base test class for ALS algorithms
class ALSTest(BaseTestCase):

	@classmethod
	def setUpClass(self):

		# Instantiate alg.model
		self.alg = algorithm(self.sc)


	def test_main(self):	
		# Check that an exception is thrown if wrong argument provided
		self.assertRaises(TypeError, self.alg.train, ['foo', 'bar'])

		# Model should only be defined after training
		self.assertFalse(hasattr(self.alg, 'model'))

		self.alg.train(self.bookings)
		self.assertTrue(hasattr(self.alg, 'model'))
		self.assertTrue(type(self.alg.model) in [MatrixFactorizationModel, ALS])

		# Check for detection of empty RDD
		self.assertRaises(ValueError, self.alg.predict, self.sc.parallelize([]))


	@classmethod
	def tearDownClass(self):
		del self.alg