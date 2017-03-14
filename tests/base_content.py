import unittest

from pyspark.sql.dataframe import DataFrame

from src.recommenders import CuisineType as algorithm
from base import BaseTestCase

# Base test class for content-based recommenders
class ContentBaseTest(BaseTestCase):

	@classmethod
	def setUpClass(self):
		self.alg = algorithm(self.sc)


	def test_main(self):
		# No output expected from training
		self.alg.train(self.bookings)

		# TODO: error, too many values to unpack
		# self.assertIsInstance(self.alg.predict(self.bookings), DataFrame)


	@classmethod
	def tearDownClass(self):
		del self.alg
