from unittest import TestCase

from pyspark.rdd import RDD
from pyspark.sql.dataframe import DataFrame

from base import BaseTestCase
from stubs.stub_config import StubConfig
from src.data import Data

class DataTest(TestCase, BaseTestCase):

	@classmethod
	def setUpClass(self):
		self.data = Data(self.sc, StubConfig)

	def test_main(self):
		# Test nearby restaurant detection
		nearby = self.data.nearby_restaurants(self.bookings)
		self.assertIsInstance(nearby, RDD)
		self.assertTrue(nearby.count() > 0)
		self.assertTrue(len(nearby.first()) == 2)

		# Test outlier filtering
		filtered = self.data.filter_outliers(self.bookings)
		self.assertIsInstance(filtered, DataFrame)
		self.assertTrue(filtered.count() < self.bookings.count())

	@classmethod
	def tearDownClass(self):
		del self.data
