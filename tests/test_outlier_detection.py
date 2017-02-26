import unittest
import sys, os
from pyspark.sql import SQLContext

from test_superclass import BaseTestCase
from src import data

class OutlierDetectionTest(BaseTestCase):

	@classmethod
	def setUpClass(self):
		self.data = data.Data(self.sc)


	def test_main(self):
		filtered = self.data.filter_outliers(self.bookings)

		# For 100 bookings, around 5 should be filtered
		self.assertTrue(filtered.count() < self.bookings.count())


	@classmethod
	def tearDownClass(self):
		del self.data
