import unittest
import sys, os
from pyspark.sql import SQLContext
from stubs import stub_algorithm
from test_superclass import BaseTestCase
from src import evaluator

class EvaluatorTest(BaseTestCase):

	# Set up fixtures that last for all test cases
	@classmethod
	def setUpClass(self):
		pass


	def test_recommendations(self):
		score = evaluator.evaluate(
			self.sc,
			stub_algorithm.StubRecommender(self.sc),
			self.bookings
		)

		self.assertTrue(isinstance(score, float));


	@classmethod
	def tearDownClass(self):
		pass
