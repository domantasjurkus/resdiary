import unittest
import sys, os
from pyspark.sql import SQLContext

from src import evaluator
from base import BaseTestCase
from stubs.stub_algorithm import StubCuisineType

class EvaluatorTest(BaseTestCase):

	# Set up fixtures that last for all test cases
	@classmethod
	def setUpClass(self):
		pass


	def test_evaluator(self):
		score = evaluator.evaluate(
			self.sc,
			StubCuisineType(self.sc),
			self.bookings
		)

		self.assertTrue(isinstance(score, float));


	@classmethod
	def tearDownClass(self):
		pass
