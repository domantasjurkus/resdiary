import unittest
import sys, os
from pyspark.sql import SQLContext

from src import evaluator
from base import BaseTestCase
from stubs.stub_algorithm import StubCuisineType
from stubs.stub_config import StubConfig

class EvaluatorTest(unittest.TestCase, BaseTestCase):

	# Set up fixtures that last for all test cases
	@classmethod
	def setUpClass(self):
		pass


	def test_evaluator(self):
		score = evaluator.evaluate(
			self.sc,
			StubCuisineType(self.sc),
			self.bookings,
			StubConfig
		)

		self.assertTrue(isinstance(score, float));


	@classmethod
	def tearDownClass(self):
		pass
