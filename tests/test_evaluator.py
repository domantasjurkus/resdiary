import unittest
import sys, os
from pyspark.sql import SQLContext

from src.evaluator import *
from base import BaseTestCase
from stubs.stub_algorithm import StubCuisineType
from stubs.stub_config import StubConfig
from src.recommenders import ExplicitALS

class EvaluatorTest(unittest.TestCase, BaseTestCase):

	# Set up fixtures that last for all test cases
	@classmethod
	def setUpClass(self):
		self.evaluator = Evaluator(self.sc,ExplicitALS)


	def test_evaluator(self):
		score = self.evaluator.evaluate(self.bookings)
		self.assertTrue(isinstance(score, float));


	@classmethod
	def tearDownClass(self):
		pass
