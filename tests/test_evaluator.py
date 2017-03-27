import unittest
import sys, os, csv, StringIO
from pyspark.sql import SQLContext
from src.data import Data
from src.evaluator import *
from base import BaseTestCase
from stubs.stub_algorithm import StubCuisineType
from stubs.stub_config import StubConfig
from src.recommenders import ExplicitALS

class EvaluatorTest(unittest.TestCase, BaseTestCase):

	# Set up fixtures that last for all test cases
	@classmethod
	def setUpClass(self):
		self.evaluator = Evaluator(self.sc,ExplicitALS(self.sc), config=StubConfig)
		self.bookings = Data(self.sc).get_bookings('test.txt')


	def test_evaluator(self):
		self.evaluator.evaluate(self.bookings)
		self.assertTrue(isinstance(self.evaluator.right_total_evaluation(self.bookings), float));


	@classmethod
	def tearDownClass(self):
		pass
