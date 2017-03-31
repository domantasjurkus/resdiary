import os
from unittest import TestCase

from src.data import Data
from src.evaluator import *
from src.recommenders import ExplicitALS

from base import BaseTestCase
from stubs.stub_config import StubConfig

class EvaluatorTest(TestCase, BaseTestCase):

	# Set up fixtures that last for all test cases
	@classmethod
	def setUpClass(self):
                self.bookings = Data(self.sc).get_bookings(os.path.join(
                        os.path.dirname(__file__), 'stubs', 'datastubs',
                        'stub_bookings.txt'))
		self.evaluator = Evaluator(self.sc, ExplicitALS(self.sc),
                                           StubConfig)

	def test_evaluator(self):
		self.evaluator.evaluate(self.bookings)
		self.assertIsInstance(self.evaluator.right_total_evaluation(
                        self.bookings), float)
