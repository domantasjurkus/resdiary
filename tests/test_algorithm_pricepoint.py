import unittest

from base_content import ContentBaseTest
from src.recommenders import CuisineType

class PricePointTest(ContentBaseTest, unittest.TestCase):
	
	@classmethod
	def setUpClass(self):
		self.alg = CuisineType(self.sc)
