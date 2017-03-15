import unittest

from base_content import ContentBaseTest
from src.recommenders import PricePoint

class CuisineTypeTest(ContentBaseTest, unittest.TestCase):

	@classmethod
	def setUpClass(self):
		self.alg = PricePoint(self.sc)
