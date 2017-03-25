import unittest

from src.recommenders import PricePoint
from base_content import ContentBaseTest
from stubs.stub_config import StubConfig

class CuisineTypeTest(ContentBaseTest, unittest.TestCase):

	@classmethod
	def setUpClass(self):
		self.alg = PricePoint(self.sc, config=StubConfig)
