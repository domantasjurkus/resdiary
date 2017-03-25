import unittest

from src.recommenders import CuisineType
from base_content import ContentBaseTest
from stubs.stub_config import StubConfig

class PricePointTest(ContentBaseTest, unittest.TestCase):
	
	@classmethod
	def setUpClass(self):
		self.alg = CuisineType(self.sc, config=StubConfig)
