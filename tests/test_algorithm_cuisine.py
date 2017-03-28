from unittest import TestCase

from src.recommenders import CuisineType
from base_content import ContentBaseTest
from stubs.stub_config import StubConfig

class CuisineTypeTest(ContentBaseTest, TestCase):

	@classmethod
	def setUpClass(self):
		self.alg = CuisineType(self.sc, config=StubConfig)
