from unittest import TestCase

from src.recommenders import ExplicitALS
from src.data import Data

from base_als import ALSTest
from stubs.stub_config import StubConfig

class ALSExplicitTest(ALSTest, TestCase):
	
	@classmethod
	def setUpClass(self):
		self.alg = ExplicitALS(self.sc, config=StubConfig)
		self.data = Data(self.sc, config=StubConfig)
