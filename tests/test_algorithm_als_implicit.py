from unittest import TestCase

from src.recommenders import ImplicitALS
from src.data import Data
from base_als import ALSTest
from stubs.stub_config import StubConfig

class ALSImplicitTest(ALSTest, TestCase):
	
	@classmethod
	def setUpClass(self):
		self.alg = ImplicitALS(self.sc, config=StubConfig)
		self.data = Data(self.sc, config=StubConfig)
