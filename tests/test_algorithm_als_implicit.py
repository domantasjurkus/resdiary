import unittest

from base_als import ALSTest
from src.recommenders import ImplicitALS
from src.data import Data

class ALSImplicitTest(ALSTest, unittest.TestCase):
	
	@classmethod
	def setUpClass(self):
		self.alg = ImplicitALS(self.sc)
		self.data = Data(self.sc)
