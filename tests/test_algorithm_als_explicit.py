import unittest

from base_als import ALSTest
from src.recommenders import ExplicitALS
from src.data import Data

class ALSExplicitTest(ALSTest, unittest.TestCase):
	
	@classmethod
	def setUpClass(self):
		self.alg = ExplicitALS(self.sc)
		self.data = Data(self.sc)
