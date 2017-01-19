import unittest
from pyspark import SparkContext

# Subclass this class so that all test cases
# share a single SparkContext
class BaseTestCase(unittest.TestCase):
	sc = SparkContext()
