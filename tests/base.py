import os
import unittest
from pyspark import SparkContext
from pyspark.sql import SQLContext

class BaseTestCase(unittest.TestCase):
	sc = SparkContext()

	bookings = SQLContext(sc).read.csv(
		os.path.dirname(__file__)+'/stubs/StubBookings.txt',
		header=True,
		inferSchema=True,
		nullValue='NULL'
	)
