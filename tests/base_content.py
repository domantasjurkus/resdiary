from pyspark.sql.dataframe import DataFrame

from base import BaseTestCase

# Base test class for content-based recommenders
class ContentBaseTest(BaseTestCase):

	def test_main(self):
		self.alg.train(self.bookings)

		rdd = self.sc.parallelize([(row['Diner Id'],
                                    row['Restaurant Id'])
                                    for row in self.bookings.collect()])

		self.assertIsInstance(self.alg.predict(rdd), DataFrame)


	@classmethod
	def tearDownClass(self):
		del self.alg
