from pyspark.mllib.recommendation import MatrixFactorizationModel, ALS

from base import BaseTestCase

# Base test class for ALS algorithms
class ALSTest(BaseTestCase):

	def test01_train(self):
		# Check that an exception is thrown if wrong argument provided
		self.assertRaises(TypeError, self.alg.train, ['foo', 'bar'])
		self.alg.train(self.bookings)

		# Training should produce a model attribute
		self.assertTrue(hasattr(self.alg, 'model'))
		self.assertTrue(type(self.alg.model) in [MatrixFactorizationModel, ALS])


	def test02_location(self):
		# Ensure there are locations returned
		self.assertTrue(self.alg.location_filtering(
			self.data.available_restaurants(self.bookings)).count() > 0)


	def test03_predict(self):
		self.alg.predict(self.data.available_restaurants(self.bookings))


	@classmethod
	def tearDownClass(self):
		del self.alg
		del self.data
