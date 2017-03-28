from pyspark.mllib.recommendation import MatrixFactorizationModel, ALS
from pyspark.sql.dataframe import DataFrame

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

	def test02_predict(self):
		self.assertIsInstance(self.alg.predict(
                        self.data.nearby_restaurants(self.bookings)), DataFrame)

	def test03_learn(self):
		self.alg.learn_hyperparameters(self.bookings)

		# Inspect if the values for rank, iterations and lambda
		# are in the expected range
		min_r = self.alg.config.get("DEFAULT", "min_rank")
		max_r = self.alg.config.get("DEFAULT", "max_rank")
		min_i = self.alg.config.get("DEFAULT", "min_iterations")
		max_i = self.alg.config.get("DEFAULT", "max_iterations")
		min_l = self.alg.config.get("DEFAULT", "min_lambda", float)
		max_l = self.alg.config.get("DEFAULT", "max_lambda", float)
		actual_r = self.alg.config.get(type(self.alg).__name__, "rank")
		actual_i = self.alg.config.get(type(self.alg).__name__, "iterations")
		actual_l = self.alg.config.get(type(self.alg).__name__, "lambda", float)
		self.assertTrue(min_r <= actual_r <= max_r)
		self.assertTrue(min_i <= actual_i <= max_i)
		self.assertTrue(min_l <= actual_l <= max_l)

	@classmethod
	def tearDownClass(self):
		del self.alg
		del self.data
