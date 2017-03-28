from ConfigParser import RawConfigParser

class StubConfig:

	rcfg = RawConfigParser()
	rcfg.read("tests/stubs/stub_config.cfg")

	@staticmethod
	def get(sec, name, type=int):
		return type(StubConfig.rcfg.get(sec, name))

	@staticmethod
	def set(sec, name, value):
		StubConfig.rcfg.set(sec, name, str(value))
		StubConfig.save_changes()

	@staticmethod
	def get_recommenders():
		recommenders = StubConfig.rcfg.sections()
		recommenders.remove('System')
		return recommenders

	@staticmethod
	def get_schema():
		return StubConfig.get('DEFAULT', 'schema', str).split(',')

	@staticmethod
	def set_weights(weights):
		for recommender, weight in zip(StubConfig.get_recommenders(),
                                               weights):
			StubConfig.rcfg.set(recommender, 'weight', str(weight))
		StubConfig.save_changes()

	@staticmethod
	def get_weights():
		weights = []
		for recommender in StubConfig.get_recommenders():
			weights.append(StubConfig.get(recommender, 'weight'))
		return weights

	@staticmethod
	def set_hyperparameters(recommender, parameters):
		for name, value in parameters.items():
			StubConfig.rcfg.set(recommender, name, str(value))
		StubConfig.save_changes()

	@staticmethod
	def save_changes():
		with open('tests/stubs/stub_config.cfg', 'wb') as configfile:
			StubConfig.rcfg.write(configfile)
