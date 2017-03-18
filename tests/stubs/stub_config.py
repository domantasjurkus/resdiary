from ConfigParser import RawConfigParser

class StubConfig:

	print "\n\nStub config loaded"
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
		print "StubConfig::set_weights"
		print weights
		for recommender, weight in zip(StubConfig.get_recommenders(), weights):
			print "From config:", recommender, weight
			StubConfig.rcfg.set(recommender, 'weight', str(weight))
		StubConfig.save_changes()

	@staticmethod
	def save_changes():
		with open('default.cfg', 'wb') as configfile:
			StubConfig.rcfg.write(configfile)
