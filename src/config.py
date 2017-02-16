from ConfigParser import ConfigParser, RawConfigParser, NoOptionError

class Config:

	rcfg = RawConfigParser()
	rcfg.read("default.cfg");


	@staticmethod
	def get(sec, name, type=int):
		if type is float:
			return float(Config.rcfg.get(sec, name))
		return int(Config.rcfg.get(sec, name))


	@staticmethod
	def set(sec, name, value):
		raise NotImplementedError("Setting config values not implemented yet")

		'''
		print "Setting "+name+" to", value
		Config.rcfg.set(sec, name, str(value))

		with open('default.cfg', 'wb') as configfile:
			Config.rcfg.write(configfile)
		'''