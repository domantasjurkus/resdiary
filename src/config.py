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
		Config.rcfg.set(sec, name, str(value))
		with open('default.cfg', 'wb') as configfile:
			Config.rcfg.write(configfile)

        @staticmethod
        def sections():
                '''Returns a list of section names in the config file (except
                Default). Also known as the names of all the recommenders.'''
                return Config.rcfg.sections()
