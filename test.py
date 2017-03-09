import os, sys
import unittest
import coverage

# Runs the unit tests with coverage
def cover():
	cov = coverage.coverage(
		branch=True,
		include='src/*.py'
	)
	cov.start()
	tests = unittest.TestLoader().discover('tests')
	unittest.TextTestRunner(verbosity=2).run(tests)
	cov.stop()
	cov.save()

	print 'Coverage Summary:'
	cov.report()
	basedir = os.path.abspath(os.path.dirname(__file__))
	covdir = os.path.join(basedir, 'coverage')
	cov.html_report(directory=covdir)
	cov.xml_report(outfile="coverage/coverage.xml")
	cov.erase()

if __name__ == "__main__":
	cover()