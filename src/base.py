class Base(object):
    '''This class is meant to be used as a superclass for any class that needs a
    SparkContext instance'''

    def __init__(self, spark):
        self.spark = spark
