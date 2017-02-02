from base import Base

class Recommender(Base):
    '''All recommenders should extend this class. Enforces a consistent
    interface.'''

    def train(self, data):
        """Takes a DataFrame of bookings. Doesn't return anything."""
        if not isinstance(data, DataFrame):
            raise TypeError('Recommender requires a DataFrame')
        raise NotImplementedError("Don't use this class, extend it")

    def predict(self, data):
        '''Takes an RDD list of diner IDs. Returns a DataFrame with the schema:
        Recommendation(userID, restaurantID, score).'''
        raise NotImplementedError("Don't use this class, extend it")
