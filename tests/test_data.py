from base import BaseTestCase

class DataTest(BaseTestCase):

    def test01_nearby_restaurants(self):
        # ensure there are locations returned
        self.assertTrue(self.data.nearby_restaurants(self.bookings))
