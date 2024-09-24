import os
import shutil
import unittest
from src import extract, transform
from datetime import datetime

class ExtractTest(unittest.TestCase):
    
    def test_get(self):
        rslt = extract.get_data()
        self.assertIn('data', rslt)
        self.assertIn('timestamp', rslt)

    def test_put(self):
        fldr = extract.put_data()
        self.assertTrue(os.path.exists(fldr))
        self.assertEqual(len(os.listdir(fldr)), 1)

class TransformTest(unittest.TestCase):

    def test_sep(self):
        fldr = datetime.now().strftime("%Y-%m")
        rtr = transform.sep_data(fldr)

        self.assertIn('FlightNumber', rtr['arrival'])
        self.assertIn('FlightNumber', rtr['departure'])

        shutil.rmtree(fldr)

class LoadTest(unittest.TestCase):
    pass

if __name__ == '__main__':
    unittest.main()