import os
import shutil
import unittest
from src import extract

class ExtractTest(unittest.TestCase):
    
    def test_get(self):
        rslt = extract.get_data()
        self.assertIn('data', rslt)
        self.assertIn('timestamp', rslt)

    def test_put(self):
        fldr = extract.put_data()

        self.assertTrue(os.path.exists(fldr))
        self.assertEqual(len(os.listdir(fldr)), 1)

        shutil.rmtree(fldr)




class TransformTest(unittest.TestCase):
    pass

class LoadTest(unittest.TestCase):
    pass

if __name__ == '__main__':
    unittest.main()