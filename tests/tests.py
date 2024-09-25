import os
import shutil
import unittest
from src import extract, transform
import json
from datetime import datetime
import pandas as pd

class ExtractTest(unittest.TestCase):
    
    def test_get(self):
        rslt = extract.get_data()
        self.assertIn('data', rslt)
        self.assertIn('timestamp', rslt)

    def test_put(self):
        
        test_jsn = json.dumps({"success": "true", "data": {"arrivals": [{"FlightNumber": "BHA610", "OrigDest": "Pokhara", "STASTD_DATE": "2024-09-25 11:20:00", "Airline": "Buddha Air", "type": "Arrival", "ETAETD_date": "2024-09-25 16:12:00", "IntDom": "0", "FlightStatus": "Landed"}, {"FlightNumber": "BHA612", "OrigDest": "Pokhara", "STASTD_DATE": "2024-09-25 12:05:00", "Airline": "Buddha Air", "type": "Arrival", "ETAETD_date": "2024-09-25 16:28:00", "IntDom": "0", "FlightStatus": "Landed"}]}})

        fldr = extract.put_data(test_jsn)
        self.assertTrue(os.path.exists(fldr))
        self.assertEqual(len(os.listdir(fldr)), 1)
        shutil.rmtree(fldr)

class TransformTest(unittest.TestCase):

    def setUp(self):
        self.data = {
            "success": True,
            "timestamp": "2024-09-25T15:00:00Z",
            "data": {
                "arrivals": [
                    {"FlightNumber": "QR646", "OrigDest": "Doha", "STASTD_DATE": "2024-09-25 16:25:00", "Airline": "Qatar Airways", "type": "Arrival", "ETAETD_date": "2024-09-25 16:36:00", "IntDom": "1", "FlightStatus": "Completed"},
                    {"FlightNumber": "NYT788", "OrigDest": "Biratnagar", "STASTD_DATE": "2024-09-25 16:55:00", "Airline": "Yeti Airlines", "type": "Arrival", "ETAETD_date": "2024-09-25 18:29:00", "IntDom": "0", "FlightStatus": None}
                ],
                "departure": [
                    {"FlightNumber": "QR647", "OrigDest": "Doha", "STASTD_DATE": "2024-09-25 17:00:00", "Airline": "Qatar Airways", "type": "Departure", "ETAETD_date": "2024-09-25 17:30:00", "IntDom": "1", "FlightStatus": "Scheduled"},
                    {"FlightNumber": "NYT789", "OrigDest": "Biratnagar", "STASTD_DATE": "2024-09-25 17:30:00", "Airline": "Yeti Airlines", "type": "Departure", "ETAETD_date": "2024-09-25 18:00:00", "IntDom": "0", "FlightStatus": "Cancelled"}
                ]
            }
        }

        self.past = pd.DataFrame({
            'FlightNumber': ['QR646', 'NYT789'],
            'OrigDest': ['Doha', 'Bharatpur'],
            'STASTD_DATE': ['2024-09-25 16:25:00', '2024-09-26 16:55:00'],
            'Airline': ['Qatar Airways', 'Yeti Airlines'],
            'type': ['Arrival', 'Arrival'],
            'ETAETD_date': ['2024-09-25 16:36:00', '2024-09-26 18:29:00'],
            'IntDom': [1, 0],
            'FlightStatus': ['Flying', 'Completed']
        })

        self.present = pd.DataFrame({
            'FlightNumber': ['QR646', 'NYT788'],
            'OrigDest': ['Doha', 'Biratnagar'],
            'STASTD_DATE': ['2024-09-25 16:25:00', '2024-09-25 16:55:00'],
            'Airline': ['Qatar Airways', 'Yeti Airlines'],
            'type': ['Arrival', 'Arrival'],
            'ETAETD_date': [
               '2024-09-25 16:36:00', '2024-09-25 18:29:00'],
            'IntDom': [1, 0],
            'FlightStatus': ['Completed','Landed']
        })

        self.present_time = '2024-09-25T16:00:00Z'
        self.past_time = '2024-09-25T15:00:00Z'

    def test_clean(self):
        new_data, new_time = transform.clean_data(self.data)

        self.assertNotIn('data', new_data)
        self.assertIn(new_time, self.data['timestamp'])

    def test_duplicate_single(self):
        new_data = transform.prevent_duplicates(self.present, self.present_time)

        expected_data = pd.DataFrame({
            'FlightNumber': ['QR646', 'NYT788'],
            'OrigDest': ['Doha', 'Biratnagar'],
            'STASTD_DATE': ['2024-09-25 16:25:00', '2024-09-25 16:55:00'],
            'Airline': ['Qatar Airways', 'Yeti Airlines'],
            'type': ['Arrival', 'Arrival'],
            'ETAETD_date': [
                [{'time': '2024-09-25 16:36:00', 'timestamp': self.present_time}],
                [{'time': '2024-09-25 18:29:00', 'timestamp': self.present_time}]
            ],
            'IntDom': [True, False],
            'FlightStatus': [
                [{'status': 'Completed', 'timestamp': self.present_time}],
                [{'status': 'Landed', 'timestamp': self.present_time}]
            ]
        })

        pd.testing.assert_frame_equal(new_data.reset_index(drop=True), expected_data.reset_index(drop=True))


    def test_duplicate_multiple(self):
        new_data = transform.prevent_duplicates(self.present, self.present_time, self.past, self.past_time)

        expected_data = pd.DataFrame({
            'FlightNumber': ['QR646', 'NYT788'],
            'OrigDest': ['Doha', 'Biratnagar'],
            'STASTD_DATE': ['2024-09-25 16:25:00', '2024-09-25 16:55:00'],
            'Airline': ['Qatar Airways', 'Yeti Airlines'],
            'type': ['Arrival', 'Arrival'],
            'ETAETD_date': [
                [{'time': '2024-09-25 16:36:00', 'timestamp': self.present_time}],
                [{'time': '2024-09-25 18:29:00', 'timestamp': self.present_time}],
                ],
            'IntDom': [True, False],
            'FlightStatus': [
                [{'status': 'Completed', 'timestamp': self.present_time}],
                [{'status': 'Landed', 'timestamp': self.present_time}],
            ]
        })

        pd.testing.assert_frame_equal(new_data.reset_index(drop=True), expected_data.reset_index(drop=True))

class LoadTest(unittest.TestCase):
    pass

if __name__ == '__main__':
    unittest.main()