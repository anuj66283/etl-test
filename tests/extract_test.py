import extract
import json
import pytest

def test_get_data():
    x = extract.get_data()

    with open('tests/test.json', 'w') as f:
        json.dump(x, f)

    assert 'timestamp' in x
    assert 'data' in x
    assert 'arrivals' in x['data']
    assert 'departure' in x['data']