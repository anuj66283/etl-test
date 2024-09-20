import transform

def test_clean_data():
    df = transform.clean_data('tests/test.json')
    assert len(df) > 0

def test_prevent_duplicate():
    df = transform.prevent_duplicates('tests')

    assert len(df) > 0

def test_sep_data():
    df = transform.sep_data('tests')

    assert 'FlightNumber' in df['arrival']
    assert 'FlightNumber' in df['departure']
