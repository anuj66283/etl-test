import transform
import load
import extract
import utils
import json

resp = json.dumps(extract.get_data())

x = extract.put_data(resp)

data = transform.read_files(x)

data = transform.sep_data(data)

arrival = data['arrival']
departure = data['departure']

conn = utils.connect_postgres()

cur = conn.cursor()

cur, conn = load.put_data(arrival, 'arrival', cur, conn)
cur, conn = load.put_data(departure, 'departure', cur, conn)

conn.close()