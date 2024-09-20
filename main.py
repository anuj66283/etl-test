import transform
import load
import extract
import utils


x = extract.put_data()
data = transform.sep_data(x)

arrival = data['arrival']
departure = data['departure']

conn = utils.connect_postgres()

cur = conn.cursor()

cur, conn = load.put_data(arrival, 'arrival', cur, conn)
cur, conn = load.put_data(departure, 'departure', cur, conn)

conn.close()