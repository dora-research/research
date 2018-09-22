from arango import ArangoClient
client = ArangoClient(host = "104.173.204.75")
db = client.db('dora', username = 'christian',password = 'Poochie')
paperCollection = db.collection('Paper')


# Get the AQL API wrapper

aql = db.aql

print(len(paperCollection.find({'Y':2017})))

#cursor = aql.execute('FOR paper IN Paper RETURN paper.CC')
