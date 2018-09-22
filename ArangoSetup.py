activate_this_file = "C:/Users/Christian/Dropbox (Personal)/dora_research/data/venv/Scripts/activate_this.py"
execfile(activate_this_file, dict(__file__=activate_this_file))

import emile_connect as emile
import pickle

db = emile.connect_to_emile()
paperCollection = db.collection('Paper')

# Get the AQL API wrapper

aql = db.aql

#print(len(paperCollection.find({'Y':2017})))
'''
FILTER_query = """FOR paper IN Paper
			LIMIT 1000000
			FILTER paper.CC > 1000
			RETURN paper"""

SORT_query = """FOR paper IN Paper
			LIMIT 2000000
			FILTER paper.CC > 2000
			SORT paper.CC DESC
			RETURN paper
			
			"""

cursor = aql.execute(SORT_query,count = True)
seed_pub_ids = []

for paper in cursor:
	print(paper['Ti'] + ' \n\t ' + str(paper['Y']) + ' \n\t ' + str(paper['CC']))
	seed_pub_ids.append(paper['Id'])

print(seed_pub_ids)


f = open('seed_pub_ids.pckl','wb')
pickle.dump(seed_pub_ids, f)
'''
f = open('seed_pub_ids.pckl', 'rb')
seed_pub_ids = pickle.load(f)
f.close()


#seeds_cursor = aql.execute("""FOR paper in Paper
#						LIMIT 2000000
#						FILTER paper.Id in """ + str(seed_pub_ids) +"""
#						RETURN paper
#	""",count=True)
'''
RId_array = []
for seed in seeds_cursor:
	if 'RId' in seed:
		RId_array.append(seed['RId'])
	else:
		RId_array.append([])
f = open('RId_array.pckl','wb')
pickle.dump(RId_array, f)
'''
f = open('RId_array.pckl','rb')
RId_array = pickle.load(f)
f.close

'''
citation_arrays = []
for seed_pub_id in seed_pub_ids:
	seed_citations_array = []
	# citations_cursor = aql.execute("""FOR paper IN Paper
	#									LIMIT 2000000
	#									FILTER """ + str(seed_pub_id) + """ in paper.RId
	#									RETURN paper
	#	""")
	

for citation in citations_cursor:
	seed_citations_array.append(citation['Id'])
print(seed_citations_array)
citation_arrays.append(seed_citations_array)
f = open('citation_arrays.pckl','wb')
pickle.dump(citation_arrays,f)

'''
f = open('citation_arrays.pckl','rb')
citations_array = pickle.load(f)
f.close

citation_arrays = []
for citation_array in citations_array:
	citation_arrays.append(citation_array)

for citation_array in citation_arrays:
	print(len(citation_array))

seeds_array_json = []
seed_paper = paperCollection.get(2126996696)
	