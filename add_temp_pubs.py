#activate virtual environment 
import dora_workspace_setup as dws
dws.activate_venv()

#import modules
import emile_connect as emile

import time

#connect to emiles machine (arango db)
db = emile.connect_to_emile()

#define collections
paperCollection = db.collection('Paper')

# Get the AQL API wrapper
aql = db.aql

count = 10000
for offset in range(0,4000000,count):
	paper_batch = aql.execute("""FOR paper IN Paper
									LIMIT """ + str(offset) + """, """+ str(count)+"""
									RETURN {'_key':paper._key, 'RId':paper.RId}
							""")
	print(offset)
	counter = 0
	t0 = time.time()
	for paper in paper_batch:
		if paper['RId'] != None:
			for RId in paper['RId']:
				date = int(round(time.time() * 1000))
				job = aql.execute(""" UPSERT { _key: \""""+ str(RId) +"""\"}
					INSERT {
						_key:\""""+str(RId)+"""\",
						Id: 0,
						Ti: "",
						L: "",
						Y: 0,
						D: null,
						CC: 0,
						ECC: 0,
						RId: [],
						W: [],
						AA: [],
						F: [],
						J:{},
						E:{DN:"",IA:{},S:[],VFN:"",V:null,I:null,FP:null,LP:null,DOI: null,PR:[],ANF:[],BV:"",BT:""},
						createDate: """+str(date)+"""
						}
					UPDATE {} IN Paper
				 """)
				counter += 1
				if counter == 1000:
					t1 = time.time()
					print(str(t1-t0))
								
		else:
			pass

