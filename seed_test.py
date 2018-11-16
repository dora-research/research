import awsdora_connect as awsdora
import pickle
import time
import webofscience as WOS
import graph_analytics as gAnalytics
import tablib 

db = awsdora.connect_to_awsdora()
paperCollection = db.collection('Paper')
edgesCollection = db.collection('Edges')
# Get the AQL API wrapper
aql = db.aql

#initialize kwd history
kwd_history = set()

seed_id_papers = ["'Paper/2133391229'","'Paper/2007560160'","'Paper/2125708747'","'Paper/2166691312'","'Paper/2137374477'"]
for seed_id in seed_id_papers:
	f = open("kwd_records_"+seed_id[7:len(seed_id)-1]+".txt","w")
	f.close
	seed_comm_papers, seed_comm_keys, leaf_node_keys = WOS.gen_seed_community(seed_id)
	print('pop of seed_community '+ str(len(seed_comm_keys)))
	seed_comm_ids = set(['Paper/'+ key for key in seed_comm_keys])
	leaf_node_ids = set(['Paper/'+key for key in leaf_node_keys])
	third_neighbor_papers = WOS.get_third_neighbors(seed_comm_ids,leaf_node_ids)
	#print(len(seed_community_keys))
	corpus_array_Ti = WOS.gen_corpus_string(seed_comm_keys,'Ti')
	corpus_array_IA = WOS.gen_corpus_string(seed_comm_keys,'E')
	corpus_array = corpus_array_Ti + corpus_array_IA
	#print(corpus_string[0:10000])
	kwd_mat = WOS.extract_keywords(corpus_array)
	all_kwds = set()

	for kwd_array in kwd_mat:
		for kwd_tuple in kwd_array[0:20]:
			all_kwds.add(kwd_tuple[0])

	all_kwds = all_kwds.difference(kwd_history)
	print('number of new keywords = ' + str(len(all_kwds)))
	kwd_history = all_kwds.union(kwd_history)
	#xls data
	data = tablib.Dataset()
	data.headers = ['keyword','bounded','num pubs','probe score','duration','feature1','feature2','feature3','feature4']

	#determine boundedness and extract features 
	for kwd in all_kwds:
		f = open("kwd_records_"+seed_id[7:]+".txt","a")
		t0 = time.time()
		print('evaluating: ' + str(kwd))
		f.write('\nevaluating: ' + str(kwd))

		KWD_BOUNDED, papers = WOS.test_boundedness(kwd,seed_comm_papers, third_neighbor_papers, seed_comm_keys, leaf_node_keys,1000,30)
		print('\tkwd bounded? => ' + str(KWD_BOUNDED))
		f.write('\n\tkwd bounded? => ' + str(KWD_BOUNDED))
		print('\tnum pubs => ' + str(len(papers)))
		f.write('\n\tnum pubs => ' + str(len(papers)))
		
		score = 0
		graph_features = [0,0,0,0]
		if KWD_BOUNDED: 
			#get associated edges
			edge_cursor = WOS.get_edges_from_docs(papers)
			edges_in_kwd_subset = []
			for edge in edge_cursor:
				edges_in_kwd_subset.append({'_to':edge['_to'],'_from':edge['_from']})
			#convert to nx graph
			G, v_dict = WOS.convert_to_nxGraph(list(papers),list(edges_in_kwd_subset))
			graph_features = gAnalytics.gen_graph_features(G)
			print('\tprobe score => ' + str(score))
			f.write('\n\tprobe score => '+ str(score))
		t1 = time.time()
		print('\tduration: ' + str(t1-t0))
		f.write('\n\tduration: ' + str(t1-t0))
		f.close()
		data.append([kwd,str(KWD_BOUNDED),str(len(papers)),str(score),str(t1-t0),str(graph_features[0]),str(graph_features[1]),str(graph_features[2]),str(graph_features[3])])
		with open("kwd_records_"+seed_id[7:]+".xls",'wb') as f2:
			f2.write(data.xls)