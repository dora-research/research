import awsdora_connect as awsdora
import pickle
import time
import networkx as nx
from gensim.summarization import keywords


db = awsdora.connect_to_awsdora()
paperCollection = db.collection('Paper')
edgesCollection = db.collection('Edges')
# Get the AQL API wrapper
aql = db.aql

def gen_seed_community(seed_pub_id):
	#get entire seed community _keys
	seed_query = """FOR v, e, p IN 1..2 
					ANY """+str(seed_pub_id)+""" 
					GRAPH 'RIdGraph'
					FILTER e.type == 'RId' OR !HAS(e,"type")  
					RETURN {'_key':v._key}"""
	cursor = aql.execute(seed_query)
	seed_comm_pub_keys = set([paper['_key'] for paper in cursor])

	#get leaf node _keys
	nearest_neighbor_query = """FOR v, e, p IN 1
					ANY """+str(seed_pub_id)+""" 
					GRAPH 'RIdGraph'
					FILTER e.type == 'RId' OR !HAS(e,"type")  
					RETURN {'_key':v._key}"""
	cursor = aql.execute(nearest_neighbor_query)
	nearest_neighbor_pub_keys = set([paper['_key'] for paper in cursor])
	leaf_nodes_pub_keys = seed_comm_pub_keys.difference(nearest_neighbor_pub_keys)

	#get full papers
	paper_query = """FOR p in Paper
					FILTER p._key IN """ + str(list(seed_comm_pub_keys))+"""
					LIMIT """ + str(len(seed_comm_pub_keys)) + """
					RETURN p
					"""
	paper_cursor = aql.execute(paper_query)
	seed_comm_papers = [paper for paper in paper_cursor]
	return seed_comm_papers, seed_comm_pub_keys, leaf_nodes_pub_keys
def gen_corpus_string(pub_ids,attribute_type):
	text_query_E = """FOR paper IN Paper
					FILTER paper._key IN """ +str(list(pub_ids)) + """
					SORT paper.CC DESC
					LIMIT 1000
					RETURN {'_key':paper._key,'E':paper.E}
				"""
	text_query_Ti = """FOR paper IN Paper
					FILTER paper._key IN """ +str(list(pub_ids)) + """
					SORT paper.CC DESC
					LIMIT 1000
					RETURN {'_key':paper._key,'Ti':paper.Ti}
					"""
	corpus_array = []

	if attribute_type == 'E':
		cursor = aql.execute(text_query_E)
		for paper in cursor:
			if 'E' in paper.keys():
				if 'IA' in paper['E'].keys(): 
					string = IA_to_str(paper['E']['IA'])
					corpus_array.append(string)
	elif attribute_type == 'Ti':
		Ti_string = ""
		cursor = aql.execute(text_query_Ti)
		for paper in cursor:
			if 'Ti' in paper.keys():
				if 'Ti' in paper.keys():
					Ti_string = Ti_string+ " " + paper['Ti']
		corpus_array = [Ti_string]
	return corpus_array


def IA_to_str(IA):
	word_array = [""]*IA['IndexLength']
	for word in IA['InvertedIndex']:
		for i in IA['InvertedIndex'][word]:
			word_array[i] = word
	string = ' '.join(word_array)
	return string

def extract_keywords(string_array):
	kwds = []
	for string in string_array:
		new_kwds = keywords(string, scores=True)
		kwds.append(new_kwds)
	return kwds
def gen_keyphrase_FILTER_arg(keyphrase):
	kwd_array = keyphrase.split(' ')
	kwd_equiv_array = []
	filter_str_query = ""#filter query for 1st nearest neighbors
	filter_str_query_2 = ""#filter query for second nearest neighbors
	for kwd in kwd_array:
		equivalent_kwds = equivalent_words(kwd)
		kwd_equiv_array.append(equivalent_kwds)
		if len(filter_str_query) == 0:
			filter_str_query = filter_str_query +  str(list(equivalent_kwds)) + " ANY IN ATTRIBUTES(v.E.IA.InvertedIndex) "
			filter_str_query_2 = filter_str_query_2 +  str(list(equivalent_kwds)) + " ANY IN ATTRIBUTES(p.vertices[2].E.IA.InvertedIndex) "
		else:
			filter_str_query = filter_str_query +  "AND " + str(list(equivalent_kwds)) + " ANY IN ATTRIBUTES(v.E.IA.InvertedIndex) "
			filter_str_query_2 = filter_str_query_2 +  "AND " + str(list(equivalent_kwds)) + " ANY IN ATTRIBUTES(p.vertices[2].E.IA.InvertedIndex) "
	return filter_str_query, filter_str_query_2

def papers_w_kwd_in_seedcomm(keyphrase,seed_pub_id):
	filter_str_query, filter_str_query_2 = gen_keyphrase_FILTER_arg(keyphrase)
	#find papers in seed community with kwd
	seed_community = gen_seed_community(seed_pub_id)
	text_query_IA = """FOR v in Paper
						FILTER v._key IN """+ str(list(seed_community)) + """
						AND """+ filter_str_query+ """
						RETURN {_id:v._id}
						"""
	InvertedIndex_cursor = aql.execute(text_query_IA)
	papers_w_kwd_subset = set()
	for paper in InvertedIndex_cursor:
		papers_w_kwd_subset.add(paper['_id'])
	return papers_w_kwd_subset

def papers_w_kwd_in_seedcomm_fast(keyphrase,seed_comm_papers):
	#obtain equivalent keyword array from keyphrase
	kwd_array = keyphrase.split(' ')
	kwd_equiv_array = []
	for kwd in kwd_array:
		equivalent_kwds = equivalent_words(kwd)
		kwd_equiv_array.append(equivalent_kwds)
	#add papers that include keyphrase array
	papers_w_kwd_subset = set()
	for paper in seed_comm_papers:
		if keyphrase_in_paper(kwd_equiv_array,paper):
			papers_w_kwd_subset.add(paper['_id'])
	return papers_w_kwd_subset
def get_third_neighbors(seed_comm_ids,leaf_node_ids):
	third_neighbor_query_to= """FOR e in Edges
								FILTER (e.type == 'RId' OR !HAS(e,"type") ) 
								AND (e._from IN """ + str(list(leaf_node_ids)) + """
									AND e._to NOT IN """ + str(list(seed_comm_ids)) + """)
								RETURN {'_id':e._to}
							"""
	third_neighbor_query_from = """
							FOR e in Edges
								FILTER (e.type == 'RId' OR !HAS(e,"type") ) 
								AND (e._to IN """ + str(list(leaf_node_ids)) + """
									AND e._from NOT IN """ + str(list(seed_comm_ids)) + """)
								RETURN {'_id':e._from}
							
						"""
	third_neighbor_cursor_to = aql.execute(third_neighbor_query_to)
	third_neighbor_ids_to = set([paper['_id'] for paper in third_neighbor_cursor_to])
	third_neighbor_cursor_from = aql.execute(third_neighbor_query_from)
	third_neighbor_ids_from = set([paper['_id'] for paper in third_neighbor_cursor_from])
	third_neighbor_ids = third_neighbor_ids_to | third_neighbor_ids_from
	print('third neighbors: ' + str(len(third_neighbor_ids)))
	papers_query = """FOR p in Paper
						FILTER p._id IN """+str(list(third_neighbor_ids)) + """
						LIMIT """ + str(len(third_neighbor_ids)) + """
						RETURN p
					"""
	paper_cursor = aql.execute(papers_query)
	third_neighbor_papers = [paper for paper in paper_cursor]
	return third_neighbor_papers

def keyphrase_in_paper(kwd_equiv_array,paper):
	KEYPHRASE_IN_PAPER = True
	try:
		paper_words = set(paper['E']['IA']['InvertedIndex'].keys())
	except:
		paper_words = set()
	for kwd_equiv_set in kwd_equiv_array:
		if len(kwd_equiv_set.intersection(paper_words)) == 0:
			KEYPHRASE_IN_PAPER = False
			break
	return KEYPHRASE_IN_PAPER

def test_boundedness(keyphrase,seed_comm_papers,seed_comm_keys,leaf_node_keys,max_limit,min_limit):
	KWD_BOUNDED = True	
	#get papers containing keyword in seedcommunity
	t0 = time.time()
	papers_w_kwd_subset = papers_w_kwd_in_seedcomm_fast(keyphrase,seed_comm_papers)
	t1 = time.time()
	print('\tseed comm time: ' + str(t1-t0))
	filter_str_query, filter_str_query_2 = gen_keyphrase_FILTER_arg(keyphrase)

	#iteratively expand graph to find pubs with keyword
	leaf_node_ids = set(['Paper/'+key for key in leaf_node_keys])
	leaf_nodes = list(leaf_node_ids.intersection(papers_w_kwd_subset))
	while len(papers_w_kwd_subset) < max_limit and len(leaf_nodes) > 0:
		new_leaf_nodes = set()
		#check first nearest neighbors of leaf nodes
		for leaf_node in leaf_nodes:
			neighbor_w_kwd_query = """FOR v, e, p IN 1..1
							ANY '""" + leaf_node + """'
							GRAPH 'RIdGraph'
							FILTER (e.type == 'RId' OR !HAS(e,"type") ) AND v._id NOT IN """+str(list(papers_w_kwd_subset))+""" 
								AND """ + filter_str_query +"""
							RETURN {_id: v._id}
							"""
			neighbor_w_kwd_cursor = aql.execute(neighbor_w_kwd_query)

			#add new papers to papers_w_kwd_subset and to new_leaf nodes
			for paper in neighbor_w_kwd_cursor:
				papers_w_kwd_subset.add(paper['_id'])
				new_leaf_nodes.add(paper['_id'])
			#check if KWD_BOUNDED condition violated
			if len(papers_w_kwd_subset) > max_limit:
				KWD_BOUNDED = False
				break
		leaf_nodes = list(new_leaf_nodes)
	if len(papers_w_kwd_subset) < min_limit:
		KWD_BOUNDED = False
	t2 = time.time()
	print('\titerative expansion: ' + str(t2-t1))
									
	return KWD_BOUNDED, papers_w_kwd_subset

def keyphrase_in_paper(keyphrase,paper):
	KEYPHRASE_IN_PAPER = False
	return KEYPHRASE_IN_PAPER

def test_boundedness_fast(keyphrase,seed_comm_papers,third_neighbor_papers,seed_comm_keys,leaf_node_keys,max_limit,min_limit):
	KWD_BOUNDED = True	
	#get papers containing keyword in seedcommunity
	t0 = time.time()
	papers_w_kwd_subset = papers_w_kwd_in_seedcomm_fast(keyphrase,seed_comm_papers)
	t1 = time.time()
	print('\tseed comm time: ' + str(t1-t0))
	filter_str_query, filter_str_query_2 = gen_keyphrase_FILTER_arg(keyphrase)

	if len(papers_w_kwd_subset) < max_limit:
		for paper in third_neighbor_papers:


	#iteratively expand graph to find pubs with keyword
	leaf_node_ids = set(['Paper/'+key for key in leaf_node_keys])
	leaf_nodes = list(leaf_node_ids.intersection(papers_w_kwd_subset))
	while len(papers_w_kwd_subset) < max_limit and len(leaf_nodes) > 0:
		new_leaf_nodes = set()
		#check first nearest neighbors of leaf nodes
		for leaf_node in leaf_nodes:
			neighbor_w_kwd_query = """FOR v, e, p IN 1..1
							ANY '""" + leaf_node + """'
							GRAPH 'RIdGraph'
							FILTER (e.type == 'RId' OR !HAS(e,"type") ) AND v._id NOT IN """+str(list(papers_w_kwd_subset))+""" 
								AND """ + filter_str_query +"""
							RETURN {_id: v._id}
							"""
			neighbor_w_kwd_cursor = aql.execute(neighbor_w_kwd_query)

			#add new papers to papers_w_kwd_subset and to new_leaf nodes
			for paper in neighbor_w_kwd_cursor:
				papers_w_kwd_subset.add(paper['_id'])
				new_leaf_nodes.add(paper['_id'])
			#check if KWD_BOUNDED condition violated
			if len(papers_w_kwd_subset) > max_limit:
				KWD_BOUNDED = False
				break
		leaf_nodes = list(new_leaf_nodes)
	if len(papers_w_kwd_subset) < min_limit:
		KWD_BOUNDED = False
	t2 = time.time()
	print('\titerative expansion: ' + str(t2-t1))
									
	return KWD_BOUNDED, papers_w_kwd_subset

def grow_seedcomm_x2():
	'''
		#check second nearest neighbors too
		if KWD_BOUNDED == True and len(new_leaf_nodes)==0:
			print('second nearest neighbors activated: ')
			for paper in papers_w_kwd_subset:
				second_neighbor_query = """FOR v, e, p IN 1..2
											ANY '"""+paper+"""'
											GRAPH 'RIdGraph'
											FILTER p.vertices[2]._id NOT IN """+str(list(papers_w_kwd_subset))+""" 
												AND """ + filter_str_query_2 +"""
											RETURN {_id:p.vertices[2]._id}
										"""
				second_neighbor_cursor = aql.execute(second_neighbor_query)
				for paper in second_neighbor_cursor:
					new_leaf_nodes.add(paper['_id'])
			papers_w_kwd_subset = papers_w_kwd_subset | new_leaf_nodes
		print(len(papers_w_kwd_subset))
		'''



def equivalent_words(word):
	def add_suffixes(stem):
		equivalents = set()
		equivalents.add(stem+'ies')
		equivalents.add(stem+'s')
		equivalents.add(stem+'ed')
		equivalents.add(stem+'ing')
		equivalents.add(stem+'ly')
		return equivalents
	def add_punctuation(kwd):
		equivalents = set()
		equivalents.add(kwd+'!')
		equivalents.add(kwd+'.')
		equivalents.add(kwd+'...')
		equivalents.add(kwd+'?')
		equivalents.add(kwd+';')
		equivalents.add(kwd+':')
		return equivalents
	def find_stem(kwd):
	    stem = kwd
	    if len(kwd)>=4:
	        if kwd[-3:] == 'ies':
	            stem = kwd[:-3]
	        elif kwd[-1] == 's':
	            stem = kwd[:-1]
	        elif kwd[-2:] == 'ed':
	            stem = kwd[:-2]
	        elif kwd[-3:] == 'ing':
	            stem = kwd[:-3]
	        elif kwd[-2:] == 'ly':
	            stem = kwd[:-2]
	    return stem
	def add_CAPs(kwd):
		equivalents = set()
		equivalents.add(kwd.upper())
		equivalents.add(kwd[0].upper()+kwd[1:])
		return equivalents
	equivalent_words = set()

	stem = find_stem(word)
	new_words_1 = add_suffixes(stem)
	new_words_2 = add_suffixes(word)
	equivalent_words = new_words_1 | new_words_2 
	equivalent_words.add(word)
	equivalent_words.add(stem)

	#add punctuation to equivalent_words set
	new_words = set(list(equivalent_words))
	for new_word in equivalent_words:
		CAPs_words = add_CAPs(new_word)
		new_words = new_words | CAPs_words
	equivalent_words = new_words
	new_words = set(list(equivalent_words))
	for new_word in equivalent_words:
		punctuated_words = add_punctuation(new_word)
		new_words = new_words | punctuated_words
	equivalent_words = new_words
	return equivalent_words

def convert_to_nxGraph(vertices,edges):
	#nodes need to be array of key_ids
	G = nx.Graph()
	#create nodes_dict
	v_dict = {}
	i= 1
	for v in vertices:
		v_dict[v] = i
		G.add_node(i,key = v)
		i = i + 1
	for e in edges:
		v_from = v_dict[e['_from']]
		v_to = v_dict[e['_to']]
		new_e = (v_from,v_to)
		G.add_edge(*new_e)
	return G, v_dict
	
def get_edges_from_docs(doc_ids):
	edge_query_IA = """FOR e in Edges
						FILTER e._from IN """ + str(list(doc_ids)) + """ 
							AND e._to IN """ + str(list(doc_ids)) + """
						RETURN e
					"""
	edge_cursor = aql.execute(edge_query_IA)
	return edge_cursor