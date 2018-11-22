from config import db
import networkx as nx
import matplotlib.pyplot as plt
import numpy as np


# randomly select microsoft fields
NUM_FIELDS = 10
fields = db.aql.execute('''
FOR doc IN Field
  SORT RAND()
  LIMIT {}
  RETURN doc
'''.format(NUM_FIELDS))
fields = [f for f in fields]

# 31827203 - tandem mass spectrometry - l3
ids = [f['Id'] for f in fields]
fields_string = ', '.join(map(str, ids))

JUMP = 100000
# total = db.aql.execute('return count(PaperDev)').next()
total = 500000

def query(offset):
  return db.aql.execute('''
  FOR doc IN PaperDev
    LIMIT {}, {}
    FILTER [ {} ] ANY IN doc.F[*].FId
    RETURN {{ Id: doc.Id, RId: doc.RId, FIds: doc.F[*].FId }}
  '''.format(offset, JUMP, fields_string))





def buildGraph(docs):
  G = nx.Graph()
  
  # add nodes
  for paper in docs:
    G.add_node(paper['Id'])

  # add edges
  for paper in docs:
    for citation in paper['RId']:
      if G.has_node(citation):
        G.add_edge(paper['Id'], citation)

  return G




def runAnalytics(G):
  # GRAPH STATISTICS
  node_connectivity_deg = nx.average_degree_connectivity(G)
  graph_connectivity_deg = np.mean(list(node_connectivity_deg))

  graph_density = nx.density(G)

  graph_node_count = len(G.node)
  graph_edge_count = G.number_of_edges()

  print('''
  density:             {}
  connectivity degree: {}
  node count:          {}
  edge count:          {}
  ---------------------------


  '''.format(graph_density, graph_connectivity_deg, graph_node_count, graph_edge_count))


  # #COMPONENT STATISTICS
  # #find size (number of nodes) of greatest connected component
  # Gc = max(nx.connected_component_subgraphs(G), key=len)
  # max_cc_size = len(Gc)
  # percent_GC = max_cc_size/graph_node_count

  # #create a list of connected components sorted by size
  # cc_list = sorted(nx.connected_component_subgraphs(G), key=len, reverse = True)

  # #find the major connected components of the map
  # major_cc_list = []
  # LCCs_node_count = 0
  # for cc in cc_list:
  #   if len(cc) >= max_cc_size/2:
  #     major_cc_list.append(cc)
  #     LCCs_node_count += len(cc)
  # percent_LCCs = LCCs_node_count/graph_node_count


  # #large connected components (LCCs) statistics
  # counter = 0
  # major_cc = Gc
  # counter += 1
  # #compute major component stats
  # node_connectivity_deg = nx.average_degree_connectivity(major_cc)
  # major_cc_connectivity_deg = np.mean(list(node_connectivity_deg))
  # major_cc_density = nx.density(major_cc)
  # LCC_node_count = len(major_cc)
  # LCC_edge_count = major_cc.number_of_edges()
  # avg_node_deg = LCC_edge_count/LCC_node_count
  # normalized_size = LCC_node_count/max_cc_size
  # print([len(G.node),nx.density(major_cc),percent_GC,major_cc_connectivity_deg])


  # plt.figure()
  # nx.draw(G, node_size=5)
  return G



def fieldInfo(id):
  for field in fields:
    if field['Id'] == id:
      print('''
  ---------------------------
  id:        {}
  name:      {}
  level:     {}
  citations: {}
      '''.format(field['Id'], field['DFN'], field['FL'], field['CC']))
      return

graphs = []

id_to_data = {}
for id in ids:
  id_to_data[id] = []
def main(offset):
  # fill id_to_data [] with field papers
  cursor = query(offset)
  for doc in cursor:
    for id in doc['FIds']:
      if id in id_to_data:
        id_to_data[id].append({ 'Id': doc['Id'], 'RId': doc['RId'] })

  print(offset)
  # either continue querying or end and build graph for networkx
  if offset < total:
    main(offset + JUMP)
  else:
    for key in id_to_data:
      fieldInfo(key)
      G = buildGraph(id_to_data[key])
      runAnalytics(G)
      graphs.append(G)

main(0)


index = 0
# for g in graphs:
#   plt.figure(index)
#   index += 1
plt.figure(1)
nx.draw(graphs[0], node_size=5)
plt.figure(2)
nx.draw(graphs[1], node_size=5)

plt.show()