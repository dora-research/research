import networkx as nx
import numpy as np
import webofscience as WOS

def probing_score(features):
    """accepts a features array as input
    features = [{'dist':n_pubs, 
                'separator': n_sep,
                'steepness': 30,
                weight: 1/len(features)}, ...]
    """
    tot_score = 0
    for feature in features:
        x = feature['dist']
        x_sep = feature['separator']
        k = feature['steepness']
        w = feature['weight']
        sub_score = w*(1.0/(1.0+np.exp(-k*(x - x_sep))))
        tot_score += sub_score
    return tot_score
def gen_RId_graph_features(keyphrase,papers_w_keyphrase):
    edge_cursor = WOS.get_edges_from_docs(papers_w_keyphrase)



def gen_graph_features(G):
    #GRAPH STATISTICS
    node_connectivity_deg = nx.average_degree_connectivity(G)
    graph_connectivity_deg = np.mean(list(node_connectivity_deg))

    graph_density = nx.density(G)

    graph_node_count = len(G.node)
    graph_edge_count = G.number_of_edges()

    
    print("\n\t graph density = " + str(graph_density),
        "\n\t graph connectivity degree = " + str(graph_connectivity_deg),
        "\n\t node count = " + str(graph_node_count),
        "\n\t edge count = " + str(graph_edge_count))
    

    #COMPONENT STATISTICS
    #find size (number of nodes) of greatest connected component
    Gc = max(nx.connected_component_subgraphs(G), key=len)
    max_cc_size = len(Gc)
    percent_GC = max_cc_size/graph_node_count

    #create a list of connected components sorted by size
    cc_list = sorted(nx.connected_component_subgraphs(G), key=len, reverse = True)

    #find the major connected components of the map
    major_cc_list = []
    LCCs_node_count = 0
    for cc in cc_list:
        if len(cc) >= max_cc_size/2:
            major_cc_list.append(cc)
            LCCs_node_count += len(cc)
    percent_LCCs = LCCs_node_count/graph_node_count


    #large connected components (LCCs) statistics
    counter = 0
    major_cc = Gc
    counter += 1
    #compute major component stats
    node_connectivity_deg = nx.average_degree_connectivity(major_cc)
    major_cc_connectivity_deg = np.mean(list(node_connectivity_deg))
    major_cc_density = nx.density(major_cc)
    LCC_node_count = len(major_cc)
    LCC_edge_count = major_cc.number_of_edges()
    avg_node_deg = LCC_edge_count/LCC_node_count
    normalized_size = LCC_node_count/max_cc_size
    return [len(G.node),nx.density(major_cc),percent_GC,major_cc_connectivity_deg]
def manually_train_parameters(feature_values):
    #create features array
    features = []
    n_pubs = feature_values[0]
    n_sep = 15.0
    steepness = 0.5
    weight = 0.25
    features.append({'dist':n_pubs,'separator':n_sep,'steepness':steepness,'weight':weight})

    rho_GC = feature_values[1]
    rho_sep = 0.125
    steepness = -50.0
    weight = 0.25
    features.append({'dist':rho_GC,'separator':rho_sep,'steepness':steepness,'weight':weight})

    perc_GC = feature_values[2]
    perc_GC_sep = 0.135
    steepness = -60
    weight = 0.25
    features.append({'dist':perc_GC,'separator':perc_GC_sep,'steepness':steepness,'weight':weight})

    K_GC = feature_values[3]
    K_sep = 10.3
    steepness = 4
    weight = 0.25
    features.append({'dist':K_GC,'separator':K_sep,'steepness':steepness,'weight':weight})
    return features
def probe(G):
    feature_values = gen_graph_features(G)
    features = manually_train_parameters(feature_values)
    tot_score = probing_score(features)
    return tot_score