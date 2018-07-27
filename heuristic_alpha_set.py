from process_route_data import *
import sys
import networkx as nx
import geopandas as gpd
import pandas as pd
import psycopg2
import numpy as np
from irl import find_svf, find_feature_expectations
import pickle
import math
np.set_printoptions(precision = 3)

def db_connect(name='project'):
    """
    Takes the name of the database and returns a connection object.
    """
    try:
        connect_str = "dbname='"+name+"'"
        return psycopg2.connect(connect_str)
    except psycopg2.Error as error:
        print("ERROR CONNECTING TO DB")
        print(error)

def load_graph(connection):
    """
    Builds a graph datastructure from a database.

    Takes a postgresql database connection as parameter.

    Returns a networkx.Graph object with distance, pollution, gamma, geom and eid attributes
    """

    query = """
        SELECT eid, gamma, distance, startnode, endnode, geom
        FROM model2.edges;
    """

    e = gpd.GeoDataFrame.from_postgis(query, con=connection)
    attrs = ['distance', 'gamma', 'geom', 'eid']   # attributes of an edge
    return nx.convert_matrix.from_pandas_edgelist(e, 'startnode', 'endnode', attrs)

node_query = """
    SELECT nid, lon, lat FROM model2.nodes;
"""

edge_query = """
    SELECT * FROM model2.edges;
"""

connection = db_connect()   # get connection to postgres database
G = load_graph(connection)  # load graph from db

# create df for nodes
nodes = pd.read_sql(node_query, connection)
edges = pd.read_sql(edge_query, connection)
nStates = edges.shape[0]

# define mapping from id to state
id_to_state_dict = {row["eid"]: index for index, row in edges.iterrows()}

# feature matrix - rows: states, columns: features
feature_matrix = edges.iloc[:, 9:-2].values

# load Strava data
user_activity = Activity(sys.argv[1])
trajectory_files = user_activity.get_file_paths()
trajectory_names = user_activity.get_file_names()

def set_alphas(feature_matrix, feature_expectations):
    tf_terms = feature_expectations
    idf_terms = np.zeros(feature_matrix.shape[1])
    n_states = feature_matrix.shape[0]
    feature_ubiquity = feature_matrix.sum(axis = 0)

    for i in range(len(idf_terms)):
        idf_terms[i] = math.log(n_states/(1 + feature_ubiquity[i]))

    alphas = tf_terms * idf_terms
    return(alphas)

def normailse021(array):
    return((array - np.amin(array))/(np.amax(array) - np.amin(array)))

if __name__ == "__main__":
    with open('trajectories_TH.pkl', 'rb') as input:
        trajectories_TH = pickle.load(input)
    # calculate alphas
    feature_expectations = find_feature_expectations(feature_matrix = feature_matrix, trajectories = trajectories_TH, id_state_mapping = id_to_state_dict)
    heuristically_set_alphas = set_alphas(feature_matrix = feature_matrix, feature_expectations = feature_expectations)
    heuristically_set_alphas = normailse021(heuristically_set_alphas)
    print(heuristically_set_alphas)
    print("")
    for i in range(len(heuristically_set_alphas)):
        alpha = heuristically_set_alphas[i]
        feature_name = edges.columns[9+i]
        print("{}:{:>10.3f}".format(feature_name, alpha))
        print("")

    # calculate reward for each state
    for i in range(len(heuristically_set_alphas)):
        edges.iloc[:, 9+i] = edges.iloc[:, 9+i] * heuristically_set_alphas[i]

    edges["reward"] = edges.iloc[:, 9:-2].sum(axis=1)


    cur = connection.cursor()

    # set weight_scaling to rewards from edges
    for index, row in edges.iterrows():
        r = row["reward"]
        eid = row["eid"]
        cur.execute("UPDATE model2.edges SET weight_scaling=%s WHERE eid=%s;", (r, eid))

    connection.commit()
    print("Ok desu!")
