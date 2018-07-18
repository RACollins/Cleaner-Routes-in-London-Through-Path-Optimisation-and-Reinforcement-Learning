from process_route_data import *
import sys
import networkx as nx
import geopandas as gpd
import pandas as pd
import psycopg2
import numpy as np

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

edge_points_query = """SELECT eid, (g.gdump).path[2] AS seqnum, ST_Y(ST_Transform((g.gdump).geom, 4326)) AS lat,
ST_X(ST_Transform((g.gdump).geom, 4326)) AS lon, startnode, endnode, ST_Transform((g.gdump).geom, 4326) AS geom
FROM (
	SELECT eid, st_dumppoints(geom) AS gdump, startnode, endnode, geom
	FROM model2.edges
) AS g;"""

connection = db_connect()   # get connection to postgres database
G = load_graph(connection)  # load graph from db

# create df for nodes
nodes = pd.read_sql(node_query, connection)
# create df for edge points
edge_points = pd.read_sql(edge_points_query, connection)

#limits_dict = find_limits(nodes)
limits_dict = {"north_limit": 51.556319,
        "south_limit": 51.468594,
        "east_limit": -0.011363,
        "west_limit": -0.152137}

user_activity = Activity(sys.argv[1])

trajectory_files = user_activity.get_file_paths()
trajectory_names = user_activity.get_file_names()

state_dict = {}
traj_count = 0
for file in trajectory_files:
    trajectory = Trajectory(file)
    coordinates = trajectory.get_coords(epsilon = 0.001)
    clipped_coordinates = clip_traj(coordinates, limits_dict)
    start_node = find_start_node(clipped_coordinates, nodes)
    if start_node != None:
        sparse_nodes = convert_to_node_list(traj = clipped_coordinates,
                                            node_df = nodes,
                                            node_0 = start_node)
    reconstructed_nodes = []
    for i in range(len(sparse_nodes)-1):
        shortest_path_segment_nodes = nx.dijkstra_path(G, sparse_nodes[i], sparse_nodes[i+1], "distance")[:-1]
        reconstructed_nodes.extend(shortest_path_segment_nodes)

    # return edge IDs
    traj_state_seq = []
    for i in range(len(reconstructed_nodes)-1):
        edgeID = G[reconstructed_nodes[i]][reconstructed_nodes[i+1]]["eid"]
        traj_state_seq.append(edgeID)

    # count states as a dictionary
    for state in traj_state_seq:
        if state not in state_dict:
            state_dict[state] = 1
        else:
            state_dict[state] += 1
    traj_count += 1
    print(state_dict)
    print("")
    print("Number of trajectories: {0}".format(traj_count))
    print("")


'''edge_dict = {}
traj_count = 0
for file in trajectory_files:
    trajectory = Trajectory(file)
    coordinates = trajectory.get_coords(epsilon = 0.0)
    clipped_coordinates = clip_traj(coordinates, limits_dict)
    start_edge = find_start_edge(clipped_coordinates, edge_points)
    if start_edge != None:
        edge_list = set(convert_to_edge_list(traj = clipped_coordinates,
                                            edge_df = edge_points,
                                            edge_0 = start_edge))
        for edge in edge_list:
            if edge not in edge_dict:
                edge_dict[edge] = 1
            else:
                edge_dict[edge] += 1
    traj_count += 1
    print(edge_dict)
    print("Number of trajectories: {0}".format(traj_count))
    print("")
we_dict = {k: 1-np.exp(-1/v) for k, v in edge_dict.items()}
print(we_dict)'''

# set coutns to 0 for all edges
update_query = "UPDATE model2.edges SET counts=0, weight_scaling=1.0;"
cur = connection.cursor()
cur.execute(update_query)

# set counts to values from edge_dict
for eid, num in state_dict.items():
    #tuple = [eid]
    cur.execute("UPDATE model2.edges SET counts=%s WHERE eid=%s;", (num, eid))

connection.commit()

print("Ok desu!")
