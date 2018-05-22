from process_route_data import *
import sys
import networkx as nx
import geopandas as gpd
import pandas as pd
import psycopg2
import numpy

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

#print(G['1339ABA8-58DD-4CEB-9AAD-FDE38D010DDC'])
### test clipping and find_start_node function
edge_dict = {}
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
print(edge_dict)

# set coutns to 0 for all edges
update_query = "UPDATE model2.edges SET counts=0;"
cur = connection.cursor()
cur.execute(update_query)

# set counts to values from edge_dict
for eid, count in edge_dict.items():
    #tuple = [eid]
    cur.execute("UPDATE model2.edges SET counts=%s WHERE eid=%s;", (count, eid))

connection.commit()

print("Ok desu!")
