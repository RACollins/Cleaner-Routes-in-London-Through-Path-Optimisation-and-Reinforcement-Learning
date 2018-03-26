"""
The second model for the running from air pollution project.

Refactored to use the networkx and geopandas libraries.

This model currently runs on a small grid with fake (not random) air pollution values of NO2.

Visualisation is done using QGIS. Matplotlib caused too many errors.
"""

from heapq import heappush, heappop
import sys
import networkx as nx
import geopandas as gpd
import psycopg2
import math


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
    e['pollution'] = e['gamma'] * e['distance']                 # create new column
    attrs = ['distance', 'pollution', 'gamma', 'geom', 'eid']   # attributes of an edge
    return nx.convert_matrix.from_pandas_edgelist(e, 'startnode', 'endnode', attrs)

def optimisePath(graph, start_v, end_v, minimise_param = "distance", own_function = False):
    optimum_path_edges = []
    if own_function:
        S, R, hq, pi, prev = [], [], [], {}, {}
        V = graph.nodes(data = False)
        for v in V:
            pi[v] = sys.maxsize
            prev[v] = None
            heappush(hq, (pi[v], v))

        pi[start_v] = 0
        heappush(hq, (pi[start_v], start_v))

        while len(hq) != 0:
            tuple = heappop(hq)     # pop node with lowest weight
            u = tuple[1]
            priority = tuple[0]
            if pi[u] == priority and u not in S:
                S.append(u)
                for v in graph.neighbors(u):
                    if pi[v] > pi[u] + graph.get_edge_data(u, v)[minimise_param]:
                        pi[v] = pi[u] + graph.get_edge_data(u, v)[minimise_param]
                        heappush(hq, (pi[v], v))
                        prev[v] = u

        previous_v = end_v
        path = [previous_v]
        while start_v not in path:
            previous_v = prev[previous_v]
            path.append(previous_v)
        #reverse node order
        mincost_nodes = path[::-1]

    elif own_function == False:
        mincost_nodes = nx.dijkstra_path(graph, start_v, end_v, minimise_param)

    #return edge IDs
    for i in range(len(mincost_nodes)-1):
        edgeID = graph[mincost_nodes[i]][mincost_nodes[i+1]]["eid"]
        optimum_path_edges.append(edgeID)
    return(optimum_path_edges)


connection = db_connect()       # get connection to postgres database
graph = load_graph(connection)  # load graph from db

#calculate sequence of edges in optimal path
eidseqDistance = optimisePath(graph, '826D215F-22DE-47D3-BBFC-541862EE8804', '4CF14236-A0DD-40C4-B644-982A59FC625E',
                        minimise_param = "distance")

eidseqPollution = optimisePath(graph, '826D215F-22DE-47D3-BBFC-541862EE8804', '4CF14236-A0DD-40C4-B644-982A59FC625E',
                        minimise_param = "pollution")

# set inpath to false for all edges
update_query = "UPDATE model2.edges SET inpath=false;"
cur = connection.cursor()
cur.execute(update_query)


for eid in eidseqPollution:
    tuple = [eid]
    cur.execute("UPDATE model2.edges SET inpath=true WHERE eid=%s;", tuple)

connection.commit()
