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
        SELECT eid, gamma, distance, startnode, endnode, geom, weight_scaling
        FROM model2.edges;
    """

    e = gpd.GeoDataFrame.from_postgis(query, con=connection)
    e['pollution'] = e['gamma'] * e['distance'] * e['weight_scaling']             # create new column
    attrs = ['distance', 'pollution', 'gamma', 'geom', 'eid', 'weight_scaling']   # attributes of an edge
    return nx.convert_matrix.from_pandas_edgelist(e, 'startnode', 'endnode', attrs)

def optimisePath(graph, start_v, end_v, minimise_param = "distance"):
    # change minimise_param to optimise distance or pollution
    mincost_nodes = nx.dijkstra_path(graph, start_v, end_v, minimise_param)

    # return edge IDs
    optimum_path_edges = []
    for i in range(len(mincost_nodes)-1):
        edgeID = graph[mincost_nodes[i]][mincost_nodes[i+1]]["eid"]
        optimum_path_edges.append(edgeID)

    return(optimum_path_edges)


connection = db_connect()       # get connection to postgres database
graph = load_graph(connection)  # load graph from db


# set inpath to false for all edges
update_query = "UPDATE model2.edges SET inpath=false, weight_scaling=1.0;"
cur = connection.cursor()
cur.execute(update_query)


# calculate sequence of edges in optimal path
eidseqDistance = optimisePath(graph, '826D215F-22DE-47D3-BBFC-541862EE8804', '4CF14236-A0DD-40C4-B644-982A59FC625E',
                        minimise_param = "distance")

eidseqPollution = optimisePath(graph, '826D215F-22DE-47D3-BBFC-541862EE8804', '4CF14236-A0DD-40C4-B644-982A59FC625E',
                        minimise_param = "pollution")


# set inpath to true for all edges in optimal path
for eid in eidseqPollution:
    tuple = [eid]
    cur.execute("UPDATE model2.edges SET inpath=true WHERE eid=%s;", tuple)

connection.commit()
