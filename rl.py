import numpy as np
import sys
import networkx as nx
import pandas as pd
import geopandas as gpd
import psycopg2
import math
import pickle
import random
import operator
import csv

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

node_query = """
    SELECT * FROM model2.nodes;
"""

edge_query = """
    SELECT * FROM model2.edges;
"""

connection = db_connect()       # get connection to postgres database
graph = load_graph(connection)  # load graph from db
cur = connection.cursor()

# create df for nodes and edges
nodes = pd.read_sql(node_query, connection)
edges = pd.read_sql(edge_query, connection)

# define mapping from id to state
#eid_to_state_dict = {row["eid"]: index for index, row in edges.iterrows()}
nid_to_state_dict = {row["nid"]: index for index, row in nodes.iterrows()}

def reward_gradient(d):
    return(500*(1 - math.exp(-(100/(1+d)))))

def set_reward_gradient(graph, targetNodeID, visualise = False):
    spDict = nx.single_source_dijkstra_path_length(graph, targetNodeID, weight = "distance")

    # set distance to target (d2t) for all nodes
    if visualise:
        for nid, d in spDict.items():
            cur.execute("UPDATE model2.nodes SET d2t=%s WHERE nid=%s;", (d, nid))

    # rewards assosiated with proximity to source target node
    d2t_rewards_dict = {nid: reward_gradient(d) for nid, d in spDict.items()}
    # convert to dataframe
    d2t_rewards_df = pd.DataFrame(list(d2t_rewards_dict.items()), columns=["nid", "d2t_reward"])
    return(d2t_rewards_df)

def update_edges_df(edges, d2t_rewards):
    extra_rewards_df = edges.merge(d2t_rewards, how = 'inner', left_on = 'startnode', right_on = 'nid')
    extra_rewards_df["total_reward"] = extra_rewards_df["reward"] + extra_rewards_df["d2t_reward"]
    return(extra_rewards_df)

def set_environment_and_qs(nodes, edges, graph):
    nStates = nodes.shape[0]
    nActions = edges.shape[0]
    env = np.empty((nStates, nActions), object)
    q_table0 = np.zeros((nStates, nActions))
    for i, _ in nodes.iterrows():
        nid = nodes.iloc[i, 0]
        adj_dict = graph[nid]
        for sPrime, attrs in adj_dict.items():
            try:
                j = list(edges.loc[edges["eid"] == attrs["eid"]].index)[0]
            except IndexError:
                continue
            reward = edges.iloc[j, -1]
            sPrimeIndex = list(nodes.loc[nodes["nid"] == sPrime].index)[0]
            env[i, j] = (sPrimeIndex, reward)
            q_table0[i, j] = reward
    return(env, q_table0)

def eps_greedy_q_learning_with_table(env, source_state, target_state, q0, num_episodes = 5000, gamma = 0.95, epsilon = 0.5,
                                        learning_rate = 0.8, decay_factor = 0.999):
    q_table = q0
    average_rewards = []
    for i in range(num_episodes):
        print("Episode: {}".format(i))
        s = source_state
        epsilon *= decay_factor
        episode_rewards = []
        while s != target_state:
            # select the action with highest cummulative reward
            if np.random.random() < epsilon or np.sum(q_table[s, :]) == 0:
                potential_actions = np.transpose(np.nonzero(env[s, :])).flatten()
                a = random.choice(potential_actions)
            else:
                a = np.argmax(q_table[s, :])
            #new_s, r, done, _ = env.step(a)
            new_s = env[s, a][0]
            r = env[s, a][1]
            q_table[s, a] += r + learning_rate * (gamma * np.max(q_table[new_s, :]) - q_table[s, a])
            s = new_s
            episode_rewards.append(r)
        average_reward_per_episode = sum(episode_rewards)/float(len(episode_rewards))
        average_rewards.append(average_reward_per_episode)
        print(average_reward_per_episode)
        print("")
    return(average_rewards)


# "agent's" initials as input from terminal
initials = sys.argv[1]

if __name__ == "__main__":

    with open('edges_with_rewards_{}.pkl'.format(initials), 'rb') as input:
        edges_with_rewards = pickle.load(input)

    rewards_gradients_df = set_reward_gradient(graph, '4CF14236-A0DD-40C4-B644-982A59FC625E')
    total_reward_df = update_edges_df(edges = edges_with_rewards, d2t_rewards = rewards_gradients_df)

    print("Updating QGIS map...")
    #for _, row in total_reward_df.iterrows():
    #    r = row["total_reward"]
    #    eid = row["eid"]
    #    cur.execute("UPDATE model2.edges SET weight_scaling=%s WHERE eid=%s;", (r, eid))

    print("Constructing environment, please wait...")
    env = set_environment_and_qs(nodes = nodes, edges = total_reward_df, graph = graph)[0]
    initial_qs = set_environment_and_qs(nodes = nodes, edges = total_reward_df, graph = graph)[1]

    # define mapping from id to state
    nid_to_state_dict = {row["nid"]: index for index, row in nodes.iterrows()}
    initial_state = nid_to_state_dict['01F8CC4A-04FE-40C5-9DF4-4A9E50B24AB5']
    target_state = nid_to_state_dict['4CF14236-A0DD-40C4-B644-982A59FC625E']


    average_rewards = eps_greedy_q_learning_with_table(env, initial_state, target_state, q0 = initial_qs)
    max_index, max_value = max(enumerate(average_rewards), key = operator.itemgetter(1))
    connection.commit()

    #print(graph['41FF673E-F74C-46EA-BCD0-7FD9B025CF24'])
    #print(nodes["d2t"].isnull().sum())
    #for index, row in nodes.iterrows():
    #    print(nodes.iloc[index, 0])
    #print(random.choice(np.transpose(np.nonzero(env[18109, :])).flatten()))
    #print(env[18108, 8699])
    #print(env[18109, 940])
    #print(env[18109, 940])
    #print(initial_qs[18109, 940])

    with open('average_rewards.csv', 'w') as file:
        wr = csv.writer(file, quoting = csv.QUOTE_ALL)
        wr.writerow(average_rewards)

    print("The route from episode {} yeilds the highest average reward of {}".format(max_index, max_value))
    print("")
    print("OK desu!")
    print("")
