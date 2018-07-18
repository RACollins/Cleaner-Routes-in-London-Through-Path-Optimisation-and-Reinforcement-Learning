import pandas as pd
import psycopg2
pd.options.mode.chained_assignment = None

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

edges = pd.read_csv("/Users/richardcollins/Desktop/Warwick_uni/CS913/running-from-air-pollution-code/csv_files/eid_roadtype.csv")
links = pd.read_csv("/Users/richardcollins/Desktop/Warwick_uni/CS913/running-from-air-pollution-code/csv_files/gid_fow_p_f.csv")

# reduce links df to include only roads that appear in edges df
relevent_links = links.loc[links["gid"].isin(edges["eid"])]

# empty df for updated edges
edges_updated = pd.DataFrame(columns = edges.columns)

# loop through roads in relevent links df
for index, link in relevent_links.iterrows():

    # corresponding road in edge df
    matched_edge = edges[edges["eid"] == link["gid"]]


    # for reference
    link_formofway = link["formofway"]
    link_function = link["function"]
    link_primary = link["__primary"]

    # update edge features
    if link_function == "A Road":
        matched_edge["a_road"] = 1
    elif link_function == "B Road":
        matched_edge["b_road"] = 1
    elif link_function == "Local Access Road":
        matched_edge["local_access_road"] = 1
    elif link_function == "Local Road":
        matched_edge["local_road"] = 1
    elif link_function == "Minor Road":
        matched_edge["minor_road"] = 1
    elif link_function == "Motorway":
        matched_edge["motorway"] = 1
    elif link_function == "Restricted Local Access Road":
        matched_edge["restricted_local_access_road"] = 1
    elif link_function == "Secondary Access Road":
        matched_edge["secondary_access_road"] = 1

    if link_formofway == "Collapsed Dual Carriageway":
        matched_edge["collapsed_dual_carriageway"] = 1
    elif link_formofway == "Dual Carriageway":
        matched_edge["dual_carriageway"] = 1
    elif link_formofway == "Roundabout":
        matched_edge["roundabout"] = 1
    elif link_formofway == "Shared Use Carriageway":
        matched_edge["shared_use_carriageway"] = 1
    elif link_formofway == "Single Carriageway":
        matched_edge["single_carriageway"] = 1
    elif link_formofway == "Slip Road":
        matched_edge["slip_road"] = 1

    if link_primary == "TRUE":
        matched_edge["primary_road"] = 1

    matched_edge["misc_road"] = 0

    edges_updated = pd.concat([edges_updated, matched_edge])

print(edges_updated["a_road"].sum()/len(edges_updated))

connection = db_connect()   # get connection to postgres database
cur = connection.cursor()
for index, edge in edges_updated.iterrows():
    eid = edge["eid"]
    cur.execute("UPDATE model2.edges SET a_road=%s WHERE eid=%s;", (edge["a_road"], eid))
    cur.execute("UPDATE model2.edges SET b_road=%s WHERE eid=%s;", (edge["b_road"], eid))
    cur.execute("UPDATE model2.edges SET local_access_road=%s WHERE eid=%s;", (edge["local_access_road"], eid))
    cur.execute("UPDATE model2.edges SET local_road=%s WHERE eid=%s;", (edge["local_road"], eid))
    cur.execute("UPDATE model2.edges SET minor_road=%s WHERE eid=%s;", (edge["minor_road"], eid))
    cur.execute("UPDATE model2.edges SET motorway=%s WHERE eid=%s;", (edge["motorway"], eid))
    cur.execute("UPDATE model2.edges SET restricted_local_access_road=%s WHERE eid=%s;", (edge["restricted_local_access_road"], eid))
    cur.execute("UPDATE model2.edges SET secondary_access_road=%s WHERE eid=%s;", (edge["secondary_access_road"], eid))
    cur.execute("UPDATE model2.edges SET collapsed_dual_carriageway=%s WHERE eid=%s;", (edge["collapsed_dual_carriageway"], eid))
    cur.execute("UPDATE model2.edges SET dual_carriageway=%s WHERE eid=%s;", (edge["dual_carriageway"], eid))
    cur.execute("UPDATE model2.edges SET roundabout=%s WHERE eid=%s;", (edge["roundabout"], eid))
    cur.execute("UPDATE model2.edges SET shared_use_carriageway=%s WHERE eid=%s;", (edge["shared_use_carriageway"], eid))
    cur.execute("UPDATE model2.edges SET single_carriageway=%s WHERE eid=%s;", (edge["single_carriageway"], eid))
    cur.execute("UPDATE model2.edges SET slip_road=%s WHERE eid=%s;", (edge["slip_road"], eid))
    cur.execute("UPDATE model2.edges SET primary_road=%s WHERE eid=%s;", (edge["primary_road"], eid))
    cur.execute("UPDATE model2.edges SET misc_road=%s WHERE eid=%s;", (edge["misc_road"], eid))

connection.commit()

print("Ok desu!")
