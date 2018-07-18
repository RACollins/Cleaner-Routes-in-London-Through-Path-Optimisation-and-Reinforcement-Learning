import glob
import os
import gpxpy
import gpxpy.geo
import pandas as pd
import matplotlib.pyplot as plt
from rdp import rdp
import math
from math import radians

class Activity:

    def __init__(self, dirpath):
        global gpx_files
        global gpx_file_names
        self.dirpath = dirpath
        gpx_file_names = os.listdir(dirpath)
        gpx_files = glob.glob(dirpath + "/*.gpx")

    def get_file_paths(self):
        return(gpx_files)

    def get_file_names(self):
        return(gpx_file_names)

    def nFiles(self):
        return(len(gpx_files))


class Trajectory:

    def __init__(self, filepath):
        global gpx
        self.filepath = filepath
        gpx_file = open(filepath, "r")
        gpx = gpxpy.parse(gpx_file)

    def summary(self):
        gpx_traj = gpx.tracks[0]
        summary_dict = {"Name": gpx_traj.name,
        "Description": str(gpx_traj.description),
        "Start": str(gpx_traj.get_time_bounds().start_time),
        "End": str(gpx_traj.get_time_bounds().end_time),
        "Duration": str(gpx_traj.get_duration())+"s",
        "Distance": "2D Distance = {0:.3f}m, 3D Distance = {1:.3f}m".format(gpx_traj.length_2d(), gpx_traj.length_3d())}
        return(summary_dict)

    def get_coords(self, epsilon = 0.0):
        track_coords = [[point.longitude, point.latitude, point.elevation]
                                        for track in gpx.tracks
                                            for segment in track.segments
                                                for point in segment.points]
        coords_df = pd.DataFrame(track_coords, columns = ['Longitude', 'Latitude', 'Altitude'])
        track_coords_reduced = rdp(coords_df[['Longitude', 'Latitude']], epsilon = epsilon)
        coords_reduced_df = pd.DataFrame(track_coords_reduced, columns = ['Longitude', 'Latitude'])
        return(coords_reduced_df)

def find_limits(df):
    north_limit = df["lat"].max()
    south_limit = df["lat"].min()
    east_limit = df["lon"].max()
    west_limit = df["lon"].min()
    dict = {"north_limit": north_limit,
            "south_limit": south_limit,
            "east_limit": east_limit,
            "west_limit": west_limit}
    return(dict)

def clip_traj(traj, limits):
    traj = traj[traj["Longitude"].between(limits["west_limit"], limits["east_limit"])
    & traj["Latitude"].between(limits["south_limit"], limits["north_limit"])]
    return(traj)

def euclid_d(x1, y1, x2, y2):
    return(gpxpy.geo.haversine_distance(y1, x1, y2, x2))

def find_start_node(traj, nodes):
    closest_node = None
    if len(traj) != 0:
        start_lon = traj["Longitude"].values[0]
        start_lat = traj["Latitude"].values[0]
        smallest_d = math.inf
        for index, row in nodes.iterrows():
            d = euclid_d(start_lon, start_lat, row["lon"], row["lat"])
            if d < smallest_d:
                closest_node = row["nid"]
                smallest_d = d
    return(closest_node)

def find_start_edge(traj, edges):
    closest_edge = None
    if len(traj) != 0:
        start_lon = traj["Longitude"].values[0]
        start_lat = traj["Latitude"].values[0]
        smallest_d = math.inf
        for index, row in edges.iterrows():
            d = euclid_d(start_lon, start_lat, row["lon"], row["lat"])
            if d < smallest_d:
                closest_edge = row["eid"]
                smallest_d = d
    return(closest_edge)

def find_local_coords(edge, edge_df, graph):
    local_coords = None
    if edge != None:
        startNode = edge_df[edge_df["eid"] == edge]["startnode"].values[0]
        endNode = edge_df[edge_df["eid"] == edge]["endnode"].values[0]
    return(None)

def convert_to_node_list(traj, node_df, node_0):
    visited_nodes = [node_0]
    for index, traj_coord in traj.iterrows():
        traj_lon = traj_coord["Longitude"]
        traj_lat = traj_coord["Latitude"]
        local_node_df = node_df[node_df["lon"].between(traj_lon-0.001, traj_lon+0.001)
                                & node_df["lat"].between(traj_lat-0.001, traj_lat+0.001)]
        smallest_d = math.inf
        for index, node_coord in local_node_df.iterrows():
            d = euclid_d(traj_lon, traj_lat, node_coord["lon"], node_coord["lat"])
            if d < smallest_d:
                closest_node = node_coord["nid"]
                smallest_d = d
            if closest_node != visited_nodes[-1]:
                visited_nodes.append(closest_node)
    return(visited_nodes)

def convert_to_edge_list(traj, edge_df, edge_0):
    visited_edges = [edge_0]
    for index, traj_coord in traj.iterrows():
        traj_lon = traj_coord["Longitude"]
        traj_lat = traj_coord["Latitude"]
        local_edge_df = edge_df[edge_df["lon"].between(traj_lon-0.001, traj_lon+0.001)
                                & edge_df["lat"].between(traj_lat-0.001, traj_lat+0.001)]
        smallest_d = math.inf
        for index, edge_coord in local_edge_df.iterrows():
            d = euclid_d(traj_lon, traj_lat, edge_coord["lon"], edge_coord["lat"])
            if d < smallest_d:
                closest_edge = edge_coord["eid"]
                smallest_d = d
            if closest_edge != visited_edges[-1]:
                visited_edges.append(closest_edge)
    return(visited_edges)
