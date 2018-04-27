import glob
import os
import gpxpy
import pandas as pd
import matplotlib.pyplot as plt
from rdp import rdp


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
        track_coords = [[point.latitude, point.longitude, point.elevation]
                                        for track in gpx.tracks
                                            for segment in track.segments
                                                for point in segment.points]
        coords_df = pd.DataFrame(track_coords, columns = ['Latitude', 'Longitude', 'Altitude'])
        track_coords_reduced = rdp(coords_df[['Latitude', 'Longitude']], epsilon = epsilon)
        coords_reduced_df = pd.DataFrame(track_coords_reduced, columns = ['Latitude', 'Longitude'])
        return(coords_reduced_df)
