import gpxpy
import pandas as pd
import matplotlib.pyplot as plt
from rdp import rdp


gpx_file = open("/Users/richardcollins/Desktop/Warwick_uni/CS913/running-from-air-pollution-code/src/richard_strava/Run_to_work_Southbank.gpx", "r")

gpx = gpxpy.parse(gpx_file)
gpx_track = gpx.tracks[0]
'''print("Name: " + gpx_track.name)
print("Description: " + str(gpx_track.description))
print("Start: " + str(gpx_track.get_time_bounds().start_time))
print("End: " + str(gpx_track.get_time_bounds().end_time))'''

bounds = gpx_track.get_bounds()
'''print("Latitude Bounds: (%f, %f)" % (bounds.min_latitude, bounds.max_latitude))
print("Longitude Bounds: (%f, %f)" % (bounds.min_longitude, bounds.max_longitude))
print("Duration (s): %i" % gpx_track.get_duration())'''

track_coords = [[point.latitude,point.longitude, point.elevation]
                                for track in gpx.tracks
                                    for segment in track.segments
                                        for point in segment.points]

coords_df = pd.DataFrame(track_coords, columns = ['Latitude', 'Longitude', 'Altitude'])
track_coords_reduced = rdp(coords_df[['Latitude', 'Longitude']], epsilon = 0.000001)
coords_reduced_df = pd.DataFrame(track_coords_reduced, columns = ['Latitude', 'Longitude'])
print("Full number of points: " + str(len(coords_df)))
print("Reduced number of points: " + str(len(coords_reduced_df)))


#plot long and lat points
plt.figure(figsize = (12, 9))
#plt.subplot(121)
plt.plot(coords_df['Longitude'], coords_df['Latitude'], color = "blue", linewidth = 2.5, alpha = 0.5)
plt.plot(coords_reduced_df['Longitude'], coords_reduced_df['Latitude'], color = "red", linewidth = 2.5, alpha = 0.5)
axes = plt.gca()
axes.set_xlim([bounds.min_longitude-0.0005, bounds.max_longitude+0.0005])
axes.set_ylim([bounds.min_latitude-0.0005, bounds.max_latitude+0.0005])

'''plt.subplot(122)
plt.plot(coords_reduced_df['Longitude'], coords_reduced_df['Latitude'], color='#A00084', linewidth=1.5)
axes = plt.gca()
axes.set_xlim([bounds.min_longitude-0.0005, bounds.max_longitude+0.0005])
axes.set_ylim([bounds.min_latitude-0.0005, bounds.max_latitude+0.0005])'''
plt.show()
