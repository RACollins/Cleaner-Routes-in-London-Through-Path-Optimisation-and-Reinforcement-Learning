import gpxpy
import pandas as pd
import matplotlib.pyplot as plt

gpx_file = open("/Users/richardcollins/Desktop/Warwick_uni/CS913/running-from-air-pollution-code/src/richard_strava/Lunch_Run.gpx", "r")

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

coords_df = pd.DataFrame(track_coords, columns = ['Latitude','Longitude','Altitude'])

fig = plt.figure(figsize = (12, 9))
coords_df.plot('Longitude', 'Latitude', color='#A00084', linewidth=1.5)
axes = plt.gca()
axes.set_xlim([bounds.min_longitude-0.0005, bounds.max_longitude+0.0005])
axes.set_ylim([bounds.min_latitude-0.0005, bounds.max_latitude+0.0005])
plt.show()
