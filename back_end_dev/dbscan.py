import matplotlib.pyplot as plt
from matplotlib.collections import PatchCollection
from descartes import PolygonPatch
import fiona
from shapely.geometry import MultiPolygon, MultiPoint, shape
import couchdb

from sklearn.cluster import DBSCAN
import numpy as np
import pandas as pd
import matplotlib.cm as cmx
import matplotlib.colors as colors
import json
from geopy.distance import great_circle
from collections import defaultdict


# extreact info from train station json
def extract_ts_cord(source):
    dct = {}
    ts_lons = []
    ts_lats = []
    for feature in source['features']:
        ts_name = feature['properties']['STATIONNAME']
        cord = feature['geometry']['coordinates']
        dct[ts_name] = cord
        ts_lons.append(cord[0])
        ts_lats.append(cord[1])
    return dct, ts_lons, ts_lats

def extract_ts_name(source):
    dct = {}
    ts_names = []
    ts_names.append('trainstation')
    ts_names.append('railwaystation')
    for feature in source['features']:
        ts_name = feature['properties']['STATIONNAME']
        ts_single_station = ts_name.strip() + 'station'
        ts_station_name = ts_name + ' Station'
        ts_train_name = ts_name + ' Train Station'
        ts_railway_name = ts_name + ' Railway Station'
        ts_names.append(ts_single_station)
        ts_names.append(ts_station_name)
        ts_names.append(ts_train_name)
        ts_names.append(ts_railway_name)
    return list(set(ts_names))

ts_melbourne = json.load(open('ts_data.json', 'r'))
ts_cord, ts_lons, ts_lats = extract_ts_cord(ts_melbourne)
ts_names = extract_ts_name(ts_melbourne)


# connect to CouchDB
class CouchDBConnection :
    def __init__(self):
        password = 'seven_eleven'
        couchdb_ip = 'localhost'
        self.server = couchdb.Server("http://admin:{}@{}:5984".format(password, couchdb_ip))

server = CouchDBConnection().server
cluster_db = server['pt_cluster']


# load Melbourne shapefile
melb_shape= fiona.open("melbourne.shp")
melbourne_geo = { "type": "Polygon", "coordinates": [
                          [
                            [ 144.3930000, -38.4369999 ], [ 144.3930000, -37.46 ],
                            [ 145.75000, -37.46 ], [ 145.75000, -38.4369999 ],
                            [ 144.3930000, -38.4369999 ]
                          ]
                        ]
                      }

melb_poly = shape(melbourne_geo)

print("Filtering map data ...")
in_melb_geom = []
for pol in melb_shape:
    s = shape(pol['geometry'])
    if melb_poly.contains(s):
        in_melb_geom.append(s)
print("done")


# initialize map
fig = plt.figure(0)
mp = MultiPolygon(in_melb_geom)
cm = plt.get_cmap('hsv')
num_colours = len(mp)
ax = fig.add_subplot(111)
minx, miny, maxx, maxy = mp.bounds
w, h = maxx - minx, maxy - miny
ax.set_xlim(minx - 0.2 * w, maxx + 0.2 * w)
ax.set_ylim(miny - 0.2 * h, maxy + 0.2 * h)
ax.set_aspect(1)

patches = []
for idx, p in enumerate(mp):
    pass
    patches.append(PolygonPatch(p, fc='w', ec='#555555', alpha=0.5, zorder=1))


# extract data from CouchDB
print("Refreshing view ...")
raw_docs = cluster_db.view('ts/cord_text', reduce=False, include_docs=True).rows
print(len(raw_docs))
train_docs = []
id_lst = []
general = ['transport', 'commute', 'myki', 'ptv']
keywords = ts_names + general
for doc in raw_docs:
    for keyword in keywords:
        if keyword.lower() in doc['doc']['text'].lower():
            train_docs.append(doc)
            id_lst.append(doc['id'])
            break
print(len(train_docs))

train_general_docs = cluster_db.view('train/cord_keyword', reduce=False, include_docs=True).rows
for doc in train_general_docs:
    if doc['id'] in id_lst:
        continue
    train_docs.append(doc)
print(len(train_docs))
print("done")

cord_lst = []
cord_id = defaultdict(list)
for doc in train_docs:
    latitude = doc['key'][0]
    longitude = doc['key'][1]
    cord = (longitude, latitude)
    cord_lst.append(cord)
    cord_id[cord].append(doc['id'])
    #point = Point(longitude, latitude)
    #colour = cm(1. * 2 / num_colours)
    #p = PolygonPatch(point.buffer(0.001), fc=colour, ec='#555555', alpha=0.09, zorder=1)
    #patches.append(p)


# DBSCAN
cord_na = np.array(cord_lst)
#print(cord_na.shape)
kms_per_radian = 6371.0088
epsilon = 0.15 / kms_per_radian
rad_cord_na = np.radians(cord_na)
#print(rad_cord_na.shape)
db = DBSCAN(eps=epsilon, min_samples=5, algorithm='ball_tree', metric='haversine').fit(rad_cord_na)
cluster_labels = db.labels_
n_clusters = len(set(cluster_labels))
#print(n_clusters)
clusters = pd.Series([cord_na[cluster_labels == n] for n in range(0, n_clusters)])

centroid_cluster = {}
lons_centroid = []
lats_centroid = []
centroids = []
for cluster in clusters:
    if cluster.size:
        centroid = (MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
        centroid_cluster[centroid] = cluster
        lons_centroid.append(centroid[0])
        lats_centroid.append(centroid[1])
        centroids.append(centroid)
print(len(centroids))


# map clusters to different color
def get_cmap(N):
    color_norm = colors.Normalize(vmin=0, vmax=N-1)
    scalar_map = cmx.ScalarMappable(norm=color_norm, cmap="nipy_spectral")
    def map_index_to_rgb_color(index):
        return scalar_map.to_rgba(index)
    return map_index_to_rgb_color


# result analysis
print("Calculating distance ...")
centroid_distances = {}
for i in range(len(centroids)):
    c_maps = get_cmap(len(centroids))
    min_ts = ""
    min_distance = 100
    for ts, cord in ts_cord.items():
        distance = great_circle(centroids[i], cord).kilometers
        if distance < min_distance:
            min_ts = ts
            min_distance = distance
    if min_distance > 0.25:
        location_cluster = centroid_cluster[centroids[i]]
        lons_select = location_cluster[:, 0]
        lats_select = location_cluster[:, 1]
        ax.scatter(lons_select, lats_select, marker='o', color=c_maps(i), edgecolors='#555555', alpha=0.3, zorder=10)

        location_cords = location_cluster.tolist()
        cords_set = set()
        for location_cord in location_cords:
            cords_set.add(tuple(location_cord))
        for cord in list(cords_set):
            cord_tuple = tuple(cord)
            for _id in cord_id[cord_tuple]:
                print(cluster_db[_id]['text'])
    centroid_distances[centroids[i]] = (min_ts, int(min_distance*1000))
    print("----------")
# for entry, tuple in centroid_distances.items():
#     print(str(tuple[0])+' '+str(tuple[1]))
print("done")

#for centroid, tuple in centroid_distances.items():
    #print("{0}: {1}({2})".format(centroid, tuple[0], tuple[1]))

#lons_na = np.array(lons_centroid)
#lats_na = np.array(lats_centroid)
#ax.scatter(lons_na, lats_na, marker='o', color='#e53333', edgecolors='#555555', alpha=0.7, zorder=10)

unique_label = np.unique(cluster_labels)
cmaps = get_cmap(n_clusters)
#for i, cluster in enumerate(clusters):
    #lons_select = cluster[:, 0]
    #lats_select = cluster[:, 1]
    #ax.scatter(lons_select, lats_select, marker='o', color=cmaps(i), edgecolors='#555555', alpha=0.3, zorder=10)

ax.scatter(ts_lons, ts_lats, s=100, marker='+', color='#000000')


ax.add_collection(PatchCollection(patches, match_original=True))
ax.set_xticks([])
ax.set_yticks([])
plt.title("Shapefile polygons rendered using Shapely")
plt.show()
