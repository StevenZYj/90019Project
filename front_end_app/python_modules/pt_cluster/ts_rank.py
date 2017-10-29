import json
import couchdb
import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from shapely.geometry import MultiPoint
from geopy.distance import great_circle
from python_modules.couchdb_con.connection import CouchDBConnection


class TsRank():
    def __init__(self):
        self.db = CouchDBConnection()

    def extract_ts(self, source):
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
            cord = feature['geometry']['coordinates']
            dct[ts_name] = cord
        return dct, list(set(ts_names))

    def filter_ts(self, ts_names):
        unfiltered_rows = self.db.pt_cluster_db.view('ts/cord_text', reduce=False, include_docs=True).rows
        filtered_rows = []
        id_lst = []
        general = ['transport', 'commute', 'myki', 'ptv']
        keywords = ts_names + general
        for row in unfiltered_rows:
            for keyword in keywords:
                if keyword.lower() in row['doc']['text'].lower():
                    filtered_rows.append(row)
                    id_lst.append(row['id'])
                    break
        return filtered_rows, id_lst

    def get_cord_lst(self, data):
        cord_lst = []
        for row in data:
            latitude = row['key'][0]
            longitude = row['key'][1]
            cord = (longitude, latitude)
            cord_lst.append(cord)
        return cord_lst

    def dbscan(self, data, threshold, num_sample):
        cord_na = np.array(data)
        kms_per_radian = 6371.0088
        epsilon = threshold / kms_per_radian
        rad_cord_na = np.radians(cord_na)
        db = DBSCAN(eps=epsilon, min_samples=num_sample, algorithm='ball_tree', metric='haversine').fit(rad_cord_na)
        cluster_labels = db.labels_
        n_clusters = len(set(cluster_labels))
        clusters = pd.Series([cord_na[cluster_labels == n] for n in range(0, n_clusters)])
        return clusters

    def get_centroids(self, clusters):
        centroids = []
        centroid_size = {}
        for cluster in clusters:
            if cluster.size:
                centroid = (MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
                centroids.append(centroid)
                centroid_size[centroid] = cluster.size
        return centroids, centroid_size

    def get_ts_stats(self, centroids, centroid_size, ts_cord):
        ts_distance = {}
        ts_size = {}
        for i in range(len(centroids)):
            min_ts = ""
            min_distance = 10
            for ts, cord in ts_cord.items():
                distance = great_circle(centroids[i], cord).kilometers
                if distance < min_distance:
                    min_ts = ts
                    min_distance = distance
            #nearest_ts_lst.append((min_ts, int(min_distance)*1000))
            if min_distance < 0.25:
                ts_distance[min_ts] = {'distance': int(min_distance*1000)}
                ts_size[min_ts] = centroid_size[centroids[i]]/2
        return ts_distance, ts_size

    def process(self):
        ts_melbourne = json.load(open('ts_data.json', 'r'))
        ts_cord, ts_names = self.extract_ts(ts_melbourne)
        train_general_rows = self.db.pt_cluster_db.view('train/cord_keyword', reduce=False, include_docs=True).rows
        filtered_rows, id_lst = self.filter_ts(ts_names)
        for row in train_general_rows:
            if row['id'] in id_lst:
                continue
            filtered_rows.append(row)

        cord_lst = self.get_cord_lst(filtered_rows)
        num_sample = 5
        threshold = 0.15
        ret = {}
        clusters = self.dbscan(cord_lst, threshold, num_sample)
        centroids, centroid_size = self.get_centroids(clusters)
        ts_distance, ts_size = self.get_ts_stats(centroids, centroid_size, ts_cord)
        sorted_ts_lst = [(k, ts_size[k]) for k in sorted(ts_size, key=ts_size.get, reverse=True)]
        ret_lst = []
        for ts, size in sorted_ts_lst:
            dct = {'name': ts, 'size': size, 'distance': ts_distance[ts]['distance']}
            ret_lst.append(dct)
        ret['stats'] = ret_lst
        return ret

# temp = TsRank().process()
# stats = temp['stats']
# for dct in stats:
#     print(dct)
