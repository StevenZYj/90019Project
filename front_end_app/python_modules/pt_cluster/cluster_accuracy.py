import json
import couchdb
import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from shapely.geometry import MultiPoint
from geopy.distance import great_circle
from python_modules.couchdb_con.connection import CouchDBConnection


class ClusterAccuracy:
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
        for cluster in clusters:
            if cluster.size:
                centroid = (MultiPoint(cluster).centroid.x, MultiPoint(cluster).centroid.y)
                centroids.append(centroid)
        return centroids

    def nearest_calc(self, centroids, ts_cord):
        correctness_count = 0
        #nearest_ts_lst = []
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
                correctness_count += 1
        accuracy = correctness_count / float(len(centroids))
        return correctness_count, accuracy

    def get_n_posts(self, clusters):
        n_posts = 0
        for cluster in clusters:
            n_posts = n_posts + cluster.size/2
        return n_posts

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
        num_sample_lst = [3, 5, 7, 10, 13, 15]
        threshold_lst = [0.05, 0.075, 0.1, 0.125, 0.15, 0.175, 0.2, 0.225, 0.25, 0.275, 0.3]
        ret = {}
        cluster_count_lst = []
        n_posts_lst = []
        correctness_count_lst = []
        for num_sample in num_sample_lst:
            accuracy_dct = {}
            for threshold in threshold_lst:
                #print("num_samples", num_sample)
                #print("threshold", threshold)
                clusters = self.dbscan(cord_lst, threshold, num_sample)
                n_posts = self.get_n_posts(clusters)
                # print(n_posts)
                n_posts_lst.append(n_posts)
                centroids = self.get_centroids(clusters)
                correctness_count, accuracy = self.nearest_calc(centroids, ts_cord)
                cluster_count_lst.append(len(centroids))
                # print("cluster_count", len(centroids))
                correctness_count_lst.append(correctness_count)
                # print("correctness_count", correctness_count)
                if num_sample <= 10:
                    accuracy_dct[threshold] = round(accuracy, 2)
                #print("----------")
            if num_sample <= 10:
                ret[num_sample] = accuracy_dct
        ret['cluster_count'] = cluster_count_lst
        ret['n_posts'] = n_posts_lst
        ret['correctness_count'] = correctness_count_lst
        return ret

#temp = ClusterAccuracy().process()
#for num_sample,dct in temp.items():
    #print("Num of Sample:", num_sample)
    #for threshold, accuracy in dct.items():
        #print(threshold, accuracy)
