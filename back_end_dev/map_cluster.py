import couchdb
import logging

from geojson import MultiPoint, Feature, FeatureCollection
from collections import defaultdict
from sklearn.cluster import DBSCAN
from nltk.corpus import stopwords
import pandas as pd
import numpy as np
import randomcolor
import string
import json
import re


class CouchDBConnection :
    def __init__(self):
        password = 'seven_eleven'
        couchdb_ip = 'localhost'
        self.server = couchdb.Server("http://admin:{}@{}:5984".format(password, couchdb_ip))

server = CouchDBConnection().server
pt_cluster_db = server['pt_cluster']
map_cluster_db = server['map_cluster']

remove_punctuation_map = dict()
for char in string.punctuation:
    remove_punctuation_map[ord(char)] = ' '

def extract_ts_name(source):
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

def filter_ts(ts_names):
        unfiltered_rows = pt_cluster_db.view('ts/cord_text', reduce=False, include_docs=True).rows
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

def extract_cords(rows):
        cord_lst = []
        cord_id = defaultdict(list)
        for row in rows:
            latitude = row['key'][0]
            longitude = row['key'][1]
            cord = (longitude, latitude)
            cord_lst.append(cord)
            cord_id[cord].append(row['id'])
        return cord_lst, cord_id

def dbscan(data, threshold, size):
        cord_na = np.array(data)
        kms_per_radian = 6371.0088
        epsilon = threshold / kms_per_radian
        rad_cord_na = np.radians(cord_na)
        db = DBSCAN(eps=epsilon, min_samples=size, algorithm='ball_tree', metric='haversine').fit(rad_cord_na)
        cluster_labels = db.labels_
        n_clusters = len(set(cluster_labels))
        clusters = pd.Series([cord_na[cluster_labels == n] for n in range(-1, n_clusters)])
        return clusters, n_clusters

def preprocess_tweet(text):
        text = re.sub(r'http\S+', '', text.encode('ascii','ignore').decode('ascii'))
        text = re.sub('\n', ' ', text)
        text = text.lower().translate(remove_punctuation_map)
        return text.split()

def preprocess_instagram(text):
        text = text.lstrip('[')
        text = text.rstrip(']')
        tokens = text.split(', ')
        return tokens

def process_text(cord_lst, cord_id):
        keyword_dct = defaultdict(int)
        for cord in cord_lst:
            cord_tuple = tuple(cord)
            id_lst = cord_id[cord_tuple]
            for _id in id_lst:
                doc = pt_cluster_db[_id]
                if "twitter" in doc:
                    tokens = preprocess_tweet(doc['text'])
                    for token in tokens:
                        keyword_dct[token] += 1
                elif "instagram" in doc:
                    text = doc['text']
                    if text != '[]':
                        tokens = preprocess_instagram(text)
                        for token in tokens:
                            keyword_dct[token] += 1
        sorted_lst = [(k, keyword_dct[k]) for k in sorted(keyword_dct, key=keyword_dct.get, reverse=True)]
        count = 0
        top_keywords = []
        for keyword, frequency in sorted_lst:
            if count < 10:
                if keyword in stopwords.words('english'):
                    continue
                count += 1
                top_keywords.append((keyword, frequency))
        return top_keywords

def process(data, threshold, size):
        cord_lst, cord_id = extract_cords(data)
        clusters, n_clusters = dbscan(cord_lst, threshold, size)
        #colorset
        rand_color = randomcolor.RandomColor()
        color_lst = rand_color.generate(count=n_clusters)
        #generate geojson
        features = []
        for i in range(len(clusters)):
            if clusters[i].size:
                cords = clusters[i].tolist()
                cords_set = set()
                for cord in cords:
                    cords_set.add(tuple(cord))
                if i != 0:
                    top_keywords = process_text(list(cords_set), cord_id)
                    summary = "Number of posts: " + str(len(cords)) + ", Top 10 Keywords:"
                    for keyword, frequency in top_keywords:
                        summary = summary + " " + keyword + "(" + str(frequency) + ")"
                cord_mp = MultiPoint(list(cords_set))
                if i == 0:
                    feature = Feature(geometry=cord_mp, properties={"color": "#826969"})
                else:
                    feature = Feature(geometry=cord_mp, properties={"color": color_lst[i-1], "summary": summary})
                features.append(feature)
        feature_collection = FeatureCollection(features)
        return feature_collection

def save_doc(_id, data):
    if _id in map_cluster_db:
        cluster_data_doc = map_cluster_db[_id]
        cluster_data_doc['data'] = data
        map_cluster_db.save(cluster_data_doc)
    else:
        cluster_data_doc = {
            '_id': _id,
            'data': data
        }
        map_cluster_db.save(cluster_data_doc)
    return True


logging.basicConfig(format='%(asctime)s %(message)s %(module)s:%(lineno)d ',
                    filename='record.log',
                    level=logging.INFO)

ts_melbourne = json.load(open('ts_data.json', 'r'))
if save_doc('ts', ts_melbourne):
    logging.info("Saved ts data")
ts_names = extract_ts_name(ts_melbourne)
filtered_rows, id_lst = filter_ts(ts_names)
train_general_rows = pt_cluster_db.view('train/cord_keyword', reduce=False, include_docs=True).rows
for row in train_general_rows:
    if row['id'] in id_lst:
        continue
    id_lst.append(row['id'])
    filtered_rows.append(row)
train_cluster_collection = process(filtered_rows, 0.15, 5)
if save_doc('train', train_cluster_collection):
    logging.info("Saved train cluster data")
tram_rows = pt_cluster_db.view('tram/cord_keyword', reduce=False, include_docs=True).rows
tram_cluster_collection = process(tram_rows, 0.15, 3)
if save_doc('tram', tram_cluster_collection):
    logging.info("Saved tram cluster data")
bus_rows = pt_cluster_db.view('bus/cord_keyword', reduce=False, include_docs=True).rows
bus_cluster_collection = process(bus_rows, 0.25, 3)
if save_doc('bus', bus_cluster_collection):
    logging.info("Saved bus cluster data")
for row in tram_rows:
    if row['id'] in id_lst:
        continue
    id_lst.append(row['id'])
    filtered_rows.append(row)
for row in bus_rows:
    if row['id'] in id_lst:
        continue
    id_lst.append(row['id'])
    filtered_rows.append(row)
general_rows = pt_cluster_db.view('general/filtered_cord_keyword', reduce=False, include_docs=True).rows
for row in general_rows:
    if row['id'] in id_lst:
        continue
    id_lst.append(row['id'])
    filtered_rows.append(row)
melb_pt_cluster_collection = process(filtered_rows, 0.15, 5)
if save_doc('melb_pt', melb_pt_cluster_collection):
    logging.info("Saved melb_pt cluster data")
