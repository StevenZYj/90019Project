import couchdb
from collections import defaultdict
import numpy as np
from sklearn.cluster import DBSCAN
import pandas as pd
from geojson import MultiPoint, Feature, FeatureCollection
import geojson
import randomcolor
import json
import re
import string
from nltk.corpus import stopwords


#connect couchdb
class CouchDBConnection :
    def __init__(self):
        password = 'seven_eleven'
        couchdb_ip = 'localhost'
        self.server = couchdb.Server("http://admin:{}@{}:5984".format(password, couchdb_ip))

server = CouchDBConnection().server
cluster_db = server['pt_cluster']


#collect train/tram/bus data
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
ts_names = extract_ts_name(ts_melbourne)

unfiltered_rows = cluster_db.view('ts/cord_text', reduce=False, include_docs=True).rows
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

train_general_rows = cluster_db.view('train/cord_keyword', reduce=False, include_docs=True).rows
for row in train_general_rows:
    if row['id'] in id_lst:
        continue
    filtered_rows.append(row)


#extract coordinates
cord_lst = []
cord_id = defaultdict(list)
for row in filtered_rows:
    latitude = row['key'][0]
    longitude = row['key'][1]
    cord = (longitude, latitude)
    cord_lst.append(cord)
    cord_id[cord].append(row['id'])


#implement dbscan
cord_na = np.array(cord_lst)
kms_per_radian = 6371.0088
epsilon = 0.15 / kms_per_radian
rad_cord_na = np.radians(cord_na)
db = DBSCAN(eps=epsilon, min_samples=5, algorithm='ball_tree', metric='haversine').fit(rad_cord_na)
cluster_labels = db.labels_
n_clusters = len(set(cluster_labels))
print(n_clusters)
clusters = pd.Series([cord_na[cluster_labels == n] for n in range(-1, n_clusters)])


#colorset
rand_color = randomcolor.RandomColor()
color_lst = rand_color.generate(count=n_clusters)


#prepare geojson
remove_punctuation_map = dict()
for char in string.punctuation:
    remove_punctuation_map[ord(char)] = None

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

def process_text(cord_lst):
    keyword_dct = defaultdict(int)
    for cord in cord_lst:
        cord_tuple = tuple(cord)
        id_lst = cord_id[cord_tuple]
        for _id in id_lst:
            doc = cluster_db[_id]
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

features = []
for i in range(len(clusters)):
    if clusters[i].size:
        cords = clusters[i].tolist()
        cords_set = set()
        for cord in cords:
            cords_set.add(tuple(cord))
        if i != 0:
            top_keywords = process_text(list(cords_set))
            summary = "Number of posts: " + str(len(cords)) + ", Top 10 Keywords:"
            for keyword, frequency in top_keywords:
                summary = summary + " " + keyword + "(" + str(frequency) + ")"
        # for cord in list(cords_set):
        #     cord_tuple = tuple(cord)
        #     if len(cord_id[cord_tuple]) != 1:
        #         text = process_text(cord_id[cord_tuple])
        #     else:
        #         doc = cluster_db[cord_id[cord_tuple][0]]
        #         if "twitter" in doc:
        #             text = "Twitter: " + doc['text']
        #         elif "instagram" in doc:
        #             text = "Instagram: " + doc['text']
        #     cord_p = Point(cord)
        #     if i == 0:
        #         feature = Feature(geometry=cord_p, properties={"color": "#826969", "text": text})
        #     else:
        #         feature = Feature(geometry=cord_p, properties={"color": color_lst[i-1], "text": text})
        #     features.append(feature)
        cord_mp = MultiPoint(list(cords_set))
        if i == 0:
            feature = Feature(geometry=cord_mp, properties={"color": "#826969"})
        else:
            feature = Feature(geometry=cord_mp, properties={"color": color_lst[i-1], "summary": summary})
        features.append(feature)
feature_collection = FeatureCollection(features)
with open('train_cluster.geojson', 'w') as outfile:
    geojson.dump(feature_collection, outfile)
