import os
import json
import couchdb
from python_modules.couchdb_con.connection import CouchDBConnection


class GeneralStats:

    def __init__(self):
        self.db = CouchDBConnection()

    def _count_prcnt(self, count, total):
        return "{:8,} ({:02.2f}%)".format(count, count*100.0/total)

    def extract_ts_name(self, source):
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

    def filter_ts_posts(self):
        ts_melbourne = json.load(open('ts_data.json', 'r'))
        ts_names = self.extract_ts_name(ts_melbourne)
        general = ['transport', 'commute', 'myki', 'ptv']
        keywords = ts_names + general
        unfiltered_rows = self.db.pt_cluster_db.view('ts/cord_text', reduce=False, include_docs=True).rows
        filtered_twi_count = 0
        filtered_insta_count = 0
        for row in unfiltered_rows:
            for keyword in keywords:
                if keyword.lower() in row['doc']['text'].lower():
                    if 'twitter' in row['doc']:
                        filtered_twi_count += 1
                    elif 'instagram' in row['doc']:
                        filtered_insta_count += 1
                    break
        return filtered_twi_count, filtered_insta_count

    def process(self):

        ret = {}

        all_count = self.db.pt_cluster_db.view('count/source_method').rows[0].value
        ret['total_posts'] = "{:8,}".format(all_count)
        twitter_count = self.db.pt_cluster_db.view('count/source_method', key='twitter').rows[0].value
        ret['twitter_posts'] = self._count_prcnt(twitter_count, all_count)
        instagram_count = self.db.pt_cluster_db.view('count/source_method', key='instagram').rows[0].value
        ret['instagram_posts'] = self._count_prcnt(instagram_count, all_count)

        twitter_train_count = self.db.pt_cluster_db.view('count/category', key='twitrain').rows[0].value
        instagram_train_count = self.db.pt_cluster_db.view('count/category', key='instatrain').rows[0].value
        train_count = twitter_train_count + instagram_train_count
        ret['twitter_train_posts'] = self._count_prcnt(twitter_train_count, train_count)
        ret['instagram_train_posts'] = self._count_prcnt(instagram_train_count, train_count)
        ret['train_posts'] = "{:8,}".format(train_count)

        twitter_tram_count = self.db.pt_cluster_db.view('count/category', key='twitram').rows[0].value
        instagram_tram_count = self.db.pt_cluster_db.view('count/category', key='instatram').rows[0].value
        tram_count = twitter_tram_count + instagram_tram_count
        ret['twitter_tram_posts'] = self._count_prcnt(twitter_tram_count, tram_count)
        ret['instatram_tram_posts'] = self._count_prcnt(instagram_tram_count, tram_count)
        ret['tram_posts'] = "{:8,}".format(tram_count)

        twitter_bus_count =  self.db.pt_cluster_db.view('count/category', key='twibus').rows[0].value
        instagram_bus_count = self.db.pt_cluster_db.view('count/category', key='instabus').rows[0].value
        bus_count = twitter_bus_count + instagram_bus_count
        ret['twitter_bus_posts'] = self._count_prcnt(twitter_bus_count, bus_count)
        ret['instagram_bus_posts'] = self._count_prcnt(instagram_bus_count, bus_count)
        ret['bus_posts'] = "{:8,}".format(bus_count)

        twitter_ts_count = self.db.pt_cluster_db.view('count/category', key='twits').rows[0].value
        instagram_ts_count = self.db.pt_cluster_db.view('count/category', key='instats').rows[0].value
        ts_count = twitter_ts_count + instagram_ts_count
        ret['twitter_ts_posts'] = self._count_prcnt(twitter_ts_count, ts_count)
        ret['instagram_ts_posts'] = self._count_prcnt(instagram_ts_count, ts_count)
        ret['ts_posts'] = "{:8,}".format(ts_count)

        twitter_general_count = self.db.pt_cluster_db.view('count/category', key='twigeneral').rows[0].value
        instagram_general_count = self.db.pt_cluster_db.view('count/category', key='instageneral').rows[0].value
        general_count = twitter_general_count + instagram_general_count
        ret['twitter_general_posts'] = self._count_prcnt(twitter_general_count, general_count)
        ret['instagram_general_posts'] = self._count_prcnt(instagram_general_count, general_count)
        ret['general_posts'] = "{:8,}".format(general_count)

        filtered_twi_count, filtered_insta_count = self.filter_ts_posts()
        filtered_ts_count = filtered_twi_count + filtered_insta_count
        ret['filtered_twitter_ts_posts'] = self._count_prcnt(filtered_twi_count, filtered_ts_count)
        ret['filtered_instagram_ts_posts'] = self._count_prcnt(filtered_insta_count, filtered_ts_count)
        ret['filtered_ts_posts'] = "{:8,}".format(filtered_ts_count)

        filtered_twitter_general_count = self.db.pt_cluster_db.view('general/myki-ptv', key='twitter').rows[0].value
        filtered_instagram_general_count = self.db.pt_cluster_db.view('general/myki-ptv', key='instagram').rows[0].value
        filtered_general_count = filtered_twitter_general_count + filtered_instagram_general_count
        ret['filtered_twitter_general_posts'] = self._count_prcnt(filtered_twitter_general_count, filtered_general_count)
        ret['filtered_instagram_general_posts'] = self._count_prcnt(filtered_instagram_general_count, filtered_general_count)
        ret['filtered_general_posts'] = "{:8,}".format(filtered_general_count)

        filtered_twitter_count = filtered_twitter_general_count + filtered_twi_count + twitter_bus_count + twitter_tram_count + twitter_train_count
        filtered_instagram_count = filtered_instagram_general_count + filtered_insta_count + instagram_bus_count + instagram_tram_count + instagram_train_count
        filtered_total_count = filtered_twitter_count + filtered_instagram_count
        ret['filtered_twitter_posts'] = self._count_prcnt(filtered_twitter_count, filtered_total_count)
        ret['filtered_instagram_posts'] = self._count_prcnt(filtered_instagram_count, filtered_total_count)
        ret['filtered_total_posts'] = "{:8,}".format(filtered_total_count)

        return ret
