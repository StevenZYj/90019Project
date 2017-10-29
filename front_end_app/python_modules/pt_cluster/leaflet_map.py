import os
import json
import couchdb
from python_modules.couchdb_con.connection import CouchDBConnection


class LeafletMap:

    def __init__(self):
        self.db = CouchDBConnection()

    def process(self):
        ret = {}
        ret['ts'] = self.db.map_cluster_db['ts']['data']
        ret['train_cluster'] = self.db.map_cluster_db['train']['data']
        ret['tram_cluster'] = self.db.map_cluster_db['tram']['data']
        ret['bus_cluster'] = self.db.map_cluster_db['bus']['data']
        ret['melb_pt_cluster'] = self.db.map_cluster_db['melb_pt']['data']
        return ret
