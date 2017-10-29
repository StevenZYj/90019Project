import couchdb
import os


class CouchDBConnection :
    instance = None
    class __Singleton:
        def __init__(self):
            password = os.environ['COUCHDB_ADMIN_PASSWORD']
            couchdb_ip = os.environ['COUCHDB_SERVER_IP']
            port = os.environ['COUCHDB_PORT']
            self.server = couchdb.Server("http://admin:{}@{}:{}".format(password, couchdb_ip, port))

    def __init__(self):
        if CouchDBConnection.instance is None:
            CouchDBConnection.instance = CouchDBConnection.__Singleton()

        self.melbourne_db = self.instance.server['melbourne']
        self.pt_cluster_db = self.instance.server['pt_cluster']
        self.map_cluster_db = self.instance.server['map_cluster']
