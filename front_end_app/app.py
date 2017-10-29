from flask import Flask
from flask import render_template
from flask import jsonify
from flask import request

import couchdb
import json
import time

from python_modules.pt_cluster.general_stats import GeneralStats
from python_modules.pt_cluster.cluster_accuracy import ClusterAccuracy
from python_modules.pt_cluster.ts_rank import TsRank
from python_modules.pt_cluster.leaflet_map import LeafletMap


app = Flask(__name__)

@app.route("/")
def hello():
    return render_template('pt_cluster_stats.html', ver=int(time.time()))

if __name__ == '__main__':
    print "running here"
    app.run(threaded=True)


@app.route("/pt_cluster/general_stats")
def pt_cluster_general_stats():
    return jsonify(GeneralStats().process())

@app.route("/pt_cluster/cluster_accuracy")
def pt_cluster_accuracy():
    return jsonify(ClusterAccuracy().process())

@app.route("/pt_cluster/ts_rank")
def pt_cluster_ts_rank():
    return jsonify(TsRank().process())

@app.route("/pt_cluster/leaflet_map")
def pt_cluster_leaflet_map():
    return jsonify(LeafletMap().process())
