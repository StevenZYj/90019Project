import React from 'react';
import ReactDOM from 'react-dom';

import {VictoryContainer, VictoryScatter, VictoryLabel, VictoryChart,
        VictoryAxis, VictoryTooltip, VictoryTheme} from 'victory';
import {Map, TileLayer, GeoJSON, LayersControl} from 'react-leaflet';
import L from 'leaflet';

import ClusterAccuracyScatter from './components/clusterAccuracyChart.js'


class PtClusterStats extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      generalStats: {},
      scatter3Data: {},
      scatter5Data: {},
      scatter7Data: {},
      scatter10Data: {},
      relationScatterData: {},
      relationScatterData2: {},
      tsRank: [],

      showGeneralStats: false,
      showScatter3: false,
      showScatter5: false,
      showScatter7: false,
      showScatter10: false,
      showRelationScatter: false,
      showRelationScatter2: false,
      showTsRank: false,

      lat: -37.8136,
      lng: 144.9631,
      zoom: 13,
      minzoon: 11,
      tsJson: null,
      trainCluster: null,
      showTrainMap: false,
      tramCluster: null,
      showTramMap: false,
      busCluster: null,
      showBusMap: false,
      melbPtCluster: null,
      showMelbPtMap: false
    }
  }

  componentDidMount() {
    this.getGeneralStats();
    this.getClusterAccuracy();
    this.getTsRank();
    this.getMapJson();
  }

  getGeneralStats() {
    $.ajax({
      url:"/pt_cluster/general_stats",
      success: (result) => {
        this.setState({ generalStats: result, showGeneralStats: true });
      },
      error: (result) => {
        console.log("Error");
      }
    })
  }

  getClusterAccuracy() {
    $.ajax({
      url: "/pt_cluster/cluster_accuracy",
      success: (result) => {
        this.processClusterAccuracyData(result);
      },
      error: (result) => {
        console.log("Error");
      }
    })
  }

  getTsRank() {
    $.ajax({
      url: "/pt_cluster/ts_rank",
      success: (result) => {
        this.setState({ tsRank: result.stats, showTsRank: true });
      },
      error: (result) => {
        console.log("Error")
      }
    })
  }

  getMapJson() {
    $.ajax({
      url: "/pt_cluster/leaflet_map",
      success: (result) => {
        this.processMapData(result);
      },
      error: (result) => {
        console.log("Error")
      }
    })
  }

  getTsFeature(feature, layer) {
    if (feature.properties && feature.properties.STATIONNAME) {
      const popupContent = `<h5>${feature.properties.STATIONNAME}</h5>`;
      layer.bindPopup(popupContent);
    }
  }

  getClusterFeature(feature, layer) {
    if (feature.properties && feature.properties.summary) {
      const popupContent = `<h5>${feature.properties.summary}</h5>`;
      layer.bindPopup(popupContent);
    }
  }

  setClusterPointToLayer(feature, latlng) {
    // renders our GeoJSON points as circle markers, rather than Leaflet's default image markers
    // parameters to style the GeoJSON markers
    var markerParams = {
      radius: 4,
      fillColor: feature.properties.color,
      color: '#fff',
      weight: 1,
      opacity: 0.5,
      fillOpacity: 0.8
    };
    return L.circleMarker(latlng, markerParams);
  }

  fetchClusterAccuracyData(num_sample, data) {
    let threshold_lst = [0.05, 0.075, 0.1, 0.125, 0.15, 0.175, 0.2, 0.225, 0.25, 0.275, 0.3];
    var accuData = [];
    for (let threshold of threshold_lst) {
      accuData.push({
        x: threshold,
        y: data[num_sample][threshold],
        label: "epsilon " + threshold + " : " + data[num_sample][threshold].toFixed(2)
      });
    }
    return accuData;
  }

  fetchRelationScatterData(data) {
    let identified = data.cluster_count
    let post_count = data.n_posts
    let correct = data.correctness_count
    var scatterData = [];
    var scatterData2 = [];
    for (let i in identified) {
      scatterData.push({
        x: identified[i],
        y: correct[i],
        label: correct[i] + " / " + identified[i]
      });
      scatterData2.push({
        x: post_count[i],
        y: correct[i],
      });
    }
    return [scatterData, scatterData2];
  }

  processClusterAccuracyData(data) {
    this.setState({
      scatter10Data: this.fetchClusterAccuracyData(10, data),
      showScatter10: true,
      scatter7Data: this.fetchClusterAccuracyData(7, data),
      showScatter7: true,
      scatter5Data: this.fetchClusterAccuracyData(5, data),
      showScatter5: true,
      scatter3Data: this.fetchClusterAccuracyData(3, data),
      showScatter3: true,
      relationScatterData: this.fetchRelationScatterData(data)[0],
      showRelationScatter: true,
      relationScatterData2: this.fetchRelationScatterData(data)[1],
      showRelationScatter2: true
    });
  }

  processMapData(data) {
    this.setState({
      tsJson: data.ts,
      trainCluster: data.train_cluster,
      showTrainMap: true,
      tramCluster: data.tram_cluster,
      showTramMap: true,
      busCluster: data.bus_cluster,
      showBusMap: true,
      melbPtCluster: data.melb_pt_cluster,
      showMelbPtMap: true
    });
  }

  render() {
    const position = [this.state.lat, this.state.lng];
    const { BaseLayer, Overlay } = LayersControl;
    return (
      <div>
        <div className="row" style={{textAlign: "center"}}>
          <h3 style={{color: "#000099"}}>General Statistics</h3>
        </div>
        <div className="row">
          { !this.state.showGeneralStats &&
              <div className={"col-md-2 col-md-offset-5"}>
                <i className={"fa fa-spinner fa-3x fa-fw fa-pulse"}></i>
              </div>
          }
          { this.state.showGeneralStats &&
            <div>
              <div className="col-md-2">
              </div>
              <div className="col-md-8">
                <table className="table table-sm">
                  <thead>
                    <tr>
                      <th>Category</th>
                      <th>Instagram</th>
                      <th>Twitter</th>
                      <th>Together</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td>Train Posts (filtered)</td>
                      <td>{this.state.generalStats.instagram_train_posts}</td>
                      <td>{this.state.generalStats.twitter_train_posts}</td>
                      <td>{this.state.generalStats.train_posts}</td>
                    </tr>
                    <tr>
                      <td>Train Station Posts (filtered)</td>
                      <td>{this.state.generalStats.filtered_instagram_ts_posts}</td>
                      <td>{this.state.generalStats.filtered_twitter_ts_posts}</td>
                      <td>{this.state.generalStats.filtered_ts_posts}</td>
                    </tr>
                    <tr>
                      <td>Tram Posts</td>
                      <td>{this.state.generalStats.instatram_tram_posts}</td>
                      <td>{this.state.generalStats.twitter_tram_posts}</td>
                      <td>{this.state.generalStats.tram_posts}</td>
                    </tr>
                    <tr>
                      <td>Bus Posts</td>
                      <td>{this.state.generalStats.instagram_bus_posts}</td>
                      <td>{this.state.generalStats.twitter_bus_posts}</td>
                      <td>{this.state.generalStats.bus_posts}</td>
                    </tr>
                    <tr>
                      <td>General Posts (filtered)</td>
                      <td>{this.state.generalStats.filtered_instagram_general_posts}</td>
                      <td>{this.state.generalStats.filtered_twitter_general_posts}</td>
                      <td>{this.state.generalStats.filtered_general_posts}</td>
                    </tr>
                    <tr>
                      <td>Total Posts (filtered)</td>
                      <td>{this.state.generalStats.filtered_instagram_posts}</td>
                      <td>{this.state.generalStats.filtered_twitter_posts}</td>
                      <td>{this.state.generalStats.filtered_total_posts}</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          }
        </div>

        <hr></hr>
        <div className="row" style={{textAlign: "center"}}>
          <h3 style={{color: "#000099"}}> Train Station Prediction </h3>
          <h5 style={{color: "grey"}}> Based on filtered train and train station data </h5>
        </div>
        <div className="row">
          <div className="col-md-4" style={{height: '450px'}}>
            <ClusterAccuracyScatter
              show={this.state.showScatter10}
              accuData={this.state.scatter10Data}
              title={"MinPts=9"}
            />
          </div>
          <div className="col-md-4" style={{height: '450px'}}>
            <ClusterAccuracyScatter
              show={this.state.showScatter7}
              accuData={this.state.scatter7Data}
              title={"MinPts=6"}
            />
          </div>
          <div className="col-md-4" style={{height: '450px'}}>
            <ClusterAccuracyScatter
              show={this.state.showScatter5}
              accuData={this.state.scatter5Data}
              title={"MinPts=4"}
            />
          </div>
          <div className="col-md-4" style={{height: '450px'}}>
            <ClusterAccuracyScatter
              show={this.state.showScatter3}
              accuData={this.state.scatter3Data}
              title={"MinPts=2"}
            />
          </div>

          <div className="col-md-4">
            { !this.state.showRelationScatter &&
                <div className={"col-md-2 col-md-offset-5"}>
                  <i className={"fa fa-spinner fa-3x fa-fw fa-pulse"}></i>
                </div>
            }
            { this.state.showRelationScatter &&
              <VictoryChart
                theme={VictoryTheme.material}
                domainPadding={10}
                containerComponent={
                  <VictoryContainer
                    height={400}
                  />
                }
              >
                <VictoryLabel
                  text={"totally identified vs correctly identified"}
                  x={100}
                  y={20}
                />
                <VictoryAxis
                  tickLabelComponent={<VictoryLabel angle={90} textAnchor={'start'} />} />
                <VictoryAxis dependentAxis label="Correctness (num)"/>
                <VictoryScatter
                  data={this.state.relationScatterData}
                  labelComponent={<VictoryTooltip/>}
                />
              </VictoryChart>
            }
          </div>

          <div className="col-md-4">
            { !this.state.showRelationScatter2 &&
                <div className={"col-md-2 col-md-offset-5"}>
                  <i className={"fa fa-spinner fa-3x fa-fw fa-pulse"}></i>
                </div>
            }
            { this.state.showRelationScatter2 &&
              <VictoryChart
                theme={VictoryTheme.material}
                domainPadding={10}
                containerComponent={
                  <VictoryContainer
                    height={400}
                  />
                }
              >
                <VictoryLabel
                  text={"total posts included vs correctly identified"}
                  x={100}
                  y={20}
                />
                <VictoryAxis
                  tickLabelComponent={<VictoryLabel angle={90} textAnchor={'start'} />} />
                <VictoryAxis dependentAxis label="Correctness (num)"/>
                <VictoryScatter
                  data={this.state.relationScatterData2}
                  labelComponent={<VictoryTooltip/>}
                />
              </VictoryChart>
            }
          </div>
        </div>

        <hr></hr>
        <div className="row" style={{textAlign: "center"}}>
          <h3 style={{color: "#000099"}}> Train Station Statistics </h3>
          <h5 style={{color: "grey"}}> Based on Eps=150m and MinPts=4 </h5>
        </div>
        <div className="row">
          { !this.state.showTsRank &&
              <div className={"col-md-2 col-md-offset-5"}>
                <i className={"fa fa-spinner fa-3x fa-fw fa-pulse"}></i>
              </div>
          }
          { this.state.showTsRank &&
            <div>
              <div className="col-md-2">
              </div>
              <div className="col-md-8">
                <table className="table table-sm">
                  <thead>
                    <tr>
                      <th>Train Station Name</th>
                      <th>Cluster Size</th>
                      <th>Distance from Cluster Center</th>
                    </tr>
                  </thead>
                  <tbody>
                    {this.state.tsRank.map((ts, index) => (
                      <tr key={index}>
                        <td>{ts.name}</td>
                        <td>{ts.size} posts</td>
                        <td>{ts.distance} metres</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          }
        </div>

        <hr></hr>
        <div className="row" style={{textAlign: "center"}}>
          <h3 style={{color: "#000099"}}> Train-related Cluster Map </h3>
          <h5 style={{color: "grey"}}> Based on Eps=150m and MinPts=4 </h5>
        </div>
        <div className="row">
          { !this.state.showTrainMap &&
              <div className={"col-md-2 col-md-offset-5"}>
                <i className={"fa fa-spinner fa-3x fa-fw fa-pulse"}></i>
              </div>
          }
          { this.state.showTrainMap &&
            <Map center={position} zoom={this.state.zoom} style={{height: "600px"}}>
              <LayersControl position="topright">
                <BaseLayer checked name="Melbourne">
                  <TileLayer
                    attribution='&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, &copy; <a href="http://cartodb.com/attributions">CartoDB</a>'
                    url='http://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png'
                  />
                </BaseLayer>
                <Overlay checked name="Train-related cluster">
                  <GeoJSON
                    data={this.state.trainCluster}
                    onEachFeature={this.getClusterFeature}
                    pointToLayer={this.setClusterPointToLayer}
                  />
                </Overlay>
                <Overlay name="Train station">
                  <GeoJSON
                    data={this.state.tsJson}
                    onEachFeature={this.getTsFeature}
                  />
                </Overlay>
              </LayersControl>
            </Map>
          }
        </div>

        <hr></hr>
        <div className="row" style={{textAlign: "center"}}>
          <h3 style={{color: "#000099"}}> Tram-related Cluster Map </h3>
          <h5 style={{color: "grey"}}> Based on Eps=150m and MinPts=2 </h5>
        </div>
        <div>
          { !this.state.showTramMap &&
              <div className={"col-md-2 col-md-offset-5"}>
                <i className={"fa fa-spinner fa-3x fa-fw fa-pulse"}></i>
              </div>
          }
          { this.state.showTramMap &&
            <Map center={position} zoom={this.state.zoom} style={{height: "600px"}}>
              <TileLayer
                attribution='&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, &copy; <a href="http://cartodb.com/attributions">CartoDB</a>'
                url='http://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png'
              />
              <GeoJSON
                data={this.state.tramCluster}
                onEachFeature={this.getClusterFeature}
                pointToLayer={this.setClusterPointToLayer}
              />
            </Map>
          }
        </div>

        <hr></hr>
        <div className="row" style={{textAlign: "center"}}>
          <h3 style={{color: "#000099"}}> Bus-related Cluster Map </h3>
          <h5 style={{color: "grey"}}> Based on Eps=250m and MinPts=2 </h5>
        </div>
        <div>
          { !this.state.showBusMap &&
              <div className={"col-md-2 col-md-offset-5"}>
                <i className={"fa fa-spinner fa-3x fa-fw fa-pulse"}></i>
              </div>
          }
          { this.state.showBusMap &&
            <Map center={position} zoom={this.state.zoom} style={{height: "600px"}}>
              <TileLayer
                attribution='&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, &copy; <a href="http://cartodb.com/attributions">CartoDB</a>'
                url='http://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png'
              />
              <GeoJSON
                data={this.state.busCluster}
                onEachFeature={this.getClusterFeature}
                pointToLayer={this.setClusterPointToLayer}
              />
            </Map>
          }
        </div>

        <hr></hr>
        <div className="row" style={{textAlign: "center"}}>
          <h3 style={{color: "#000099"}}> Public-transport-related Cluster Map </h3>
          <h5 style={{color: "grey"}}> Based on Eps=150m and MinPts=4 </h5>
        </div>
        <div>
          { !this.state.showMelbPtMap &&
              <div className={"col-md-2 col-md-offset-5"}>
                <i className={"fa fa-spinner fa-3x fa-fw fa-pulse"}></i>
              </div>
          }
          { this.state.showMelbPtMap &&
            <Map center={position} zoom={this.state.zoom} style={{height: "600px"}}>
              <LayersControl position="topright">
                <BaseLayer checked name="Melbourne">
                  <TileLayer
                    attribution='&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, &copy; <a href="http://cartodb.com/attributions">CartoDB</a>'
                    url='http://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png'
                  />
                </BaseLayer>
                <Overlay checked name="Train-related cluster">
                  <GeoJSON
                    data={this.state.melbPtCluster}
                    onEachFeature={this.getClusterFeature}
                    pointToLayer={this.setClusterPointToLayer}
                  />
                </Overlay>
                <Overlay name="Train station">
                  <GeoJSON
                    data={this.state.tsJson}
                    onEachFeature={this.getTsFeature}
                  />
                </Overlay>
              </LayersControl>
            </Map>
          }
        </div>
      </div>
    );
  }
}

export default PtClusterStats;
