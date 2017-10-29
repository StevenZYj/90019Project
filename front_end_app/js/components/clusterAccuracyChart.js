import React from 'react';
import {VictoryContainer, VictoryScatter, VictoryLabel, VictoryChart, VictoryAxis, VictoryTooltip, VictoryTheme} from 'victory';

class ClusterAccuracyScatter extends React.Component {
  constructor(props) {
    super(props);
  }

  render() {
    return (
      <div>
        { !this.props.show &&
            <div className={"col-md-2 col-md-offset-5"}>
              <i className={"fa fa-spinner fa-3x fa-fw fa-pulse"}></i>
            </div>
        }
        { this.props.show &&
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
              text={this.props.title}
              x={100}
              y={20}
            />
            <VictoryAxis
              tickLabelComponent={<VictoryLabel angle={90} textAnchor={'start'} />} />
            <VictoryAxis dependentAxis label="Accuracy (%)"/>
            <VictoryScatter
              labelComponent={<VictoryTooltip/>}
              style={{ data: { fill: "#c43a31" } }}
              data={this.props.accuData}
            />
          </VictoryChart>
        }
      </div>
    );
  }
}

export default ClusterAccuracyScatter;
