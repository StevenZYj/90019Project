import PtClusterStats from './ptClusterStats.js';

import React from 'react';
import ReactDOM from 'react-dom';

if (document.getElementById('pt-cluster-stats')) {
  ReactDOM.render(<PtClusterStats/>, document.getElementById('pt-cluster-stats'));
}
