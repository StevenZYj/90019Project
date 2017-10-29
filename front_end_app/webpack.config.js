var webpack = require('webpack');
module.exports = {
  entry: [
    "./js/app.js"
  ],
  output: {
    path: __dirname + '/static',
    filename: "bundle.js"
  },
  module: {
    loaders: [
      {
        test: /\.js?$/,
        loader: 'babel-loader',
        query: {
          presets: ['es2015', 'react']
        },
        exclude: /node_modules/
      },
      {test: /\.css$/, loader: 'style-loader!css-loader'}
    ]
  },
  devtool: "cheap-module-source-map",
  plugins: [
    new webpack.DefinePlugin({
      'process.env': {
        'NODE_ENV': JSON.stringify('dev')
      }
    })
  ],
};
