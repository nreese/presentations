### Load data set
Requires [node.js](https://nodejs.org)
1) download CSV from https://data.sandiego.gov/datasets/get-it-done-311/ into this directory and rename to `get_it_done_2019.csv`
2) Update ES credentials `username` and `password` in load.js if different from developer defaults.
3) run `npm install`
4) run `node load.js`
5) In Kibana Maps, use geojson upload to index `council_districts_datasd.geojson`
