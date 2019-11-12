'use strict'

const { Client } = require('@elastic/elasticsearch');
const csv = require('csv-parser');
const fs = require('fs');
const readline = require('readline');

const client = new Client({
  node: 'http://localhost:9200',
  auth: {
    username: 'elastic',
    password: 'changeme'
  }
});

const ES_INDEX_NAME = 'get_it_done';
const BULK_INSERT_SIZE = 500;

async function load () {
  const doesIndexExistResp = await client.indices.exists({
    index: ES_INDEX_NAME,
  });
  if (!doesIndexExistResp.body) {
    createIndex();
  }

  await loadData('./get_it_done_2019.csv', bulkInsert);
}

async function createIndex() {
  console.log(`Creating index ${ES_INDEX_NAME}`);
  await client.indices.create({
    index: ES_INDEX_NAME,
    body: {
      settings: { index: { number_of_shards: 1, auto_expand_replicas: '0-1' } },
      mappings: {
        properties: {
          location: {
            type: 'geo_point'
          }
        }
      },
    },
  });
}

async function bulkInsert(docs) {
  console.log(`Indexing ${docs.length} documents into ${ES_INDEX_NAME}`);

  const bulk = [];
  docs.forEach(async (doc) => {
    bulk.push({ index: { _index: ES_INDEX_NAME } });
    bulk.push(doc);
  });

  const resp = await client.bulk({
    body: bulk
  });
  if (resp.errors) {
    console.log(`Faild to load some docs`);
  }
}

function loadData(path, bulkInsert) {
  return new Promise((resolve, reject) => {
    let count = 0;
    let docs = [];
    let isPaused = false;

    const onClose = async () => {
      if (docs.length > 0) {
        try {
          await bulkInsert(docs);
        } catch (err) {
          reject(err);
          return;
        }
      }
      resolve(count);
    };

    const options = {
      // pause does not stop lines already in buffer. Use smaller buffer size to avoid bulk inserting to many records
      highWaterMark: 1024 * 4,
      encoding: 'utf8'
    };
    const readStream = fs.createReadStream(path, options)
      .on('error', (error) => {
        if (error.code === 'ENOENT') {
          console.log(`Unable to open ${path}, did you download the data from https://data.sandiego.gov/datasets/get-it-done-311/?`);
        }
        reject(error);
        return;
      })
      .pipe(csv())
      .on('end', async () => {
        if (docs.length > 0) {
          try {
            console.log(count);
            await bulkInsert(docs);
          } catch (err) {
            reject(err);
            return;
          }
        }
        resolve(count);
      })
      .on('data', async row => {
        count++;
        row.location = {
          lat: row.lat,
          lon: row.lng
        }
        delete row.lat;
        delete row.lng;
        docs.push(row);

        if (docs.length >= BULK_INSERT_SIZE && !isPaused) {
          readStream.pause();

          // readline pause is leaky and events in buffer still get sent after pause
          // need to clear buffer before async call
          const docstmp = docs.slice();
          docs = [];
          try {
            console.log(count);
            await bulkInsert(docstmp);
            readStream.resume();
          } catch (err) {
            readStream.close();
            reject(err);
          }
        }
      })
      .on('pause', () => {
        isPaused = true;
      })
      .on('resume', () => {
        isPaused = false;
      });
  });
}


load().catch(console.log);
