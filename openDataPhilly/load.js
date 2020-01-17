const { Client } = require('@elastic/elasticsearch');
const fs = require('fs');
const oboe = require('oboe');

const geojsonFileName = process.argv[2] || 'feature_collection.geojson';
const ES_INDEX_NAME = 'odp';
const BULK_INSERT_SIZE = 500;
const esClient = new Client({
  node: 'http://localhost:9200',
  auth: {
    username: 'elastic',
    password: 'changeme'
  }
});

async function ingest() {
  try {
    await esClient.indices.delete({ index: ES_INDEX_NAME });
  } catch (e) {
    console.warn(e);
  }

  try {
    await esClient.indices.create({
      index: ES_INDEX_NAME,
      body: {
        mappings: {
          properties: {
            geometry: {
              type: 'geo_point',
              ignore_malformed: true
            }
          }
        }
      }
    });
  } catch (e) {
    console.error(e);
    throw e;
  }

  const readStream = fs.createReadStream(geojsonFileName);
  let count = 0;
  let invalidDocsCount = 0;
  let docs = [];
  let isPaused = false;
  oboe(readStream)
    .node('features.*', async feature => {
      if (!feature || !feature.geometry || !feature.geometry.coordinates) {
        invalidDocsCount++;
        return;
      }

      docs.push({
        ...feature.properties,
        geometry: feature.geometry.coordinates
      });
      count++;

      if (docs.length >= BULK_INSERT_SIZE && !isPaused) {
        isPaused = true;
        readStream.pause();

        // readline pause is leaky and events in buffer still get sent after pause
        // need to clear buffer before async call
        const docstmp = docs.slice();
        docs = [];
        try {
          console.log(count);
          await bulkInsert(docstmp);
          readStream.resume();
          isPaused = false;
        } catch (err) {
          readStream.close();
          throw err;
        }
      }
    })
    .done(()=>{
      console.log(`done processing, rejected ${invalidDocsCount} features`);
    });
}

async function bulkInsert(docs) {
  console.log(`Indexing ${docs.length} documents into ${ES_INDEX_NAME}`);

  const bulk = [];
  docs.forEach(async (doc) => {
    bulk.push({ index: { _index: ES_INDEX_NAME } });
    bulk.push(doc);
  });

  const resp = await esClient.bulk({
    body: bulk
  });
  if (resp.errors) {
    console.log(`Faild to load some docs`);
  }
}

ingest();
