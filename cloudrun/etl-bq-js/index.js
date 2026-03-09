const { Storage } = require('@google-cloud/storage');
const { BigQuery } = require('@google-cloud/bigquery');

const storage = new Storage();
const bigquery = new BigQuery();

async function loadCSV(bucketName, filename, datasetId, tableId) {

    // Define the configuration for the load job
    const options = {
        sourceFormat: 'CSV',
        skipLeadingRows: 1, // Skip the header row if your CSV has one
        autodetect: false,
        // Specify write disposition (e.g., 'WRITE_APPEND' or 'WRITE_TRUNCATE')
        writeDisposition: 'WRITE_APPEND', // Append data to the table
    };

    // Get a reference to the file in GCS

    const file = storage.bucket(bucketName).file(filename);

    console.log(`start insert into BigQuery: ${datasetId}.${tableId} from file: ${filename}`);

    // Start the load job
    const [job] = await bigquery
        .dataset(datasetId)
        .table(tableId)
        .load(file, options);

    console.log(`Job ${job.id} initiated.`);

}


exports.loadBigQueryFromGCS = async (event, context) => {
    if (!event || !event.bucket) {
        throw new Error('Invalid event data. Missing bucket information.');
    }

    const bucketName = event.bucket;
    const fileName = event.name;

    const datasetId = process.env.DATASET;
    const tableId = process.env.TABLE;

    // BigQuery configuration
    // const datasetId = 'my_df_etl';
    // const tableId = 'go_txn'

    console.log(`Processing file: ${fileName} from bucket: ${bucketName} into BigQuery dataset: ${datasetId}, table: ${tableId} `);

    try {
        await loadCSV(bucketName, fileName, datasetId, tableId);
        console.log('CSV file loaded successfully into BigQuery.');
    } catch (error) {
        console.error('Error processing the file:', error);
    }

    console.log(`Finished processing functinon for file: ${fileName}`);
};