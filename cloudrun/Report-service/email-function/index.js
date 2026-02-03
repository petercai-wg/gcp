const functions = require('@google-cloud/functions-framework');


function sendEmail(id) {
    console.log(`sendEmail() function for Report ${id} ...`);
}



// Register a CloudEvent callback with the Functions Framework that will
// be executed when the Pub/Sub trigger topic receives a message.
functions.cloudEvent('emailPubSub', cloudEvent => {
    // The Pub/Sub message is passed as the CloudEvent's data payload.
    const data = cloudEvent.data.message.data;
    console.dir(data);

    const labReport = JSON.parse(Buffer.from(data, 'base64').toString());

    console.log(`cloud function received: ${labReport.id} , ${labReport.status} `);

    sendEmail(labReport.id);
});
