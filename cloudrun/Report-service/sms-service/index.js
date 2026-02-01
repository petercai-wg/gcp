const express = require('express');
const app = express();
const bodyParser = require('body-parser');
app.use(bodyParser.json());

const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log('Listening on port', port);
});

app.get('/health', (req, res) => {
  res.send('sms-service Health status: OK');
});

app.post('/', async (req, res) => {
  const labReport = decodeBase64Json(req.body.message.data);
  try {
    console.log(`send SMS Service: Report ${labReport.id}  ${labReport.status} `);
    sendSms(labReport.id);

    console.log(`after sendSms(): Report ${labReport.id} `);

    res.status(204).send();
  }
  catch (ex) {
    console.log(`SMS Service: Report ${labReport.id} failure: ${ex}`);
    res.status(500).send();
  }
})

function decodeBase64Json(data) {
  return JSON.parse(Buffer.from(data, 'base64').toString());
}

function sendSms(id) {
  performOperation().then(() => {
    console.log(`Sms sent successfully for Report ${id}`);
  }).catch((err) => {
    console.log(`sendSms failed for Report ${id}, log error `, err);
  });
}

async function performOperation() {
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      if (Math.random() > 0.001) {
        resolve('Success');
      }
      else {
        reject('Failure');
      }
    }, 1000)
  })
}
