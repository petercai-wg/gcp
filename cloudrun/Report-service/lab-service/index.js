const { PubSub } = require('@google-cloud/pubsub');
const pubsub = new PubSub();
const express = require('express');
const app = express();
const bodyParser = require('body-parser');
app.use(bodyParser.json());
const port = process.env.PORT || 8080;

app.listen(port, () => {
  console.log('Listening on port', port);
});

app.get('/about', (req, res) => {
  res.send('About lab-service status: OK');
});

app.post('/', async (req, res) => {
  try {
    const labReport = req.body;
    console.log(` received post ${JSON.stringify(labReport)}`);
    await publishPubSubMessage(labReport);
    res.status(204).send();
  }
  catch (ex) {
    console.log(ex);
    res.status(500).send(ex);
  }
})

async function publishPubSubMessage(labReport) {
  const buffer = Buffer.from(JSON.stringify(labReport));
  console.log(` publishPubSubMessage  ${buffer} ... to  my-lab-topic`);
  await pubsub.topic('my-lab-topic').publish(buffer);
}