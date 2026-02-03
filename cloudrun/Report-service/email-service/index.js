const express = require('express');
const app = express();
const bodyParser = require('body-parser');
app.use(bodyParser.json());

const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log('Listening on port', port);
});

app.get('/health', (req, res) => {
  res.send('email-service Health status: OK \n\n');
});

app.post('/', async (req, res) => {
  const labReport = decodeBase64Json(req.body.message.data);
  try {
    sendEmail(labReport.id);
    console.log(`Email Service: Report ${labReport.id} email sent success :-)`);
    res.status(204).send();
  } catch (ex) {
    console.log(`Email Service: Report ${labReport.id} failure: ${ex}, wait for retry ...`);
    res.status(500).send();
  }
})

function decodeBase64Json(data) {
  return JSON.parse(Buffer.from(data, 'base64').toString());
}

async function sendEmail(id) {
  console.log(`sendEmail() function for Report ${id} ...`);
  //  wait for result before returning confirmation
  await attemptFlakeyOperation().then(() => {
    console.log(`Email sent successfully for Report ${id}`);
  }).catch((err) => {
    console.log(`sendEmail() failed for Report ${id},  `, err);
    throw err;
  });

}


async function attemptFlakeyOperation() {
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      if (Math.random() > 0.3) {
        resolve('Success');
      } else {
        reject('Failure');
      }
    }, 1000 + Math.random() * 1000)
  })
}
