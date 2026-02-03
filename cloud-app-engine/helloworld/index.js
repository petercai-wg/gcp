
'use strict';

const express = require('express');
const path = require('path');

const app = express();

app.use(express.urlencoded({ extended: true }));


const APPTYPE = process.env.APPTYPE || 'Standard';

app.get('/', (req, res) => {
    res.send(`Hello from App Engine ${APPTYPE}! \n `);
});

app.get('/submit', (req, res) => {
    res.sendFile(path.join(__dirname, '/views/form.html'));
});

app.post('/submit', (req, res) => {
    console.log({
        name: req.body.name,
        message: req.body.message,
    });
    res.send(`Received message: ${req.body.message},  thank you ${req.body.name}! `);
});

// Listen to the App Engine-specified port, or 8080 otherwise
const PORT = parseInt(process.env.PORT) || 8080;

app.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}...`);
});

module.exports = app;