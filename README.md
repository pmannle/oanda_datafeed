Trading View UDF-compatible Oanda data server
==============

This repository contains a sample of UDF-compatible data server.

Register at Oanda.com to get a free API key.

Use NodeJS to launch `oanda.js` with your Oanda API key set in the environment variable:

```bash
APIKEY_LIVE=YOUR_KEY nodejs oanda.js
```
Change the source URL in `index.html` file of the Charting Library:

```javascript
datafeed: new Datafeeds.UDFCompatibleDatafeed("http://localhost:8888")
```
Request access to the Trading View Charting Library repo on github, then download it and boot up the server:

To boot up the chart server:

```bash
cd charting_library
npx serve
```

This will serve the charting index, with default of http://0.0.0.0:5000/

Then, clone this repo in a directory next to the charting_library, install dependencies, and start up UDF server:

```bash
cd oanda_datafeed
npm install
node oanda.js
```

