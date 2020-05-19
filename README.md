Trading View UDF-compatible Oanda data server
==============

This repository contains a sample of UDF-compatible data server.

Register at Oanda.com to get a free API key.

First, request access to the Trading View Charting Library repo on github.

Then download it and change the source URL in `index.html` file of the Charting Library:

```javascript
datafeed: new Datafeeds.UDFCompatibleDatafeed("http://localhost:8888")
```

and boot up the server and boot up the chart server:

```bash
cd charting_library
npx serve
```

This will serve the charting index, with default of http://0.0.0.0:5000/

Then, clone this repo in a directory next to the charting_library, install dependencies, and start up UDF server:

```bash
cd oanda_datafeed
npm install
APIKEY_LIVE=YOUR_KEY node oanda.js
```
