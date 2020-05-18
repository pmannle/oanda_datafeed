/*
	This file is a node.js module.

	This is a sample implementation of UDF-compatible datafeed wrapper for oanda (historical data) and yahoo.finance (quotes).
	Some algorithms may be incorrect because it's rather an UDF implementation sample
	then a proper datafeed implementation.
*/

/* global require */
/* global console */
/* global exports */
/* global process */

"use strict";

var version = '0.0.1';

var https = require("https");
var http = require("http");
var Bottleneck = require("bottleneck/es5");

var oandaCacheCleanupTime = 24 * 60 * 60 * 1000; // 24 hours
var oandaMinimumDate = '1970-01-01';

var minute = 60 * 1000; // milliseconds in a minute
var hour = 60 * minute; // milliseconds in a hour
var day = 24 * hour; // milliseconds in a day

var config = {
	supports_search: true,
	supports_group_request: false,
	supports_marks: false,
	supports_timescale_marks: false,
	supports_time: true,
	has_intraday: true,
	has_daily: true,
	updateFrequency: 1000 * 60 * 60,
	exchanges: [
		{
			value: "",
			name: "All Exchanges",
			desc: ""
		}
	],
	symbols_types: [
		{
			name: "All types",
			value: ""
		},
		{
			name: "Forex",
			value: "forex"
		}
	],
	supported_resolutions: ["1", "5", "15", "60", "240", "D"]
};

// this cache is intended to reduce number of requests to Oanda
setInterval(function () {
	this.oandaCache = {};
	console.warn(dateForLogs() + 'Oanda cache cleared');
}, oandaCacheCleanupTime);

function dateForLogs() {
	return (new Date()).toISOString() + ': ';
}

var defaultResponseHeader = {
	"Content-Type": "text/plain",
	'Access-Control-Allow-Origin': '*'
};

var defaultRequestHeader = {
	"Content-Type": "text/plain",
	'Access-Control-Allow-Origin': '*',
	'Host': 'api-fxtrade.oanda.com', // api-fxpractice.oanda.com, api-fxtrade.oanda.com
	'Connection': 'Keep-Alive',
	'Pragma': 'no-cache',
	'Cache-Control': 'no-cache',
	'authorization': 'Bearer ' + process.env.APIKEY_LIVE, // process.env.APIKEY, process.env.APIKEY_LIVE
	'content-type': 'application/json',

};

function sendJsonResponse(response, jsonData) {
	response.writeHead(200, defaultResponseHeader);
	response.write(JSON.stringify(jsonData));
	response.end();
}

function dateToYMD(date) {
	var obj = new Date(date);
	var year = obj.getFullYear();
	var month = obj.getMonth() + 1;
	var day = obj.getDate();
	return year + "-" + month + "-" + day;
}

function getValidOandaToken() {
	// placeholder
	return null;
}

function sendError(error, response) {
	response.writeHead(200, defaultResponseHeader);
	response.write("{\"s\":\"error\",\"errmsg\":\"" + error + "\"}");
	response.end();
}

function httpGet(datafeedHost, path, key, callback) {
	var options = {
		host: datafeedHost,
		path: path,
		headers: defaultRequestHeader,
	};

	function onDataCallback(response) {
		var result = '';

		response.on('data', function (chunk) {
			result += chunk;
			if (response.statusCode !== 200 && result && JSON.parse(result).errorMessage) {
				console.log(dateForLogs() + key + "Error oanda response " + JSON.parse(result).errorMessage);
			}

		});

		response.on('end', function () {
			if (response.statusCode !== 200) {
				callback({ status: 'ERR_STATUS_CODE', errmsg: response.statusMessage || '' });
				return;
			}

			callback({ status: 'ok', data: result });
		});
	}

	var retry = function() {
		console.log('Retry: ' + path)
		httpGet(datafeedHost, path, key, callback); //retry
	};

	var req = https.request(options, onDataCallback)

	req.on('socket', function (socket) {
		socket.setTimeout(5000);
		socket.on('timeout', function () {
			console.log(dateForLogs() + key + '****** timeout');
			req.abort();
		});
	});

	req.on('error', retry).setTimeout(20000, function(){
		this.socket.destroy();
	});

	req.end();
}

function convertOandaHistoryToUDFFormat(data) {
	function parseDate(input) {
		var parts = input.split('-');
		return Date.UTC(parts[0], parts[1] - 1, parts[2]);
	}

	var result = {
		t: [],
		c: [],
		o: [],
		h: [],
		l: [],
		v: [],
		s: "ok"
	};

	try {

		var json = JSON.parse(data);

		json.candles.map((candle) => {

			result.t.push(Date.parse(candle.time)/1000);
			result.o.push(candle.mid.o);
			result.h.push(candle.mid.h);
			result.l.push(candle.mid.l);
			result.c.push(candle.mid.c);
			result.v.push(candle.volume);
		})

	} catch (error) {
		return null;
	}

	return result;
}

function proxyRequest(controller, options, response) {
	controller.request(options, function (res) {
		var result = '';

		res.on('data', function (chunk) {
			result += chunk;
		});

		res.on('end', function () {
			if (res.statusCode !== 200) {
				response.writeHead(200, defaultResponseHeader);
				response.write(JSON.stringify({
					s: 'error',
					errmsg: 'Failed to get news'
				}));
				response.end();
				return;
			}
			response.writeHead(200, defaultResponseHeader);
			response.write(result);
			response.end();
		});
	}).end();
}

function RequestProcessor(symbolsDatabase) {

	this._symbolsDatabase = symbolsDatabase;

}



RequestProcessor.prototype._sendConfig = function (response) {

	response.writeHead(200, defaultResponseHeader);
	response.write(JSON.stringify(config));
	response.end();
};

RequestProcessor.prototype._oandaCache = function() {

	// configure cache with supported resolutions & symbols

	var oandaCache = {};

	this._symbolsDatabase.cacheSymbols.forEach((symbol) => {
		oandaCache[symbol.name] = {};
		config.supported_resolutions.forEach((res) => {
			oandaCache[symbol.name][res] = {};
		})
	});

	return oandaCache;

};

RequestProcessor.prototype._sendMarks = function (response) {
	var now = new Date();
	now = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate())) / 1000;
	var day = 60 * 60 * 24;

	var marks = {
		id: [0, 1, 2, 3, 4, 5],
		time: [now, now - day * 4, now - day * 7, now - day * 7, now - day * 15, now - day * 30],
		color: ["red", "blue", "green", "red", "blue", "green"],
		text: ["Today", "4 days back", "7 days back + Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.", "7 days back once again", "15 days back", "30 days back"],
		label: ["A", "B", "CORE", "D", "EURO", "F"],
		labelFontColor: ["white", "white", "red", "#FFFFFF", "white", "#000"],
		minSize: [14, 28, 7, 40, 7, 14]
	};

	response.writeHead(200, defaultResponseHeader);
	response.write(JSON.stringify(marks));
	response.end();
};

RequestProcessor.prototype._sendTime = function (response) {

	var now = new Date();
	response.writeHead(200, defaultResponseHeader);
	response.write(Math.floor(now / 1000) + '');
	response.end();

};

RequestProcessor.prototype._sendTimescaleMarks = function (response) {
	var now = new Date();
	now = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate())) / 1000;
	var day = 60 * 60 * 24;

	var marks = [
		{
			id: "tsm1",
			time: now,
			color: "red",
			label: "A",
			tooltip: ""
		},
		{
			id: "tsm2",
			time: now - day * 4,
			color: "blue",
			label: "D",
			tooltip: ["Dividends: $0.56", "Date: " + new Date((now - day * 4) * 1000).toDateString()]
		},
		{
			id: "tsm3",
			time: now - day * 7,
			color: "green",
			label: "D",
			tooltip: ["Dividends: $3.46", "Date: " + new Date((now - day * 7) * 1000).toDateString()]
		},
		{
			id: "tsm4",
			time: now - day * 15,
			color: "#999999",
			label: "E",
			tooltip: ["Earnings: $3.44", "Estimate: $3.60"]
		},
		{
			id: "tsm7",
			time: now - day * 30,
			color: "red",
			label: "E",
			tooltip: ["Earnings: $5.40", "Estimate: $5.00"]
		},
	];

	response.writeHead(200, defaultResponseHeader);
	response.write(JSON.stringify(marks));
	response.end();
};

RequestProcessor.prototype._sendSymbolSearchResults = function (query, type, exchange, maxRecords, response) {
	if (!maxRecords) {
		throw "wrong_query";
	}

	var result = this._symbolsDatabase.search(query, type, exchange, maxRecords);

	response.writeHead(200, defaultResponseHeader);
	response.write(JSON.stringify(result));
	response.end();
};

RequestProcessor.prototype._prepareSymbolInfo = function (symbolName) {
	var symbolInfo = this._symbolsDatabase.symbolInfo(symbolName);

	if (!symbolInfo) {
		throw "unknown_symbol " + symbolName;
	}

	return {
		"name": symbolInfo.name,
		"exchange-traded": symbolInfo.exchange,
		"exchange-listed": symbolInfo.exchange,
		"timezone": "Etc/UTC",
		"minmov": 1,
		"minmov2": 0,
		"pointvalue": 1,
		"session": "1;0000-2400|0000-2400:1",
		"has_intraday": true,
		"has_no_volume": symbolInfo.type !== "stock",
		"description": symbolInfo.description.length > 0 ? symbolInfo.description : symbolInfo.name,
		"type": symbolInfo.type,
		"supported_resolutions": ["1", "5", "15", "60", "240", "D"],
		"pricescale": 100000,
		"ticker": symbolInfo.name.toUpperCase()
	};
};

RequestProcessor.prototype._sendSymbolInfo = function (symbolName, response) {
	var info = this._prepareSymbolInfo(symbolName);

	response.writeHead(200, defaultResponseHeader);
	response.write(JSON.stringify(info));
	response.end();
};

RequestProcessor.prototype._sendSymbolHistory = function (symbol, startDateTimestamp, endDateTimestamp, resolution, response) {

	let key = symbol + "[" + resolution + "] ";

	var resolution_mapping = {
		'1': 'M1',
		'5': 'M5',
		'15': 'M15',
		'60': 'H1',
		'240': 'H4',
		'D': 'D'
	};

	var secsInResolution = (resolution) => {

		switch(resolution) {
			case "240": return (4 * hour)/1000;
				break;
			case "60": return (hour)/1000;
				break;
			case "15": return (15 * minute)/1000;
				break;
			case "5": return (5 * minute)/1000;
				break;
			case "1": return (minute)/1000;
				break;
			default: return (day)/1000;
		}

	};

	var oanda_resolution;

	var oandaCache = this.oandaCache;

	// Supporting functions

	function sendResult(content) {
		var header = Object.assign({}, defaultResponseHeader);
		header["Content-Length"] = content.length;
		response.writeHead(200, header);
		response.write(content, null, function () {
			response.end();
		});
	}

	function secondsToISO(sec) {
		if (sec === null || sec === undefined) {
			return 'n/a';
		}
		return (new Date(sec * 1000).toISOString());

	}

	function calcPeriod(resolution, period) {

		// return number of seconds in period for time calculations

		if (resolution == 'D') {
			return day;
		} else if (resolution >= 60) {
			return (resolution/60) * hour;
		} else {
			return resolution * minute;
		}

	}

	function timeFloor(lookup_to, resolution) {

		// do math in milliseconds, then convert back to seconds (divide by 1000)
		lookup_to *= 1000;


		if (resolution == 'D') {

			let period = day;

			return [new Date(Math.round(lookup_to / day) * day).setUTCHours(21), period]; // Oanda UTC NY Close

		} else if (resolution >= 60) {

			let period = (resolution/60) * hour;

			return [new Date(Math.round(lookup_to / hour) * hour).setUTCMinutes(0), period];

		} else {

			let remainder = new Date(lookup_to).getMinutes() % resolution;

			lookup_to = lookup_to -=  remainder * minute;

			let period = resolution * minute;

			return [new Date(Math.round(lookup_to / minute) * minute).setUTCSeconds(0), period];

		}

	}

	console.log(dateForLogs() + key + "Got history request from " + secondsToISO(startDateTimestamp) + " to " + secondsToISO(endDateTimestamp));

	// var queryBarsInMilliseconds = (endDateTimestamp - startDateTimestamp) * 1000;

	var getQueryDates = () => {

		var from, to, query; // these are ISO timestamps; for day resolution, leave off H-M-S

		return new Promise(async resolve => {

			if (resolution == 'D') {


				from = dateToYMD(secondsToISO(startDateTimestamp));
				to = dateToYMD(secondsToISO(endDateTimestamp));
				oanda_resolution = resolution;

				query = { from: from, to: to, oanda_resolution: oanda_resolution };

			} else {

				let currentTime = new Date().getTime() / 1000;
				let finalTo = (endDateTimestamp * 1000 > currentTime) ? currentTime : endDateTimestamp;

				to = (endDateTimestamp * 1000 > currentTime) ? currentTime : endDateTimestamp;
				from = secondsToISO(new Date(startDateTimestamp * 1000).setUTCSeconds(0) / 1000);
				to = secondsToISO(new Date(new Date(to * 1000).setUTCSeconds(0)).setUTCMilliseconds(0) / 1000);
				oanda_resolution = resolution_mapping[resolution];

				query = { from: from, to: to, oanda_resolution: oanda_resolution };

			}

			resolve(query);


		})


	};

	var checkCachedDataToUpdateQuery = (new_query) => { // new_query

		console.log(dateForLogs() + key + 'Checking cache to build query for needed data');

		// deal only in seconds
		new_query.from  = new Date(new_query.from).getTime()/1000;
		new_query.to = new Date(new_query.to).getTime()/1000;

		return new Promise(resolve => {

			// var checkCache = (new_query) => {

				var query = {};
				var prepend_cache = {};
				var append_cache = {};

				// check cache, build queries - should implement redis and/or nginx for prod
				if (Object.keys(oandaCache[symbol][resolution]).length) {

					if (Object.keys(oandaCache[symbol][resolution]['t']).length) {

						var cache_timestamps = oandaCache[symbol][resolution]['t'];
						var cache_start = cache_timestamps[0];
						var cache_end = cache_timestamps[cache_timestamps.length - 1];

						// build prepend query
						if (new_query.from < cache_start && ((cache_start- new_query.from) > secsInResolution(resolution))) {
							// call oanda to get prepend data not in cache
							prepend_cache.from = new_query.from;
							prepend_cache.to = cache_start;

							console.log(dateForLogs() + key + '*** prepend query: ' + secondsToISO(new_query.from) + ' to ' + secondsToISO(prepend_cache.to) +
								' / cache: ' + secondsToISO(cache_start) + ' to ' + secondsToISO(cache_end));

							return resolve({ query, prepend_cache, append_cache })

						}

						/*
						let cache_lookup_to, period;
						[cache_lookup_to, period] = timeFloor(new_query.to, resolution);

						// build append query, subtract current unclosed candle from timeframe
						if ((cache_lookup_to - calcPeriod(resolution))/1000 > cache_end) {

							append_cache.from = ((cache_end * 1000) + period)/1000; // get timestamp of next candle
							append_cache.to = (cache_lookup_to - calcPeriod(resolution))/1000;

							let dayOfWeek = new Date(append_cache.to * 1000).getDay();

							// ignore if nothing new, or it's Saturday and markets closed
							if (append_cache.from ==  append_cache.to || dayOfWeek === 6) {
								append_cache = {}; // nothing to see here
							}

							console.log(dateForLogs() + key + '*** append query: ' + secondsToISO(append_cache.from) + ' to ' + secondsToISO(append_cache.to) +
								' / cache: ' + secondsToISO(cache_start) + ' to ' + secondsToISO(cache_end));

							return resolve({ query, prepend_cache, append_cache })

						}
						*/

						// this code works to get updated new data
						// if ((cache_lookup_to - calcPeriod(resolution))/1000 > cache_end) {

						if (new_query.to > cache_end) {

							// append_cache.from = ((cache_end * 1000) + period)/1000; // get timestamp of next candle
							append_cache.from = cache_end; // get timestamp of next candle
							// append_cache.to = (cache_lookup_to - calcPeriod(resolution))/1000;
							append_cache.to = new_query.to;

							let dayOfWeek = new Date(append_cache.to * 1000).getDay();

							// ignore if nothing new, or it's Saturday and markets closed
							if (append_cache.from ==  append_cache.to || dayOfWeek === 6) {
								append_cache = {}; // nothing to see here
							}

							console.log(dateForLogs() + key + '*** append query: ' + secondsToISO(append_cache.from) + ' to ' + secondsToISO(append_cache.to) +
								' / cache: ' + secondsToISO(cache_start) + ' to ' + secondsToISO(cache_end));

							return resolve({ query, prepend_cache, append_cache })

						}


						return resolve({ query, prepend_cache, append_cache })

					} else {

						query.from = new_query.from;
						query.to = new_query.to;

						return resolve({ query, prepend_cache, append_cache })

					}

				} else {

					query.from = new_query.from;
					query.to = new_query.to;

					console.log(dateForLogs() + key + 'no data in cache for this query...');

					return resolve({ query, prepend_cache, append_cache })

				}

		});






	};

	var httpGetWrapper = (address) => {

		return new Promise(async resolve => {


				httpGet("api-fxtrade.oanda.com", address, key, function (result) {

					let key = symbol + "[" + resolution + "] ";

					console.log(dateForLogs() + key + "Sending request to oanda url=" + address);

					if (response.finished) {
						// we can be here if error happened on socket disconnect
						return;
					}

					if (result.status !== 'ok') {
						if (result.status === 'ERR_SOCKET') {
							console.log('Socket problem with request: ' + result.errmsg);
							sendError("Socket problem with request " + result.errmsg, response);
							return;
						}

						console.error(dateForLogs() + key + "Error response from oanda, message: " + result.errmsg);

						sendError("Error oanda response " + result.errmsg, response);

						return;
					}

					console.log(dateForLogs() + key + "Got response from oanda - Try to parse.");

					var data = convertOandaHistoryToUDFFormat(result.data);

					if (data === null) {
						var dataStr = typeof result === "string" ? result.slice(0, 100) : result;
						console.error(dateForLogs() + " failed to parse: " + dataStr);
						sendError("Invalid oanda response", response);
						return;
					}

					return resolve(data);

				})

		}).catch((error) => {

			console.log(dateForLogs() + key + 'ERROR: ' + error)

		})


	};

	var buildQueryLink = (startingCandleTime, endingCandleTime, millisecsInRes) => {


		var date = new Date(startingCandleTime);
		var dayOfWeek = date.toUTCString().split(',')[0];
		var time, marketClose, marketOpen, includeFirst;

		// if startingCandleTime is Fri after close, Sat, or Sunday before open, then includeFirst candle;

		switch (dayOfWeek) {
			case 'Fri':
				time = date.getTime();
				marketClose = new Date(date.getFullYear(), date.getMonth(), date.getDate(), 20, 59, 0).getTime()
				includeFirst = (time > marketClose) ? 'True' : 'False';
				break;
			case 'Sat':
				includeFirst = 'True';
				break;
			case 'Sun':
				time = date.getTime();
				marketOpen = new Date(date.getFullYear(), date.getMonth(), date.getDate(), 21, 0, 0).getTime()
				includeFirst = (time <= marketOpen) ? 'True' : 'False';
				break;
			default:
				includeFirst = 'False'

		}


		var address = "/v3/instruments/" + symbol + "/candles" +
			"?from=" + startingCandleTime +
			"&to=" + endingCandleTime +
			"&smooth=True" +
			"&granularity=" + resolution_mapping[resolution] +
			"&includeFirst=" + includeFirst; // +

		let startDayOfTheWeek = new Date(startingCandleTime).toUTCString().toString().split(',')[0];
		let endDayOfTheWeek = new Date(endingCandleTime).toUTCString().toString().split(',')[0];

		let barsInQuery = (new Date(endingCandleTime).getTime() - new Date(startingCandleTime).getTime()) / millisecsInRes;

		console.log(dateForLogs() + key + 'Query added: ' + startDayOfTheWeek + ' ' + startingCandleTime + ' to ' + endDayOfTheWeek + ' ' + endingCandleTime + ' (' + barsInQuery + ' bars) includeFirst: ' + includeFirst);

		return address;

	};

	var returnCachedDataOrQueryAPI = (request) => {

		console.log(dateForLogs() + key + 'Fetching queries...');


		// if there are no queries, then we've determined data is already in cache
		if (!request.query.from && !request.prepend_cache.from && !request.append_cache.from) {

			let cachedCandleTimeStamps = oandaCache[symbol][resolution].t;

			let index_from = cachedCandleTimeStamps.indexOf(cachedCandleTimeStamps.find(t => t > startDateTimestamp));

			let index_to = cachedCandleTimeStamps.indexOf(
				cachedCandleTimeStamps.find(t => t > endDateTimestamp) ? cachedCandleTimeStamps.find(t => t > endDateTimestamp) :
					cachedCandleTimeStamps[cachedCandleTimeStamps.length-1]
				);

			console.log(dateForLogs() + key + '*** Cached data lookup: ' +
				secondsToISO(oandaCache[symbol][resolution].t[index_from]) + ' to ' +
				secondsToISO(oandaCache[symbol][resolution].t[index_to]) +
				' / cache: ' + secondsToISO(oandaCache[symbol][resolution].t[0]) + ' to ' +
			    secondsToISO(oandaCache[symbol][resolution].t[oandaCache[symbol][resolution].t.length-1]));

			// return a promise so our .then function works
			return new Promise(async resolve => {

				let bars = cachedCandleTimeStamps.slice(index_from, index_to);

				console.log(dateForLogs() + key + "Returning cached data: " + bars.length + " bars.");

				resolve([{
					t: oandaCache[symbol][resolution].t.slice(index_from, index_to),
					o: oandaCache[symbol][resolution].o.slice(index_from, index_to),
					h: oandaCache[symbol][resolution].h.slice(index_from, index_to),
					l: oandaCache[symbol][resolution].l.slice(index_from, index_to),
					c: oandaCache[symbol][resolution].c.slice(index_from, index_to),
					v: oandaCache[symbol][resolution].v.slice(index_from, index_to),
					s: 'cached_data_only' // update this to 'ok' before sending response
				}]);

			})


		}

		// otherwise, the data, or not all the data requested, is in the cache - so hit api for hit
		return new Promise(async resolve => {

			// make sure we don't exceed api request limit of 5000 candles

			let request_from = request.query.from ? request.query.from : request.prepend_cache.from ? request.prepend_cache.from : request.append_cache.from;
			let request_to= request.query.to ? request.query.to : request.prepend_cache.to ? request.prepend_cache.to : request.append_cache.to;

			var queries = [];

				let from = request_from;
				let to = request_to;

				let currentTime = new Date().getTime()/1000;

				to = (to > currentTime) ? currentTime : to;

				let millisecsInRes = secsInResolution(resolution) * 1000;  // from 2016-12-29T14:15:00.000Z 2017-01-02T23:01:00.000Z

				var queryBarsInMilliseconds = (to - from) * 1000;

			// oanda maxes out of 5000
				let totalNumberOfAPICalls = (queryBarsInMilliseconds/millisecsInRes) < 5000 ? 1 : (queryBarsInMilliseconds/millisecsInRes) / 5000;

				let apiCalls = [];

				for (let i=1; i <= Math.ceil(totalNumberOfAPICalls); i++) { apiCalls.push(i) }

				console.log(dateForLogs() + key + '*** Number of API calls to get data: ' + apiCalls.length);

				let callPercentage = 100/apiCalls.length * .01;

				apiCalls.map((x, index) => {

					let percentMultiplier = index + 1;

					queries.push(
						{
							from: secondsToISO(new Date(from * 1000 + (queryBarsInMilliseconds * (callPercentage * index)))/1000), // .setUTCSeconds(0).setUTCMilliseconds(0)
							to: secondsToISO(new Date(new Date((to * 1000) - (queryBarsInMilliseconds * (callPercentage * (apiCalls.length - percentMultiplier))))/1000)), // .setUTCSeconds(0)).setUTCMilliseconds(0)
							oanda_resolution: resolution_mapping[resolution]
						});


				});

				queries.map((query, index) => queries[index].query_url = buildQueryLink(query.from, query.to, millisecsInRes));


			// to execute 3 requests per second - minTime: 333
				const limiter = new Bottleneck({
					maxConcurrent: 100,
					// minTime: 1000
				});

					Promise.all(queries.map((query, index) => {

						var data = [];

						return limiter.schedule(httpGetWrapper, query.query_url)
							.then((row) => { return row }).catch(error => console.log(error))

						})).then((data) => {

						console.log(dateForLogs() + key + '*** Recieved API responses: ' + data.length)

						data.sort((a, b) => (a.t > b.t) ? 1 : -1);

						let concat_data = [{
							t: [],
							o: [],
							h: [],
							l: [],
							c: [],
							v: []
						}];


						if (data.length > 1 && data[0].t.length >= 1) {

							data.forEach((row, index) => {

								concat_data[0].t = concat_data[0].t.concat(row.t);
								concat_data[0].o = concat_data[0].o.concat(row.o);
								concat_data[0].h = concat_data[0].h.concat(row.h);
								concat_data[0].l = concat_data[0].l.concat(row.l);
								concat_data[0].c = concat_data[0].c.concat(row.c);
								concat_data[0].v = concat_data[0].v.concat(row.v);

								let count = index + 1;
								console.log(dateForLogs() + key + 'Response[' + count + ']: ' + new Date(row.t[0]*1000).toISOString() +
									' to: ' + new Date(row.t[row.t.length-1]*1000).toISOString() );

							});

						} else {

							if (data[0].t.length === 0) {
								data[0].s = 'no_data_prepend'
							}
							concat_data = data;

						}

					    return resolve(concat_data);

				}).catch((error) => {

						if (error instanceof Bottleneck.BottleneckError) {

							console.log('Bottleneck error' + error)

						}

					});

		});



	};

	var addDataToCacheAndMergeWithAPIData = (data, symbol, resolution, fromSeconds, toSeconds) => {

		return new Promise(async resolve => {

			if (!Object.keys(data).length) {
				return resolve([{
					s: 'no_data'}]);
			}

			if (data && data.t.length === 0) {
				// return resolve([{
				// 	s: 'no_data'
				// }]);
			}

			if (data && data.s && data.s === 'cached_data_only') {
				console.log(dateForLogs() + key + 'Returning data from cache only, no api data required')
			}

			if (data && data.t.length >= 0 && data.s !== 'cached_data_only') {

				if (!oandaCache[symbol][resolution].t) {

					oandaCache[symbol][resolution] = data;

					console.log(dateForLogs() + key + "*** query data added to cache: from " +
						secondsToISO(oandaCache[symbol][resolution].t[0]) + "(" + oandaCache[symbol][resolution].t[0] + ") to " +
						secondsToISO(oandaCache[symbol][resolution].t[oandaCache[symbol][resolution].t.length-1]) + " (" +
						oandaCache[symbol][resolution].t[oandaCache[symbol][resolution].t.length-1] + ") - " +
						data.t.length + " bars");

				} else if(data.t[data.t.length-1] < oandaCache[symbol][resolution].t[0] || data.s == 'no_data_prepend') {

					// prepend to beginning of cache

					// if we have 'no_data_prepend' it likely means we got no data back from api because market
					// was closed during that period (Saturday) so leave cache as is

					if (data.s !== 'no_data_prepend' ) {

						// sanity check
						if (!oandaCache[symbol][resolution]['t'].indexOf(data.t[0]) < 0) {
							console.log('ERROR: we already have ' + new Date(data.t[0] * 1000).toISOString() + " in the cache")
						}

						console.log(dateForLogs() + key + "Data ending with candle close of " + new Date(data.t[data.t.length - 1] * 1000).toISOString() + " (" + data.t[data.t.length - 1] + ")"
							+ " prepended to cache starting at " + new Date(oandaCache[symbol][resolution].t[0] * 1000).toISOString()
							+ " (" + oandaCache[symbol][resolution].t[0] + ")");

						oandaCache[symbol][resolution].t = data.t.concat(oandaCache[symbol][resolution].t);
						oandaCache[symbol][resolution].o = data.o.concat(oandaCache[symbol][resolution].o);
						oandaCache[symbol][resolution].h = data.h.concat(oandaCache[symbol][resolution].h);
						oandaCache[symbol][resolution].l = data.l.concat(oandaCache[symbol][resolution].l);
						oandaCache[symbol][resolution].c = data.c.concat(oandaCache[symbol][resolution].c);
						oandaCache[symbol][resolution].v = data.v.concat(oandaCache[symbol][resolution].v);

						console.log(dateForLogs() + key + "*** prepended " + data.t.length + " bars to cache");

					}

				} else {

					// append to end of cache
					// check last entries of cache, and don't duplicate current candle when adding new data
					let lengthOfAppend = data.t.length;
					let columnsToCheck = oandaCache[symbol][resolution].t.slice(-lengthOfAppend);
					let columnsToDelete = [];
					columnsToCheck.forEach((entry, index) => {
						data.t.forEach((appEntry, appIndex) => {
							if (entry === appEntry) {
								columnsToDelete.push(index)
							}
						});
					});

					columnsToDelete.map((col) => {
						data.t.splice(col);
						data.o.splice(col);
						data.h.splice(col);
						data.l.splice(col);
						data.c.splice(col);
					});


					if (data.t.length > 0)  {

						console.log(dateForLogs() + key + "data starting with candle close of " + secondsToISO(data.t[0]) + " (" + data.t[0] + ")"
							+ " appended to cache ending at " + secondsToISO(oandaCache[symbol][resolution].t[oandaCache[symbol][resolution].t.length-1])
							+  " (" + oandaCache[symbol][resolution].t[oandaCache[symbol][resolution].t.length-1] + ")");

						oandaCache[symbol][resolution].t = oandaCache[symbol][resolution].t.concat(data.t);
						oandaCache[symbol][resolution].o = oandaCache[symbol][resolution].o.concat(data.o);
						oandaCache[symbol][resolution].h = oandaCache[symbol][resolution].h.concat(data.h);
						oandaCache[symbol][resolution].l = oandaCache[symbol][resolution].l.concat(data.l);
						oandaCache[symbol][resolution].c = oandaCache[symbol][resolution].c.concat(data.c);
						oandaCache[symbol][resolution].v = oandaCache[symbol][resolution].v.concat(data.v);

						console.log(dateForLogs() + "*** append data added to cache " + data.t.length + " bars.");

					} else {
						return resolve([{
							s: 'no_data'
						}]);
					}

				}



				var fromIndex = null;
				var toIndex = null;
				var times = oandaCache[symbol][resolution].t;
				for (var i = 0; i < times.length; i++) {
					var time = times[i];
					if (fromIndex === null && time >= fromSeconds) {
						fromIndex = i;
					}
					if (toIndex === null && time >= toSeconds) {
						toIndex = time > toSeconds ? i - 1 : i;
					}
					if (fromIndex !== null && toIndex !== null) {
						break;
					}
				}

				fromIndex = fromIndex || 0;
				toIndex = toIndex ? toIndex + 1 : times.length;

				//var s = data.s;
				var s = 'ok';

				// if (toSeconds < times[0]) {
				// 	s = 'no_data';
				// }

				// toIndex = Math.min(fromIndex + 1000, toIndex); // do not send more than 1000 bars for server capacity reasons


				return resolve({
					t: oandaCache[symbol][resolution].t.slice(fromIndex, toIndex),
					o: oandaCache[symbol][resolution].o.slice(fromIndex, toIndex),
					h: oandaCache[symbol][resolution].h.slice(fromIndex, toIndex),
					l: oandaCache[symbol][resolution].l.slice(fromIndex, toIndex),
					c: oandaCache[symbol][resolution].c.slice(fromIndex, toIndex),
					v: oandaCache[symbol][resolution].v.slice(fromIndex, toIndex),
					s: s
				});

			}


			if (data.t.length === 0 && data.s !== 'cached_data_only') {

				return resolve({
					s: 'no_data',
					nextTime: new Date((toSeconds * 1000) - day).getTime()/1000
				});

			} else {

				return resolve({
					t: oandaCache[symbol][resolution].t,
					o: oandaCache[symbol][resolution].o,
					h: oandaCache[symbol][resolution].h,
					l: oandaCache[symbol][resolution].l,
					c: oandaCache[symbol][resolution].c,
					v: oandaCache[symbol][resolution].v,
					s: 'ok'
				});

			}





		});


	};

	getQueryDates().then((queryDates) => {

		var api_data = [];

		(async () => {

			var api_promise = await checkCachedDataToUpdateQuery(queryDates).then(returnCachedDataOrQueryAPI);

			api_data = await Promise.all(api_promise).then(function(values) {

				return (values);

			});

			var merged_and_cached_data = api_data.map((result) => {

				return addDataToCacheAndMergeWithAPIData(result, symbol, resolution, startDateTimestamp, endDateTimestamp);

			});

			var response = await Promise.all(merged_and_cached_data);

			sendResult(JSON.stringify(response[0]));

		})();

	})

};

RequestProcessor.prototype._quotesOandaWorkaround = function (tickersMap, resolution) {
	var from = oandaMinimumDate;
	var to = dateToYMD(Date.now());

	var result = {
		s: "ok",
		d: [],
		source: 'Oanda',
	};

	Object.keys(tickersMap).forEach(function(symbol) {
		var key = symbol + "|" + from + "|" + to + "|" + resolution;
		var ticker = tickersMap[symbol];

		var data = quandlCache[key];
		var length = data === undefined ? 0 : data.c.length;

		if (length > 0) {
			var lastBar = {
				o: data.o[length - 1],
				h: data.o[length - 1],
				l: data.o[length - 1],
				c: data.o[length - 1],
				v: data.o[length - 1],
			};

			result.d.push({
				s: "ok",
				n: ticker,
				v: {
					ch: 0,
					chp: 0,

					short_name: symbol,
					exchange: '',
					original_name: ticker,
					description: ticker,

					lp: lastBar.c,
					ask: lastBar.c,
					bid: lastBar.c,

					open_price: lastBar.o,
					high_price: lastBar.h,
					low_price: lastBar.l,
					prev_close_price: length > 1 ? data.c[length - 2] : lastBar.o,
					volume: lastBar.v,
				}
			});
		}
	});

	return result;
};

RequestProcessor.prototype._sendQuotes = function (tickersString, resolution, response) {
	var tickersMap = {}; // maps YQL symbol to ticker

	var tickers = tickersString.split(",");
	[].concat(tickers).forEach(function (ticker) {
		var yqlSymbol = ticker.replace(/.*:(.*)/, "$1");
		tickersMap[yqlSymbol] = ticker;
	});

	sendJsonResponse(response, this._quotesOandaWorkaround(tickersMap, resolution));
	console.log("Quotes request : " + tickersString + ' processed from oanda cache');
};

RequestProcessor.prototype._sendNews = function (symbol, response) {
	var options = {
		host: "feeds.finance.yahoo.com",
		path: "/rss/2.0/headline?s=" + symbol + "&region=US&lang=en-US"
	};

	proxyRequest(https, options, response);
};

RequestProcessor.prototype._sendFuturesmag = function (response) {
	var options = {
		host: "www.oilprice.com",
		path: "/rss/main"
	};

	proxyRequest(http, options, response);
};

RequestProcessor.prototype.processRequest = function (action, query, response) {
	try {
		if (action === "/config") {
			this._sendConfig(response);
		}
		else if (action === "/symbols" && !!query["symbol"]) {
			this._sendSymbolInfo(query["symbol"], response);
		}
		else if (action === "/search") {
			this._sendSymbolSearchResults(query["query"], query["type"], query["exchange"], query["limit"], response);
		}
		else if (action === "/history") {
			let from = query["from"] * 1000;
			let to = query["to"] * 1000;
			query["resolution"] = (query["resolution"] == '1D' || !query["resolution"]) ? 'D' : query["resolution"];
			// console.log('Received resolution \"' + query["resolution"] + '\" query from ' + new Date(from).toUTCString() + " to " + new Date(to).toUTCString())
			this._sendSymbolHistory(query["symbol"], query["from"], query["to"], query["resolution"].toUpperCase(), response);
		}
		else if (action === "/quotes") {
			this._sendQuotes(query["symbols"], query["resolution"], response);
		}
		else if (action === "/marks") {
			this._sendMarks(response);
		}
		else if (action === "/time") {
			this._sendTime(response);
		}
		else if (action === "/timescale_marks") {
			this._sendTimescaleMarks(response);
		}
		else if (action === "/news") {
			this._sendNews(query["symbol"], response);
		}
		else if (action === "/futuresmag") {
			this._sendFuturesmag(response);
		} else {
			response.writeHead(200, defaultResponseHeader);
			response.write('Datafeed version is ' + version +
				'\nValid keys count is ' + String(quandlKeys.length - invalidQuandlKeys.length) +
				'\nCurrent key is ' + (getValidOandaToken() || '').slice(0, 3) +
				(invalidQuandlKeys.length !== 0 ? '\nInvalid keys are ' + invalidQuandlKeys.reduce(function(prev, cur) { return prev + cur.slice(0, 3) + ','; }, '') : ''));
			response.end();
		}
	}
	catch (error) {
		sendError(error, response);
		console.error('Exception: ' + error);
	}
};

exports.RequestProcessor = RequestProcessor;
