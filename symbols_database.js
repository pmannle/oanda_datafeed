/*
	This file is a node.js module intended for use in different UDF datafeeds.
*/
//	This list should contain all the symbols available through your datafeed.
//	The current version is extremely incomplete (as it's just a sample): Oanda has much more of them.

"use strict";


/* global exports */

var symbols = [
{"name":"EUR_USD", "ticker":"EUR_USD","description":"Euro/USD","exchange":"Oanda","type":"forex",
	"session":"1;0000-2400|0000-2400:1", "timezone":"Etc/UTC",
	"has_daily":true,  "has_intraday":true,   "supported_resolutions": [
		"1",
		"5",
		"15",
		"30",
		"60",
		"240",
		"D"
	], "intraday_multipliers": [
		"1",
		"5",
		"15",
		"30",
		"60",
		"240",
		"D"
	]},
{"name":"GBP_USD","ticker":"GBP_USD","description":"British Pound/USD","exchange":"Oanda","type":"forex",
	"session":"1;0000-2400|0000-2400:1", "timezone":"Etc/UTC",
	"has_daily": true,   "has_intraday":true, "supported_resolutions": [
		"1",
		"5",
		"15",
		"30",
		"60",
		"240",
		"D"
	], "intraday_multipliers": [
		"1",
		"5",
		"15",
		"30",
		"60",
		"240",
		"D"
	]}];

function searchResultFromDatabaseItem(item) {
	return {
		symbol: item.name,
		full_name: item.name,
		description: item.description,
		exchange: item.exchange,
		type: item.type
	};
}


exports.search = function (searchString, type, exchange, maxRecords) {
	var MAX_SEARCH_RESULTS = !!maxRecords ? maxRecords : 50;
	var results = []; // array of WeightedItem { item, weight }
	var queryIsEmpty = !searchString || searchString.length === 0;
	var searchStringUpperCase = searchString.toUpperCase();

	for (var i = 0; i < symbols.length; ++i) {
		var item = symbols[i];

		if (type && type.length > 0 && item.type != type) {
			continue;
		}
		if (exchange && exchange.length > 0 && item.exchange != exchange) {
			continue;
		}

		var positionInName = item.name.toUpperCase().indexOf(searchStringUpperCase);
		var positionInDescription = item.description.toUpperCase().indexOf(searchStringUpperCase);

		if (queryIsEmpty || positionInName >= 0 || positionInDescription >= 0) {
			var found = false;
			for (var resultIndex = 0; resultIndex < results.length; resultIndex++) {
				if (results[resultIndex].item == item) {
					found = true;
					break;
				}
			}
			if (!found) {
				var weight = positionInName >= 0 ? positionInName : 8000 + positionInDescription;
				results.push({ item: item, weight: weight });
			}
		}
	}

	return results
		.sort(function (weightedItem1, weightedItem2) { return weightedItem1.weight - weightedItem2.weight; })
		.map(function (weightedItem) { return searchResultFromDatabaseItem(weightedItem.item); })
		.slice(0, Math.min(results.length, MAX_SEARCH_RESULTS));
};


exports.addSymbols = function(newSymbols) {
	symbols = symbols.concat(newSymbols);
};

exports.cacheSymbols = symbols;

exports.symbolInfo = function (symbolName) {

	var data = symbolName.split(':');
	var exchange = (data.length > 1 ? data[0] : "").toUpperCase();
	var symbol = (data.length > 1 ? data[1] : symbolName).toUpperCase();

	for (var i = 0; i < symbols.length; ++i) {
		var item = symbols[i];

		if (item.name.toUpperCase() == symbol && (exchange.length === 0 || exchange == item.exchange.toUpperCase())) {
			return item;
		}
	}

	return null;
};
