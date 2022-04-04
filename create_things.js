var fs = require('fs');
var http = require('http');
var dir = './stations';
var csv = require('csv');
var path = require('path');
var async = require('async');
var request = require('request');
var reqURL = "http://dd.weather.gc.ca/hydrometric/doc/hydrometric_StationList.csv";
var url = "http://184.70.194.230/OGCSensorThings/v1.0/Things";
var headers = {
	'Content-Type': 'application/json'
};

if (!fs.existsSync(dir)) {
	fs.mkdirSync(dir);
}
var outputCSV = dir + "/stations.csv";
var file = fs.createWriteStream(outputCSV);
request.get(reqURL, function(error, response, body) {
	fs.writeFile(outputCSV, body, function(err) {
		if (err) {
			console.log(err);
		} else {
			console.log("saved files");
		}
	});
	if (!error && response.statusCode == 200) {
		parseCSV(body);
		//console.log(body) 
	}

});

function parseCSV(csvFile) {
	csv.parse(csvFile, function(err, data) {
		var all_stations = [];
		for (var i = 1; i < data.length; i++) {
			var station_Things = {};
			var vars = data[i];
			//console.log(data[1]);
			//if (vars[4] == "AB") {};
			var lat = parseFloat(vars[2]);
			var lon = parseFloat(vars[3]);
			station_Things.properties = {};
			station_Things.properties.stationID = vars[0];
			station_Things.description = vars[1];
			station_Things.Locations = [{}];
			station_Things.Locations[0]['encodingType'] = "application/vnd.geo+json";
			station_Things.Locations[0]['description'] = vars[1] + " Weather Station";
			station_Things.Locations[0]['location'] = {};
			station_Things.Locations[0]['location'].coordinates = [lon, lat];
			station_Things.Locations[0]['location'].type = "Point";
			all_stations.push(station_Things);
			
		};
		//all_stations = all_stations.slice(0, 2);

		console.log(all_stations.length);
		var funcs = {};
		all_stations.forEach(function(item) {
			var staID = item.properties.stationID;
			funcs[staID] = function(callback) {
				request.post({
					uri: url,
					headers: headers,
					body: JSON.stringify(item)
				}, function(e, r, body) {
				    try {
     				   	var resBody = JSON.parse(body);			   	
 				   		console.log(resBody);
						callback(null, resBody['id'])									
 					 } catch (e) {
    					callback(null, -1);
 					 }								
				});
			}
		});
		async.series(funcs,
			function(err, results) {
				if (err) {console.log(err)};
				var outputFilename = 'stationID_thingID_Dict.json';
				console.log(results);
				for (ids in results) {
					if (results[ids]==-1) {
						//console.log(ids);
						delete results[ids]
					};
				};
				fs.writeFile(outputFilename, JSON.stringify(results), function(err) {
					if (err) {
						console.log(err);
					} else {
						console.log("JSON saved to " + outputFilename);
					}
				});
			});
	});
}

