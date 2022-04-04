var http = require('http');
var fs = require('fs');
var request = require('request');
var csv = require('csv-stream');
var later = require('later');
var cron = require('node-schedule');
var async = require('async');
var obserURL = "http://184.70.194.230/OGCSensorThings/v1.0/Observations";
var canada_stations = ['AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'NT', 'NU', 'ON', 'PE', 'QC', 'SK', 'YT'];
//var reqURL = "http://dd.weather.gc.ca/hydrometric/csv/AB/daily/AB_daily_hydrometric.csv";
var urls = ['http://dd.weather.gc.ca/hydrometric/csv/AB/daily/AB_daily_hydrometric.csv',
	'http://dd.weather.gc.ca/hydrometric/csv/BC/daily/BC_daily_hydrometric.csv',
	'http://dd.weather.gc.ca/hydrometric/csv/MB/daily/MB_daily_hydrometric.csv',
	'http://dd.weather.gc.ca/hydrometric/csv/NB/daily/NB_daily_hydrometric.csv',
	'http://dd.weather.gc.ca/hydrometric/csv/NL/daily/NL_daily_hydrometric.csv',
	'http://dd.weather.gc.ca/hydrometric/csv/NS/daily/NS_daily_hydrometric.csv',
	'http://dd.weather.gc.ca/hydrometric/csv/NT/daily/NT_daily_hydrometric.csv',
	'http://dd.weather.gc.ca/hydrometric/csv/NU/daily/NU_daily_hydrometric.csv',
	'http://dd.weather.gc.ca/hydrometric/csv/ON/daily/ON_daily_hydrometric.csv',
	'http://dd.weather.gc.ca/hydrometric/csv/PE/daily/PE_daily_hydrometric.csv',
	'http://dd.weather.gc.ca/hydrometric/csv/QC/daily/QC_daily_hydrometric.csv',
	'http://dd.weather.gc.ca/hydrometric/csv/SK/daily/SK_daily_hydrometric.csv',
	'http://dd.weather.gc.ca/hydrometric/csv/YT/daily/YT_daily_hydrometric.csv',
];
var streamID_stationID = "datastreamID_stationID_Dict.json";
var headers = {
	'Content-Type': 'application/json'
};
//var start_time = new Date();
var runtimes = 0;
var rule = new cron.RecurrenceRule();
rule.dayOfWeek = [1, 2, 3, 4, 5, 6, 0];
rule.hour = 1;
rule.minute = 30;
cron.scheduleJob(rule, init);
var IDfile;

function init() {
	fs.readFile(streamID_stationID, 'utf8', function(err, IDdata) {
		IDfile = JSON.parse(IDdata);
	});
	async.series([

		function(callback) {
			csv_parser(urls[0], callback)
		},
		function(callback) {
			csv_parser(urls[1], callback)
		},
		function(callback) {
			csv_parser(urls[2], callback)
		},
		function(callback) {
			csv_parser(urls[3], callback)
		},
		function(callback) {
			csv_parser(urls[4], callback)
		},
		function(callback) {
			csv_parser(urls[5], callback)
		},
		function(callback) {
			csv_parser(urls[6], callback)
		},
		function(callback) {
			csv_parser(urls[7], callback)
		},
		function(callback) {
			csv_parser(urls[8], callback)
		},
		function(callback) {
			csv_parser(urls[9], callback)
		},
		function(callback) {
			csv_parser(urls[10], callback)
		},
		function(callback) {
			csv_parser(urls[11], callback)
		},
		function(callback) {
			csv_parser(urls[12], callback)
		},
		function(callback) {
			callback(null, "data process finished this time..")
		},
	], function(error, result) {
		console.timeEnd("process-time");
		console.log(result);
	});
};


function csv_parser(url, callback) {

	var options = {
		delimiter: '\t',
		endLine: '\n',
		columns: ['ID', 'Date', 'Water Level']
	};
	var csvStream = csv.createStream(options);
	var r = request(url).pipe(csvStream);
	var obser_data = [];
	var totalData = '';
	var province_name = url.split("/")[5];

	r.on('error', function(err) {
		console.error(err);
	});

	r.on('data', function(data) {
		console.time('process-time');
		totalData += data;
		console.log(totalData.length + "data has been received");
		console.log("streaming data ...");
		var true_data = data.ID.split(',').splice(0, 3);
		var date1 = new Date(true_data[1]);
		date1.setUTCHours(0, 0, 0, 0);
		var date2 = new Date(new Date().setDate(new Date().getDate() - 1));
		date2.setUTCHours(0, 0, 0, 0);

		if (date1.getTime() == date2.getTime()) {
			var stationID = true_data[0];
			var time = new Date(true_data[1]);
			var waterLevel = true_data[2];
			if (IDfile.hasOwnProperty(stationID)) {
				var observation = {};
				observation.Datastream = {};
				observation.Datastream.id = IDfile[stationID];
				observation.result = waterLevel;
				observation.phenomenonTime = time;
				obser_data.push(observation);
			};
		}
	});

	r.on('end', function() {
		var postFuncList = [];
		obser_data.forEach(function(item){
			postFuncList.push(function(sub_callback) {
				request.post({
					uri: obserURL,
					headers: headers,
					body: JSON.stringify(item)
				}, function(e, r, body) {					
					console.log(body);
					sub_callback(null, null);
				});
			})
		});
		
		async.series(postFuncList, function(error, result) {
			//var date = new Date();
			var output_observation = province_name + "observations.json";
			fs.writeFile(output_observation, JSON.stringify(obser_data), function(err) {
			if (err) {
				console.log(err);
			} else {
				console.log("JSON saved to " + output_observation);

			}
		});
			callback(null, null);
		});
		

		
		
	})

}