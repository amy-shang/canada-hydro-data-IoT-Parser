var fs = require('fs');
var http = require('http');
var csv = require('csv');
var path = require('path');
var util = require('util');
var async = require('async');
var request = require('request');
var headers = {
  'Content-Type': 'application/json'
};
var ObservedProperty = {
  "name": "Water Level",
  "description": "Water level or ga(u)ge height or stage is the elevation of the free surface of a stream, lake or reservoir relative to a specified datum.A water level is a device used for matching elevations of locations that are too far apart for a spirit level to span. The simplest water level is a section of clear tubing, partially filled with water. Water is easily procured for use, and easily discarded after use. The ends are held vertical, and the rest of the tubing lies on the ground or floor.",
  "definition": "http://dbpedia.org/page/Water_level"
};
var sensor = {  
  "encodingType": "http://schema.org/description",
    "metadata": "a Water Level Sensor used by a weather station.",
    "description": "a Water Level Sensor used by a weather station."
};

var propertyURL = "http://184.70.194.230/OGCSensorThings/v1.0/ObservedProperties";
var sensorURL = "http://184.70.194.230/OGCSensorThings/v1.0/Sensors";
var datastreamURL = "http://184.70.194.230/OGCSensorThings/v1.0/Datastreams";
var IDfile = "stationID_thingID_Dict.json";
var propertyID;
var sensorID;
var ThingID_list;
fs.readFile(IDfile, 'utf8', function(err, data) {
  if (err) throw err;
  var funcDict = {};
  ThingID_list = JSON.parse(data);
  //console.log("Thing and Station ID length is :" + ThingID_list.length)
    //console.log(ThingID_list);
  funcDict['propertyID'] = function(callback) {
    request.post({
      uri: propertyURL,
      headers: headers,
      body: JSON.stringify(ObservedProperty)
    }, function(e, r, body) {
      var resBody = JSON.parse(body);
      callback(null, resBody['id'])
    });
  }

  funcDict['sensorID'] = function(callback) {
    request.post({
      uri: sensorURL,
      headers: headers,
      body: JSON.stringify(sensor)
    }, function(e, r, body) {
      var resBody = JSON.parse(body);
      callback(null, resBody['id'])
    });
  }
  async.parallel(funcDict,
    function(err, results) {
      createDatastream(ThingID_list, results.propertyID, results.sensorID);
    });

})

function createDatastream(thingIDs, proID, senID) {
  var datastreams = {};
  Object.keys(thingIDs).forEach(function(key) {
    var staID = key;
    var thingID = thingIDs[staID];
    var datastream = {
      "unitOfMeasurement": {
        "name": "meter",
        "symbol": "m",
        "definition": "http://dbpedia.org/page/Metre"
      },
      "Thing": {
        "id": thingID
      },
      "description": "This is a datastream for measuring the water level.",
      "Sensor": {
        "id": senID
      },
      "ObservedProperty": {
        "id": proID
      },
      "observationType": "http://www.opengis.net/def/observationType/OGCOM/2.0/OM_Measurement"
    };
    datastreams[staID] = function(callback) {
      
          request.post({
            uri: datastreamURL,
            headers: headers,
            body: JSON.stringify(datastream)
          }, function(e, r, body) {
            try {
              var resBody = JSON.parse(body);
              console.log(resBody);
              callback(null, resBody['id'])
            } catch (e) {
              callback(null, -1);
            }
          });
     

  };
});
  //console.log(datastreams);
  async.series(datastreams,
    function(err, results) {
      if (err) {
        console.log(err)
      };
      console.log(results.length);
      for (ids in results) {
        if (results[ids] == -1) {
          //console.log(ids);
          delete results[ids]
        };
      };
      var output_streamID_StaID = "datastreamID_stationID_Dict.json";
      fs.writeFile(output_streamID_StaID, JSON.stringify(results), function(err) {
        if (err) {
          console.log(err);
        } else {
          console.log("JSON saved to " + output_streamID_StaID);
        }
      });
    });


}