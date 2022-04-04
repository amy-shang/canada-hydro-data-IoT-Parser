### This is a Canadian Hydrometric  CSV Data parsing tool for Internet of Things Rest API
### Developed with node.js 
### Step-by-step guide 
#### Get the required dependency
    install node environment, then execute the following in command line
	npm install csv
	npm install path
	npm install async
	npm install request
	npm install later
	npm install csv-stream
	npm install node-schedule
#### Create Things 
	node create_things.js
	node create_datastream.js
#### Retrieve daily observation data (from http://dd.weather.gc.ca/hydrometric/csv/)and parse. Code will be run automatically at 8:30am everyday to retrieve data of previous day and upload to sensorThings service
	node daily_csv_parser.js
