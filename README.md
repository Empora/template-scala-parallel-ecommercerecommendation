#Documentation for FashionFreax customized version on live server

## Installation from scratch

### Initialization

For installing the PredictionIO framework refer to

https://docs.prediction.io/templates/ecommercerecommendation/quickstart/

There can be found a really good step-by-step description for installation.
Follow the steps

1. Install and run PredictionIO
2. Create a new Engine from an Engine Template
3. Generate an App ID and Access Key

On live server, find in file /home/ubuntu/UPGRADE_HINTS.txt some more hints and configuration issues that take care of timeout issues and more

### Using the customized template

After having performed steps 1, 2 and 3 from above and all is running fine then
you have a folder with the specified name, e.g. MyApp. This folder contains amongst
others:

- folders: src, project, target, ...
- files: build.sbt, engine.json, ...

For using the customized template of this repository, copy the content of the repo's src folder
into the folder MyApp/src

Then run "pio build". If everything works fine, you are ready to go

## Remarks on the live server version of this template

### Location of template and crontab

- on the server, the live app's template can be found in the folder

	/home/ubuntu/FFXRecommender

- in FFXRecommender/EventLog the log files of training can be found
- training if performed every night, via crontab
- use "crontab -l" to display the crontab

### Display status of engine

- in browser, go to http://recommendation.fashionfreax.net:8000/, there you can see training start and end times of the model that is used 

- if the page doesn't load, the engine might be down, else, everything is fine, probably

### Check whether the training finished correctly or not

- go into folder FFXRecommender/EventLog
- there, files with names like train-deploy-18\_03\_2016\_02\_45\_01.log can be found, the log file for training at march 3
- if everything worked fine, i.e. the training and redeploy of the engine worked, then the last line of the log file reads

	Deploy ended with return value 0 at Fri Mar 18 03:53:12 UTC 2016

- if not, something bad happened, for example it could be that it reads

	Training ended with return value 137 at Wed Mar 16 03:19:12 UTC 2016
	
### If engine is not deployed

- in terminal, go to folder FFXRecommender and redeploy engine via

	sudo pio undeploy --port 8000
	sudo pio deploy --port 8000 -- --executor-memory 4g --driver-memory 4g
	
- maybe it will be good to restart predictionIO
	
	sudo pio undeploy --port 8000
	sudo pio-stop-all
	sudo pio-start-all
	sudo pio deploy --port 8000 -- --executor-memory 4g --driver-memory 4g
	
### If training failed

- train manually, if desired. While engine is deployed (and also if not deployed), you can train via
	
	sudo pio train -- --executor-memory 4g --driver-memory 4g

- after training finished (which takes about 60-70 minutes at the moment (18-03-2016)) redeploy engine
	
### Export all data that is used on live server for usage elsewhere

- for exporting data, go to FFXRecommender and do the following

	sudo pio export --appid 8 --output /home/ubuntu/FFXRecommender/ExportedData/jsonExport -- --executor-memory 4g --driver-memory 4g

- in folder FFXRecommender/ExportedData/jsonExport you find some files part-0000x that contain the data in json format that could be used for import to another app without the need the preprocess

- at the moment (18-03-2016), the folder with exported data will be of size ~10GB

# E-Commerce Recommendation Template
## (official documentation)

## Documentation

Please refer to http://docs.prediction.io/templates/ecommercerecommendation/quickstart/

## Versions

### v0.4.0

- Change from ALSAlgorithm.scala to ECommAlgorithm.scala

  * return popular bought items when no information is found for the user.
  * add "similarEvents" parameter for configuration what user-to-item events are used for finding similar items
  * re-structure the Algorithm code for easier customization and testing

- add some unit tests for testing code that may be customized

### v0.3.1

- use INVALID_APP_NAME as default appName in engine.json

### v0.3.0

- update for PredictionIO 0.9.2, including:

  - use new PEventStore and LEventStore API
  - use appName in DataSource and Algorithm parameters


### v0.2.0

- update build.sbt and template.json for PredictionIO 0.9.2

### v0.1.1

- update for PredictionIO 0.9.0

### v0.1.0

- initial version


## Development Notes

### import sample data

```
$ python data/import_eventserver.py --access_key <your_access_key>
```

### query

normal:

```
$ curl -H "Content-Type: application/json" \
-d '{
  "user" : "u1",
  "num" : 10 }' \
http://localhost:8000/queries.json \
-w %{time_connect}:%{time_starttransfer}:%{time_total}
```

```
$ curl -H "Content-Type: application/json" \
-d '{
  "user" : "u1",
  "num": 10,
  "categories" : ["c4", "c3"]
}' \
http://localhost:8000/queries.json \
-w %{time_connect}:%{time_starttransfer}:%{time_total}
```

```
curl -H "Content-Type: application/json" \
-d '{
  "user" : "u1",
  "num": 10,
  "whiteList": ["i21", "i26", "i40"]
}' \
http://localhost:8000/queries.json \
-w %{time_connect}:%{time_starttransfer}:%{time_total}
```

```
curl -H "Content-Type: application/json" \
-d '{
  "user" : "u1",
  "num": 10,
  "blackList": ["i21", "i26", "i40"]
}' \
http://localhost:8000/queries.json \
-w %{time_connect}:%{time_starttransfer}:%{time_total}
```

unknown user:

```
curl -H "Content-Type: application/json" \
-d '{
  "user" : "unk1",
  "num": 10}' \
http://localhost:8000/queries.json \
-w %{time_connect}:%{time_starttransfer}:%{time_total}
```

### handle new user

new user:

```
curl -H "Content-Type: application/json" \
-d '{
  "user" : "x1",
  "num": 10}' \
http://localhost:8000/queries.json \
-w %{time_connect}:%{time_starttransfer}:%{time_total}
```

import some view events and try to get recommendation for x1 again.

```
accessKey=<YOUR_ACCESS_KEY>
```

```
curl -i -X POST http://localhost:7070/events.json?accessKey=$accessKey \
-H "Content-Type: application/json" \
-d '{
  "event" : "view",
  "entityType" : "user"
  "entityId" : "x1",
  "targetEntityType" : "item",
  "targetEntityId" : "i2",
  "eventTime" : "2015-02-17T02:11:21.934Z"
}'

curl -i -X POST http://localhost:7070/events.json?accessKey=$accessKey \
-H "Content-Type: application/json" \
-d '{
  "event" : "view",
  "entityType" : "user"
  "entityId" : "x1",
  "targetEntityType" : "item",
  "targetEntityId" : "i3",
  "eventTime" : "2015-02-17T02:12:21.934Z"
}'

```

## handle unavailable items

Set the following items as unavailable (need to specify complete list each time when this list is changed):

```
curl -i -X POST http://localhost:7070/events.json?accessKey=$accessKey \
-H "Content-Type: application/json" \
-d '{
  "event" : "$set",
  "entityType" : "constraint"
  "entityId" : "unavailableItems",
  "properties" : {
    "items": ["i43", "i20", "i37", "i3", "i4", "i5"],
  }
  "eventTime" : "2015-02-17T02:11:21.934Z"
}'
```

Set empty list when no more items unavailable:

```
curl -i -X POST http://localhost:7070/events.json?accessKey=$accessKey \
-H "Content-Type: application/json" \
-d '{
  "event" : "$set",
  "entityType" : "constraint"
  "entityId" : "unavailableItems",
  "properties" : {
    "items": [],
  }
  "eventTime" : "2015-02-18T02:11:21.934Z"
}'
```
