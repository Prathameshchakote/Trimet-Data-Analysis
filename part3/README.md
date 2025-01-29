# Lets find out what every file does
## archiver.py
this has the code needed to upload the messages into the gcp bucket which is for our use named as "activity_2". This file is imported into subscriber and the function inside this is used to load data into bucket "activity_2"
## assertions_for_stop_event.py
this file as all the validations for the stop event data the, assertions are 5 in count 
## create_event_info.py
this file helps add data from the event stop to our table event_info in  the psql
## end_trip_pipeline.sql
this file has the sql queries used to create our psql table called event_info
## index.html
the UI for generating the map using .geojson files, it uses mapbox for it 
## publisher_stop_event.py
this file helps to publish indvidual rows in event stop data to the pub/sub
## server.py
this file helps to run the index.html to a predefined port 
## subscriber_Stop_event.py
this is a file that has code to pull data from pub sub, process it, transform it and then add it to psql table
## tsv_data_generator.py
helps to get the data out of psql table into a .tsv file
## tsv_to_geojson_generator_1.py
this heps in getting geojson data, we have two kind of .tsv files on that has 3 columns and rows containing data against each column, other has arrays of data i.e we have array for latitude, longitude and speed. So current file is used for visualization part 1, 2, 3,5b, and 5c. Here the data is simple and has no arrays.
## tsv_to_geojson_generator_2.py
used to convert .tsv file for visualization 4, and 5a which has data in form of sereral arrays into .tsv file

to run:
- run publisher_stop_event.py 
- run subscriber_for_stop_event.py (please make sure you change project_id, topic name and subscriber name inside the code)
- now to genrate .tsv file run the tsv_data_generator.py with the query of your choide
- depending on .tsv file please use either tsv_to_geojson_generator_1.py or tsv_to_geojson_generator_2.py to get the geojson dat a
- finally u can visualise geojson in gitub or by using index.html, then run server.py
