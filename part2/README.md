# README
## Lets find our what each file does

#### archiver.py
this has the code needed to upload the messages into the gcp bucket which is for our use names as "activity_2". This file is imported into subscriber and the function inside this is used to load data into bucket "activity_2"

#### publisher.py
- this helps to get the data published to pub/sub

#### subscriber.py
- helps in getting the data pulled out of pub/sub

#### transform.py
- helps to transform our data after validation, we have two transformations here one is to calculate speed, other to insert values into empty fields.
  
#### asserttions.py
- This has all the validations we want our data to go through, we have defined 11 assertitions for which we validate our data, this file is used in subscriber to validate our vehicle data

#### clean_topic.py
- This has the major role of cleaning the topic each day so we dont get a redundant data. Every day we run this file to clean out our topic and get the new data published into the topic so that we can pull out the fresh data of the day
  
#### insertbreadcrumb.py
- as the name suggest this file helps to insert the breadcrumb data into breadcrumb table
  
####  insertTrip.py
- as the name suggest it adds the values into trip table in psql database
  
#### pipeline.sql
- This file is run once, it creates a table for trip and breadcrumb where we insert our data, for this project table remains
constant so this file is executed at very start of this project and then never touched

## Vms
### Vm1: (we named it publisher)
- ran the publisher here 

### Vm2: (we named it subscriber)
- ran the subscriber here 




