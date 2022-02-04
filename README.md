# Project 3: Carolina Arriaga, Daniel Ryu, Jonathan Moges, Ziwei Zhao

## Understanding user behavior

The game company we work for has two events that we want to track: buy an item and join a guild. Each of them has metadata characteristic of such events.

## Our Business Questions

Given the influence the business question has on the design on the pipeline, we chose to begin our presentation with the focus on our business question. Our questions focused on the trends of user behavior and internet host providers. To have a more basic and affordable pipeline, we chose to use daily batch processing of our events. The rationale behind this is we would not expect strong deviations in host providers or user events from day to day.

## Pipeline description

Before describing our pipeline, those who wish to replicate the pipeline should do so by running the steps described in the terminal and not a Jupyter notebook.

1. **Sourcing:** Events are sourced to our system through the use of two python files "game_api_with_extended_json_events" and "event_generator" run through Flask. The game_api file produces two main logs to kafka - purchasing an item or joining a guild. These are the main two actions a user can take in the game. The event_generator takes the universe of use cases and serves as a proxy for daily user events. The file allows us the flexibility to create as many events as we desire. The events are as flat as possible to reduce the amount of data munging steps.

2. **Ingesting:** Events are ingested through kafka where we have created a topic called "events." Given the event structure and the fact that the "event_generator" file represents the universe of events, notifications were not needed and we only utilized one partition. The topic did not require further utilization of brokers.

3. **Storing:** Once all events have been created, we will read the associated kafka topic (events) and  use the "filtered_writes" python file to unroll the json files and write the data to a parquet file. This parquet file will be the starting point for the analysis needed to answer the business questions. 

4. **Analysis**: Analysis begins by utilizing pyspark, we also need to use NumPy, Pandas and SparkSQL to sufficiently prepare the data to answer the business questions.

# Creating a pipeline

A docker container will manage all the services required in each step of the process via Zookeeper.

To capture the information we want to analyze later, we propose the following data ingestion:

YML Requirement: Zookeeper, which is used as the coordination service for our distributed applications. We're exposing certain ports to enable connections with other nodes in the cluster and to enable other users to connect via port 2181. We also introduce an extra-hosts port ("moby:127.0.0.1") that represents another process that is running as a service

```YML
version: '2'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 32181
      ZOOKEEPER_TICK_TIME: 2000
    expose:
      - "2181"
      - "2888"
      - "32181"
      - "3888"
    extra_hosts:
      - "moby:127.0.0.1"
```
Cloudera will be used to run Hadoop, our distributed data store. This is where will store the parquet files before analysis. We set our ports to default for the name node and apache hue services. The latter is the web interface that supports Hadoop related data analysis. 
```YML
  cloudera:
    image: midsw205/cdh-minimal:latest
    expose:
      - "8020" # nn
      - "50070" # nn http
      - "8888" # hue
    #ports:
    #- "8888:8888"
    extra_hosts:
      - "moby:127.0.0.1"
```
Spark is our data processing tool. It's pupose is the take our code and turn it into multiple tasks that are executed via worker nodes.
```YML
  spark:
    image: midsw205/spark-python:0.0.5
    stdin_open: true
    tty: true
    volumes:
      - ~/w205:/w205
    expose:
      - "8888"
    ports:
      - "8888:8888"
    depends_on:
      - cloudera
    environment:
      HADOOP_NAMENODE: cloudera
    extra_hosts:
      - "moby:127.0.0.1"
    command: bash
```

The mids container is used as the connection to between the local files (volumes) services used and the extra host that can be accessible to other users. 
```YML
  mids:
    image: midsw205/base:0.1.9
    stdin_open: true
    tty: true
    volumes:
      - ~/w205:/w205
    expose:
      - "5000"
    ports:
      - "5000:5000"
    extra_hosts:
      - "moby:127.0.0.1"
```

### Instrument the API server to log events to Kafka

In this first step, the user interacts with our mobile app. The mobile app makes calls to the web service and the API server handles the requests: buy an item or join a guild. Then the request is logged into Kafka.

YML Requirement: kafka

- depends on Zookeeper
- gathers all logs existent
- exposes ip address to connect with live data

```YML
kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:32181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    expose:
      - "9092"
      - "29092"
    extra_hosts:
      - "moby:127.0.0.1"
```

## Create a topic in kafka

In docker we can create an event in kafka were all the events will be registered.

Using Zookeeper and Kafka we can test our API server logging events to Kafka.

Spin up the cluster:

```
docker-compose up -d
```

Create a topic **events**

```
docker-compose exec kafka \
   kafka-topics \
     --create \
     --topic events \
     --partitions 1 \
     --replication-factor 1 \
     --if-not-exists \
     --zookeeper zookeeper:32181
```
After running in the CL, it should show the topic events has been created.

## API POST requests

The game company must redirect the events to specific HTTPS (API endpoint) for example:

- POST/purchase/item_name
- POST/join_a_guild

Examples:

| Items | Event | API endpoint |\
| --- | --- | --- |\
| sword | Purchase | POST/purchase/sword |\
| shield | Purchase | POST/purchase/shield |\
| helmet | Purchase | POST/purchase/helmet |\
| knife | Purchase | POST/purchase/knife |\
| gauntlet | Purchase | POST/purchase/gauntlet |\
| NA | Join a guild | POST/ join_a_guild |

Similarly, we can use GET to retrieve specific call using

GET/join_a_guild

For our project, we will use the suggested endpoints.

# Logging events

We are proposing using Flask to route the event calls and log them.

```python
#!/usr/bin/env python
import json
from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')


def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())
    
@app.route("/purchase/<item_name>")
def purchase_a_sword(item_name):
    purchase_event = {'event_type': '{} purchased'.format(item_name)}
    log_to_kafka('events', purchase_event)
    return "{} Purchased!\n".format(item_name)

@app.route("/join_a_guild")
def join_guild():
    join_guild_event = {'event_type': 'join_guild'}
    log_to_kafka('events', join_guild_event)
    return "Joined guild.\n"
```

This file will be included in the same folder under as a python file under the name game_api_with_extended_json_events

### Run Flask
Running Flask will require you to use another terminal

```python
docker-compose exec mids env FLASK_APP=/w205/Project_3/carolina_daniel_jonathan_ziwei/game_api_with_extended_json_events.py flask run
```

# Testing the Pipeline

We've created a script (event_generator.py) that hits the flask app with various quantities and combinations of event types, with randomly generated host names. This generated data will go through the pipeline and analysis. 

```
pip install numpy
python event_generator.py n #will run n number of separate ab commands, each with an inverse-logarithmic number of iterations.
```

### Reading from Kafka

We can read all the logged events using from the "events" topic:

```
docker-compose exec mids bash -c "kafkacat -C -b kafka:29092 -t events -o beginning -e"
```

### Example of output

After using flask to capture data the API requests and processed them using Flaks this is how the log should look like this:

### Separating events and land in HDFS

filtered_writes.py parses and separates purchase events and join guild events into two separate files in HDFS, ready to be queried and analyzed.

#### python file that filters and writes events into files by type

```python
#!/usr/bin/env python
"""Extract events from kafka and write them to hdfs
"""
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf


@udf('boolean')
def is_purchase(event_as_json):
    event = json.loads(event_as_json)
    if 'purchase' in event['event_type']:
        return True
    return False

@udf('boolean')
def is_join_guild(event_as_json):
    event = json.loads(event_as_json)
    if event['event_type']=='join_guild':
        return True
    return False


def main():
    """main
    """
    spark = SparkSession \
        .builder \
        .appName("ExtractEventsJob") \
        .getOrCreate()

    raw_events = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "events") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    purchase_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .filter(is_purchase('raw'))

    extracted_purchase_events = purchase_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()
    extracted_purchase_events.printSchema()
    extracted_purchase_events.show()

    extracted_purchase_events \
        .write \
        .mode('overwrite') \
        .parquet('/tmp/purchases')

    join_events = raw_events \
        .select(raw_events.value.cast('string').alias('raw'),
                raw_events.timestamp.cast('string')) \
        .filter(is_join_guild('raw'))

    extracted_join_events = join_events \
        .rdd \
        .map(lambda r: Row(timestamp=r.timestamp, **json.loads(r.raw))) \
        .toDF()
    extracted_join_events.printSchema()
    extracted_join_events.show()

    extracted_join_events \
        .write \
        .mode('overwrite') \
        .parquet('/tmp/join_guild')


if __name__ == "__main__":
    main()
```

#### Run it 

```
docker-compose exec spark spark-submit /w205/Project_3/carolina_daniel_jonathan_ziwei/filtered_writes.py
```

Check output 
```
docker-compose exec cloudera hadoop fs -ls /tmp/
```

Should look like this, files join_guild and purchases were created
```
drwxrwxrwt   - mapred mapred              0 2018-02-06 18:27 /tmp/hadoop-yarn
drwx-wx-wx   - root   supergroup          0 2021-04-03 22:24 /tmp/hive
drwxr-xr-x   - root   supergroup          0 2021-04-05 00:09 /tmp/join_guild
drwxr-xr-x   - root   supergroup          0 2021-04-05 00:09 /tmp/purchases
```

These files are ready for query in the next step.

##### Using Pyspark to analyze the data

First, spin up a Pyspark cluster

```
docker-compose exec spark pyspark 
```

import the necessary tools

```python
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
```
transform the parquet file to spark dataframes

```python
purchases = spark.read.parquet('/tmp/purchases')
join = spark.read.parquet('/tmp/join_guild')
```
redefine the host provider list to answer the second business question. the host provider list represents the universe of internet service providers

```python
host_provider_list = ['comcast','att','google','frontier','cox','earthlink']
```

Our first business question is what are the most popular events across our user base?

```python
purchases.groupBy('event_type').count().sort(col("count").desc()).show()
```

We see that purchasing a knife is by far the most popular event, with 49 instances. The second is purchasing a helmet with 30 instances and purchasing a shield at 29 instances.

This insight allows us to improve gameplay by creating more knife varieties and using future data analysis to inform strategies to encourage an increase of user events around less popular event types (e.g. purchasing gauntlets)


To answer the next business question, we will need to transform the pyspark data frame to a Pandas data frame

```python
purchases_2 = purchases.toPandas()
```

Our next business question builds from the initial question and ask what internet host providers do our users utilize and what events are most popular on those hosts?
However, the internet host provider information needs to be parsed since we cannot clearly aggregate the data because there is a one to one relationship between users and host.

We are going to define a matcher function to parse through the variable "host_provider_list"

```python
def matcher(x): 
...     for i in host_provider_list:
...         if i in x:
...             return i
...     else:
...         return np.nan 
```

We will them apply the matcher function to our Pandas data frame and create a new column called "Host" to represent the parsed host name

```python
purchases_2['Host'] = purchases_2['Host'].apply(matcher)
```
We will then group the data frame by host names and event types and show the data frame

```python
purchases_3 = purchases_2.groupby(['Host', 'event_type']).count()[['Accept']]

purchases_3.rename(columns={"Accept":"Count"})
```

We can see our users mainly use cox and earthlink as their internet hosts, with both providers hosting 38 people. Additionally, we see that most of our knife purchases come from users hosted on earthlink and most of our helmets are purchased from users using cox. 

The importance of this discovery is that we can focus on developing relationships with local providers to understand any potential service issues that might impact gameplay. We can explore the option of prioritizing traffic from earthlink and cox since they represent a significant amount of our users. Further, we can observe host trends as a proxy of user behavior. For example, if most of our users move from earthlink to att since the service is more affordability, and att has reliability issues, we may see a decrease in user purchases. This external impact would mean we could save time and effort on understanding if this issue was platform driven.
