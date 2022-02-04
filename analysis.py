#!/usr/bin/env python
"""Transform parquet files and analyze user behavior
"""
#First, spin up a Pyspark cluster

#docker-compose exec spark pyspark

#import the necessary tools


import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf
from pyspark.sql.functions import col

#transform the parquet file to spark dataframes


purchases = spark.read.parquet('/tmp/purchases')
join = spark.read.parquet('/tmp/join_guild')

#redefine the host provider list to answer the second business question. the host provider list represents the universe of internet service providers


host_provider_list = ['comcast','att','google','frontier','cox','earthlink']


#Our first business question is what are the most popular events across our user base?


purchases.groupBy('event_type').count().sort(col("count").desc()).show()


#We see that purchasing a knife is by far the most popular event, with 49 instances. The second is purchasing a helmet with 30 instances and purchasing a shield at 29 instances.

#This insight allows us to improve gameplay by creating more knife varieties and using future data analysis to inform strategies to encourage an increase of user events around less popular event types (e.g. purchasing gauntlets)


#To answer the next business question, we will need to transform the pyspark data frame to a Pandas data frame

purchases_2 = purchases.toPandas()

#Our next business question builds from the initial question and ask what internet host providers do our users utilize and what events are most popular on those hosts?
#However, the internet host provider information needs to be parsed since we cannot clearly aggregate the data because there is a one to one relationship between users and host.

#We are going to define a matcher function to parse through the variable "host_provider_list"

def matcher(x):
    for i in host_provider_list:
        if i in x:
            return i
    else:
        return np.nan 

#We will them apply the matcher function to our Pandas data frame and create a new column called "Host" to represent the parsed host name

purchases_2['Host'] = purchases_2['Host'].apply(matcher)

#We will then group the data frame by host names and event types and show the data frame

purchases_3 = purchases_2.groupby(['Host', 'event_type']).count()[['Accept']]

purchases_3.rename(columns={"Accept":"Count"})

#We can see our users mainly use cox and earthlink as their internet hosts, with both providers hosting 38 people. Additionally, we see that most of our knife purchases come from users hosted on earthlink and most of our helmets are purchased from users using cox. 

#The importance of this discovery is that we can focus on developing relationships with local providers to understand any potential service issues that might impact gameplay. We can explore the option of prioritizing traffic from earthlink and cox since they represent a significant amount of our users. Further, we can observe host trends as a proxy of user behavior. For example, if most of our users move from earthlink to att since the service is more affordability, and att has reliability issues, we may see a decrease in user purchases. This external impact would mean we could save time and effort on understanding if this issue was platform driven.


