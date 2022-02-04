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
