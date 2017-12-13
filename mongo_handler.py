import paho.mqtt.client as mqtt
import pymongo
from pymongo import MongoClient
import datetime
import json

def get_nodesinfo():
    print("getting the nodesinfo")
    return "nodes_info"


def on_connect(lclient, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    lclient.subscribe("#")

def on_message(client, userdata, msg):
    topic_parts = msg.topic.split('/')
    if( (len(topic_parts) == 3) and (topic_parts[0] == "Nodes") ):
        nodeid = topic_parts[1]
        sensor = topic_parts[2]
        post = {
            "node": int(nodeid),
            sensor:float(msg.payload),
            "ts":datetime.datetime.utcnow()
        }
        #TODO update the webscoket with this post
        print(msg.topic+" "+str(msg.payload)+" to be broadcasted ")
        

config = json.load(open('config.json'))

# -------------------- Mongo Client -------------------- 
client = MongoClient(config["mongodb"]["uri"])
db = client[config["mongodb"]["db"]]
sensors = db.sensors

# -------------------- Mqtt Client -------------------- 
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect( config["mqtt"]["host"], config["mqtt"]["port"], 
                config["mqtt"]["keepalive"])
client.loop_forever()

