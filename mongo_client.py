import paho.mqtt.client as mqtt
import pymongo
from pymongo import MongoClient
import datetime
import logging as log
import cfg

# -------------------- mqtt events -------------------- 
def on_connect(lclient, userdata, flags, rc):
    log.info("mqtt connected with result code "+str(rc))
    #subscribing to nodes and actions
    lclient.subscribe("#")

def on_message(client, userdata, msg):
    topic_parts = msg.topic.split('/')
    try:
        value = float(str(msg.payload))
        if( (len(topic_parts) == 3) and (topic_parts[0] == "Nodes") ):
            nodeid = topic_parts[1]
            sensor = topic_parts[2]
            post = {
                "node": int(nodeid),
                sensor:value,
                "ts":datetime.datetime.utcnow()
            }
            post_id = sensors.insert_one(post).inserted_id
            log.debug(msg.topic+" "+str(msg.payload)+" posted @ "+str(post_id))
    except ValueError:
        log.error(" ValueError with : "+msg.topic+" "+str(msg.payload))
        

config = cfg.get_local_json("config.json")
# -------------------- logging -------------------- 
log.basicConfig(    filename=config["mongodb"]["logfile"],
                    level=log.INFO,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%d %H:%M:%S'
                    )
log.getLogger('').addHandler(log.StreamHandler())

log.info("mqtt_mongo client started @ :"+str(datetime.datetime.utcnow()))

# -------------------- Mongo Client -------------------- 
clientDB = MongoClient(config["mongodb"]["uri"])
db = clientDB[config["mongodb"]["db"]]
sensors = db.sensors

# -------------------- Mqtt Client -------------------- 
clientMQTT = mqtt.Client()
clientMQTT.on_connect = on_connect
clientMQTT.on_message = on_message

clientMQTT.connect(config["mqtt"]["host"], config["mqtt"]["port"], 3600)
clientMQTT.loop_forever()
