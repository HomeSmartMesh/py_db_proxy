import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import datetime
import logging as log
import cfg

# -------------------- mqtt events -------------------- 
def on_connect(lclient, userdata, flags, rc):
    log.info("mqtt connected with result code "+str(rc))
    lclient.subscribe("#")

def on_message(client, userdata, msg):
    topic_parts = msg.topic.split('/')
    try:
        if( (len(topic_parts) == 3) and (topic_parts[0] == "Nodes") ):
            nodeid = topic_parts[1]
            sensor = topic_parts[2]
            measurement = "node"+nodeid
            value = float(str(msg.payload))
            post = [
                {
                    "measurement": measurement,
                    "time": datetime.datetime.utcnow(),
                    "fields": {
                        sensor: value
                    }
                }
            ]
            clientDB.write_points(post)
            log.debug(msg.topic+" "+str(msg.payload)+" posted")
    except ValueError:
        log.error(" ValueError with : "+msg.topic+" "+str(msg.payload))



config = cfg.get_local_json("config.json")

# -------------------- logging -------------------- 
log.basicConfig(    filename=config["influxdb"]["logfile"],
                    level=log.INFO,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%d %H:%M:%S'
                    )
log.getLogger('').addHandler(log.StreamHandler())

log.info("influx client started @ :"+str(datetime.datetime.utcnow()))


# -------------------- influxDB client -------------------- 
clientDB = InfluxDBClient(    config["influxdb"]["host"], 
                            config["influxdb"]["port"], 
                            'root', 'root', 
                            config["influxdb"]["db"])



#clientDB.create_database(config["influxdb"]["db"])
#print("database created")
#clientDB.write_points(post)
#result = clientDB.query('select temperature from node15;')
#print("Query Result: {0}".format(result))

# -------------------- Mqtt Client -------------------- 
clientMQTT = mqtt.Client()
clientMQTT.on_connect = on_connect
clientMQTT.on_message = on_message

clientMQTT.connect(config["mqtt"]["host"], config["mqtt"]["port"], 3600)
clientMQTT.loop_forever()
