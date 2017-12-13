# MQTT client
- using mosquitto as MQTT broquer
- using paho as mqtt client for python
https://pypi.python.org/pypi/paho-mqtt
```bash
pip install paho-mqtt
```
# Mongodb client
```bash
pip install pymongo
```
# Websocket server
```bash
pip install websockets
```

# MQTT broquer
```bash
sudo apt-get install Mosquitto
```
config file: /etc/mosquitto/mosquitto.conf

/usr/share/doc/mosquitto/examples/mosquitto.conf.example

config should be placed in	/etc/mosquitto/conf.d/

##mosquitto service
sudo service mosquitto stop

##Test with command line
###Install
sudo apt-get install mosquitto-clients	
###Run
in one window
```bash
mosquitto_sub -t 'test/topic' -v
```
in another window
```bash
mosquitto_pub -t 'test/topic' -m 'hello world5' -h 10.0.0.2
```

