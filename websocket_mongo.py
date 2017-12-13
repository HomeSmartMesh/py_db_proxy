import asyncio
import websockets
import json
import signal
import sys
import mongo_handler as mdb

def signal_handler(signal, frame):
        print('You pressed Ctrl+C!')
        sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)
print('Press Ctrl+C')



async def request_handler(websocket, path):
    async for message in websocket:
        if(path == "/measures"):
            request = json.loads(message)
            r_type = request["request"]["type"]
            if(r_type == "update"):
                print("update")
            elif(r_type == "nodesinfo"):
                response = mdb.get_nodesinfo()
            elif(r_type == "kill"):
                print("kill")
                sys.exit(0)
            else:
                print("unsupported command: ", r_type)
        response = {
            "id": 1,
            "complete":{
                "14":{
                    "temperature":{
                        "Times":[0],
                        "Values":[33]
                    }
                }
            }
        }
        await websocket.send(message)

config = json.load(open('config.json'))
print("Entering webscoket loop")
asyncio.get_event_loop().run_until_complete(
    websockets.serve(request_handler,config["websocket"]["url"],config["websocket"]["port"]))
asyncio.get_event_loop().run_forever()
