import meshtastic.serial_interface
import datetime
from pubsub import pub
import time
import json
import signal
import sys
import os
# Variables initialization
encoding = 'utf-8'
i = 0
interface = (meshtastic.serial_interface.SerialInterface ())
receivedPackets = []
os.makedirs("./data", exist_ok=True)
def save_and_exit():
  """ Exit signal handler function"""
  print("\nStopped.")
  with open('data/receivedPackets.json', 'w') as f:
    json.dump(receivedPackets, f,indent=4)
  interface.close()
  sys.exit(0)
# Set the signal hanglers
signal.signal(signal.SIGINT, save_and_exit) # CTRL-C
signal.signal(signal.SIGTERM , save_and_exit) # systemd stop
def onReceive(packet, interface) -> None:
  """Callback invoked when a packet arrives"""
  timestamp = str(datetime.datetime.now())
  global i
  print(packet)
  if str(packet["decoded"]["payload"], encoding).startswith("test"):
    # We only care about "test" text packets
    i +=1
    receivedPackets.append({"i" : i,
        "destination": packet["to"],
        "origin": packet["from"],
        "packet_id": packet["id"],
        "time" : timestamp,
        "payload" :  str(packet["decoded"]["payload"], encoding) if packet["decoded"] else None #convert from byte string to character string
        }
      )
# When we receive a text, run onReceive
pub.subscribe(onReceive, "meshtastic.receive.text")
while True:
  time.sleep(1)
