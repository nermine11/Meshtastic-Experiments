import meshtastic.serial_interface
import datetime
from pubsub import pub
import time
import json
import signal
import sys
import os
# Variables initialization
i = 0
j= 0
interface = (meshtastic.serial_interface.SerialInterface ())
receivedPackets = []
receivedRadio = []
os.makedirs("./data", exist_ok=True)

def save_logs():
  """ Save the logs"""
  with open('data/receivedPackets.json', 'w') as f:
    json.dump(receivedPackets, f,indent=4)
  with open('data/receivedRadio.json', 'w') as f:
    json.dump(receivedRadio, f,indent=4)

def save_and_exit(signal, frame):
  """ Exit signal handler function"""
  # Wait 2 mins
  #time.sleep(120)
  save_logs()
  interface.close()
  sys.exit(0)

def onReceive(packet, interface) -> None:
  """Callback invoked when a packet arrives"""
  timestamp = str(datetime.datetime.now())
  global i
  global j
  if not packet["decoded"]:
    return
  if not packet["decoded"]["text"]:
    return
  if packet["decoded"]["text"].startswith("test"):
    # We only care about "test" text packets
    i +=1
    receivedPackets.append({"i" : i,
        "destination": packet["toId"],
        "origin": packet["fromId"],
        "packet_id": hex(packet["id"]),
        "time" : timestamp,
        "rx_snr": packet["rxSnr"],
        "rx_rssi": packet["rxRssi"],
        "hop_start": packet["hopStart"],
        "hop_limit": packet["hopLimit"],
        "payload" :  packet["decoded"]["text"] if packet["decoded"] else None #convert from byte string to character string
        }
      )
  elif packet["decoded"]["text"].startswith("RX_DONE"):
    # We only care about "test" text packets
    j +=1
    receivedRadio.append({"i" : j,
        "time" : timestamp,
        "payload" :  packet["decoded"]["text"] if packet["decoded"] else None
        }
      )

if(__name__ == "__main__"):
  # When we receive a text, run onReceive
  pub.subscribe(onReceive, "meshtastic.receive.text")
  # Set the signal hanglers
  signal.signal(signal.SIGINT, save_and_exit) # CTRL-C
  signal.signal(signal.SIGTERM , save_and_exit) # systemd stop
  while True:
    time.sleep(1)
