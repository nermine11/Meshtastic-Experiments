import meshtastic.serial_interface
import datetime
import time
import json
import sys
import signal
import os
from pubsub import pub
  # Variables initialization
encoding = 'utf-8'
interface = (meshtastic.serial_interface.SerialInterface())
sentLog = []
sentRadio = []
j = 0
pending = set()
os.makedirs("./data", exist_ok=True)

def send_packet():
  """ Send packets"""
  for i in range(1, nbPackets + 1):
    timestamp = str(datetime.datetime.now())
    packet = interface.sendText(text = "test" + str(i) + " "+ timestamp,
                              destinationId=destination,
                              wantAck=False
             )
    sentLog.append(
        {"i" : i,
          "destination": packet.to,
          "packet_id": hex(packet.id),
          "time" : timestamp,
          "hop_limit": packet.hop_limit,
          "payload" : str(packet.decoded.payload, encoding) if packet.decoded else None
          }
        )
    time.sleep(10) # to check how much to sleep

def save_logs():
  """ Save the logs"""
  with open('./data/sentPackets.json', 'w') as f:
      json.dump(sentLog, f,indent=4)
  with open('./data/sentRadio.json', 'w') as f:
      json.dump(sentRadio, f,indent=4)

def save_and_exit(signal, frame):
  """ Exit signal handler function"""
  save_logs()
  interface.close()
  sys.exit(0)

def onReceive(packet, interface) -> None:
  """Callback invoked when a packet arrives"""
  timestamp = str(datetime.datetime.now())
  global j
  if not packet["decoded"]:
    return
  if not packet["text"]:
    return
  if packet["decoded"]["text"].startswith("TX_DONE"):
    # We only care about "test" text packets
    j +=1
    sentRadio.append({"i" : j,
        "time" : timestamp,
        "payload" :  packet["decoded"]["text"] if packet["decoded"] else None
        }
      )

if(__name__ == "__main__"):
  nbPackets   = int(sys.argv[1])
  destination = sys.argv[2]
    # Set the signal hanglers
  signal.signal(signal.SIGINT, save_and_exit) # CTRL-C
  signal.signal(signal.SIGTERM , save_and_exit) # systemd stop
  pub.subscribe(onReceive, "meshtastic.receive.text")
  send_packet()
  # Wait 2 mins
  time.sleep(120)
  save_logs()
  interface.close()
  sys.exit(0)
