import meshtastic.serial_interface
import datetime
from pubsub import pub
import time
import json
# Variables initialization
encoding = 'utf-8'
i = 0
interface = (
    meshtastic.serial_interface.SerialInterface ()
)
receivedPackets = []
def onReceive(packet, interface) -> None:
  """Callback invoked when a packet arrives"""
  timestamp = str(datetime.datetime.now())
  global i
  print(packet)
  #put timestamp here
  if str(packet["decoded"]["payload"], encoding).startswith("test"):
    # We only care about Test text packets
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
print("Listening... Press Ctrl-C to stop")
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\nStopped.")
    with open('data/receivedPackets.json', 'w') as f:
      json.dump(receivedPackets, f,indent=4)
    interface.close()
