import meshtastic.serial_interface
import datetime
import time
import json
import sys
import signal
import os
  # Variables initialization
encoding = 'utf-8'
interface = (meshtastic.serial_interface.SerialInterface())
sentLog = []
pending = set()
os.makedirs("./data", exist_ok=True)
def save_and_exit(signal, frame):
  """ Exit signal handler function"""
  print("\nStopped.")
  with open('./data/sentPackets.json', 'w') as f:
      json.dump(sentLog, f,indent=4)
  interface.close()
  sys.exit(0)
# Set the signal hanglers
signal.signal(signal.SIGINT, save_and_exit) # CTRL-C
signal.signal(signal.SIGTERM , save_and_exit) # systemd stop
if(__name__ == "__main__"):
  nbPackets   = int(sys.argv[1])
  destination = sys.argv[2]
  # Sending loop
  for i in range(1, nbPackets + 1):
    timestamp = str(datetime.datetime.now())
    packet = interface.sendText(text = "test" + str(i) + " "+ timestamp,
                              destinationId=destination,
                              wantAck=True
             )
    sentLog.append(
        {"i" : i,
          "destination": packet.to,
          "packet_id": packet.id,
          "time" : timestamp,
          "hop_limit": packet.hop_limit,
          "payload" : str(packet.decoded.payload, encoding) if packet.decoded else None
          }
        )
    time.sleep(3) # to check how much to sleep
  #sleep 10 mins
  time.sleep(600)
  with open('./data/sentPackets.json', 'w') as f:
    json.dump(sentLog, f,indent=4)
  interface.close()

