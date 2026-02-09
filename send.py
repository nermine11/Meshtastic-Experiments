import meshtastic.serial_interface
import datetime
import time
import json
import sys
import os
if(__name__ == "__main__"):
  nbPackets   = int(sys.argv[1])
  destination = sys.argv[2]
  # Variables initialization
  os.makedirs("./data", exist_ok=True)
  encoding = 'utf-8'
  interface = (meshtastic.serial_interface.SerialInterface())
  sentLog = []
  # Sending loop
  for i in range(nbPackets):
    timestamp = str(datetime.datetime.now())
    packet = interface.sendText(text = "test" + str(i) + " "+ timestamp,
                              destinationId=destination,
                              wantAck=True
            )
    print(packet)
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
  time.sleep(1)
  with open('./data/sentPackets.json', 'w') as f:
      json.dump(sentLog, f,indent=4)
  interface.close()
