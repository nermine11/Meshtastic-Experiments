import meshtastic.serial_interface
import datetime
import time
import json
import sys
if(__name__ == "__main__"):
  nbPackets   = int(sys.argv[1])
  destination = sys.argv[2]

  # Variables initialization
  encoding = 'utf-8'
  interface = (
      meshtastic.serial_interface.SerialInterface()
      )
  sentLog = []
  #nbPackets = 5
  #destination = "!49242450" #"^all"
  # Sending loop
  for i in range(nbPackets):
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
          "payload" : str(packet.decoded.payload, encoding) if packet.decoded else None
          }
        )
    time.sleep(2) # to check how much to sleep
  time.sleep(1)
  with open('./data/sentPackets.json', 'w') as f:
      json.dump(sentLog, f,indent=4)
  interface.close()
