import matplotlib.pyplot as plt
import json
import os
results = []
root_path = '/home/nelkilan/Carthago/Meshtastic_tests/tests'
for dirpath, dirnames, filenames in os.walk(root_path):
    for dirname in dirnames:
        path = os.path.join(dirpath, dirname)
        print(path)
        for dirpath, dirnames, filenames in os.walk(path):
            for filename in filenames:
                if filename.startswith("sentPacket"):
                    with open(filename, 'r') as file:
                        sentPackets = json.load(file)
                    # eliminate duplicate packet using packet ID
                    sentPacketsIds = set(p["packet_id"] for p in sentPackets)
                    # packets count
                    sentCount = len(sentPacketsIds)
                if filename.startswith("receivedPacket"):
                    with open(filename, 'r') as file:
                        receivedPackets = json.load(file)
                    # eliminate duplicate packet using packet ID
                    receivedPacketsIds = set(p["packet_id"] for p in receivedPackets)
                    # packets count
                    receivedCount = len(receivedPacketsIds)
        results.append({
            "sent": sentCount,
            "received": receivedCount
            }
        )
with open('data/results.json', 'w') as f:
    json.dump(results, f,indent=4)
