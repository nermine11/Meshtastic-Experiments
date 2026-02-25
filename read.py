import serial
import os
import json

ser = serial.Serial(
    port='/dev/ttyACM0',
    baudrate=115200,
)
os.makedirs("./data", exist_ok=True)
print("Listening to collector...")
data = {}
collecting = False

while(True):
    line = ser.readline().decode().strip()
    print(line)
    if line == "DUMP_BEGIN":
        collecting = True
        continue
    elif line == "DUMP_END":
        with open('data/data.json', 'w') as f:
            json.dump(data, f,indent=4)
            print("saved json")
            break
    if(collecting):
        A, globalBroadcastPDR, B, dmPDR, broadcastPDR, RTT = line.split(",")
        #print(f"A={A}, B={B}, DM_PDR={dmPDR}, Broadcast_PDR={broadcastPDR}, RTT={RTT}")
        A = int(A)
        B = int(B)
        globalBroadcastPDR = float(globalBroadcastPDR)
        dmPDR = float(dmPDR)
        broadcastPDR = float(broadcastPDR)
        RTT = float(RTT)
        if(A not in data):
            data[A] = {
                "globalBroadcastPDR": globalBroadcastPDR,
                "links": []
            }
        data[A]["links"].append({
            "neighbor node": B,
            "DM PDR": dmPDR,
            "Broadcast PDR": broadcastPDR,
            "RTT": RTT
        })
ser.close()