import matplotlib.pyplot as plt
import json
import numpy as np

# load data
with open('data/d1.json', 'r') as file:
    disaster = json.load(file)
with open('data/d2.json', 'r') as file:
    cycling = json.load(file)
    
#metrics calculations
def compute_average_dm_pdr(data):
    values = []
    for node in data.values():
        for link in node["links"]:
            values.append(link["DM PDR"])
    return np.mean (values)
def compute_average_broadcast_pdr(data):
    values = []
    for node in data.values():
        for link in node["links"]:
            values.append(link["Broadcast PDR"])
    return np.mean (values)
def compute_average_rtt(data):
    values = []
    for node in data.values():
        for link in node["links"]:
            values.append(link["RTT"])
    return np.mean(values)
def compute_average_global_broadcast_pdr(data):
    values = []
    for node in data.values():
            values.append(node["globalBroadcastPDR"])
    return np.mean(values)

# Metrics Calculations and printing
disasterDm = compute_average_dm_pdr(disaster)
cyclingDm  = compute_average_dm_pdr(cycling)

disasterBroadcast = compute_average_broadcast_pdr(disaster)
cyclingBroadcast  = compute_average_broadcast_pdr(cycling)

disasterGlobalBroadcast = compute_average_global_broadcast_pdr(disaster)
cyclingGlobalBroadcast = compute_average_global_broadcast_pdr(cycling)

disasterRtt = compute_average_rtt(disaster)
cyclingRtt  = compute_average_rtt(cycling)

# graph 1 : average dm pdr
plt.figure()
plt.bar(["disaster", "cycling"], [disasterDm, cyclingDm])
plt.ylabel("Average Dm PDR")
plt.title("Average DM PDR per Scenario")
plt.show()
# graph 2 : average broadcast pdr
plt.figure()
plt.bar(["disaster", "cycling"], [disasterBroadcast, cyclingBroadcast])
plt.ylabel("Average broadcast PDR")
plt.title("Average broadcast PDR per Scenario")
plt.show()
# graph 3 : average global broadcast PDR
plt.figure()
plt.bar(["disaster", "cycling"], [disasterGlobalBroadcast, cyclingGlobalBroadcast])
plt.ylabel("Average global broadcast PDR")
plt.title("Average global broadcast PDR per Scenario")
plt.show()
# graph 4 : average RTT
plt.figure()
plt.bar(["disaster", "cycling"], [disasterRtt, cyclingRtt])
plt.ylabel("Average RTT (ms)")
plt.title("Average RTT per Scenario")
plt.show()

# Metrics printing
print(f"Disaster DM_pdr{disasterDm}")
print(f"Cycling DM_pdr{cyclingDm}")
print(f"Disaster Broadcast_pdr{disasterBroadcast}")
print(f"Cycling Broadcast_pdr{cyclingBroadcast}")
print(f"Disaster Gloabal Broadcast_pdr{disasterGlobalBroadcast}")
print(f"Cycling Gloabal Broadcast_pdr{cyclingGlobalBroadcast}")
print(f"Disaster RTT{disasterRtt}")
print(f"Cycling RTT{cyclingRtt}")
