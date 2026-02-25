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
#build matrix
def build_heatmap_matrix(data, metricName):
    # sort the nodes
    nodes = list(data.keys())
    nodesIds = sorted(nodes, key=int)
    # create empty matrix
    size = len(nodesIds)
    matrix = np.zeros((size, size))
    # map node ID -> Matrix Index
    nodeIndex = {}
    i = 0
    for nodeId in nodesIds:
        nodeIndex[nodeId] = i
        i += 1
    # fill the matrix
    for nodeId, node in data.items():
        i = nodeIndex[nodeId]
        for link in node["links"]:
            neighbor = str(link["neighbor node"])
            j = nodeIndex[neighbor]
            matrix[i][j] = link[str(metricName)]
    # create labels for the node: Node 1, Node 2
    labels = [f"Node {i+1}" for i in range(size)]
    return matrix, labels

# Metrics Calculations
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
plt.ylabel("Average RTT")
plt.title("Average RTT per Scenario")
plt.show()

#heatmap for DMs

matrixDisaster, labels = build_heatmap_matrix(disaster,"DM PDR" )
plt.figure()
plt.imshow(matrixDisaster)
plt.xticks(range(len(labels)), labels, rotation=90)
plt.yticks(range(len(labels)), labels)
plt.title(" DM heatmap for disaster scenario")
plt.colorbar(label = "Broadcast PDR")
plt.show()
