import matplotlib.pyplot as plt
import json
import numpy as np

with open('results.json', 'r') as file:
    results = json.load(file)

sent = [r["sent"] for r in results]
received = [r["received"] for r in results]
print(sent)
print(received)
xpoints = np.array(sent)
ypoints = np.array(received)
# Plotting
plt.scatter(xpoints, ypoints)
# annotate the points
for x, y in zip(sent, received):
    plt.annotate(
        f"({x}, {y})",
        (x, y),
        textcoords="offset points",
        xytext=(5, 5),
        fontsize=9
    )
plt.xlabel( 'Num. of packets sent')
plt.ylabel('Num. of packets received')
plt.title('PDR')
plt.grid(True)
plt.show()
