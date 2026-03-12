import json
import matplotlib.pyplot as plt
import sys
# -------------------------------------------------------
# Global constants
# -------------------------------------------------------
devices = [
    0x52e99376, 0x49242450, 0xbf935464,
    0x6d4d1ba2, 0x9287389e, 0x33f7e0ed,
    0x91d71daf, 0x7aa01783, 0x59d388e5,
    0x31c0c4f1
]
node_id_to_idx = {str(dev): i+1 for i, dev in enumerate(devices)}

# -------------------------------------------------------
# Global constants
# -------------------------------------------------------

def rtt_per_node(data):
    """Extract average RTT per node."""
    x, y, labels = [], [], []
    for node_id, node_info in data.items():
        links = node_info.get('links', [])
        valid_rtts = [link['rtt'] for link in links if link.get('rtt', 0) != 0]
        avg_rtt = sum(valid_rtts) / len(valid_rtts) if valid_rtts else 0
        x.append(node_id_to_idx[node_id])
        y.append(avg_rtt)
        labels.append(f"Node {node_id_to_idx[node_id]}")
    return _sort(x, y, labels)

def pdr_per_node(data):
    """Extract average DM PDR per node."""
    x, y, labels = [], [], []
    for node_id, node_info in data.items():
        links = node_info.get('links', [])
        pdr_vals = []
        for link in links:
            sent = link.get('num_pkgen_data_sent', 0)
            replies = link.get('num_pkgen_reply_received', 0)
            if sent > 0:
                pdr_vals.append(replies / sent)
        avg_pdr = sum(pdr_vals) / len(pdr_vals) if pdr_vals else 0.0
        x.append(node_id_to_idx[node_id])
        y.append(avg_pdr)
        labels.append(f"Node {node_id_to_idx[node_id]}")
    return _sort(x, y, labels)

def _sort(x, y, labels):
    """Sort by node index."""
    sorted_data = sorted(zip(x, y, labels))
    if sorted_data:
        x, y, labels = zip(*sorted_data)
        return list(x), list(y), list(labels)
    else:
        return [], [], []

def plot_metric(x, y, labels, ylabel, title, filename=None) -> None:
    """ create a plot"""
    plt.figure(figsize=(10, 6))
    plt.plot(x, y, marker='o', linestyle='-', linewidth=2, markersize=6)
    plt.xlabel('Node')
    plt.ylabel(ylabel)
    plt.title(title)
    plt.xticks(ticks=x, labels=labels, rotation=45)
    # Annotate each point with its value
    for xi, yi in zip(x, y):
        if ylabel.startswith('PDR'):
            plt.text(xi, yi, f'{yi*100:.1f}%', ha='center', va='bottom', fontsize=9)
        else:
            plt.text(xi, yi, f'{yi:.2f}', ha='center', va='bottom', fontsize=9)
    plt.tight_layout()
    if filename:
        plt.savefig(filename)
    plt.show()

if __name__ == "__main__":
    with open('data/stats.json') as f:
        data = json.load(f)
    if len(sys.argv) > 1:
        if sys.argv[1] == 'pdr':
            x, y, labels = pdr_per_node(data)
            plot_metric(x, y, labels, 'Average DM PDR', 'Average Packet Delivery Ratio per node')
        elif sys.argv[1] == 'rtt' :
            x, y, labels = rtt_per_node(data)
            plot_metric(x, y, labels, 'Average RTT (ms)', 'Average RTT per node')