import meshtastic.serial_interface
import datetime
from pubsub import pub
import experiment_pb2
from queue import Queue
from dataclasses import dataclass
import time
import json
import signal
import sys
import os
import random
# Variables initialization
PortNum_PRIVATE_APP             = 256
NB_NODES                        = 9
NODENUM_BROADCAST               = 0xffffffff
HOUR                            = 3600
STATS_INTERVAL                  = 900         #15 mins
interface                       = (meshtastic.serial_interface.SerialInterface ())
received_packets                = Queue() 
# Our node numbers
nodes = [
    1391039350, 1227105360, 3214103652, 1833769890, 
    2458335390, 871882989, 2446794159, 2057312131, 
    1507035365
]
# --------- stats structure ----------
@dataclass
class LinkStats:
    """ Stats between each two nodes Link ."""
    sent_dms:int              = 0
    received_dms:int          = 0
    received_broadcasts:int   = 0
    rtt_sum:int               = 0 
    rtt_count:int             = 0
sent_broadcasts = {}
network_stats   = {}

# initialize sent_broadcasts and network_stats
def initilize_stats():
    for A in nodes:
        network_stats[A]   = {}
        sent_broadcasts[A] = 0
        for B in nodes:
            if B == A:
                continue
            network_stats[A][B] = LinkStats()

def initialize_state():
    """ initalize the data structures of the collector node """
    global pkgen_config_responses 
    global received_stats
    global received_stats_cleared
    global stats_start_time
    global pkgen_start_time
    global state
    initialize_stats()
    pkgen_config_responses          = set()
    received_stats                  = set()
    received_stats_cleared          = set()
    state                           = "IDLE"
    stats_start_time                = time.monotonic()
    pkgen_start_time                = time.monotonic()
""" -----------Process incoming packets---------------"""
def packet_is_stats_response(packet):
    """ check if packet is a stats"""
    return packet["decoded"]["portnum"] == PortNum_PRIVATE_APP

def packet_is_clear_stats_response(packet):
    """ check if packet is a stats"""
    return packet["decoded"]["text"].startswith("clear ok")

def packet_is_pkgen_response(packet):
    """ check if packet is a stats"""
    return packet["decoded"]["text"].startswith("pkgen ok")

def packet_is_text(packet):
    """ check if packet is intended to another node so 
    we don't save save it in the queue
    """
    return packet["decoded"]["text"].startswith("packet")

def process_stats(packet):
    """ process stats and save them"""
    global received_stats
    # save the stat 
    received_stats.add(packet["fromId"])
    # Get the data
    stats = experiment_pb2.ExperimentStats()
    raw_data = packet["decoded"]["payload"]
    stats.ParseFromString(raw_data)
    # Save the data
    A = stats.sender_node
    sent_broadcasts[A] = stats.sentBroadcasts
    for(s in stats.stats):
        B = s.node_id
        network_stats[A][B].sent_dms = s.dm_sent
        network_stats[A][B].received_dms = s.dm_received
        network_stats[A][B].received_broadcasts = s.broadcasts_received
        network_stats[A][B].rtt_sum = s.rtt_sum
        network_stats[A][B].rtt_count = s.rtt_count

def process_clear_stats_response(packet):
    global received_stats_cleared
    """ add packet sender to received_stats_cleared set """
    received_stats_cleared.add(packet["fromId"])

def process_pkgen_response(packet):
    global pkgen_config_responses
    """ add packet sender to all_pkgen_config_responded set """
    pkgen_config_responses.add(packet["fromId"])

def process_packet(packet):
    """ processes the packets by calling the corresponding function"""
    if(packet_is_stats_response(packet)):
        process_stats(packet)
    elif(packet_is_clear_stats_response(packet)):
        process_clear_stats_response(packet)
    elif (packet_is_pkgen_response(packet)):
        process_pkgen_response(packet)

def process_all_packets():
    """ Process all packets currently in the queue """
    while not received_packets.empty():
        packet = received_packets.get()
        process_packet(packet)

def onReceive(packet, interface):
    """Callback invoked when a packet arrives"""
    if not(packet["decoded"]) or packet_is_text(packet):
        return
    received_packets.put(packet)

""" -----------Commands---------------"""
def stats_request(destination):
    """
    Sends a stats_request to all nodes
    Only send if all nodes responded to the pkgen_config_request 
    (all_pkgen_config_responded = true)
    """
    text = "stats"
    interface.sendText(text = text,
            destinationId=destination,
            wantAck=False)

def clear_stats(destination):
    """sends a clear_stats_request to all nodes
    Only send if all stats have been sent (all_stats_cleared = true )
    """
    text = "clear"
    interface.sendText(text = text,
            destinationId=destination,
            wantAck=False)

def pkgen_request():

    destination = random.choice(nodes) 
    duration    = random.randint(120000, 300000) # between 2 and 5 minutes
    interval    = random.randint(2000, 5000)     # between 2 and 5 seconds
    text     = 'config request: Dest: {}, burst test, {}, {}'.format(destination, duration, interval)
    interface.sendText(text = text,
            destinationId=NODENUM_BROADCAST,
            wantAck=False)

def check_pkgen_timeout()-> bool:
    current_time = time.monotonic()
    return current_time - pkgen_start_time >= HOUR

def check_stats_timeout()-> bool:
    current_time = time.monotonic()
    return current_time - stats_start_time >= STATS_INTERVAL

""" -----------Update states---------------"""
def update_state_machine():
    global state
    global pkgen_start_time
    global stats_start_time
    if state == "WAIT_CONFIG" and len(pkgen_config_responses) == NB_NODES:
        stats_request(NODENUM_BROADCAST)
        state = "WAIT_STATS"
    elif state == "WAIT_STATS" and len(received_stats) == NB_NODES:
        clear_stats(NODENUM_BROADCAST)
        state   = "WAIT_STATS_CLEARED"
    elif state == "WAIT_STATS_CLEARED" and len(received_stats_cleared) == NB_NODES:
        initialize_state()
        state   = "IDLE"
    elif state == "IDLE" and check_pkgen_timeout():
        pkgen_request()
        pkgen_start_time = time.monotonic()
        state = "WAIT_CONFIG"
    elif state == "IDLE" and check_stats_timeout():
        stats_request(NODENUM_BROADCAST)
        stats_start_time = time.monotonic()
        state = "WAIT_STATS"


if(__name__ == "__main__"):
    initialize_state()
    # When we receive a text, run onReceive
    pub.subscribe(onReceive, "meshtastic.receive.text")
    while True:
        process_all_packets()
        update_state_machine()


    

    
