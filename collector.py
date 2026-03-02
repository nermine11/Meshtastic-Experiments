import meshtastic.serial_interface
from pubsub import pub
import experiment_pb2
from queue import Queue
from dataclasses import dataclass
import time
import json
import sys
import os
import random
from enum import Enum, auto

# Variables initialization
PortNum_PRIVATE_APP             = 256
NB_NODES                        = 9
NODENUM_BROADCAST               = 0xffffffff
HOUR                            = 3600
STATS_INTERVAL                  = 900         #15 mins
WAIT_PHASE_TIMEOUT              = 60          # safety timeout per state
MAX_RETRIES                     = 1
class State(Enum):
    IDLE                        = auto()  
    WAIT_CONFIG                 = auto()
    WAIT_STATS                  = auto()
    WAIT_STATS_CLEARED          = auto()

class Commands(Enum):
    PKGEN_CONFIG_REQUEST        = 0X1 
    STATS_REQUEST               = 0X2
    CLEAR_STATS_REQUEST         = 0X3

# --------- stats structure ----------
@dataclass
class LinkStats:
    """ Stats between each two nodes Link ."""
    sent_dms:int                = 0
    received_dms:int            = 0
    received_broadcasts:int     = 0
    rtt_sum:int                 = 0 
    rtt_count:int               = 0

# --------- Collector Controller ----------
class CollectorController():
    def __init__(self):
        self.nodes = {
         1227105360, 3214103652, 1833769890, 
        2458335390, 871882989, 2446794159}
        self.interface                      = meshtastic.serial_interface.SerialInterface()
        self.received_packets               = Queue()
        self.reset()
        pub.subscribe(self.on_receive, "meshtastic.receive.text")
    
    #----------initialization-----------------------
    def reset(self):
        self.state                           = State.IDLE
        self.pkgen_config_responsed_nodes    = set()
        self.stats_responded_nodes           = set()
        self.stats_cleared_nodes             = set()
        self.retries                         = 0
        self.pkgen_start_time                = time.monotonic()
        self.phase_start_time                = time.monotonic()
        self.stats_start_time               = time.monotonic()
        self.cleared_nodes                   = set(self.nodes)
        self.sent_broadcasts                 = {}
        self.network_stats                   = {}
        for A in self.nodes:
            self.network_stats[A]   = {}
            self.sent_broadcasts[A] = 0
            for B in self.nodes:
                if B == A:
                    continue
                self.network_stats[A][B] = LinkStats()
    """ -----------Process incoming packets---------------"""
    def on_receive(self, packet, interface):
        """Callback invoked when a packet arrives"""
        if not(packet["decoded"]) or self.packet_is_text(packet):
            return
        self.received_packets.put(packet)

    def packet_is_text(self, packet):
        """ check if packet is intended to another node so 
        we don't save save it in the queue
        """
        print("packet is text")
        text = packet["decoded"].get("text")
        if not text:
            return False
        else:
            return packet["decoded"]["text"].startswith("packet")

    def process_all_packets(self):
        """ Process all packets currently in the queue """
        while not self.received_packets.empty():
            packet = self.received_packets.get()
            self.process_packet(packet)

    def process_packet(self, packet):
        """ processes the packets by calling the corresponding function"""
        # we received stats_response
        portnum = packet["decoded"].get("portnum")
        text = packet["decoded"].get("text")
        if portnum and packet["decoded"]["portnum"] == PortNum_PRIVATE_APP:
            self.process_stats(packet)
        # we received stats_clear_response
        elif text and packet["decoded"]["text"].startswith("clear ok"):
            self.process_clear_stats_response(packet)
        # we received pkgen_config_response
        elif text and packet["decoded"]["text"].startswith("pkgen ok"): 
            self.process_pkgen_response(packet)

    def process_stats(self, packet):
        """ process stats and save them"""
        # Indicate that this node sent its stats
        self.stats_responded_nodes.add(packet["fromId"])
        # Get the data
        stats = experiment_pb2.ExperimentStats()
        raw_data = packet["decoded"]["payload"]
        stats.ParseFromString(raw_data)
        print(stats)
        # Save the data
        A = stats.sender_node
        # only handle stats of nodes that cleared their previous stats
        if A not in self.cleared_nodes:
            return
        self.sent_broadcasts[A] = stats.sentBroadcasts
        for s in stats.stats :
            B = s.node_id
            # only handle stats of nodes that cleared their previous stats
            if B not in self.cleared_nodes:
                continue
            if B in self.network_stats[A]:
                self.network_stats[A][B].sent_dms = s.dm_sent
                self.network_stats[A][B].received_dms = s.dm_received
                self.network_stats[A][B].received_broadcasts = s.broadcasts_received
                self.network_stats[A][B].rtt_sum = s.rtt_sum
                self.network_stats[A][B].rtt_count = s.rtt_count

    def process_clear_stats_response(self, packet):
        """ add packet sender to stats_cleared_nodes set """
        self.stats_cleared_nodes.add(packet["fromId"])

    def process_pkgen_response(self, packet):
        """ add packet sender to all_pkgen_config_responded set """
        self.pkgen_config_responsed_nodes.add(packet["fromId"])

    """ -----------Commands---------------"""
    def stats_request(self, destination):
        """Sends a stats_request to all nodes"""
        text = "stats"
        print("asked for stats")
        self.interface.sendText(text = text,
                destinationId=destination,
                wantAck=False)

    def clear_stats_request(self, destination):
        """ Sends a clear_stats_request to all nodes"""
        text = "clear"
        self.interface.sendText(text = text,
                destinationId=destination,
                wantAck=False)

    def pkgen_request(self):
        destination = random.choice(list(self.nodes)) 
        duration    = random.randint(120000, 300000) # between 2 and 5 minutes
        interval    = random.randint(2000, 5000)     # between 2 and 5 seconds
        text     = 'config request: Dest: {}, burst test, {}, {}'.format(destination, duration, interval)
        print(text)
        self.interface.sendText(text = text,
                destinationId=NODENUM_BROADCAST,
                wantAck=False)

    """ -----------Timers---------------"""
    def is_pkgen_request_due(self):
        """ check if it is time to send pkgen_config_request"""
        current_time = time.monotonic()
        return current_time - self.pkgen_start_time >= HOUR

    def is_stats_request_due(self):
        """ check if it is time to send stats_request"""
        current_time = time.monotonic()
        return current_time - self.stats_start_time >= STATS_INTERVAL

    def phase_timeout(self):
        """ check if it is time to stop waiting for responses from all nodes"""
        current_time = time.monotonic()
        return current_time - self.phase_start_time >= WAIT_PHASE_TIMEOUT

    """ -----------Update states---------------"""

    def pkgen(self, now):
        self.pkgen_request()
        self.pkgen_start_time = now
        self.phase_start_time = now   # How long we wait to hear a response from the nodes

    def ask_for_stats(self, now):
        self.stats_request(NODENUM_BROADCAST)
        self.phase_start_time = now   

    def clear_stats(self, now):
        self.clear_stats_request(NODENUM_BROADCAST)
        self.phase_start_time = now

    def handle_wait_config(self, now):
        self.stats_request(NODENUM_BROADCAST)
        self.phase_start_time = now

    def handle_wait_stats(self, now):
        self.clear_stats(now)
        self.retries = 0

    def handle_wait_stats_cleared(self):
        self.save_json()
        self.reset()
        self.retries = 0

    def mark_cleared(self, responding_nodes: set):
        self.cleared_nodes = responding_nodes

    def handle_idle_state(self, now):
        # time to ask for stats every 15 mins
        if self.is_stats_request_due():
            self.ask_for_stats(now)
            self.state = State.WAIT_STATS 
        # time to send pkgen every 1h
        elif self.is_pkgen_request_due():
            self.pkgen(now)
            self.state = State.WAIT_CONFIG

    def handle_wait_config_state(self, now):
        """ handles waiting for pkgen_config_responses
        If not all nodes responded within the timeout,
        Don't ask to send pkgen again
        move on with unfinished data
        """
        # all nodes answered, move to next state
        if self.pkgen_config_responsed_nodes == self.nodes:
            self.handle_wait_config(now)
            self.state = State.WAIT_STATS
            return
        if self.phase_timeout():
            self.handle_wait_config(now)
            self.state = State.WAIT_STATS
            return

    def handle_wait_stats_state(self, now):
        """ Wait for stats to be received from only the nodes that
            cleared their stats the previous round """
        # if all cleared nodes sent their stats
        if self.cleared_nodes.issubset(self.stats_responded_nodes):
            self.handle_wait_stats(now)
            self.state = State.WAIT_STATS_CLEARED
            return
        #Not all nodes answered
        #so retry again asking for stats 
        if self.phase_timeout() and self.retries < MAX_RETRIES:
            self.ask_for_stats(now)
            self.retries += 1
            return 
        #Move on with non finished data
        if self.retries >= MAX_RETRIES:
            self.handle_wait_stats(now)
            self.state = State.WAIT_STATS_CLEARED
            return

    def handle_wait_stats_cleared_state(self, now):
        if self.stats_cleared_nodes == self.nodes:
            self.handle_wait_stats_cleared()
            self.mark_cleared(self.stats_cleared_nodes)
            self.state = State.IDLE 
            return
        #Not all nodes answered
        #so retry again asking to clear stats 
        if self.phase_timeout() and self.retries < MAX_RETRIES:
            self.clear_stats(now)
            self.retries += 1
            return
        #Move on with non finished data
        #mark responding nodes as cleared
        if self.retries >= MAX_RETRIES :
            self.handle_wait_stats_cleared()
            self.mark_cleared(self.stats_cleared_nodes)
            self.state = State.IDLE
            return 

    def update_state_machine(self):
        """ update the states and call corresponding function """
        now = time.monotonic()
        # IDLE means not asking for stats or pkgen
        if self.state == State.IDLE:
            self.handle_idle_state(now)
        # waiting for pkgen_response from all nodes
        elif self.state == State.WAIT_CONFIG:
            self.handle_wait_config_state(now)
        # only wait for stats from previously cleared nodes
        elif self.state == State.WAIT_STATS:
            self.handle_wait_stats_state(now)
        # wait for confirmation that all nodes cleared their stats
        elif self.state == State.WAIT_STATS_CLEARED:
            self.handle_wait_stats_cleared_state(now)


    """ ----------JSON ---------------"""
    def save_json(self):
        return
   
    """ ----------MAIN loop---------------"""
    def run(self):
        while True:
            self.process_all_packets()
            self.update_state_machine()

""" ----------RUN---------------"""
if(__name__ == "__main__"):
    collector = CollectorController()
    collector.run()
    #collector.pkgen_request()
