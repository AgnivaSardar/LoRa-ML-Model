"""
Distributed LoRa Mesh Node Process.
Communicates over UDP sockets. Handles packet forwarding, sequence tracking,
CRC checksum verification, telemetry generation, and route table updates from Hub.
"""

import sys
import os
import time
import json
import socket
import select
import argparse
import random
from typing import Dict, Any, Optional, Tuple

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

# ANSI Color Codes for terminal readability
COLOR_RESET = "\033[0m"
COLOR_BOLD = "\033[1m"
COLOR_GREEN = "\033[32m"
COLOR_CYAN = "\033[36m"
COLOR_YELLOW = "\033[33m"
COLOR_RED = "\033[31m"
COLOR_MAGENTA = "\033[35m"
COLOR_BLUE = "\033[34m"

# Default Node Port Map
NODE_PORTS = {
    "HUB": 9000,
    "A": 9001,
    "B": 9002,
    "C": 9003,
    "D": 9004,
    "E": 9005
}


def calculate_crc(payload: str) -> int:
    checksum = 0
    for char in payload.encode('utf-8'):
        checksum ^= char
    return checksum & 0xFFFF


class DistributedMeshNode:
    def __init__(self, node_id: str, host: str = "127.0.0.1", port: Optional[int] = None, hub_port: int = 9000):
        self.node_id = node_id.upper()
        self.host = host
        self.port = port if port is not None else NODE_PORTS.get(self.node_id, 9000 + ord(self.node_id) % 10)
        self.hub_port = hub_port
        
        # Socket setup
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        
        # Node operational state
        self.battery_pct = random.uniform(85.0, 100.0)
        self.temperature_c = random.uniform(25.0, 35.0)
        self.queue_pct = random.uniform(5.0, 20.0)
        
        # Default initial next-hop mapping
        initial_next_hops = {
            "A": "B",
            "B": "D",
            "C": "E",
            "D": "HUB",
            "E": "HUB"
        }
        
        self.routing_table: Dict[str, Any] = {
            "HUB": {
                "primary_next_hop": initial_next_hops.get(self.node_id, "HUB"),
                "backup_next_hop": "C" if self.node_id in ["A", "B"] else "E",
                "route_version": 1
            }
        }
        
        # Neighbor default metrics
        self.neighbor_metrics: Dict[str, Dict[str, float]] = {}
        self.received_sequence_history = set()
        self.last_telemetry_time = 0.0

    def log(self, message: str, color: str = COLOR_RESET):
        timestamp = time.strftime("%H:%M:%S")
        print(f"{COLOR_BOLD}[{timestamp}] [{self.node_id}:{self.port}]{COLOR_RESET} {color}{message}{COLOR_RESET}", flush=True)

    def send_udp(self, target_port: int, packet_dict: Dict[str, Any]):
        try:
            data = json.dumps(packet_dict).encode('utf-8')
            self.sock.sendto(data, (self.host, target_port))
        except Exception as e:
            self.log(f"UDP Send Error to port {target_port}: {e}", COLOR_RED)

    def send_telemetry(self):
        """Sends periodic operational telemetry to Central ML Hub."""
        telemetry_pkt = {
            "type": "TELEMETRY",
            "node_id": self.node_id,
            "port": self.port,
            "battery_pct": round(self.battery_pct, 1),
            "temperature_c": round(self.temperature_c, 1),
            "queue_pct": round(self.queue_pct, 1),
            "timestamp": time.time()
        }
        self.send_udp(self.hub_port, telemetry_pkt)

    def process_incoming_packet(self, data_bytes: bytes, addr: Tuple[str, int]):
        try:
            pkt = json.loads(data_bytes.decode('utf-8'))
        except Exception:
            self.log("Received malformed non-JSON frame", COLOR_RED)
            return

        pkt_type = pkt.get("type", "DATA")

        # 1. Route Table Update from Hub
        if pkt_type == "ROUTE_UPDATE":
            route_data = pkt.get("routing_table", {})
            self.routing_table.update(route_data)
            ver = pkt.get("route_version", 1)
            self.log(f"🗺️  Updated Routing Table (v{ver}): HUB -> Primary: {self.routing_table.get('HUB', {}).get('primary_next_hop')}, Backup: {self.routing_table.get('HUB', {}).get('backup_next_hop')}", COLOR_CYAN)
            return

        # 2. Control / Fault Injection Command
        if pkt_type == "FAULT_INJECTION":
            target = pkt.get("target_node")
            if target == self.node_id or target == "ALL":
                if "battery_pct" in pkt: self.battery_pct = pkt["battery_pct"]
                if "temperature_c" in pkt: self.temperature_c = pkt["temperature_c"]
                if "queue_pct" in pkt: self.queue_pct = pkt["queue_pct"]
                self.log(f"⚠️  HEALTH DEGRADATION INJECTED: Bat={self.battery_pct}%, Temp={self.temperature_c}°C, Queue={self.queue_pct}%", COLOR_YELLOW)
            return

        # 3. Data Packet Handling
        if pkt_type == "DATA":
            source = pkt.get("source_id")
            dest = pkt.get("destination_id")
            seq = pkt.get("sequence")
            payload = pkt.get("payload", "")
            crc = pkt.get("crc_check")

            self.log(f"📩 Received DATA Packet #{seq} from {source} (Dest: {dest})", COLOR_GREEN)

            # Check CRC Checksum
            expected_crc = calculate_crc(payload)
            if crc != expected_crc:
                self.log(f"❌ CRC Mismatch! Received {crc}, Expected {expected_crc}. Dropping packet #{seq}.", COLOR_RED)
                return

            # Check Duplicate Sequence
            if seq in self.received_sequence_history:
                self.log(f"⚠️  Duplicate sequence #{seq} detected. Dropping duplicate frame.", COLOR_YELLOW)
                return
            self.received_sequence_history.add(seq)

            # If THIS node is the destination (e.g. HUB)
            if dest == self.node_id:
                self.log(f"🎉 PACKET DELIVERED AT DESTINATION {self.node_id}! Payload: '{payload}'", COLOR_BOLD + COLOR_GREEN)
                ack_pkt = {
                    "type": "ACK",
                    "source_id": self.node_id,
                    "destination_id": source,
                    "sequence": seq,
                    "status": "DELIVERED"
                }
                sender_port = addr[1]
                self.send_udp(sender_port, ack_pkt)
                return

            # Forwarding logic for Intermediate Nodes
            route_info = self.routing_table.get(dest, {})
            primary_hop = route_info.get("primary_next_hop")
            backup_hop = route_info.get("backup_next_hop")

            next_hop = primary_hop if primary_hop else backup_hop
            if not next_hop or next_hop == self.node_id:
                self.log(f"❌ No valid next-hop route for destination {dest}. Dropping packet #{seq}.", COLOR_RED)
                return

            target_port = NODE_PORTS.get(next_hop)
            if not target_port:
                self.log(f"❌ Unknown port for next-hop node {next_hop}. Cannot forward.", COLOR_RED)
                return

            self.log(f"⏩ Forwarding Packet #{seq} to Next Hop -> Node {next_hop} (Port {target_port})", COLOR_MAGENTA)
            self.send_udp(target_port, pkt)

        elif pkt_type == "ACK":
            seq = pkt.get("sequence")
            src = pkt.get("source_id")
            self.log(f"✅ Received ACK for Packet #{seq} from Node {src}", COLOR_GREEN)

    def run(self):
        self.log(f"🚀 Node {self.node_id} Online on UDP Port {self.port}. ML Hub at Port {self.hub_port}.", COLOR_BOLD + COLOR_GREEN)
        self.send_telemetry()
        
        while True:
            try:
                # Check for incoming UDP socket data (timeout 0.2s)
                r, _, _ = select.select([self.sock], [], [], 0.2)
                if r:
                    data_bytes, addr = self.sock.recvfrom(4096)
                    self.process_incoming_packet(data_bytes, addr)

                # Periodic Telemetry (every 3 seconds)
                now = time.time()
                if now - self.last_telemetry_time >= 3.0:
                    self.send_telemetry()
                    self.last_telemetry_time = now

            except KeyboardInterrupt:
                self.log("Shutting down node cleanly...", COLOR_YELLOW)
                break
            except Exception as e:
                self.log(f"Node execution loop error: {e}", COLOR_RED)
                time.sleep(1.0)

        self.sock.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed LoRa Mesh Node")
    parser.add_argument("--node", type=str, required=True, help="Node ID (e.g. A, B, C, D, E, HUB)")
    parser.add_argument("--port", type=int, default=None, help="UDP listening port")
    parser.add_argument("--hub-port", type=int, default=9000, help="Central Hub UDP port")
    args = parser.parse_args()

    node = DistributedMeshNode(node_id=args.node, port=args.port, hub_port=args.hub_port)
    node.run()
