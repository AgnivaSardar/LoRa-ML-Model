"""
Central ML Routing Hub Process.
Listens on UDP Port 9000 for telemetry packets from all nodes.
Predicts link probabilities via Random Forest model, runs Dijkstra with hysteresis,
and broadcasts updated route tables to all active mesh nodes.
"""

import sys
import os
import time
import json
import socket
import select
import argparse
from typing import Dict, Any, Tuple

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

from src.routing import LoRaMeshRouter, DEFAULT_EDGES
from src.cli_node import NODE_PORTS, COLOR_RESET, COLOR_BOLD, COLOR_GREEN, COLOR_CYAN, COLOR_YELLOW, COLOR_RED, COLOR_MAGENTA, COLOR_BLUE

class CentralMLHubProcess:
    def __init__(self, host: str = "127.0.0.1", port: int = 9000):
        self.host = host
        self.port = port
        
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        
        self.router = LoRaMeshRouter()
        self.link_metrics: Dict[Tuple[str, str], Dict[str, float]] = {}
        self.node_states: Dict[str, Dict[str, float]] = {}
        
        self.current_route_version = 1
        self.last_broadcast_time = 0.0
        self.last_status_print_time = 0.0

    def log(self, message: str, color: str = COLOR_RESET):
        timestamp = time.strftime("%H:%M:%S")
        print(f"{COLOR_BOLD}[{timestamp}] [CENTRAL ML HUB:{self.port}]{COLOR_RESET} {color}{message}{COLOR_RESET}", flush=True)

    def broadcast_route_updates(self, primary_path: list, backup_path: list):
        """Broadcasts personalized next-hop routing tables to all nodes."""
        self.current_route_version += 1
        
        for node_id, node_port in NODE_PORTS.items():
            if node_id == "HUB":
                continue
                
            # Find next hop for this node along primary path
            primary_next = None
            if primary_path and node_id in primary_path:
                idx = primary_path.index(node_id)
                if idx < len(primary_path) - 1:
                    primary_next = primary_path[idx + 1]

            # Find next hop for this node along backup path
            backup_next = None
            if backup_path and node_id in backup_path:
                idx = backup_path.index(node_id)
                if idx < len(backup_path) - 1:
                    backup_next = backup_path[idx + 1]

            route_update_pkt = {
                "type": "ROUTE_UPDATE",
                "route_version": self.current_route_version,
                "routing_table": {
                    "HUB": {
                        "primary_next_hop": primary_next,
                        "backup_next_hop": backup_next,
                        "route_version": self.current_route_version
                    }
                }
            }

            try:
                data = json.dumps(route_update_pkt).encode('utf-8')
                self.sock.sendto(data, (self.host, node_port))
            except Exception as e:
                self.log(f"Failed to send route update to Node {node_id}:{node_port}: {e}", COLOR_RED)

        self.log(f"📡 Broadcasted Route Update v{self.current_route_version} to all nodes!", COLOR_CYAN + COLOR_BOLD)

    def process_telemetry_or_control(self, data_bytes: bytes, addr: Tuple[str, int]):
        try:
            pkt = json.loads(data_bytes.decode('utf-8'))
        except Exception:
            return

        pkt_type = pkt.get("type")

        if pkt_type == "TELEMETRY":
            node_id = pkt.get("node_id")
            if not node_id:
                return
                
            self.node_states[node_id] = {
                "battery_pct": pkt.get("battery_pct", 90.0),
                "temperature_c": pkt.get("temperature_c", 30.0),
                "queue_pct": pkt.get("queue_pct", 10.0),
                "last_seen": time.time()
            }
            
        elif pkt_type == "LINK_METRICS_OVERRIDE":
            u = pkt.get("u")
            v = pkt.get("v")
            if u and v:
                metrics = pkt.get("metrics", {})
                self.link_metrics[(u, v)] = metrics
                self.link_metrics[(v, u)] = metrics
                self.log(f"⚠️ Received Link Metric Degradation for Edge {u}->{v}", COLOR_YELLOW)

    def recompute_and_evaluate(self):
        """Reevaluates ML probabilities and updates routes."""
        self.router.update_graph_metrics(self.link_metrics, self.node_states)
        primary, backup, info = self.router.calculate_routes("A", "HUB")

        if info.get("route_switched", False):
            self.log(f"⚡ ROUTE SWITCH DETECTED BY ML HUB! New Primary Route: {' -> '.join(primary)}", COLOR_YELLOW + COLOR_BOLD)
            self.broadcast_route_updates(primary, backup)
            
        return primary, backup, info

    def print_status_dashboard(self, primary: list, backup: list, info: dict):
        self.log("=" * 65, COLOR_BLUE)
        self.log(f"📊 LIVE MESH TOPOLOGY & ML PREDICTION STATUS", COLOR_BOLD + COLOR_CYAN)
        self.log(f"  Active Primary Route : {' -> '.join(primary) if primary else 'NONE'}", COLOR_GREEN + COLOR_BOLD)
        self.log(f"  Active Backup Route  : {' -> '.join(backup) if backup else 'NONE'}", COLOR_BLUE)
        
        p_m = info.get("primary_metrics", {})
        if p_m:
            self.log(f"  End-to-End P(success): {p_m.get('p_e2e_reliability', 0.0)*100:.1f}% | Latency: {p_m.get('total_latency_ms', 0):.0f}ms | Hops: {p_m.get('hop_count')}", COLOR_CYAN)
            
        self.log("-" * 65, COLOR_BLUE)
        for u, v in DEFAULT_EDGES:
            p_s = self.router.graph[u][v].get("p_success", 0.9)
            cost = self.router.graph[u][v].get("weight", 0.0)
            status_color = COLOR_GREEN if p_s > 0.70 else (COLOR_YELLOW if p_s > 0.40 else COLOR_RED)
            self.log(f"  Link {u}->{v:3s} | ML P(success): {p_s*100:5.1f}% | Cost: {cost:6.4f}", status_color)
        self.log("=" * 65, COLOR_BLUE)

    def run(self):
        self.log(f"🚀 Central ML Routing Hub Listening on UDP Port {self.port}", COLOR_BOLD + COLOR_GREEN)
        
        # Broadcast initial startup routes
        primary, backup, info = self.recompute_and_evaluate()
        self.broadcast_route_updates(primary, backup)
        self.print_status_dashboard(primary, backup, info)
        self.last_status_print_time = time.time()

        while True:
            try:
                r, _, _ = select.select([self.sock], [], [], 0.3)
                if r:
                    data_bytes, addr = self.sock.recvfrom(4096)
                    self.process_telemetry_or_control(data_bytes, addr)

                primary, backup, info = self.recompute_and_evaluate()

                now = time.time()
                if now - self.last_status_print_time >= 5.0:
                    self.print_status_dashboard(primary, backup, info)
                    self.last_status_print_time = now

            except KeyboardInterrupt:
                self.log("Shutting down Central ML Hub...", COLOR_YELLOW)
                break
            except Exception as e:
                self.log(f"Hub loop error: {e}", COLOR_RED)
                time.sleep(1.0)

        self.sock.close()


if __name__ == "__main__":
    hub = CentralMLHubProcess()
    hub.run()
