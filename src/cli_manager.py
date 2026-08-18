"""
Unified Multi-Node Manager with State-Transition Central Hub Logging,
Continuous Physics Noise Engine, and Fixed 6-Node Mesh Topology.
"""

import sys
import os
import time
import json
import socket
import select
import threading
import random
import math
from typing import Dict, List, Tuple, Any, Optional
from collections import deque

from src.routing import LoRaMeshRouter, NODE_GPS_COORDINATES, calculate_distance_km, haversine_distance_km
from src.cli_node import NODE_PORTS, calculate_crc
from src.train_model import predict_link_success_probability, load_trained_model

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')


class MeshNetworkManager:
    def __init__(self):
        self.log_buffers: Dict[str, deque] = {
            "HUB": deque(maxlen=80),
            "A": deque(maxlen=80),
            "B": deque(maxlen=80),
            "C": deque(maxlen=80),
            "D": deque(maxlen=80),
            "E": deque(maxlen=80),
            "CENTRAL_HUB": deque(maxlen=80)
        }
        
        self.node_gps: Dict[str, Tuple[float, float]] = dict(NODE_GPS_COORDINATES)
        self.node_radii: Dict[str, float] = {n: 2.8 for n in ["A", "B", "C", "D", "E", "HUB"]}
        
        self.node_metrics: Dict[str, Dict[str, Any]] = {
            "A": {"battery_pct": 100.0, "temperature_c": 25.0, "queue_pct": 10.0, "primary_next": "B", "backup_next": "C"},
            "B": {"battery_pct": 95.0, "temperature_c": 28.0, "queue_pct": 12.0, "primary_next": "D", "backup_next": "C"},
            "C": {"battery_pct": 92.0, "temperature_c": 26.0, "queue_pct": 8.0, "primary_next": "E", "backup_next": "E"},
            "D": {"battery_pct": 98.0, "temperature_c": 30.0, "queue_pct": 15.0, "primary_next": "HUB", "backup_next": "HUB"},
            "E": {"battery_pct": 90.0, "temperature_c": 27.0, "queue_pct": 10.0, "primary_next": "HUB", "backup_next": "HUB"},
            "HUB": {"battery_pct": 100.0, "temperature_c": 32.0, "queue_pct": 5.0, "primary_next": "NONE", "backup_next": "NONE"}
        }

        self.active_node_ids: List[str] = ["A", "B", "C", "D", "E", "HUB"]

        self.link_baseline_features: Dict[Tuple[str, str], Dict[str, float]] = {}
        self.link_current_noisy_features: Dict[Tuple[str, str], Dict[str, float]] = {}
        self.init_default_link_features()

        self.sequence_counter = 1100
        self.model_artifact = load_trained_model()
        self.router = LoRaMeshRouter(model_artifact=self.model_artifact)
        
        self.sockets: Dict[str, socket.socket] = {}
        self.threads: Dict[str, threading.Thread] = {}
        self.running = True
        
        self.topology_status = "HEALTHY"
        self.last_logged_topology_status = "HEALTHY"
        self.is_partitioned = False
        self.is_live_mode = False  # Static / Manual mode by default

        self.log("CENTRAL_HUB", "[SYS] Central ML Routing Engine Active (Mode: STATIC MANUAL)", "SYSTEM")
        self.start_node_sockets()
        self.rebuild_proximity_mesh_topology()
        self.reevaluate_ml_routes()

        self.noise_thread = threading.Thread(target=self._continuous_physics_noise_loop, daemon=True)
        self.noise_thread.start()

    def set_live_mode(self, enabled: bool) -> bool:
        self.is_live_mode = enabled
        mode_str = "LIVE REAL-TIME DYNAMIC CPS NOISE" if enabled else "STATIC MANUAL (FROZEN)"
        self.log("CENTRAL_HUB", f"[MODE] Simulation Engine Switched to: {mode_str}", "SYSTEM")
        if not enabled:
            # Sync noisy features back to baseline in static mode
            for k, v in self.link_baseline_features.items():
                self.link_current_noisy_features[k] = dict(v)
            self.reevaluate_ml_routes()
        return self.is_live_mode

    def init_default_link_features(self):
        """Initializes 100% healthy default features for ALL links at startup."""
        self.link_baseline_features.clear()
        self.link_current_noisy_features.clear()

        default_healthy = {
            "rssi": -72.0, "snr": 12.5, "pdr": 0.99, "latency_ms": 75.0,
            "retries": 0, "etx": 1.01, "queue_pct": 8.0, "battery_pct": 95.0,
            "temperature_c": 28.0, "time_on_air_s": 0.12, "spreading_factor": 8,
            "bandwidth_khz": 125.0
        }

        for i in range(len(self.active_node_ids)):
            for j in range(i + 1, len(self.active_node_ids)):
                u, v = self.active_node_ids[i], self.active_node_ids[j]
                lat1, lon1 = self.node_gps.get(u, (12.9716, 79.1588))
                lat2, lon2 = self.node_gps.get(v, (12.9740, 79.2180))
                dist = haversine_distance_km(lat1, lon1, lat2, lon2)

                feat = dict(default_healthy)
                feat["distance_km"] = dist
                feat["latency_ms"] = round(70.0 + dist * 10.0, 1)
                
                self.link_baseline_features[(u, v)] = dict(feat)
                self.link_baseline_features[(v, u)] = dict(feat)
                self.link_current_noisy_features[(u, v)] = dict(feat)
                self.link_current_noisy_features[(v, u)] = dict(feat)

    def _continuous_physics_noise_loop(self):
        while self.running:
            try:
                if not self.is_live_mode:
                    time.sleep(0.4)
                    continue

                self.rebuild_proximity_mesh_topology()

                for (u, v), base in self.link_baseline_features.items():
                    lat1, lon1 = self.node_gps.get(u, (12.9716, 79.1588))
                    lat2, lon2 = self.node_gps.get(v, (12.9740, 79.2180))
                    dist = haversine_distance_km(lat1, lon1, lat2, lon2)
                    r_u = self.node_radii.get(u, 2.8)
                    r_v = self.node_radii.get(v, 2.8)

                    in_range = dist <= (r_u + r_v)

                    if not in_range:
                        noisy = {
                            "rssi": -130.0, "snr": -25.0, "pdr": 0.0, "latency_ms": 2500.0,
                            "retries": 7, "etx": 10.0, "queue_pct": 100.0, "battery_pct": 0.0,
                            "temperature_c": 80.0, "time_on_air_s": 1.8, "spreading_factor": 12,
                            "bandwidth_khz": 125.0, "distance_km": dist
                        }
                    else:
                        rssi_noise = random.gauss(0, 0.4)
                        snr_noise = random.gauss(0, 0.2)
                        pdr_noise = random.uniform(-0.005, 0.005)
                        
                        noisy = dict(base)
                        noisy["distance_km"] = dist
                        noisy["rssi"] = round(base["rssi"] + rssi_noise, 1)
                        noisy["snr"] = round(base["snr"] + snr_noise, 1)
                        pdr_val = max(0.0, min(1.0, base["pdr"] + pdr_noise))
                        noisy["pdr"] = round(pdr_val, 3)
                        noisy["etx"] = round(1.0 / max(pdr_val, 0.01), 2)

                        src_data = self.node_metrics.get(u, {})
                        dest_data = self.node_metrics.get(v, {})
                        if src_data.get("battery_pct", 95.0) <= 0.0 or dest_data.get("battery_pct", 95.0) <= 0.0:
                            noisy["battery_pct"] = 0.0
                            noisy["pdr"] = 0.0
                            noisy["rssi"] = -130.0
                            noisy["snr"] = -25.0
                            noisy["retries"] = 7
                            noisy["etx"] = 10.0
                        else:
                            noisy["battery_pct"] = round(max(0.0, dest_data.get("battery_pct", 95.0) + random.uniform(-0.05, 0.05)), 1)
                            noisy["temperature_c"] = round(dest_data.get("temperature_c", 28.0) + random.uniform(-0.1, 0.1), 1)

                    self.link_current_noisy_features[(u, v)] = noisy

                self.reevaluate_ml_routes()
                # 3-5 second responsive pacing in live mode
                time.sleep(3.5)

            except Exception:
                time.sleep(1.0)

    def rebuild_proximity_mesh_topology(self):
        G = self.router.graph
        
        for i in range(len(self.active_node_ids)):
            for j in range(i + 1, len(self.active_node_ids)):
                u = self.active_node_ids[i]
                v = self.active_node_ids[j]

                lat1, lon1 = self.node_gps.get(u, (12.9716, 79.1588))
                lat2, lon2 = self.node_gps.get(v, (12.9740, 79.2180))
                dist = haversine_distance_km(lat1, lon1, lat2, lon2)

                r_u = self.node_radii.get(u, 2.8)
                r_v = self.node_radii.get(v, 2.8)

                if dist <= (r_u + r_v):
                    if not G.has_edge(u, v):
                        G.add_edge(u, v, distance_km=dist)
                        G.add_edge(v, u, distance_km=dist)
                else:
                    if G.has_edge(u, v):
                        G.remove_edge(u, v)
                    if G.has_edge(v, u):
                        G.remove_edge(v, u)

    def update_node_position(self, node_id: str, lat: float, lon: float):
        if node_id in self.node_gps:
            self.node_gps[node_id] = (lat, lon)
            self.rebuild_proximity_mesh_topology()
            self.reevaluate_ml_routes()

    def update_node_radius(self, node_id: str, radius_km: float):
        if node_id == "ALL":
            for n in self.node_radii:
                self.node_radii[n] = radius_km
        elif node_id in self.node_radii:
            self.node_radii[node_id] = radius_km
        self.rebuild_proximity_mesh_topology()
        self.reevaluate_ml_routes()

    def log(self, target: str, message: str, level: str = "INFO"):
        timestamp = time.strftime("%H:%M:%S")
        line = f"[{timestamp}] [{level}] {message}"
        if target in self.log_buffers:
            # Deduplicate consecutive identical warning messages
            if level in ["WARN", "ALERT", "FAIL"] and len(self.log_buffers[target]) > 0:
                last_line = self.log_buffers[target][-1]
                if message in last_line:
                    return
            self.log_buffers[target].append(line)

    def start_node_sockets(self):
        ports = {"CENTRAL_HUB": 9000}
        ports.update({n: NODE_PORTS[n] for n in ["A", "B", "C", "D", "E"]})
        ports["HUB"] = 9006
        NODE_PORTS["HUB"] = 9006

        for node_id, port in ports.items():
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind(("127.0.0.1", port))
                self.sockets[node_id] = sock

                t = threading.Thread(target=self._node_socket_loop, args=(node_id, sock), daemon=True)
                t.start()
                self.threads[node_id] = t
                self.log(node_id, f"[SYS] Socket Listener Active on Port {port}", "SYSTEM")
            except Exception as e:
                self.log("CENTRAL_HUB", f"[ERROR] Socket bind error {node_id}:{port}: {e}", "ERROR")

    def _node_socket_loop(self, node_id: str, sock: socket.socket):
        while self.running:
            try:
                r, _, _ = select.select([sock], [], [], 0.2)
                if r:
                    data_bytes, addr = sock.recvfrom(4096)
                    self._process_node_packet(node_id, data_bytes, addr)
            except Exception:
                time.sleep(0.1)

    def _process_node_packet(self, receiving_node: str, data_bytes: bytes, addr: Tuple[str, int]):
        try:
            pkt = json.loads(data_bytes.decode('utf-8'))
        except Exception:
            return

        pkt_type = pkt.get("type", "DATA")

        if pkt_type == "DATA":
            source = pkt.get("source_id")
            dest = pkt.get("destination_id")
            seq = pkt.get("sequence")
            payload = pkt.get("payload", "")
            crc = pkt.get("crc_check")

            expected_crc = calculate_crc(payload)
            if crc != expected_crc:
                self.log(receiving_node, f"[CRC_ERR] Packet #{seq} CRC mismatch. Dropped.", "WARN")
                return

            if receiving_node == "HUB" or dest == receiving_node:
                primary, _, info = self.router.calculate_routes(source, "HUB")
                path_str = " -> ".join(primary) if primary else "UNKNOWN"
                p_metrics = info.get("primary_metrics") or {}
                rel = p_metrics.get("p_e2e_reliability", 0.9) * 100.0
                lat = p_metrics.get("total_latency_ms", 270.0)
                dist = p_metrics.get("total_distance_km", 5.8)

                msg = f"[DESTINATION RECEIVED] Packet #{seq} DELIVERED AT NODE HUB! Payload: '{payload}'"
                path_msg = f"[PATH TAKEN] {path_str} | Total Latency: {lat:.0f}ms | Reliability: {rel:.1f}% | Distance: {dist:.1f}km"

                self.log("HUB", msg, "DELIVERED")
                self.log("HUB", path_msg, "PATH")
                self.log("CENTRAL_HUB", msg, "DELIVERED")
                self.log("CENTRAL_HUB", path_msg, "PATH")
                return

            primary_next = self.node_metrics.get(receiving_node, {}).get("primary_next", "HUB")
            target_port = NODE_PORTS.get(primary_next, 9006)
            self.log(receiving_node, f"[FWD] Packet #{seq} (from {source}) -> Next Hop: NODE {primary_next} (Port {target_port})", "FORWARD")

            self.send_udp_json(target_port, pkt)

    def send_udp_json(self, target_port: int, pkt: dict):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            data = json.dumps(pkt).encode('utf-8')
            sock.sendto(data, ("127.0.0.1", target_port))
            sock.close()
        except Exception:
            pass

    def send_manual_packet(self, source: str = "A", destination: str = "HUB", payload: str = "Tactical CPS Telemetry Payload"):
        self.sequence_counter += 1
        seq = self.sequence_counter
        crc = calculate_crc(payload)

        pkt = {
            "type": "DATA",
            "source_id": source,
            "destination_id": destination,
            "sequence": seq,
            "route_version": 1,
            "payload": payload,
            "crc_check": crc
        }

        self.log(source, f"[TRAFFIC] Transmitting Data Packet #{seq} -> Dest: {destination}", "TRAFFIC")
        target_port = NODE_PORTS.get(source, 9001)
        self.send_udp_json(target_port, pkt)
        return seq

    def inject_manual_chaos(self, scenario_name: str, target_node: str = "B", target_edge: Tuple[str, str] = ("B", "D")):
        u, v = target_edge
        self.log("CENTRAL_HUB", f"[CHAOS] Injecting Fault Scenario: {scenario_name} (Node: {target_node}, Edge: {u}->{v})", "CHAOS")

        if scenario_name == "Healthy / Recover All":
            self.init_default_link_features()
            self.router.active_primary_path = None
            for n in self.node_metrics:
                self.node_metrics[n]["battery_pct"] = round(random.uniform(88.0, 98.0), 1)
                self.node_metrics[n]["temperature_c"] = round(random.uniform(25.0, 32.0), 1)
                self.node_metrics[n]["queue_pct"] = round(random.uniform(5.0, 15.0), 1)
            self.log("CENTRAL_HUB", "[RECOVER] All mesh links and nodes restored to 100% healthy baseline parameters.", "SUCCESS")

        elif scenario_name == "Link Fading":
            m = {
                "rssi": -120.0, "snr": -15.0, "pdr": 0.15, "latency_ms": 950.0, "retries": 5,
                "etx": 6.6, "queue_pct": 25.0, "battery_pct": 75.0, "temperature_c": 30.0,
                "time_on_air_s": 0.45, "spreading_factor": 11, "bandwidth_khz": 125.0, "distance_km": calculate_distance_km(u, v)
            }
            self.link_baseline_features[(u, v)] = dict(m)
            self.link_baseline_features[(v, u)] = dict(m)
            self.log("CENTRAL_HUB", f"[DEGRADE] Target Edge {u}->{v} Degraded: RSSI=-120dBm, SNR=-15dB, PDR=0.15", "WARN")

        elif scenario_name == "Overheat & Congestion":
            if target_node in self.node_metrics:
                self.node_metrics[target_node]["temperature_c"] = 65.0
                self.node_metrics[target_node]["queue_pct"] = 95.0
            m = {
                "rssi": -85.0, "snr": 4.0, "pdr": 0.80, "latency_ms": 900.0, "retries": 4,
                "etx": 1.25, "queue_pct": 95.0, "battery_pct": 75.0, "temperature_c": 65.0,
                "time_on_air_s": 0.25, "spreading_factor": 9, "bandwidth_khz": 125.0, "distance_km": calculate_distance_km(u, v)
            }
            self.link_baseline_features[(u, v)] = dict(m)
            self.log(target_node, f"[ALERT] Thermal Overheat (65C) & Queue Backlog (95%) on Node {target_node}", "ALERT")

        elif scenario_name == "Battery Depletion":
            if target_node in self.node_metrics:
                self.node_metrics[target_node]["battery_pct"] = 10.0
            self.log(target_node, f"[ALERT] Critical Low Battery (10%) on Node {target_node}", "ALERT")

        elif scenario_name == "Complete Node Failure":
            if target_node in self.node_metrics:
                self.node_metrics[target_node]["battery_pct"] = 0.0
                self.node_metrics[target_node]["temperature_c"] = 80.0
            for link_u, link_v in self.router.graph.edges():
                if link_u == target_node or link_v == target_node:
                    m = {
                        "rssi": -130.0, "snr": -25.0, "pdr": 0.01, "latency_ms": 2500.0, "retries": 7,
                        "etx": 10.0, "queue_pct": 100.0, "battery_pct": 0.0, "temperature_c": 80.0,
                        "time_on_air_s": 1.8, "spreading_factor": 12, "bandwidth_khz": 125.0, "distance_km": calculate_distance_km(link_u, link_v)
                    }
                    self.link_baseline_features[(link_u, link_v)] = dict(m)
                    self.link_baseline_features[(link_v, link_u)] = dict(m)
            self.log("CENTRAL_HUB", f"[FAIL] Node {target_node} Completely Failed! All adjacent links down.", "FAIL")

        # In static mode, immediately sync current features with modified baseline
        if not self.is_live_mode:
            for k, v in self.link_baseline_features.items():
                self.link_current_noisy_features[k] = dict(v)

        return self.reevaluate_ml_routes()

    def reevaluate_ml_routes(self):
        self.router.rebuild_full_proximity_graph(
            node_gps=self.node_gps,
            node_radii=self.node_radii,
            edge_metrics=self.link_current_noisy_features,
            node_metrics=self.node_metrics
        )

        primary, backup, info = self.router.calculate_routes("A", "HUB")

        self.is_partitioned = info.get("is_partitioned", False)
        self.topology_status = "NETWORK_PARTITIONED" if self.is_partitioned else "HEALTHY"

        if self.is_partitioned and self.last_logged_topology_status != "NETWORK_PARTITIONED":
            self.last_logged_topology_status = "NETWORK_PARTITIONED"
            self.log("CENTRAL_HUB", "[FATAL ALARM] NETWORK PARTITION DETECTED! NO PATH TO DESTINATION HUB.", "FAIL")
        elif not self.is_partitioned and self.last_logged_topology_status == "NETWORK_PARTITIONED":
            self.last_logged_topology_status = "HEALTHY"
            self.log("CENTRAL_HUB", "[RECOVER] Network Path Restored to Destination Hub.", "SUCCESS")

        if info.get("route_switched", False):
            prev_p = info.get("previous_p_success_pct", 95.0)
            curr_p = info.get("current_p_success_pct", 98.0)
            prev_path = info.get("previous_path", [])
            prev_str = " -> ".join(prev_path) if prev_path else "NONE"
            curr_str = " -> ".join(primary) if primary else "NONE"
            delta = info.get("delta_p_pct", 0.0)
            delta_str = f"+{delta}%" if delta >= 0 else f"{delta}%"
            reason = info.get("switch_reason", "ML Route Recomputed")

            self.log("CENTRAL_HUB", f"[ROUTE_SWITCH] Active: {curr_str} (P={curr_p}%) | Prev: {prev_str} (P={prev_p}%) | Δ={delta_str} [{reason}]", "ROUTE")
            
            for node_id in self.active_node_ids:
                if node_id in self.node_metrics:
                    if primary and node_id in primary:
                        idx = primary.index(node_id)
                        if idx < len(primary) - 1:
                            self.node_metrics[node_id]["primary_next"] = primary[idx + 1]
                    if backup and node_id in backup:
                        idx = backup.index(node_id)
                        if idx < len(backup) - 1:
                            self.node_metrics[node_id]["backup_next"] = backup[idx + 1]

        return primary, backup, info

    def get_full_telemetry_matrix(self) -> List[Dict[str, Any]]:
        matrix = []
        for u, v in self.router.graph.edges():
            feat = self.link_current_noisy_features.get((u, v), {})
            p_succ = predict_link_success_probability(self.model_artifact, feat)
            edge_cost = self.router.graph[u][v].get("weight", 0.0)
            dist_km = self.router.graph[u][v].get("distance_km", calculate_distance_km(u, v))
            
            matrix.append({
                "link": f"{u}->{v}",
                "distance_km": dist_km,
                "rssi": feat.get("rssi", -75.0),
                "snr": feat.get("snr", 9.0),
                "pdr": feat.get("pdr", 0.95),
                "latency_ms": feat.get("latency_ms", 85.0),
                "retries": feat.get("retries", 0),
                "etx": feat.get("etx", 1.05),
                "queue_pct": feat.get("queue_pct", 8.0),
                "battery_pct": feat.get("battery_pct", 95.0),
                "temperature_c": feat.get("temperature_c", 28.0),
                "time_on_air_s": feat.get("time_on_air_s", 0.12),
                "sf": feat.get("spreading_factor", 8),
                "bw_khz": feat.get("bandwidth_khz", 125.0),
                "ml_p_success": round(p_succ * 100.0, 1),
                "edge_cost": round(edge_cost, 4)
            })
        return matrix

    def get_logs(self, node_id: str) -> List[str]:
        return list(self.log_buffers.get(node_id, []))


_manager_instance: Optional[MeshNetworkManager] = None

def get_mesh_manager() -> MeshNetworkManager:
    global _manager_instance
    if _manager_instance is None:
        _manager_instance = MeshNetworkManager()
    return _manager_instance
