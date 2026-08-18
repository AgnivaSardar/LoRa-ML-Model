"""
Network Telemetry Simulator & Deterministic Packet Protocol Engine.
Implements CRC/integrity check, sequence numbers, duplicate detection, end-to-end ACKs,
and 5 live network degradation test scenarios.
"""

import random
import time
from typing import Dict, List, Tuple, Any, Optional

from src.routing import LoRaMeshRouter, DEFAULT_NODES


class DeterministicPacketProtocol:
    """
    Deterministic Packet Protocol to ensure message correctness.
    As specified in Section 10 of project plan: ML is NOT the integrity mechanism.
    Correctness requires explicit sequence numbers, CRC checks, and ACK verification.
    """

    def __init__(self):
        self.received_sequence_history: Dict[str, set] = {}

    def create_packet(
        self,
        source_id: str,
        destination_id: str,
        sequence: int,
        route_version: int,
        payload: str
    ) -> Dict[str, Any]:
        crc = self.calculate_crc(payload)
        return {
            "version": 1,
            "source_id": source_id,
            "destination_id": destination_id,
            "sequence": sequence,
            "route_version": route_version,
            "payload": payload,
            "payload_len": len(payload),
            "crc_check": crc
        }

    def calculate_crc(self, payload: str) -> int:
        """Simple XOR CRC checksum simulation."""
        checksum = 0
        for char in payload.encode('utf-8'):
            checksum ^= char
        return checksum & 0xFFFF

    def receive_packet(self, packet: Dict[str, Any], receiving_node: str, link_p_success: float) -> Tuple[bool, str]:
        """
        Receives a packet at a specific node along the path, checks integrity, drops duplicates, and returns status.
        """
        seq = packet["sequence"]
        
        # 1. Physical wireless layer simulation
        if random.random() > link_p_success:
            return False, "PHYSICAL_LINK_LOSS"
            
        # 2. Check integrity (CRC)
        computed_crc = self.calculate_crc(packet["payload"])
        if packet["crc_check"] != computed_crc:
            return False, "CRC_INTEGRITY_FAIL"
            
        # 3. Reject duplicate sequence numbers at receiving node
        if receiving_node not in self.received_sequence_history:
            self.received_sequence_history[receiving_node] = set()
            
        if seq in self.received_sequence_history[receiving_node]:
            return False, "DUPLICATE_SEQUENCE_REJECT"
            
        self.received_sequence_history[receiving_node].add(seq)
        return True, "ACK_SUCCESS"


class MeshNetworkSimulator:
    """
    Simulates multi-hop LoRa packet routing over the network graph.
    """

    def __init__(self, router: Optional[LoRaMeshRouter] = None):
        self.router = router if router is not None else LoRaMeshRouter()
        self.protocol = DeterministicPacketProtocol()
        self.sequence_counter = 100
        self.route_version = 1
        
        # Simulation telemetry metrics
        self.stats = {
            "packets_sent": 0,
            "packets_delivered": 0,
            "packets_lost": 0,
            "total_retries": 0,
            "route_changes": 0
        }
        
        # Link override dictionary for scenario injection
        self.link_overrides: Dict[Tuple[str, str], Dict[str, float]] = {}
        self.node_overrides: Dict[str, Dict[str, float]] = {}

    def set_scenario(self, scenario_name: str, target_edge: Tuple[str, str] = ("B", "D"), target_node: str = "B") -> Dict[str, Any]:
        """
        Applies one of the 5 test scenarios specified in Section 15 of project plan.
        """
        self.link_overrides.clear()
        self.node_overrides.clear()
        u, v = target_edge
        
        if scenario_name == "Healthy":
            desc = "All links operating normally under optimal signal conditions."
            
        elif scenario_name == "Scenario 1: Link Fading":
            desc = f"Severely degraded RSSI (-118 dBm) and SNR (-14 dB) on edge {u}->{v}."
            self.link_overrides[(u, v)] = {
                "rssi": -118.0, "snr": -14.0, "pdr": 0.25, "latency_ms": 750.0,
                "retries": 4, "etx": 4.0, "queue_pct": 20.0, "battery_pct": 80.0,
                "temperature_c": 30.0, "time_on_air_s": 0.45, "spreading_factor": 11, "bandwidth_khz": 125.0
            }
            
        elif scenario_name == "Scenario 2: Congestion":
            desc = f"Heavy queue backlog (92%) and high latency (850 ms) at Node {target_node}."
            self.link_overrides[(u, v)] = {
                "rssi": -85.0, "snr": 5.0, "pdr": 0.80, "latency_ms": 850.0,
                "retries": 3, "etx": 1.25, "queue_pct": 92.0, "battery_pct": 75.0,
                "temperature_c": 40.0, "time_on_air_s": 0.25, "spreading_factor": 9, "bandwidth_khz": 125.0
            }
            self.node_overrides[target_node] = {"queue_pct": 92.0}
            
        elif scenario_name == "Scenario 3: Retransmissions":
            desc = f"High retry count (5 retries) and reduced PDR (0.35) on edge {u}->{v}."
            self.link_overrides[(u, v)] = {
                "rssi": -98.0, "snr": -4.0, "pdr": 0.35, "latency_ms": 950.0,
                "retries": 5, "etx": 2.85, "queue_pct": 35.0, "battery_pct": 70.0,
                "temperature_c": 35.0, "time_on_air_s": 0.40, "spreading_factor": 10, "bandwidth_khz": 125.0
            }
            
        elif scenario_name == "Scenario 4: Node Health Stress":
            desc = f"Depleted battery (12%) and thermal overheat (62°C) at Node {target_node}."
            self.node_overrides[target_node] = {"battery_pct": 12.0, "temperature_c": 62.0}
            self.link_overrides[(u, v)] = {
                "rssi": -88.0, "snr": 2.0, "pdr": 0.70, "latency_ms": 300.0,
                "retries": 1, "etx": 1.43, "queue_pct": 50.0, "battery_pct": 12.0,
                "temperature_c": 62.0, "time_on_air_s": 0.20, "spreading_factor": 8, "bandwidth_khz": 125.0
            }
            
        elif scenario_name == "Scenario 5: Node / Link Failure":
            desc = f"Complete link failure (p_success ~ 0, RSSI -125 dBm, PDR 0) on edge {u}->{v}."
            self.link_overrides[(u, v)] = {
                "rssi": -125.0, "snr": -20.0, "pdr": 0.01, "latency_ms": 2000.0,
                "retries": 7, "etx": 10.0, "queue_pct": 95.0, "battery_pct": 5.0,
                "temperature_c": 70.0, "time_on_air_s": 1.5, "spreading_factor": 12, "bandwidth_khz": 125.0
            }
        else:
            desc = "Custom network condition."

        # Update router graph with scenario metrics
        self.router.update_graph_metrics(self.link_overrides, self.node_overrides)
        primary, backup, info = self.router.calculate_routes("A", "HUB")
        
        if info.get("route_switched", False):
            self.stats["route_changes"] += 1
            self.route_version += 1

        return {
            "scenario": scenario_name,
            "description": desc,
            "primary_path": primary,
            "backup_path": backup,
            "info": info
        }

    def simulate_packet_transmission(self, source: str = "A", target: str = "HUB", payload: str = "Telemetry Payload") -> Dict[str, Any]:
        """
        Simulates end-to-end packet transmission along the primary path.
        """
        self.sequence_counter += 1
        self.stats["packets_sent"] += 1
        
        primary_path, _, info = self.router.calculate_routes(source, target)
        if not primary_path or len(primary_path) < 2:
            self.stats["packets_lost"] += 1
            return {
                "success": False,
                "reason": "NO_ROUTE_AVAILABLE",
                "path": [],
                "sequence": self.sequence_counter
            }

        packet = self.protocol.create_packet(
            source_id=source,
            destination_id=target,
            sequence=self.sequence_counter,
            route_version=self.route_version,
            payload=payload
        )

        # Trace packet hop-by-hop
        hop_results = []
        overall_success = True
        failure_reason = "ACK_SUCCESS"

        for i in range(len(primary_path) - 1):
            u, v = primary_path[i], primary_path[i + 1]
            p_succ = self.router.graph[u][v].get("p_success", 0.9)
            
            # Hop attempt
            succ, reason = self.protocol.receive_packet(packet, receiving_node=v, link_p_success=p_succ)
            hop_results.append({
                "hop": f"{u} -> {v}",
                "p_success": p_succ,
                "success": succ,
                "reason": reason
            })
            
            if not succ:
                overall_success = False
                failure_reason = f"Failed at hop {u}->{v} ({reason})"
                break

        if overall_success:
            self.stats["packets_delivered"] += 1
        else:
            self.stats["packets_lost"] += 1

        return {
            "success": overall_success,
            "reason": failure_reason,
            "path": primary_path,
            "sequence": self.sequence_counter,
            "hop_results": hop_results,
            "stats": self.stats.copy()
        }


if __name__ == "__main__":
    sim = MeshNetworkSimulator()
    print("--- Testing Scenario Presets ---")
    for sc in ["Healthy", "Scenario 1: Link Fading", "Scenario 2: Congestion", "Scenario 4: Node Health Stress", "Scenario 5: Node / Link Failure"]:
        res = sim.set_scenario(sc)
        print(f"\n[{sc}]")
        print("  Description:", res["description"])
        print("  Primary Path:", res["primary_path"])
        print("  Backup Path:", res["backup_path"])
        
    print("\n--- Testing Packet Transmission Simulation ---")
    tx_res = sim.simulate_packet_transmission()
    print("Transmission Result:", tx_res)
