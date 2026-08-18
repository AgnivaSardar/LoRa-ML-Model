"""
Graph Routing Engine with Full-Mesh Any-to-Any Dynamic Proximity Connectivity,
Haversine GPS Physics, ML Cost Weighting, and Real-Time Dijkstra Rerouting.
"""

import math
import threading
import networkx as nx
from typing import Dict, List, Tuple, Any, Optional

from src.train_model import predict_link_success_probability, load_trained_model

# Real Tactical GPS Coordinates (Latitude °N, Longitude °E)
NODE_GPS_COORDINATES: Dict[str, Tuple[float, float]] = {
    "A": (12.9716, 79.1588),   # Source Base
    "B": (12.9860, 79.1730),   # North Relay Alpha
    "C": (12.9580, 79.1760),   # South Relay Bravo
    "D": (12.9930, 79.1960),   # North-East Relay Charlie
    "E": (12.9520, 79.1990),   # South-East Relay Delta
    "HUB": (12.9740, 79.2180)  # Destination Central Command Hub
}


def haversine_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculates Haversine physical distance in kilometers between two GPS coordinates."""
    r = 6371.0  # Earth's radius in kilometers
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    
    a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return round(r * c, 2)


def calculate_distance_km(u: str, v: str, gps_dict: Optional[Dict[str, Tuple[float, float]]] = None) -> float:
    coords = gps_dict if gps_dict is not None else NODE_GPS_COORDINATES
    lat1, lon1 = coords.get(u, (12.9716, 79.1588))
    lat2, lon2 = coords.get(v, (12.9740, 79.2180))
    return haversine_distance_km(lat1, lon1, lat2, lon2)


def compute_edge_cost(
    p_success: float,
    latency_ms: float,
    retries: float,
    queue_pct: float,
    battery_pct: float = 100.0,
    temp_c: float = 25.0,
    distance_km: float = 1.5
) -> float:
    """
    Pure Data & ML-Driven Dynamic Edge Cost Formula:
    - Primary weight is driven directly by ML prediction P(success).
    - Distance penalty is strictly proportional to physical distance (km).
    - Latency penalty is strictly proportional to measured latency (ms).
    No arbitrary hop constants or hardcoded biases.
    """
    p_clamped = max(p_success, 1e-6)
    ml_penalty = -math.log(p_clamped) * 10.0
    
    dist_penalty = 0.05 * distance_km
    latency_penalty = 0.001 * latency_ms
    retries_penalty = 0.05 * retries
    
    # Battery depletion / overheat hardware penalties
    battery_penalty = (20.0 - battery_pct) * 0.20 if battery_pct < 20.0 else 0.0
    temp_penalty = (temp_c - 55.0) * 0.10 if temp_c > 55.0 else 0.0
        
    total_cost = ml_penalty + dist_penalty + latency_penalty + retries_penalty + battery_penalty + temp_penalty
    return round(total_cost, 4)


class LoRaMeshRouter:
    def __init__(self, model_artifact: Optional[Dict[str, Any]] = None):
        self.model_artifact = model_artifact if model_artifact is not None else load_trained_model()
        self.graph = nx.DiGraph()
        self.graph_lock = threading.RLock()
        for node in ["A", "B", "C", "D", "E", "HUB"]:
            self.graph.add_node(node)

        self.hysteresis_threshold: float = 0.20
        self.active_primary_path: Optional[List[str]] = None
        self.active_path_cost: Optional[float] = None
        self.previous_path: Optional[List[str]] = None
        self.previous_p_success: Optional[float] = None
        self.current_p_success: Optional[float] = None
        self.switch_reason: str = "System Initialized"

    def rebuild_full_proximity_graph(
        self,
        node_gps: Dict[str, Tuple[float, float]],
        node_radii: Dict[str, float],
        edge_metrics: Dict[Tuple[str, str], Dict[str, float]],
        node_metrics: Optional[Dict[str, Dict[str, float]]] = None
    ) -> None:
        """
        FULL-MESH ANY-TO-ANY DYNAMIC PROXIMITY GRAPH:
        Rebuilds graph atomically so readers never encounter an empty/transient state.
        """
        new_graph = nx.DiGraph()
        nodes = list(node_gps.keys())
        for n in nodes:
            new_graph.add_node(n)

        for i in range(len(nodes)):
            for j in range(i + 1, len(nodes)):
                u, v = nodes[i], nodes[j]
                lat1, lon1 = node_gps[u]
                lat2, lon2 = node_gps[v]
                dist = haversine_distance_km(lat1, lon1, lat2, lon2)

                r_u = node_radii.get(u, 2.8)
                r_v = node_radii.get(v, 2.8)

                if dist <= (r_u + r_v):
                    for src, dst in [(u, v), (v, u)]:
                        feat = edge_metrics.get((src, dst), None)
                        if feat is None:
                            feat = {
                                "rssi": -75.0, "snr": 10.0, "pdr": 0.99, "latency_ms": 70.0 + dist * 10.0,
                                "retries": 0, "etx": 1.01, "queue_pct": 5.0, "battery_pct": 95.0,
                                "temperature_c": 28.0, "time_on_air_s": 0.12, "spreading_factor": 8,
                                "bandwidth_khz": 125.0, "distance_km": dist
                            }

                        # Check node hardware status
                        src_health = (node_metrics or {}).get(src, {})
                        dst_health = (node_metrics or {}).get(dst, {})
                        bat = dst_health.get("battery_pct", feat.get("battery_pct", 95.0))
                        temp = dst_health.get("temperature_c", feat.get("temperature_c", 28.0))

                        if src_health.get("battery_pct", 95.0) <= 0.0 or dst_health.get("battery_pct", 95.0) <= 0.0:
                            feat = dict(feat)
                            feat["battery_pct"] = 0.0
                            feat["pdr"] = 0.0
                            feat["rssi"] = -130.0
                            feat["snr"] = -25.0
                            bat = 0.0

                        # Predict ML transmission success probability
                        p_success = predict_link_success_probability(self.model_artifact, feat)
                        p_success = max(0.01, min(0.99, float(p_success)))
                        
                        # Dijkstra link cost (pure data-driven)
                        edge_cost = compute_edge_cost(
                            p_success=p_success,
                            latency_ms=feat.get("latency_ms", 80.0),
                            retries=feat.get("retries", 0),
                            queue_pct=feat.get("queue_pct", 5.0),
                            battery_pct=bat,
                            temp_c=temp,
                            distance_km=dist
                        )
                        
                        if bat <= 0.0 or feat.get("pdr", 0.9) <= 0.0:
                            edge_cost = 9999.0
                            p_success = 0.0

                        new_graph.add_edge(
                            src, dst,
                            weight=round(edge_cost, 4),
                            distance_km=round(dist, 2),
                            p_success=round(p_success, 4),
                            metrics=feat
                        )

        with self.graph_lock:
            self.graph = new_graph

    def update_graph_metrics(
        self,
        edge_metrics: Dict[Tuple[str, str], Dict[str, float]],
        node_metrics: Optional[Dict[str, Dict[str, float]]] = None
    ) -> None:
        pass

    def calculate_routes(
        self,
        source: str = "A",
        target: str = "HUB"
    ) -> Tuple[List[str], Optional[List[str]], Dict[str, Any]]:
        """
        Computes Primary Route and Backup Route using Dijkstra over active proximity graph.
        Tracks previous vs. current route probabilities and accurately maintains route history.
        """
        with self.graph_lock:
            try:
                candidate_path = nx.shortest_path(self.graph, source=source, target=target, weight="weight")
                candidate_cost = nx.shortest_path_length(self.graph, source=source, target=target, weight="weight")
                if candidate_cost > 5000.0:
                    raise nx.NetworkXNoPath
            except (nx.NetworkXNoPath, nx.NodeNotFound):
                if self.active_primary_path:
                    self.previous_path = list(self.active_primary_path)
                    self.previous_p_success = self.current_p_success or 0.0
                self.active_primary_path = []
                self.active_path_cost = None
                self.current_p_success = 0.0
                self.switch_reason = "Network Partitioned (No Valid Path)"
                return [], None, {
                    "error": "NETWORK_PARTITIONED",
                    "topology_status": "NETWORK_PARTITIONED",
                    "is_partitioned": True,
                    "primary_path": [],
                    "previous_path": self.previous_path,
                    "previous_p_success_pct": round((self.previous_p_success or 0.0) * 100, 1),
                    "current_p_success_pct": 0.0,
                    "delta_p_pct": 0.0,
                    "switch_reason": self.switch_reason
                }

            # Backup Path Computation
            G_backup = self.graph.copy()
            for i in range(len(candidate_path) - 1):
                u, v = candidate_path[i], candidate_path[i + 1]
                if G_backup.has_edge(u, v):
                    G_backup[u][v]["weight"] += 1000.0

            try:
                backup_path = nx.shortest_path(G_backup, source=source, target=target, weight="weight")
                backup_cost = nx.shortest_path_length(G_backup, source=source, target=target, weight="weight")
                if backup_cost > 500.0:
                    backup_path = None
            except (nx.NetworkXNoPath, nx.NodeNotFound):
                backup_path = None

            candidate_metrics = self.compute_path_metrics(candidate_path)
            candidate_p_e2e = candidate_metrics.get("p_e2e_reliability", 0.9) if candidate_metrics else 0.9

            # Route Flapping Control & Probability Comparison
            route_switched = False
            if not self.active_primary_path:
                if not self.previous_path:
                    self.previous_path = list(candidate_path)
                    self.previous_p_success = candidate_p_e2e
                self.active_primary_path = candidate_path
                self.active_path_cost = candidate_cost
                self.current_p_success = candidate_p_e2e
                self.switch_reason = "Initial Route Established"
                route_switched = (self.previous_path != candidate_path)
            else:
                active_still_valid = True
                active_cost_now = 0.0
                for i in range(len(self.active_primary_path) - 1):
                    u, v = self.active_primary_path[i], self.active_primary_path[i + 1]
                    if not self.graph.has_edge(u, v) or self.graph[u][v].get("weight", 0) > 5000.0:
                        active_still_valid = False
                        break
                    active_cost_now += self.graph[u][v]["weight"]

                active_metrics = self.compute_path_metrics(self.active_primary_path) if active_still_valid else None
                active_p_e2e = active_metrics.get("p_e2e_reliability", 0.0) if active_metrics else 0.0

                if not active_still_valid or active_p_e2e <= 0.50 or active_cost_now > 5.0:
                    # Active path is physically broken or degraded -> switch immediately
                    self.previous_path = list(self.active_primary_path)
                    self.previous_p_success = self.current_p_success or 0.0
                    self.active_primary_path = candidate_path
                    self.active_path_cost = candidate_cost
                    self.current_p_success = candidate_p_e2e
                    self.switch_reason = "Active Path Degraded / Node Failed"
                    route_switched = True
                else:
                    # Active path is healthy
                    rel_diff = candidate_p_e2e - active_p_e2e
                    active_dist = active_metrics.get("total_distance_km", 0.0) if active_metrics else 0.0
                    cand_dist = candidate_metrics.get("total_distance_km", 0.0) if candidate_metrics else 0.0
                    cost_improvement = active_cost_now - candidate_cost
                    
                    # Check for significant reliability gain OR restored shorter path OR lower cost path
                    has_rel_gain = (rel_diff >= 0.08)
                    has_cost_gain = (cost_improvement >= 0.20 and candidate_p_e2e >= 0.85)
                    has_shorter_restore = (candidate_p_e2e >= 0.88 and (cand_dist <= (active_dist - 1.2) or len(candidate_path) < len(self.active_primary_path)))
                    
                    if (has_rel_gain or has_cost_gain or has_shorter_restore) and candidate_path != self.active_primary_path:
                        self.previous_path = list(self.active_primary_path)
                        self.previous_p_success = active_p_e2e
                        self.active_primary_path = candidate_path
                        self.active_path_cost = candidate_cost
                        self.current_p_success = candidate_p_e2e
                        
                        if has_cost_gain or has_shorter_restore:
                            self.switch_reason = f"Lower Cost / Shorter Route Optimized ({round(cand_dist, 1)}km, Cost: {round(candidate_cost, 2)})"
                        else:
                            self.switch_reason = f"Significant Reliability Gain (+{round(rel_diff*100, 1)}%)"
                            
                        route_switched = True
                    else:
                        # Keep existing active route rock-solid
                        self.active_path_cost = active_cost_now
                        self.current_p_success = active_p_e2e

        primary_metrics = self.compute_path_metrics(self.active_primary_path)
        backup_metrics = self.compute_path_metrics(backup_path) if backup_path else None

        prev_pct = round((self.previous_p_success or (self.current_p_success or 0.95)) * 100, 1)
        curr_pct = round((self.current_p_success or 0.95) * 100, 1)
        delta_p = round(curr_pct - prev_pct, 1)

        info = {
            "primary_path": self.active_primary_path,
            "backup_path": backup_path,
            "candidate_path": candidate_path,
            "previous_path": self.previous_path or self.active_primary_path,
            "previous_p_success_pct": prev_pct,
            "current_p_success_pct": curr_pct,
            "delta_p_pct": delta_p,
            "switch_reason": self.switch_reason,
            "primary_cost": self.active_path_cost,
            "candidate_cost": candidate_cost,
            "route_switched": route_switched,
            "primary_metrics": primary_metrics,
            "backup_metrics": backup_metrics,
            "topology_status": "HEALTHY",
            "is_partitioned": False
        }

        return self.active_primary_path, backup_path, info

    def compute_path_metrics(self, path: Optional[List[str]]) -> Optional[Dict[str, float]]:
        if not path or len(path) < 2:
            return None

        p_e2e = 1.0
        total_latency = 0.0
        total_retries = 0.0
        total_distance = 0.0
        total_cost = 0.0

        for i in range(len(path) - 1):
            u, v = path[i], path[i + 1]
            if self.graph.has_edge(u, v):
                edge = self.graph[u][v]
                m = edge.get("metrics", {})
                p_e2e *= edge.get("p_success", 0.9)
                total_latency += m.get("latency_ms", 100.0)
                total_retries += m.get("retries", 0)
                total_distance += edge.get("distance_km", 1.5)
                total_cost += edge.get("weight", 0.1)

        return {
            "p_e2e_reliability": p_e2e,
            "total_latency_ms": total_latency,
            "total_retries": total_retries,
            "total_distance_km": round(total_distance, 2),
            "total_cost": round(total_cost, 4),
            "hop_count": len(path) - 1
        }
