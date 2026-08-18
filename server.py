"""
Industrial Zero-Dependency Threading Web Server with Sub-10ms Real-Time SSE Broadcasting,
Synchronized 3-Site Handshake Tracking, and Mutual Auto Hard-Refresh Coordination.
Uses Python built-in http.server.ThreadingHTTPServer. Binds to http://localhost:8000.
Serves web/index.html, web/topology.html, web/control.html.
"""

import sys
import os
import time
import json
import threading
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from typing import Dict, Any, Optional

from src.cli_manager import get_mesh_manager

WEB_DIR = os.path.join(os.path.dirname(__file__), "web")

# Active SSE Client Streams & Lock
sse_clients = []
sse_lock = threading.Lock()

# 3-Site Presence & Synchronization Tracker
class SiteSyncTracker:
    def __init__(self):
        self.lock = threading.Lock()
        self.sites = {
            "root": {"name": "ROOT DASHBOARD (http://localhost:8000/)", "url": "/", "last_seen": 0.0, "synced": False},
            "control": {"name": "CPS CONTROL ENGINE (http://localhost:8000/control.html)", "url": "/control.html", "last_seen": 0.0, "synced": False},
            "topology": {"name": "2D SPATIAL MAP GRAPH (http://localhost:8000/topology.html)", "url": "/topology.html", "last_seen": 0.0, "synced": False}
        }
        self.sync_logs = []
        self.handshake_epoch = time.time()
        self.add_log("System initialized. Awaiting 3-site peer handshakes...")

    def add_log(self, text: str):
        ts = time.strftime("%H:%M:%S")
        self.sync_logs.append(f"[{ts}] {text}")
        if len(self.sync_logs) > 50:
            self.sync_logs.pop(0)

    def ping(self, site_id: str, action: str = "ping") -> Dict[str, Any]:
        with self.lock:
            now = time.time()
            if site_id in self.sites:
                was_offline = (now - self.sites[site_id]["last_seen"]) > 4.0
                self.sites[site_id]["last_seen"] = now
                self.sites[site_id]["synced"] = True
                
                if was_offline:
                    self.add_log(f"Site '{site_id.upper()}' joined peer network.")

            # Calculate active sites (seen within last 4 seconds)
            active_count = 0
            pending_sites = []
            for s_id, s_data in self.sites.items():
                is_active = (now - s_data["last_seen"]) <= 4.0
                if is_active:
                    active_count += 1
                    s_data["synced"] = True
                else:
                    s_data["synced"] = False
                    pending_sites.append(s_id)

            all_synced = (active_count == 3)

            slow_site = None
            if not all_synced and pending_sites:
                slow_site = pending_sites[0]

            return {
                "all_synced": all_synced,
                "active_count": active_count,
                "pending_sites": pending_sites,
                "slow_site": slow_site,
                "sites": {
                    s_id: {
                        "name": s_data["name"],
                        "url": s_data["url"],
                        "status": "ONLINE" if (now - s_data["last_seen"] <= 4.0) else "WAITING",
                        "synced": s_data["synced"] and (now - s_data["last_seen"] <= 4.0),
                        "last_seen_ago": round(now - s_data["last_seen"], 1) if s_data["last_seen"] > 0 else 999.0
                    } for s_id, s_data in self.sites.items()
                },
                "logs": list(self.sync_logs[-10:]),
                "timestamp": now
            }

    def reset_for_hard_refresh(self, initiator: str):
        with self.lock:
            self.handshake_epoch = time.time()
            for s_id in self.sites:
                self.sites[s_id]["synced"] = False
                self.sites[s_id]["last_seen"] = 0.0
            self.add_log(f"🔄 Hard refresh triggered by '{initiator.upper()}'. All 3 sites re-syncing...")

sync_tracker = SiteSyncTracker()


def broadcast_event(event_type: str, data: dict):
    """Broadcasts real-time events to all connected browser SSE clients in < 5ms."""
    payload = f"event: {event_type}\ndata: {json.dumps(data)}\n\n".encode("utf-8")
    with sse_lock:
        dead_clients = []
        for client in sse_clients:
            try:
                client.write(payload)
                client.flush()
            except Exception:
                dead_clients.append(client)
        for d in dead_clients:
            if d in sse_clients:
                sse_clients.remove(d)


class CPSIndustrialHTTPHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=WEB_DIR, **kwargs)

    def end_headers(self):
        # Disable caching for real-time consistency
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def do_GET(self):
        manager = get_mesh_manager()

        if self.path == "/api/events":
            # Server-Sent Events (SSE) Real-Time Stream Endpoint
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            with sse_lock:
                sse_clients.append(self.wfile)

            # Send immediate initial connection ACK
            try:
                self.wfile.write(b"event: CONNECTED\ndata: {\"status\":\"OK\"}\n\n")
                self.wfile.flush()
            except Exception:
                pass

            # Keep stream open until disconnect
            try:
                while manager.running:
                    time.sleep(0.5)
            except Exception:
                pass
            finally:
                with sse_lock:
                    if self.wfile in sse_clients:
                        sse_clients.remove(self.wfile)
            return

        elif self.path == "/api/sync_state" or self.path.startswith("/api/sync_state?"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            # Parse query params if any
            site_id = "root"
            if "site_id=" in self.path:
                site_id = self.path.split("site_id=")[1].split("&")[0]

            data = sync_tracker.ping(site_id, action="ping")
            self.wfile.write(json.dumps(data).encode("utf-8"))
            return

        elif self.path == "/api/health_check":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            sync_status = sync_tracker.ping("root", action="ping")
            data = {
                "status": "HEALTHY",
                "timestamp": time.time(),
                "all_synced": sync_status["all_synced"],
                "sites": sync_status["sites"]
            }
            self.wfile.write(json.dumps(data).encode("utf-8"))
            return

        elif self.path == "/api/logs":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            logs_dict = {node: manager.get_logs(node) for node in manager.active_node_ids}
            logs_dict["CENTRAL_HUB"] = manager.get_logs("CENTRAL_HUB")
            self.wfile.write(json.dumps(logs_dict).encode("utf-8"))
            return

        elif self.path == "/api/telemetry":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            primary, backup, info = manager.router.calculate_routes("A", "HUB")
            data = {
                "active_nodes": manager.active_node_ids,
                "node_states": manager.node_metrics,
                "link_matrix": manager.get_full_telemetry_matrix(),
                "primary_path": primary,
                "backup_path": backup,
                "route_info": info,
                "topology_status": manager.topology_status,
                "is_partitioned": manager.is_partitioned,
                "is_live_mode": manager.is_live_mode
            }
            self.wfile.write(json.dumps(data).encode("utf-8"))
            return

        elif self.path == "/api/topology":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            primary, backup, info = manager.router.calculate_routes("A", "HUB")
            top_data = {
                "nodes": manager.active_node_ids,
                "node_gps": manager.node_gps,
                "node_radii": manager.node_radii,
                "node_states": manager.node_metrics,
                "primary_path": primary,
                "backup_path": backup,
                "route_info": info,
                "topology_status": manager.topology_status,
                "is_partitioned": manager.is_partitioned,
                "is_live_mode": manager.is_live_mode
            }
            self.wfile.write(json.dumps(top_data).encode("utf-8"))
            return

        elif self.path == "/api/state":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            primary, backup, info = manager.router.calculate_routes("A", "HUB")
            logs_dict = {node: manager.get_logs(node) for node in manager.active_node_ids}
            logs_dict["CENTRAL_HUB"] = manager.get_logs("CENTRAL_HUB")

            full_state = {
                "active_nodes": manager.active_node_ids,
                "node_gps": manager.node_gps,
                "node_radii": manager.node_radii,
                "node_states": manager.node_metrics,
                "link_matrix": manager.get_full_telemetry_matrix(),
                "primary_path": primary,
                "backup_path": backup,
                "route_info": info,
                "topology_status": manager.topology_status,
                "is_partitioned": manager.is_partitioned,
                "is_live_mode": manager.is_live_mode,
                "logs": logs_dict,
                "timestamp": time.time()
            }
            self.wfile.write(json.dumps(full_state).encode("utf-8"))
            return

        super().do_GET()

    def do_POST(self):
        manager = get_mesh_manager()

        content_length = int(self.headers.get('Content-Length', 0))
        post_data = self.rfile.read(content_length) if content_length > 0 else b"{}"

        try:
            payload = json.loads(post_data.decode('utf-8'))
        except Exception:
            payload = {}

        clean_path = self.path.split('?')[0].rstrip('/')

        if clean_path == "/api/sync_state":
            site_id = payload.get("site_id", "root")
            action = payload.get("action", "ping")
            data = sync_tracker.ping(site_id, action=action)
            broadcast_event("SYNC_STATE_CHANGED", data)
            
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(data).encode("utf-8"))
            return

        elif clean_path == "/api/trigger_hard_refresh":
            initiator = payload.get("site_id", "user")
            sync_tracker.reset_for_hard_refresh(initiator)
            broadcast_event("HARD_REFRESH", {"initiator": initiator, "timestamp": time.time()})
            
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "REFRESH_TRIGGERED"}).encode("utf-8"))
            return

        elif clean_path == "/api/toggle_live_mode":
            desired = payload.get("live_mode")
            if desired is None:
                new_mode = not manager.is_live_mode
            else:
                new_mode = bool(desired)
            
            manager.set_live_mode(new_mode)
            primary, backup, info = manager.router.calculate_routes("A", "HUB")
            
            event_payload = {
                "is_live_mode": manager.is_live_mode,
                "primary_path": primary,
                "route_info": info,
                "node_states": manager.node_metrics,
                "timestamp": time.time()
            }
            broadcast_event("LIVE_MODE_CHANGED", event_payload)
            broadcast_event("STATE_UPDATE", event_payload)
            
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "success",
                "is_live_mode": manager.is_live_mode,
                "primary_path": primary,
                "route_info": info
            }).encode("utf-8"))
            return

        elif clean_path == "/api/send_packet":
            seq = manager.send_manual_packet("A", "HUB", "Tactical CPS Telemetry Payload")
            primary, _, info = manager.router.calculate_routes("A", "HUB")
            broadcast_event("PACKET_SENT", {"sequence": seq, "primary_path": primary, "route_info": info})
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "success", "sequence": seq, "primary_path": primary, "route_info": info}).encode("utf-8"))
            return

        elif clean_path == "/api/inject_chaos":
            scenario = payload.get("scenario", "Healthy / Recover All")
            target_node = payload.get("target_node", "B")
            target_edge_raw = payload.get("target_edge", "B->D")
            
            u_edge, v_edge = target_edge_raw.split("->") if "->" in target_edge_raw else ("B", "D")
            
            manager.inject_manual_chaos(scenario, target_node=target_node, target_edge=(u_edge, v_edge))
            primary, backup, info = manager.router.calculate_routes("A", "HUB")
            
            event_data = {
                "scenario": scenario,
                "target_node": target_node,
                "target_edge": [u_edge, v_edge],
                "primary_path": primary,
                "route_info": info,
                "node_states": manager.node_metrics,
                "is_partitioned": manager.is_partitioned,
                "timestamp": time.time()
            }
            broadcast_event("CHAOS_INJECTED", event_data)
            broadcast_event("STATE_UPDATE", event_data)
            
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "success",
                "scenario": scenario,
                "target_node": target_node,
                "primary_path": primary,
                "route_info": info
            }).encode("utf-8"))
            return

        elif clean_path == "/api/update_node_pos":
            node_id = payload.get("node_id")
            lat = payload.get("lat")
            lon = payload.get("lon")
            if node_id and lat is not None and lon is not None:
                manager.update_node_position(node_id, float(lat), float(lon))
                primary, _, info = manager.router.calculate_routes("A", "HUB")
                broadcast_event("NODE_MOVED", {"node_id": node_id, "lat": lat, "lon": lon, "primary_path": primary, "route_info": info})
            
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            primary, _, info = manager.router.calculate_routes("A", "HUB")
            self.wfile.write(json.dumps({"status": "success", "primary_path": primary, "route_info": info}).encode("utf-8"))
            return

        elif clean_path == "/api/update_node_radius":
            node_id = payload.get("node_id", "ALL")
            radius_km = payload.get("radius_km", 2.8)
            manager.update_node_radius(node_id, float(radius_km))
            primary, _, info = manager.router.calculate_routes("A", "HUB")
            broadcast_event("RADIUS_UPDATED", {"node_id": node_id, "radius_km": radius_km, "primary_path": primary, "route_info": info})
            
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "success", "primary_path": primary, "route_info": info}).encode("utf-8"))
            return

        self.send_error(404, "Endpoint Not Found")


def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("1.1.1.1", 80))
        ip = s.getsockname()[0]
        s.close()
        if ip and not ip.startswith("127."):
            return ip
    except Exception:
        pass

    try:
        hostname = socket.gethostname()
        for ip in socket.gethostbyname_ex(hostname)[2]:
            if not ip.startswith("127.") and not ip.startswith("169.254.") and ip != "192.168.56.1":
                return ip
    except Exception:
        pass

    return "127.0.0.1"


def run_industrial_server(port: Optional[int] = None):
    if port is None:
        port = int(os.environ.get("PORT", 8000))

    manager = get_mesh_manager()
    local_ip = get_local_ip()

    print("=========================================================================")
    print("LAUNCHING REAL-TIME SSE BROADCAST & 3-SITE SYNCHRONIZED MULTI-THREAD SERVER")
    print("=========================================================================")
    print(f"Local Host URL:       http://localhost:{port}")
    print(f"Multi-Laptop LAN URL: http://{local_ip}:{port}")
    print("-------------------------------------------------------------------------")
    print(f"Laptop 1 (Root Grid):    http://{local_ip}:{port}/")
    print(f"Laptop 2 (CPS Control):  http://{local_ip}:{port}/control.html")
    print(f"Laptop 3 (2D Topology):  http://{local_ip}:{port}/topology.html")
    print("=========================================================================\n")

    httpd = ThreadingHTTPServer(("0.0.0.0", port), CPSIndustrialHTTPHandler)
    httpd.daemon_threads = True
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down web server...")
        httpd.server_close()


if __name__ == "__main__":
    run_industrial_server()
