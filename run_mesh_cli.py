"""
Unified CLI Multi-Node Orchestrator.
Spawns all mesh node processes (A, B, C, D, E, HUB), Central ML Routing Hub,
and Traffic/Chaos Injector concurrently in a single terminal session.
"""

import sys
import os
import time
import subprocess
from typing import List

# Force UTF-8 encoding for Windows terminals
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

# Nodes to start
NODES = ["HUB", "A", "B", "C", "D", "E"]


def start_mesh_simulation():
    print("=========================================================================")
    print("[+] LAUNCHING DISTRIBUTED LORA MESH MULTI-NODE CLI SIMULATION")
    print("=========================================================================")
    print("Starting Central ML Hub (Port 9000)...")
    print("Starting Mesh Nodes (Ports 9001 - 9005)...")
    print("Starting Traffic & Chaos Generator...")
    print("Press Ctrl+C to stop all nodes.\n")

    processes: List[subprocess.Popen] = []

    try:
        # 1. Start Central ML Hub
        hub_proc = subprocess.Popen([sys.executable, "-m", "src.cli_hub"])
        processes.append(hub_proc)
        time.sleep(1.5)

        # 2. Start Mesh Nodes (A, B, C, D, E)
        for node_id in ["A", "B", "C", "D", "E"]:
            node_proc = subprocess.Popen([sys.executable, "-m", "src.cli_node", "--node", node_id])
            processes.append(node_proc)
            time.sleep(0.3)

        time.sleep(2.0)

        # 3. Start Traffic & Chaos Generator
        chaos_proc = subprocess.Popen([sys.executable, "-m", "src.cli_chaos", "--interval", "2.5"])
        processes.append(chaos_proc)

        # Keep main process alive
        while True:
            time.sleep(1.0)

    except KeyboardInterrupt:
        print("\n\n🛑 STOPPING ALL MESH NODES AND CENTRAL HUB...")
        for proc in processes:
            try:
                proc.terminate()
            except Exception:
                pass
        print("All processes terminated. Goodbye!")


if __name__ == "__main__":
    start_mesh_simulation()
