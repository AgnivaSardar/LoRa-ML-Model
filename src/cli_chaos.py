"""
Chaos Fault Injector & Real-Time Traffic Generator.
Generates telemetry packet traffic from Node A to HUB, and periodically injects
random node failures/link fading to demonstrate live re-routing.
"""

import sys
import os
import time
import json
import socket
import random
import argparse

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

from src.cli_node import calculate_crc, NODE_PORTS, COLOR_RESET, COLOR_BOLD, COLOR_GREEN, COLOR_CYAN, COLOR_YELLOW, COLOR_RED, COLOR_MAGENTA


def send_udp_json(target_port: int, pkt: dict, host: str = "127.0.0.1"):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        data = json.dumps(pkt).encode('utf-8')
        sock.sendto(data, (host, target_port))
        sock.close()
    except Exception as e:
        print(f"{COLOR_RED}Error sending payload to port {target_port}: {e}{COLOR_RESET}")


def run_traffic_and_chaos(interval_sec: float = 2.5, auto_chaos: bool = True):
    print(f"{COLOR_BOLD}{COLOR_GREEN}🚀 Starting LoRa Mesh Traffic & Chaos Fault Generator...{COLOR_RESET}", flush=True)
    print(f"{COLOR_CYAN}Sending packets from Node A -> HUB every {interval_sec} seconds.{COLOR_RESET}\n", flush=True)

    sequence = 100
    events = [
        ("Scenario 1: Fading on Link B->D", "B", "D", {"rssi": -118.0, "snr": -14.0, "pdr": 0.20, "latency_ms": 850.0, "retries": 5, "etx": 5.0, "queue_pct": 20.0}),
        ("Scenario 2: Node B Overheated & High Queue", "B", "D", {"rssi": -85.0, "snr": 4.0, "pdr": 0.80, "latency_ms": 900.0, "retries": 4, "etx": 1.25, "queue_pct": 95.0}),
        ("Scenario 3: Battery Depleted on Node B (10%)", "A", "B", {"rssi": -90.0, "snr": 0.0, "pdr": 0.50, "latency_ms": 600.0, "retries": 4, "etx": 2.0, "battery_pct": 10.0}),
        ("Scenario 4: Complete Link Failure on B->D", "B", "D", {"rssi": -125.0, "snr": -20.0, "pdr": 0.01, "latency_ms": 2000.0, "retries": 7, "etx": 10.0, "queue_pct": 99.0}),
        ("Scenario 5: Link Recovery B->D (Healthy)", "B", "D", {"rssi": -75.0, "snr": 10.0, "pdr": 0.98, "latency_ms": 80.0, "retries": 0, "etx": 1.02, "queue_pct": 10.0})
    ]
    
    event_idx = 0
    start_time = time.time()

    while True:
        try:
            sequence += 1
            payload = f"Mesh Data Packet Payload #{sequence}"
            crc = calculate_crc(payload)

            data_pkt = {
                "type": "DATA",
                "source_id": "A",
                "destination_id": "HUB",
                "sequence": sequence,
                "route_version": 1,
                "payload": payload,
                "crc_check": crc
            }

            # Send data packet to Node A (Port 9001)
            send_udp_json(NODE_PORTS["A"], data_pkt)
            print(f"{COLOR_BOLD}[TRAFFIC] 📤 Injected Data Packet #{sequence} into Node A{COLOR_RESET}", flush=True)

            # Every ~12 seconds, trigger a chaos event if auto_chaos is True
            elapsed = time.time() - start_time
            if auto_chaos and elapsed > 12.0:
                name, u, v, metrics = events[event_idx % len(events)]
                event_idx += 1
                start_time = time.time()

                print(f"\n{COLOR_BOLD}{COLOR_YELLOW}💥 INJECTING CHAOS EVENT: {name}{COLOR_RESET}", flush=True)
                
                # Send Link Metrics Override to Hub (Port 9000)
                override_pkt = {
                    "type": "LINK_METRICS_OVERRIDE",
                    "u": u,
                    "v": v,
                    "metrics": metrics
                }
                send_udp_json(NODE_PORTS["HUB"], override_pkt)

            time.sleep(interval_sec)

        except KeyboardInterrupt:
            print(f"{COLOR_YELLOW}Stopping traffic generator...{COLOR_RESET}")
            break
        except Exception as e:
            print(f"{COLOR_RED}Traffic generator error: {e}{COLOR_RESET}")
            time.sleep(2.0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LoRa Mesh Chaos & Traffic Injector")
    parser.add_argument("--interval", type=float, default=2.5, help="Packet interval in seconds")
    parser.add_argument("--no-chaos", action="store_true", help="Disable random fault injection")
    args = parser.parse_args()

    run_traffic_and_chaos(interval_sec=args.interval, auto_chaos=not args.no_chaos)
