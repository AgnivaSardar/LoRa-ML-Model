"""
Rich-based Terminal Mesh Dashboard.
Renders a 7-panel terminal grid layout inside a single Command Prompt window using rich.live.
"""

import sys
import os
import time
import json
import socket
from typing import Dict, List

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

try:
    from rich.live import Live
    from rich.layout import Layout
    from rich.panel import Panel
    from rich.console import Console
    from rich.table import Table
    from rich import box
except ImportError:
    print("Rich package not available. Install via pip install rich")
    sys.exit(1)

from src.cli_manager import get_mesh_manager

console = Console()

def make_layout() -> Layout:
    layout = Layout()
    layout.split(
        Layout(name="header", size=3),
        Layout(name="metrics", size=7),
        Layout(name="body", ratio=1)
    )
    
    layout["body"].split_row(
        Layout(name="col1"),
        Layout(name="col2"),
        Layout(name="col3"),
        Layout(name="col4")
    )
    
    layout["body"]["col1"].split_column(
        Layout(name="HUB_CENTRAL", ratio=1),
        Layout(name="NODE_D", ratio=1)
    )
    
    layout["body"]["col2"].split_column(
        Layout(name="NODE_A", ratio=1),
        Layout(name="NODE_E", ratio=1)
    )
    
    layout["body"]["col3"].split_column(
        Layout(name="NODE_B", ratio=1),
        Layout(name="NODE_HUB", ratio=1)
    )

    layout["body"]["col4"].split_column(
        Layout(name="NODE_C", ratio=1),
        Layout(name="HELP", ratio=1)
    )
    
    return layout

def update_rich_dashboard(layout: Layout, manager):
    # Header
    layout["header"].update(Panel("[bold green]📡 ADAPTIVE LORA MESH — UNIFIED SINGLE-SCREEN TERMINAL GRID[/bold green]", box=box.ROUNDED))

    # Metrics Table
    table = Table(box=box.SIMPLE_HEAD, expand=True)
    table.add_column("Node", style="bold cyan")
    table.add_column("Battery", style="green")
    table.add_column("Temp", style="yellow")
    table.add_column("Queue", style="magenta")
    table.add_column("Active Next Hop", style="bold green")

    states = manager.node_states
    for n in ["A", "B", "C", "D", "E", "HUB"]:
        s = states.get(n, {})
        table.add_row(
            f"Node {n}",
            f"{s.get('battery_pct', 90):.0f}%",
            f"{s.get('temperature_c', 25):.0f}°C",
            f"{s.get('queue_pct', 10):.0f}%",
            f"{s.get('primary_next', 'HUB')}"
        )

    layout["metrics"].update(Panel(table, title="[bold yellow]Live Node State Matrix[/bold yellow]", box=box.ROUNDED))

    # Logs panels
    mapping = {
        "HUB_CENTRAL": ("CENTRAL_HUB", "Central ML Hub"),
        "NODE_A": ("A", "Node A (Port 9001)"),
        "NODE_B": ("B", "Node B (Port 9002)"),
        "NODE_C": ("C", "Node C (Port 9003)"),
        "NODE_D": ("D", "Node D (Port 9004)"),
        "NODE_E": ("E", "Node E (Port 9005)"),
        "NODE_HUB": ("HUB", "Node HUB (Port 9000)")
    }

    for panel_key, (node_key, title_str) in mapping.items():
        logs = manager.get_logs(node_key)
        log_text = "\n".join(logs[-12:]) if logs else "[Online]"
        layout["body"][panel_key].update(Panel(log_text, title=f"[bold blue]{title_str}[/bold blue]", box=box.ROUNDED))

    layout["body"]["HELP"].update(Panel("[green]Press Ctrl+C to exit dashboard.[/green]", title="Help", box=box.ROUNDED))


def run_rich_terminal():
    manager = get_mesh_manager()
    layout = make_layout()

    with Live(layout, refresh_per_second=2, console=console) as live:
        while True:
            try:
                update_rich_dashboard(layout, manager)
                time.sleep(0.5)
            except KeyboardInterrupt:
                break


if __name__ == "__main__":
    run_rich_terminal()
