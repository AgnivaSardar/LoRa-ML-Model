@echo off
title LoRa Mesh Multi-Node CLI Launcher
echo =========================================================================
echo 🚀 LAUNCHING DISTRIBUTED LORA MESH MULTI-TERMINAL CLI NODES
echo =========================================================================
echo Opening separate Command Prompt windows for Central Hub, Nodes, and Chaos Injector...

start "CENTRAL ML HUB (Port 9000)" cmd /k "python -m src.cli_hub"
timeout /t 2 /nobreak > NUL

start "NODE A - Source (Port 9001)" cmd /k "python -m src.cli_node --node A"
start "NODE B (Port 9002)" cmd /k "python -m src.cli_node --node B"
start "NODE C (Port 9003)" cmd /k "python -m src.cli_node --node C"
start "NODE D (Port 9004)" cmd /k "python -m src.cli_node --node D"
start "NODE E (Port 9005)" cmd /k "python -m src.cli_node --node E"

timeout /t 3 /nobreak > NUL

start "TRAFFIC & CHAOS GENERATOR" cmd /k "python -m src.cli_chaos --interval 2.5"

echo.
echo All 7 terminal windows launched! Arrange them side-by-side on your desktop to view live node logs and ML dynamic rerouting.
pause
