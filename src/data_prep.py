"""
Data Preparation & Synthetic LoRa Dataset Generator.
Calibrated using physical LoRa propagation ranges (ChirpBox / MDPI LoRaWAN dataset bounds).
"""

import os
import numpy as np
import pandas as pd

# Define default dataset path
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
DATA_FILE = os.path.join(DATA_DIR, "synthetic_demo.csv")

FEATURE_COLUMNS = [
    "rssi",
    "snr",
    "pdr",
    "latency_ms",
    "retries",
    "etx",
    "queue_pct",
    "battery_pct",
    "temperature_c",
    "time_on_air_s",
    "spreading_factor",
    "bandwidth_khz"
]

TARGET_COLUMN = "link_success"


def compute_lora_time_on_air(sf: int, bw_khz: float, payload_bytes: int = 32, cr: int = 1) -> float:
    """
    Computes LoRa Time-on-Air (ToA) in seconds based on Semtech LoRa formula.
    """
    bw_hz = bw_khz * 1000.0
    t_sym = (2 ** sf) / bw_hz
    t_preamble = (8 + 4.25) * t_sym
    
    # Payload symbol calculation
    n_payload = 8 + max(
        np.ceil((8 * payload_bytes - 4 * sf + 28 + 16) / (4 * (sf - 2 * (1 if sf >= 11 else 0)))) * (cr + 4),
        0
    )
    t_payload = n_payload * t_sym
    return float(t_preamble + t_payload)


def generate_lora_mesh_dataset(n_samples: int = 25000, random_state: int = 42) -> pd.DataFrame:
    """
    Generates a realistic multi-hop LoRa link quality dataset with ground-truth binary labels.
    """
    np.random.seed(random_state)
    
    # 1. Radio Parameters
    spreading_factors = np.random.choice([7, 8, 9, 10, 11, 12], size=n_samples, p=[0.25, 0.25, 0.20, 0.15, 0.10, 0.05])
    bandwidths = np.random.choice([125, 250, 500], size=n_samples, p=[0.70, 0.20, 0.10])
    
    # Base RSSI & SNR distributions based on distance/fading
    # RSSI ranges from -125 dBm to -60 dBm
    rssi = np.random.uniform(-125.0, -60.0, size=n_samples)
    
    # SNR correlates with RSSI plus noise
    snr_noise = np.random.normal(0, 3.5, size=n_samples)
    snr = 0.25 * (rssi + 100.0) + snr_noise
    snr = np.clip(snr, -20.0, 15.0)
    
    # Calculate Time-on-Air for each sample
    time_on_air_s = np.array([
        compute_lora_time_on_air(sf=int(sf), bw_khz=float(bw))
        for sf, bw in zip(spreading_factors, bandwidths)
    ])
    
    # 2. Link Quality & Delivery Metrics
    # PDR (Packet Delivery Ratio) correlates with SNR and SF
    # Lower SNR or lower SF under weak signal leads to lower PDR
    snr_factor = 1.0 / (1.0 + np.exp(-(snr + 5.0) / 2.5))  # Sigmoid centered at -5 dB SNR
    pdr_raw = snr_factor + np.random.uniform(-0.1, 0.1, size=n_samples)
    pdr = np.clip(pdr_raw, 0.0, 1.0)
    
    # ETX = Expected Transmission Count (1 / PDR)
    etx = 1.0 / np.maximum(pdr, 0.01)
    etx = np.clip(etx, 1.0, 10.0)
    
    # Retries: High ETX or low PDR produces more retransmission attempts
    retries_mean = np.maximum(0, (etx - 1.0) * 1.5)
    retries = np.random.poisson(lam=retries_mean)
    retries = np.clip(retries, 0, 7)
    
    # Latency: Base round-trip/one-hop delay + retransmissions * ToA + queue delay
    queue_pct = np.random.uniform(5.0, 95.0, size=n_samples)
    base_latency_ms = 40.0 + time_on_air_s * 1000.0
    queue_delay_ms = (queue_pct / 100.0) ** 2 * 350.0
    retry_delay_ms = retries * (time_on_air_s * 1000.0 + 150.0)
    latency_ms = base_latency_ms + queue_delay_ms + retry_delay_ms + np.random.normal(0, 15, size=n_samples)
    latency_ms = np.clip(latency_ms, 30.0, 2000.0)
    
    # Node Operational State
    battery_pct = np.random.uniform(10.0, 100.0, size=n_samples)
    temperature_c = np.random.uniform(15.0, 65.0, size=n_samples)
    
    # 3. Ground-Truth Target Label Determination (link_success = 1 or 0)
    # Success probability physics model:
    # Physical constraints:
    # - If PDR < 0.15 or SNR < -12.0 or Retries >= 6 -> Link is DEAD (0% success probability)
    is_dead_link = (pdr < 0.15) | (snr < -12.0) | (retries >= 6) | (battery_pct < 10.0) | (temperature_c > 75.0)

    prob_success = np.where(
        is_dead_link,
        0.001,  # Extreme low probability for dead/broken links
        (
            0.35 * (1.0 / (1.0 + np.exp(-(snr + 5.0) / 2.0))) +
            0.35 * pdr +
            0.15 * (1.0 - np.clip(retries / 5.0, 0.0, 1.0)) +
            0.10 * (1.0 - np.clip((latency_ms - 100.0) / 1000.0, 0.0, 1.0)) +
            0.05 * np.where(battery_pct > 15.0, 1.0, 0.2)
        )
    )
    prob_success = np.clip(prob_success, 0.0, 1.0)
    
    # Sample binary outcome (1 = success, 0 = loss/timeout)
    link_success = np.random.binomial(1, prob_success)
    
    df = pd.DataFrame({
        "rssi": np.round(rssi, 2),
        "snr": np.round(snr, 2),
        "pdr": np.round(pdr, 4),
        "latency_ms": np.round(latency_ms, 2),
        "retries": retries,
        "etx": np.round(etx, 3),
        "queue_pct": np.round(queue_pct, 2),
        "battery_pct": np.round(battery_pct, 2),
        "temperature_c": np.round(temperature_c, 2),
        "time_on_air_s": np.round(time_on_air_s, 4),
        "spreading_factor": spreading_factors,
        "bandwidth_khz": bandwidths,
        "link_success": link_success,
        "data_type": "SYNTHETIC-DEMO"
    })
    
    return df


def load_or_generate_dataset(n_samples: int = 25000, force_recreate: bool = False) -> pd.DataFrame:
    """
    Loads dataset from CSV or generates a new synthetic dataset if missing.
    """
    os.makedirs(DATA_DIR, exist_ok=True)
    if not force_recreate and os.path.exists(DATA_FILE):
        print(f"Loading existing dataset from {DATA_FILE}")
        return pd.read_csv(DATA_FILE)
    
    print(f"Generating new synthetic LoRa dataset ({n_samples} samples)...")
    df = generate_lora_mesh_dataset(n_samples=n_samples)
    df.to_csv(DATA_FILE, index=False)
    print(f"Dataset saved to {DATA_FILE}")
    return df


if __name__ == "__main__":
    df = load_or_generate_dataset(force_recreate=True)
    print("Dataset shape:", df.shape)
    print("\nClass distribution:\n", df[TARGET_COLUMN].value_counts(normalize=True))
    print("\nFeature Summary:\n", df[FEATURE_COLUMNS].describe().T[["mean", "std", "min", "max"]])
