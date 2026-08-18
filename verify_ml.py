"""
ML Model Live Verification Script.
Loads the trained Random Forest artifact (models/link_success_rf.joblib)
and performs live probability inference on 12-feature test vectors to prove
that Scikit-Learn predictions are actively executing without hardcoded values.
"""

import sys
import os
import joblib
import pandas as pd
import numpy as np

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

MODEL_PATH = os.path.join(os.path.dirname(__file__), "models", "link_success_rf.joblib")

def run_ml_verification():
    print("=========================================================================")
    print("🔍 LIVE ML MODEL INFERENCE VERIFICATION TOOL")
    print("=========================================================================")
    
    if not os.path.exists(MODEL_PATH):
        print(f"❌ Model artifact not found at {MODEL_PATH}")
        return

    artifact = joblib.load(MODEL_PATH)
    model = artifact["model"]
    feature_cols = artifact["feature_columns"]
    
    print(f"✅ Loaded Scikit-Learn Model: {type(model).__name__}")
    print(f"✅ Input Feature Vector (12 features): {feature_cols}\n")

    # Feature Importance Ranking
    print("--- Top 12 Feature Importance Ranking (Gini Impurity) ---")
    if hasattr(model, "feature_importances_"):
        fi = sorted(zip(feature_cols, model.feature_importances_), key=lambda x: x[1], reverse=True)
        for feat, imp in fi:
            print(f"  {feat:18s} : {imp:.4f}")
            
    print("\n=========================================================================")
    print("🧪 RUNNING LIVE INFERENCE TEST CASES")
    print("=========================================================================")

    test_cases = [
        ("1. Optimal Healthy Baseline Link", {
            "rssi": -72.0, "snr": 12.5, "pdr": 0.99, "latency_ms": 80.0,
            "retries": 0, "etx": 1.01, "queue_pct": 8.0, "battery_pct": 98.0,
            "temperature_c": 28.0, "time_on_air_s": 0.12, "spreading_factor": 8, "bandwidth_khz": 125.0
        }),
        ("2. Mild Congestion (High Queue & Temp)", {
            "rssi": -82.0, "snr": 7.0, "pdr": 0.88, "latency_ms": 350.0,
            "retries": 2, "etx": 1.14, "queue_pct": 75.0, "battery_pct": 80.0,
            "temperature_c": 52.0, "time_on_air_s": 0.22, "spreading_factor": 9, "bandwidth_khz": 125.0
        }),
        ("3. Severe Thermal Overheat (65°C) & Queue Backlog (95%)", {
            "rssi": -92.0, "snr": 2.0, "pdr": 0.35, "latency_ms": 950.0,
            "retries": 5, "etx": 2.85, "queue_pct": 95.0, "battery_pct": 70.0,
            "temperature_c": 65.0, "time_on_air_s": 0.65, "spreading_factor": 11, "bandwidth_khz": 125.0
        }),
        ("4. Critical Link Fading (Low RSSI/SNR & SF12)", {
            "rssi": -120.0, "snr": -15.0, "pdr": 0.15, "latency_ms": 1100.0,
            "retries": 6, "etx": 6.66, "queue_pct": 85.0, "battery_pct": 40.0,
            "temperature_c": 35.0, "time_on_air_s": 1.40, "spreading_factor": 12, "bandwidth_khz": 125.0
        }),
        ("5. Total Link Failure", {
            "rssi": -130.0, "snr": -25.0, "pdr": 0.01, "latency_ms": 2500.0,
            "retries": 7, "etx": 10.0, "queue_pct": 99.0, "battery_pct": 5.0,
            "temperature_c": 80.0, "time_on_air_s": 1.80, "spreading_factor": 12, "bandwidth_khz": 125.0
        })
    ]

    for name, feat_dict in test_cases:
        df_single = pd.DataFrame([feat_dict])[feature_cols]
        prob_success = model.predict_proba(df_single)[0, 1]
        pred_class = model.predict(df_single)[0]
        
        print(f"\n{name}:")
        print(f"  Input SNR={feat_dict['snr']}dB, PDR={feat_dict['pdr']}, Retries={feat_dict['retries']}, Temp={feat_dict['temperature_c']}°C, SF={feat_dict['spreading_factor']}")
        print(f"  --> ML Output P(success): {prob_success * 100.0:.2f}% | Predicted Class: {pred_class}")


if __name__ == "__main__":
    run_ml_verification()
