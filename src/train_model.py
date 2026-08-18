"""
Machine Learning Model Training & Evaluation Engine.
Trains Random Forest classifier to predict link success probability P(success) on tabular LoRa features.
Saves model artifact to models/link_success_rf.joblib.
"""

import os
import joblib
import numpy as np
import pandas as pd
from typing import Dict, Any, Tuple

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    confusion_matrix
)

from src.data_prep import load_or_generate_dataset, FEATURE_COLUMNS, TARGET_COLUMN

MODEL_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "models")
MODEL_FILE = os.path.join(MODEL_DIR, "link_success_rf.joblib")


def train_and_evaluate_models(df: pd.DataFrame) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Trains Random Forest, Gradient Boosting, and Logistic Regression models.
    Returns best model dictionary and evaluation results comparison.
    """
    X = df[FEATURE_COLUMNS].copy()
    y = df[TARGET_COLUMN].values
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.20, random_state=42, stratify=y
    )
    
    # Preprocessing scaler
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # Define models
    models = {
        "RandomForest": RandomForestClassifier(
            n_estimators=120, max_depth=12, random_state=42, n_jobs=-1
        ),
        "GradientBoosting": GradientBoostingClassifier(
            n_estimators=100, max_depth=6, random_state=42
        ),
        "LogisticRegression": LogisticRegression(
            max_iter=1000, random_state=42
        )
    }
    
    results = {}
    trained_estimators = {}
    
    for name, clf in models.items():
        if name == "LogisticRegression":
            clf.fit(X_train_scaled, y_train)
            y_pred = clf.predict(X_test_scaled)
            y_prob = clf.predict_proba(X_test_scaled)[:, 1]
        else:
            clf.fit(X_train, y_train)
            y_pred = clf.predict(X_test)
            y_prob = clf.predict_proba(X_test)[:, 1]
            
        acc = accuracy_score(y_test, y_pred)
        prec = precision_score(y_test, y_pred, zero_division=0)
        rec = recall_score(y_test, y_pred, zero_division=0)
        f1 = f1_score(y_test, y_pred, zero_division=0)
        auc = roc_auc_score(y_test, y_prob)
        cm = confusion_matrix(y_test, y_pred).tolist()
        
        feature_importance = {}
        if hasattr(clf, "feature_importances_"):
            feature_importance = dict(zip(FEATURE_COLUMNS, clf.feature_importances_))
            
        results[name] = {
            "accuracy": float(acc),
            "precision": float(prec),
            "recall": float(rec),
            "f1": float(f1),
            "roc_auc": float(auc),
            "confusion_matrix": cm,
            "feature_importance": feature_importance
        }
        trained_estimators[name] = clf
        
    rf_model = trained_estimators["RandomForest"]
    
    os.makedirs(MODEL_DIR, exist_ok=True)
    model_artifact = {
        "model": rf_model,
        "scaler": scaler,
        "feature_columns": FEATURE_COLUMNS,
        "metrics": results["RandomForest"],
        "all_results": results
    }
    
    joblib.dump(model_artifact, MODEL_FILE)
    print(f"Model saved successfully to {MODEL_FILE}")
    
    return model_artifact, results


def load_trained_model() -> Dict[str, Any]:
    """
    Loads saved model artifact or trains a new one if missing.
    """
    if os.path.exists(MODEL_FILE):
        return joblib.load(MODEL_FILE)
    
    print("No saved model found. Training Random Forest model...")
    df = load_or_generate_dataset()
    artifact, _ = train_and_evaluate_models(df)
    return artifact


def predict_link_success_probability(model_artifact: Dict[str, Any], link_features: Dict[str, float]) -> float:
    """
    Predicts p_success probability for a single link feature dictionary.
    """
    model = model_artifact["model"]
    feature_cols = model_artifact["feature_columns"]
    
    # Construct DataFrame row with default fallbacks for any missing feature
    default_vals = {
        "rssi": -85.0,
        "snr": 5.0,
        "pdr": 0.90,
        "latency_ms": 120.0,
        "retries": 0,
        "etx": 1.11,
        "queue_pct": 15.0,
        "battery_pct": 85.0,
        "temperature_c": 35.0,
        "time_on_air_s": 0.15,
        "spreading_factor": 8,
        "bandwidth_khz": 125.0
    }
    
    row_data = {col: link_features.get(col, default_vals.get(col, 0.0)) for col in feature_cols}
    df_single = pd.DataFrame([row_data])
    
    p_success = float(model.predict_proba(df_single)[0, 1])
    return p_success


if __name__ == "__main__":
    df = load_or_generate_dataset()
    artifact, results = train_and_evaluate_models(df)
    
    print("\n--- Model Benchmark Results ---")
    for name, m in results.items():
        print(f"[{name}] Acc: {m['accuracy']:.4f} | Prec: {m['precision']:.4f} | Rec: {m['recall']:.4f} | F1: {m['f1']:.4f} | ROC-AUC: {m['roc_auc']:.4f}")
        
    print("\n--- Top Random Forest Feature Importances ---")
    rf_fi = sorted(results["RandomForest"]["feature_importance"].items(), key=lambda x: x[1], reverse=True)
    for k, v in rf_fi:
        print(f"  {k:18s}: {v:.4f}")
