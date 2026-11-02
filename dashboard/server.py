#!/usr/bin/env python3
"""
SparkShield Analytics Dashboard Server
Lightweight HTTP server serving real pipeline metrics and interactive web UI.
"""

import os
import sys
import json
import glob
import csv
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler

from urllib.parse import urlparse, parse_qs
import datetime

PORT = 8088
DASHBOARD_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DASHBOARD_DIR)
RESULTS_DIR = os.path.join(PROJECT_ROOT, "results")

def load_json(path):
    if not path or not os.path.exists(path):
        return {"status": "unavailable", "message": f"Artifact not found: {os.path.basename(path) if path else 'unknown'}"}
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                return {"status": "unavailable", "message": f"Artifact is empty: {os.path.basename(path)}"}
            return json.loads(content)
    except Exception as e:
        return {"status": "error", "message": f"Malformed JSON in {os.path.basename(path)}: {str(e)}"}

def resolve_ml_run_dir(requested_run=None):
    if requested_run:
        target = os.path.join(RESULTS_DIR, "ml", requested_run)
        if os.path.isdir(target):
            return target, requested_run

    # Check environment variable override
    env_run = os.environ.get("SPARKSHIELD_ML_RUN")
    if env_run:
        target = os.path.join(RESULTS_DIR, "ml", env_run)
        if os.path.isdir(target):
            return target, env_run

    # Check latest run pointer file
    ptr_file = os.path.join(RESULTS_DIR, "ml", "latest_run.txt")
    if os.path.exists(ptr_file):
        try:
            with open(ptr_file, "r", encoding="utf-8") as f:
                ptr_path = f.read().strip()
                if ptr_path:
                    abs_path = os.path.join(PROJECT_ROOT, ptr_path) if not os.path.isabs(ptr_path) else ptr_path
                    if os.path.isdir(abs_path):
                        return abs_path, os.path.basename(abs_path)
        except Exception:
            pass

    # Find latest run directory in results/ml/
    ml_base = os.path.join(RESULTS_DIR, "ml")
    if os.path.isdir(ml_base):
        runs = [d for d in os.listdir(ml_base) if d.startswith("run-") and os.path.isdir(os.path.join(ml_base, d))]
        if runs:
            latest_run = max(runs)
            return os.path.join(ml_base, latest_run), latest_run

    return None, None

def load_latest_streaming_metrics():
    search_pattern = os.path.join(RESULTS_DIR, "streaming-ml", "demo-*", "metrics", "streaming-metrics.json")
    files = glob.glob(search_pattern)
    if not files:
        return {"status": "unavailable", "message": "No streaming telemetry runs found. Execute: ./scripts/streaming-pipeline.sh run-ml-demo"}
    latest_file = max(files, key=os.path.getmtime)
    data = load_json(latest_file)
    if isinstance(data, dict):
        data["source_file"] = os.path.relpath(latest_file, PROJECT_ROOT)
    return data

def load_sample_predictions():
    search_pattern = os.path.join(
        RESULTS_DIR, "streaming-ml", "demo-*",
        "predictions", "csv", "**", "*.csv"
    )
    files = glob.glob(search_pattern, recursive=True)
    if not files:
        return []

    latest_file = max(files, key=os.path.getmtime)
    records = []
    try:
        with open(latest_file, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i >= 10:
                    break
                records.append(row)
    except (OSError, csv.Error, UnicodeDecodeError):
        return []
    return records

def load_dashboard_data(requested_run=None):
    dq_path = os.path.join(RESULTS_DIR, "metrics", "data-quality-report.json")
    sql_path = os.path.join(RESULTS_DIR, "metrics", "sql-analytics-summary.json")
    if not os.path.exists(sql_path):
        alt_sql = os.path.join(RESULTS_DIR, "sql-analytics", "sql-analytics-summary.json")
        if os.path.exists(alt_sql):
            sql_path = alt_sql

    graph_path = os.path.join(RESULTS_DIR, "graph-analytics", "graph-analytics-summary.json")
    if not os.path.exists(graph_path):
        alt_graph = os.path.join(RESULTS_DIR, "metrics", "graph-analytics-summary.json")
        if os.path.exists(alt_graph):
            graph_path = alt_graph

    ml_dir, ml_run_id = resolve_ml_run_dir(requested_run)
    if ml_dir:
        ml_metrics = load_json(os.path.join(ml_dir, "metrics.json"))
        cm_metrics = load_json(os.path.join(ml_dir, "confusion-matrix.json"))
    else:
        ml_metrics = {"status": "unavailable", "message": "No ML evaluation runs found in results/ml"}
        cm_metrics = {"status": "unavailable", "message": "No confusion matrix found in results/ml"}

    data_quality = load_json(dq_path)
    if isinstance(data_quality, dict) and data_quality.get("input_file"):
        data_quality["input_file"] = os.path.basename(data_quality["input_file"])

    return {
        "status": "online",
        "project": "SparkShield",
        "server_timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
        "data_quality": data_quality,
        "sql_analytics": load_json(sql_path),
        "ml_run_id": ml_run_id or "unavailable",
        "ml_evaluation": ml_metrics,
        "confusion_matrix": cm_metrics,
        "streaming_telemetry": load_latest_streaming_metrics(),
        "recent_predictions": load_sample_predictions(),
        "graph_analytics": load_json(graph_path)
    }

class DashboardRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DASHBOARD_DIR, **kwargs)

    def do_HEAD(self):
        parsed = urlparse(self.path)
        if parsed.path in ("/api/data", "/api/data/"):
            qs = parse_qs(parsed.query)
            requested_run = qs.get("run", [None])[0]
            try:
                data = load_dashboard_data(requested_run)
                payload = json.dumps(data, indent=2).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
            except Exception as e:
                err_payload = json.dumps({"error": "Internal server error", "detail": str(e)}).encode("utf-8")
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(err_payload)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
        elif parsed.path in ("/", ""):
            self.path = "/index.html"
            super().do_HEAD()
        else:
            super().do_HEAD()

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path in ("/api/data", "/api/data/"):
            qs = parse_qs(parsed.query)
            requested_run = qs.get("run", [None])[0]
            try:
                data = load_dashboard_data(requested_run)
                payload = json.dumps(data, indent=2).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(payload)
            except Exception as e:
                err_payload = json.dumps({"error": "Internal server error", "detail": str(e)}).encode("utf-8")
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(err_payload)))
                self.end_headers()
                self.wfile.write(err_payload)
        elif parsed.path in ("/", ""):
            self.path = "/index.html"
            super().do_GET()
        else:
            super().do_GET()

    def log_message(self, format, *args):
        # Concise logging to stdout
        sys.stdout.write(f"[Dashboard] {self.address_string()} - {format % args}\n")
        sys.stdout.flush()

def run_server(port=PORT):
    server_address = ("127.0.0.1", port)
    httpd = ThreadingHTTPServer(server_address, DashboardRequestHandler)
    print(f"============================================================")
    print(f"  SparkShield Analytics Dashboard Server")
    print(f"  URL    : http://127.0.0.1:{port}")
    print(f"  Root   : {DASHBOARD_DIR}")
    print(f"============================================================")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping dashboard server...")
        httpd.server_close()

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else PORT
    run_server(port)
