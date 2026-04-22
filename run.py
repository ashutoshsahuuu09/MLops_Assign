import argparse
import pandas as pd
import numpy as np
import yaml
import logging
import time
import json
import sys
import os


def setup_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def load_config(config_path):
    if not os.path.exists(config_path):
        raise FileNotFoundError("Config file not found")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    required = ["seed", "window", "version"]
    for key in required:
        if key not in config:
            raise ValueError(f"Missing config key: {key}")

    return config


def load_data(input_path):
    print("Loading file:", input_path)

    if not os.path.exists(input_path):
        raise FileNotFoundError("Input CSV not found")

    df = pd.read_csv(input_path)

    if df.empty:
        raise ValueError("CSV file is empty")

    # ✅ Normalize column names (important fix)
    df.columns = df.columns.str.strip().str.lower()

    logging.info(f"Columns found: {df.columns.tolist()}")

    if "close" not in df.columns:
        raise ValueError(
            f"Missing 'close' column. Found: {df.columns.tolist()}"
        )

    return df


def write_metrics(output_path, metrics):
    with open(output_path, "w") as f:
        json.dump(metrics, f, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--log-file", required=True)

    args = parser.parse_args()

    setup_logging(args.log_file)
    start_time = time.time()

    try:
        logging.info("Job started")

        config = load_config(args.config)
        seed = config["seed"]
        window = config["window"]
        version = config["version"]

        np.random.seed(seed)
        logging.info(f"Config loaded: {config}")

        df = load_data(args.input)
        logging.info(f"Rows loaded: {len(df)}")

        logging.info("Computing rolling mean")
        df["rolling_mean"] = df["close"].rolling(window=window).mean()

        logging.info("Generating signals")
        df["signal"] = np.where(df["close"] > df["rolling_mean"], 1, 0)

        valid_df = df.dropna()

        rows_processed = len(valid_df)
        signal_rate = float(valid_df["signal"].mean())

        latency_ms = int((time.time() - start_time) * 1000)

        metrics = {
            "version": version,
            "rows_processed": rows_processed,
            "metric": "signal_rate",
            "value": round(signal_rate, 4),
            "latency_ms": latency_ms,
            "seed": seed,
            "status": "success",
        }

        logging.info(f"Metrics: {metrics}")

        write_metrics(args.output, metrics)

        print(json.dumps(metrics, indent=2))
        logging.info("Job completed successfully")

        sys.exit(0)

    except Exception as e:
        logging.error(f"Error: {str(e)}")

        latency_ms = int((time.time() - start_time) * 1000)

        error_metrics = {
            "version": "v1",
            "rows_processed": 0,
            "metric": "signal_rate",
            "value": None,
            "latency_ms": latency_ms,
            "seed": None,
            "status": "error",
            "error_message": str(e),
        }

        try:
            write_metrics(args.output, error_metrics)
        except Exception as write_err:
            logging.error(f"Failed to write metrics file: {write_err}")

        print(json.dumps(error_metrics, indent=2))

        sys.exit(1)


if __name__ == "__main__":
    main()