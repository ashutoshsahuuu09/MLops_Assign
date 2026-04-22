# MLOps Batch Job

## Overview

This project implements a minimal MLOps-style batch pipeline:

* Config-driven execution
* Deterministic runs via seed
* Logging + metrics
* Dockerized execution

---

## Local Run

```bash
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
```

---

## Docker

```bash
docker build -t mlops-task .
docker run --rm mlops-task
```

---

## Example Output (Success)

```json
{
  "version": "v1",
  "rows_processed": 9996,
  "metric": "signal_rate",
  "value": 0.4987,
  "latency_ms": 120,
  "seed": 42,
  "status": "success"
}
```

---

## Example Output (Error)

Example when input file is missing or invalid:

```json
{
  "version": "v1",
  "status": "error",
  "error_message": "Input CSV not found"
}
```

---

## Notes

* The pipeline always writes `metrics.json` in both success and error cases.
* Success and error outputs follow strict schemas as required.
* Error handling includes:

  * Missing input file
  * Invalid CSV format
  * Empty dataset
  * Missing required column (`close`)
  * Invalid configuration
