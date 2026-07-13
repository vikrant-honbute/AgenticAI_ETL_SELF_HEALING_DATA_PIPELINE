# Agentic ETL Self-Healing Pipeline

An end-to-end, production-style **Agentic ETL pipeline** that autonomously ingests, validates, heals, and performs AI-powered sentiment analysis on Yelp reviews — all orchestrated by **Apache Airflow** and powered by a locally running **Ollama LLM**.

---

## Overview

This pipeline demonstrates a real-world self-healing data engineering workflow. It automatically detects and fixes data quality issues in raw Yelp review data, then uses a local Large Language Model to classify the sentiment of each review. A health report is generated at the end of every run, giving full observability into the pipeline's quality.

**Sample run results (100 reviews):**
- ✅ **88%** success rate (clean, unmodified reviews)
- 🩹 **12%** healing rate (data issues detected and fixed automatically)
- 💀 **0%** degradation rate (no failures)
- 🟢 Pipeline health status: **HEALTHY**
- 🤖 Average model confidence: **91.5%**

---

## Architecture

```
Raw Yelp JSON
      │
      ▼
┌─────────────────┐
│   load_model    │  → Connects to Ollama, validates LLM is ready
└────────┬────────┘
         │
┌────────▼────────┐
│  load_reviews   │  → Reads a configurable batch from the input file
└────────┬────────┘
         │
┌────────▼──────────────────┐
│  diagnose_and_heal_batch  │  → Detects & fixes data quality issues
└────────┬──────────────────┘
         │
┌────────▼────────────────────┐
│  batch_analyze_sentiment    │  → Sends each review to the local LLM
└────────┬────────────────────┘
         │
┌────────▼────────────┐
│  aggregate_results  │  → Compiles stats, writes output JSON
└────────┬────────────┘
         │
┌────────▼──────────────────┐
│  generate_health_report   │  → Assesses pipeline health, writes report
└───────────────────────────┘
```

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Orchestration | Apache Airflow 2.9.2 |
| Containerisation | Docker + Docker Compose |
| LLM Inference | Ollama (llama3.2:3b) — runs locally on host |
| Database | PostgreSQL 13 (Airflow metadata) |
| Language | Python 3.11 |
| Dataset | Yelp Academic Dataset (reviews) |

---

## Self-Healing Capabilities

The `diagnose_and_heal_batch` task automatically detects and repairs the following data quality issues without stopping the pipeline:

| Issue Type | Detection | Action Taken |
|------------|-----------|-------------|
| `missing_text` | `text` field is `null` | Filled with placeholder |
| `empty_text` | Text is blank or whitespace only | Filled with placeholder |
| `wrong_type` | Text is not a string (e.g. integer) | Type-converted to string |
| `special_characters_only` | Text contains no alphanumeric characters | Replaced with `[Non-text content]` |
| `too_long` | Text exceeds 1000 characters | Truncated to 1000 characters |

Every healed record is flagged in the output with `was_healed: true`, `error_type`, and `action_taken` fields for full traceability.

---

## Project Structure

```
Agentic_ETL_SELF_HEALING_PIPELINE/
├── dags/
│   └── agentic_pipeline_dag.py   # Main pipeline DAG
├── input/
│   └── yelp_academic_dataset_review.json  # Input dataset (not committed to git)
├── output/
│   ├── sentiment_analysis_summary_*.json  # Per-run analysis results
│   └── health_report_*.json               # Per-run health reports
├── plugins/                               # Airflow custom plugins (empty)
├── Dockerfile                             # Custom Airflow image with dependencies
├── docker-compose.yml                     # Full Airflow stack definition
├── requirements.txt                       # Python dependencies
└── .gitignore
```

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (running)
- [Ollama for Windows](https://ollama.com/download/windows) (installed and running locally)
- The Yelp Academic Dataset (`yelp_academic_dataset_review.json`) — available free from [yelp.com/dataset](https://www.yelp.com/dataset)

---

## Setup & Running

### 1. Pull the Ollama model

Open a terminal and run:

```bash
ollama pull llama3.2:3b
```

### 2. Place the dataset

Copy `yelp_academic_dataset_review.json` into the `input/` directory:

```
input/yelp_academic_dataset_review.json
```

### 3. Start the Airflow stack

```bash
# First time only — initialise the database
docker compose up airflow-init

# Start all services in the background
docker compose up -d
```

### 4. Open the Airflow UI

Navigate to [http://localhost:8080](http://localhost:8080) in your browser.

- **Username:** `admin`
- **Password:** `admin`

### 5. Trigger the pipeline

1. Find the `self_healing_pipeline` DAG in the list
2. Click the **▶ Trigger** button (use "Trigger DAG w/ config" to customise parameters)
3. Watch all 6 tasks turn green in the Graph view
4. Check the `output/` folder for your results

---

## Configuration Parameters

These can be set when triggering the DAG via the Airflow UI:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `batch_size` | `100` | Number of reviews to process per run |
| `offset` | `0` | Line offset to start reading from in the input file |
| `input_file` | `/opt/airflow/input/yelp_academic_dataset_review.json` | Path to the input file inside the container |
| `ollama_model` | `llama3.2:3b` | Ollama model to use for sentiment analysis |

> **Tip:** For demos, set `batch_size` to `10` so the pipeline finishes in under 90 seconds.

---

## Output Files

Every pipeline run produces two files in the `output/` directory:

### `sentiment_analysis_summary_TIMESTAMP_OffsetN.json`

Contains the full results for every processed review, including:
- `predicted_sentiment` — `POSITIVE`, `NEGATIVE`, or `NEUTRAL`
- `confidence` — model confidence score (0.0–1.0)
- `status` — `success`, `healed`, or `degraded`
- `healing_applied`, `healing_action`, `error_type` — full healing traceability
- Aggregate statistics: sentiment distribution, star-sentiment correlation, healing breakdown

### `health_report_TIMESTAMP_OffsetN.json`

A concise pipeline health assessment:
- `health_status` — `HEALTHY`, `WARNING`, `DEGRADED`, or `CRITICAL`
- Success, healing, and degradation rates
- Average model confidence by status
- Sentiment distribution summary

---

## Environment Variables

Override defaults by editing the `.env` file or setting environment variables in `docker-compose.yml`:

| Variable | Default | Description |
|----------|---------|-------------|
| `OLLAMA_HOST` | `http://host.docker.internal:11434` | Ollama server URL |
| `OLLAMA_MODEL` | `llama3.2:3b` | Default model |
| `OLLAMA_TIMEOUT` | `120` | Request timeout in seconds |
| `OLLAMA_RETRIES` | `3` | Number of retry attempts per review |
| `PIPELINE_MAX_TEXT_LENGTH` | `1000` | Maximum characters before truncation |

---

## Stopping the Stack

```bash
docker compose down
```

To also remove all stored data (database, logs):

```bash
docker compose down -v
```

---

## Author

**Vikrant** — Built as part of an Agentic AI & Data Engineering learning project.
